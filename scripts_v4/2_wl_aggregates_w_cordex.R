# DELETE AND UNCOMMENT BELOW
# MODIFY calc_stats_wl IN processing_functions.R

# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")

library(tidyverse)
library(stars)
library(furrr)
library(PCICt)
source("functions/general_tools.R")

options(
  future.fork.enable = T,
  future.rng.onMisuse = "ignore",
  future.globals.maxSize = 1000 * 1024^2
)

plan(multicore)

source("scripts_v4/0_output_vars.R")
source("scripts_v4/processing_functions.R")

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")
wls <- sprintf("%.1f", seq(0.5, 3.0, 0.5))

dir_gs_cordex <- "gs://clim_data_reg_useast1/cordex"

dir_rawdata <- "/mnt/pers_disk/rawdata"
dir_res <- "/mnt/pers_disk/results"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_res)


wl_yrs <-
  "cmip5_model_temp_thresholds.csv" |>
  read_delim() %>%
  suppressMessages() %>%
  select(1:6) %>%
  pivot_longer(-Model, names_to = "wl", values_to = "year") |>
  mutate(wl = str_sub(wl, start = 3) |> as.numeric())


# loop output_vars
output_vars |>
  walk(\(v) {
    # v = output_vars[24]

    message(str_glue("PROCESSING VAR {which(v == output_vars)} / {length(output_vars)}"))

    walk(doms, \(dom) {
      # dom = doms[1]

      message(str_glue("  DOMAIN {which(dom == doms)} / {length(doms)} ({dom})"))

      model_dirs <-
        rt_gs_list_files(str_glue("{dir_gs_cordex}/annual_aggregates/{v}/{dom}/"))

      gcms <-
        model_dirs |>
        str_split("/") |>
        map(~ .x[8]) |>
        map(~ str_split(.x, "_", simplify = T)[, 2]) |>
        unlist()

      s_gcm_wl <-
        map2(model_dirs, gcms, \(d, gcm) {
          # d = model_dirs[1]
          # gcm = gcms[1]

          message(str_glue("      model {which(d == model_dirs)} / {length(model_dirs)}"))

          wl_yrs_gcm <-
            wl_yrs |>
            filter(str_detect(gcm, Model)) |>
            arrange(wl)

          yrs <-
            seq(1971, last(wl_yrs_gcm$year) + 10)

          yr_subset <-
            str_glue("yr_{yrs}-") |>
            str_flatten("|")

          ff <-
            rt_gs_list_files(d) |>
            str_subset("-01.nc") |> # no versions
            str_subset(yr_subset) |>
            rt_gs_download_files(dir_rawdata, quiet = T)

          yrs_f <-
            ff |>
            str_extract("(?<=yr_)[:digit:]{4}") |>
            as.numeric()

          ss <-
            ff |>
            future_map(read_ncdf, proxy = F) |>
            suppressMessages()

          ss <-
            do.call(c, c(ss, along = "time")) |>
            st_set_dimensions("time", values = yrs_f)

          ff |>
            fs::file_delete()

          wl_0p5 <-
            ss |>
            filter(time >= 1971, time <= 2000)

          s_wls <-
            wl_yrs_gcm$year |>
            map(\(yr) {
              ss |>
                filter(time >= yr - 10, time <= yr + 10)
            })

          s_wls <-
            c(list(wl_0p5), s_wls) |>
            set_names(wls)

          return(s_wls)
        })

      message("      calculating stats")

      s_wl <-
        s_gcm_wl |>
        transpose() |>
        map(~ do.call(c, c(.x, along = "time")))

      un <-
        s_wl[[1]] |>
        pull() |>
        units::deparse_unit()

      s_wl_stats <-
        s_wl |>
        map(calc_stats_wl, un)

      message("      saving results")

      s_wl_stats |>
        iwalk(\(s, wl) {
          f_res <- str_glue(
            # "{dir_res}/CORDEX-CORE-ens_{dom}_{str_replace_all(v, '_', '-')}_wl-{wl}_stats.nc"
            "{dir_res}/CORDEX-CORE-ens_{dom}_{str_replace_all(v, '_', '-')}_wl-{wl}_98-99-199.nc"
          )

          rt_write_nc(
            s,
            f_res,
            gatt_name = "source_code",
            gatt_val = "https://github.com/Probable-Futures/map-data-processing"
          )

          # UNCOMMENT THIS!!!
          # str_glue(
          #   "gcloud storage mv {f_res} {dir_gs_cordex}/warming_level_aggregates/{v}/{dom}/CORDEX_CORE_ensemble/"
          # ) |>
          #   system(ignore.stdout = T, ignore.stderr = T)
        })
    })
  })

fs::dir_delete(dir_rawdata)
fs::dir_delete(dir_res)
