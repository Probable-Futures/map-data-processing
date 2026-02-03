# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)
# library(units)

plan(multicore, workers = parallelly::availableCores())

source("scripts_v3/functions.R") # other functions
source("scripts_v3/setup.R") # load main directory routes
source("side_projs/all_days/tile.R")
source("scripts_v4/functions/processing_functions.R")


# load thresholds table
thresholds <-
  str_glue("cmip5_model_temp_thresholds.csv") %>%
  read_delim() %>%
  suppressMessages() %>%
  select(1:6) %>%
  pivot_longer(-Model, names_to = "wl") %>%

  mutate(wl = str_sub(wl, 3)) %>%
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl)) %>%

  # add institutes to model names (for joins to work)
  mutate(
    Model = case_when(
      str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
      str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
      str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
      str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
      str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
      TRUE ~ Model
    )
  )

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

dir_data <- "/mnt/pd-pf/all_days"
fs::dir_create(dir_data)

dir_raw_data <- str_glue("{dir_data}/raw")
fs::dir_create(dir_raw_data)

dir_cat <- str_glue("{dir_data}/cat")
fs::dir_create(dir_cat)


# load table of all variables
source("scripts_v3/tb_vars_all.R")

vari <- "maximum_temperature"

dom <- "NAM"


tb_files <-
  fn_data_table("maximum_temperature") %>%
  filter(str_detect(file, "MISSING", negate = T))

# extract models
tb_models <-
  unique(tb_files[, c("gcm", "rcm")]) %>%
  arrange(rcm, gcm)

# ignore RegCM in these domains
if (dom %in% c("SAM", "AUS", "CAS")) {
  tb_models <-
    tb_models %>%
    filter(str_detect(rcm, "RegCM", negate = T))
}

# download files in parallel
tb_files %>%
  future_pwalk(function(loc, file, ...) {
    loc_ <-
      loc %>%
      str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")

    str_glue("{loc_}/{file}") %>%
      {
        system(
          str_glue("gcloud storage cp {.} {dir_raw_data}"),
          ignore.stdout = T,
          ignore.stderr = T
        )
      }
  })

# annual files
tb_files %>%
  future_pwalk(function(file, t_i, t_f, gcm, rcm, ...) {
    # extract first and last year included in the file
    yr_i <- year(as_date(t_i))
    yr_f <- year(as_date(t_f))

    f <- str_glue("{dir_raw_data}/{file}")

    # if (str_detect(vari, "wetbulb")) {
    #   # already split annually

    #   f_new <- str_glue("{dir_raw_data}/yrfix_{yr_i}.nc")

    #   # fix time
    #   system(
    #     str_glue("cdo -a setdate,{yr_i}-01-01 {f} {f_new}"),
    #     ignore.stdout = T,
    #     ignore.stderr = T
    #   )

    #   file.remove(f)
    #   #
    # } else {
    # extract variable's (short) name
    v <- str_split(file, "_") %>% .[[1]] %>% .[1]

    # split annually
    system(
      str_glue("cdo splityear {f} {dir_raw_data}/{v}_yrsplit_{gcm}_{rcm}_"),
      ignore.stdout = T,
      ignore.stderr = T
    )

    # fix time (only of the files that came from the file above)
    dir_raw_data %>%
      list.files(full.names = T) %>%
      str_subset(v) %>%
      str_subset(str_glue("yrsplit_{gcm}_{rcm}")) %>%
      str_subset(str_flatten(yr_i:yr_f, "|")) %>%

      walk2(seq(yr_i, yr_f), function(f2, yr) {
        f_new <- str_glue("{dir_raw_data}/{v}_yrfix_{gcm}_{rcm}_{yr}.nc")

        system(
          str_glue("cdo -a setdate,{yr}-01-01 {f2} {f_new}"),
          ignore.stdout = T,
          ignore.stderr = T
        )

        file.remove(f2)
      })

    file.remove(f)
    # }
  })

# check if some files could not be year-split/time-fixed
bad_remnants <-
  dir_raw_data %>%
  list.files(full.names = T) %>%
  str_subset("yrsplit")

if (length(bad_remnants) > 0) {
  print(str_glue("   ({length(bad_remnants)} bad file(s) - deleted)"))

  bad_remnants %>%
    walk(file.remove)
}

# common grid

ff <-
  dir_raw_data |>
  fs::dir_ls()

g <-
  pmap(tb_models, \(gcm, rcm) {
    ff |>
      str_subset(str_glue("{gcm}_{rcm}")) |>
      first() |>
      read_ncdf(
        make_time = F,
        ncsub = cbind(start = c(1, 1, 1), count = c(NA, NA, 1))
      ) |>
      adrop() |>
      suppressMessages()
  })

g <- do.call(c, c(g, along = "t"))

gg <-
  g |>
  units::drop_units() |>
  fix_coords() |>
  st_apply(
    c(1, 2),
    \(x) {
      if (any(is.na(x))) {
        NA
      } else {
        1
      }
    },
    .fname = "g"
  )

# tile
tb_tiles <-
  rt_tile_table(gg, 25, gg)

tb_tiles_t <-
  tb_tiles |>
  filter(land == T)
#

for (wl in wls) {
  #

  ff_wl <-
    map(seq(6), \(i) {
      #
      gcm = tb_models$gcm[i]
      rcm = tb_models$rcm[i]

      ff <-
        fs::dir_ls(dir_raw_data) |>
        str_subset(rcm) |>
        str_subset(gcm)

      # baseline:
      if (wl == "0.5") {
        yr_i = 1971
        yr_f = 2000

        # other warming levels:
      } else {
        thres_val <-
          thresholds %>%
          filter(str_detect(Model, str_glue("{gcm}$"))) %>%
          filter(wl == {{ wl }})

        yr_i = thres_val$value - 10
        yr_f = thres_val$value + 10
      }

      ff |>
        str_subset(str_flatten(str_glue("_{seq(yr_i, yr_f)}.nc"), '|'))
    }) |>
    unlist()

  for (i_tile in seq(nrow(tb_tiles_t))) {
    #
    message(str_glue(
      "PROCESSING TILE {i_tile} / {nrow(tb_tiles_t)} :: WL {wl}"
    ))

    mask <-
      ff_wl[1] |>
      read_ncdf(
        ncsub = cbind(
          start = c(
            tb_tiles_t$start_x[i_tile],
            tb_tiles_t$start_y[i_tile],
            1
          ),
          count = c(
            tb_tiles_t$count_x[i_tile],
            tb_tiles_t$count_y[i_tile],
            1
          )
        ),
        make_time = F
      ) |>
      suppressMessages() |>
      adrop()

    mask <-
      gg |>
      st_crop(mask, normalize = T) |>
      suppressWarnings()

    r <-
      future_map_dfr(seq(length(ff_wl)), \(i_file) {
        #
        r_ <-
          ff_wl[i_file] |>
          read_ncdf(
            ncsub = cbind(
              start = c(
                tb_tiles_t$start_x[i_tile],
                tb_tiles_t$start_y[i_tile],
                1
              ),
              count = c(
                tb_tiles_t$count_x[i_tile],
                tb_tiles_t$count_y[i_tile],
                NA
              )
            ),
            make_time = F
          ) |>
          suppressMessages() |>
          fix_coords() |>
          setNames("value") |>
          mutate(value = units::set_units(value, degC) |> round(2)) |>
          units::drop_units()

        r_[is.na(mask)] <- NA

        r_ |>
          as_tibble() |>
          select(-time) |>
          filter(!is.na(value))
      })

    # # winnipeg
    # r |>
    #   filter(
    #     lon == lon[which.min(abs(lon - -97))],
    #     lat == lat[which.min(abs(lat - 50))]
    #   ) |>
    #   ggplot(aes(x = value)) +
    #   geom_histogram(bins = 100)

    f_r <- str_glue(
      "{dir_cat}/var-tasmax_dom-NAM_wl-{wl}_tile-{str_pad(i_tile, 3, 'left', '0')}.csv"
    )

    write_csv(r, f_r)

    "gcloud storage mv {f_r} gs://clim_data_reg_useast1/results/probable_futures/all_days/" |>
      str_glue() |>
      system(ignore.stdout = T, ignore.stderr = T)

    # "gsutil cp {f_r} s3://global-pf-data-engineering/climate-data-full-model-raw/" |>
    #   str_glue() |>
    #   system(ignore.stdout = T, ignore.stderr = T)

    # fs::file_delete(f_r)
  }

  # transfer
}


# ********

dir_gs <- "gs://clim_data_reg_useast1/results/probable_futures/all_days/"
dir_s3 <- "s3://global-pf-data-engineering/climate-data-full-model-raw/"

ff <-
  rt_gs_list_files(dir_gs) |>
  str_subset(".csv")

for (f in ff) {
  "gcloud storage cp {f} {dir_cat}" |>
    str_glue() |>
    system(ignore.stdout = T, ignore.stderr = T)

  f_ <- str_glue("{dir_cat}/{basename(f)}")

  "aws s3 cp {f_} s3://global-pf-data-engineering/climate-data-full-model-raw/" |>
    str_glue() |>
    system()

  fs::file_delete(f_)

  "gcloud storage rm {f}" |>
    str_glue() |>
    system(ignore.stdout = T, ignore.stderr = T)
}
