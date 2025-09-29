# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")

# TOP LEVEL PARAMETERS ****************

ov_in <- 24 #c(1:14,20:24) # output vars (indices)
dir_temp <- "/mnt/pers_disk/derived_cordex"


# *************************************

library(tidyverse)
library(stars)
library(mirai)
library(PCICt)

box::use(functions / general_tools[...], functions / tile[...])

cores <- parallel::detectCores()
daemons(cores - 1)

source("scripts_v4/0_output_vars.R")
output_vars <- output_vars[ov_in]

source("scripts_v4/var_info_list.R")
source("scripts_v4/fn_derived.R")
source("scripts_v4/processing_functions.R")

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")
rcms <- c("REMO2015", "RegCM4")


fs::dir_create(dir_temp)
dir_rawdata <- str_glue("{dir_temp}/rawdata")
dir_tiles <- str_glue("{dir_temp}/tiles")
dir_res <- str_glue("{dir_temp}/results")


yrs <- seq(1970, 2099) # add 1 year at the beginning


# get all file names

input_vars_full <-
  var_info_list[output_vars] |>
  map(pluck, "name_cordex") |>
  unname() |>
  unlist() |>
  unique()

input_vars_sh <-
  var_info_list[output_vars] |>
  map(pluck, "input_vars") |>
  unname() |>
  unlist() |>
  unique()

input_vars <-
  input_vars_full |>
  set_names(input_vars_sh)

df_files <-
  input_vars |>
  map(\(var) {
    str_glue("data_tables/df-files_{var}.csv") |>
      read_csv(show_col_types = FALSE) |>
      filter(!(str_detect(rcm, "RegCM") & domain == "SAM-22")) |>
      filter(!(str_detect(rcm, "RegCM") & domain == "AUS-22")) |>
      mutate(domain = str_sub(domain, end = 3))
  })


for (dom in doms) {
  message(str_glue("PROCESSING domain {which(dom == doms)} / {length(doms)} ({dom})"))

  for (rcm in rcms) {
    df_files_rcm <-
      df_files |>
      map(\(df) {
        df |>
          filter(domain == dom, str_detect(rcm, {{ rcm }}))
      })

    if (nrow(df_files_rcm[[1]]) == 0) {
      next
    }

    gcms <-
      df_files_rcm |>
      pluck(1) |>
      pull(gcm) |>
      unique()

    for (gcm in gcms) {
      fs::dir_create(dir_rawdata)
      fs::dir_create(dir_tiles)
      fs::dir_create(dir_res)

      message(str_glue(
        "  RCM {which(rcm == rcms)} / {length(rcms)} ({rcm})  |  GCM {which(gcm == gcms)} / {length(gcms)} ({gcm})"
      ))

      df_files_rcm_gcm <-
        df_files_rcm |>
        map(\(df) {
          df |>
            filter(gcm == {{ gcm }})
        })

      ff_sub <-
        df_files_rcm_gcm |>
        map(\(df) {
          df |>
            filter(year(end_date) >= first(yrs), year(start_date) <= last(yrs))
        }) |>
        map(\(df) {
          df |>
            pull(file)
        })

      # reference grid
      f_proxy <-
        rt_gs_download_files(ff_sub[[1]][1], dir_rawdata, quiet = T)

      s_proxy <-
        f_proxy |>
        read_ncdf(ncsub = cbind(start = c(1, 1, 1), count = c(NA, NA, 1))) |>
        suppressMessages() |>
        adrop()

      s_proxy <-
        s_proxy |>
        fix_coords()

      # land
      land_r <- land() |> suppressWarnings()

      # TILE *****

      tile_size <- 20
      count_xy <- 1

      while (count_xy < 2) {
        df_tiles <- rt_tile_table(s_proxy, tile_size, land_r)
        count_xy <- min(min(df_tiles$count_x), min(df_tiles$count_y))
        tile_size <- tile_size + 2
      }

      df_tiles_land <-
        df_tiles |>
        filter(land == T)

      # download all files

      message(str_glue("    downloading files..."))

      ff_sub <-
        ff_sub |>
        map(\(f) rt_gs_download_files(f, dir_rawdata, quiet = T))

      # ff_sub <-
      #   ff_sub |>
      #   map(\(f) str_glue("{dir_rawdata}/{fs::path_file(f)}"))

      # ff_sub[[1]] <-
      #   ff_sub[[1]][1:5]

      output_vars |>
        walk(\(dir_v) fs::dir_create(str_glue("{dir_tiles}/{dir_v}")))

      # loop through tiles
      message(str_glue("    importing tile:"))

      pwalk(
        df_tiles_land,
        function(
          tile_id,
          start_x,
          start_y,
          end_x,
          end_y,
          count_x,
          count_y,
          ...
        ) {
          # tile_id = "262"
          # start_x = 320
          # count_x = 20
          # start_y = 100
          # count_y = 20

          # tile_id = "012"
          # start_x = 1
          # end_x = 19
          # count_x = 19
          # start_y = 219
          # end_y = 238
          # count_y = 20

          # tile_id = "409"
          # start_x = 499
          # count_x = 20
          # end_x = 518
          # start_y = 160
          # count_y = 19
          # end_y = 178

          message(str_glue(
            "      {which(df_tiles_land$tile_id == tile_id)} / {nrow(df_tiles_land)}"
          ))

          # tictoc::tic()
          s_tile <-
            prepare_tile(start_x, start_y, count_x, count_y)
          # tictoc::toc()

          # s_tile |>
          #   write_rds(str_glue("{dir_temp}/s_tile.rds"))
          # s_tile <-
          #   read_rds(str_glue("{dir_temp}/s_tile.rds"))

          # verify equal time dims JUST FOR FIRST TILE
          time_dims <-
            map(s_tile, \(s) {
              st_get_dimension_values(s, "time") |>
                str_sub(end = 10)
            })

          if (map_dbl(time_dims, length) |> unique() |> length() != 1) {
            message("NOT THE SAME LENGTH!")
          }

          s_tile_grid_fixed <-
            s_tile |>
            map(\(s) {
              s |>
                st_set_dimensions(
                  1,
                  values = st_get_dimension_values(s_proxy, 1, center = F)[
                    start_x:end_x
                  ]
                ) |>
                st_set_dimensions(
                  2,
                  values = st_get_dimension_values(s_proxy, 2, center = F)[
                    start_y:end_y
                  ]
                ) |>
                st_set_crs(4326)
            })

          # plan(multicore, workers = 8)
          # run functions
          output_vars |>
            walk(\(ov) {
              message(str_glue("          processing {ov}"))

              fun_list[[ov]](s_tile_grid_fixed) |>
                rt_write_nc(str_glue("{dir_tiles}/{ov}/tile_{tile_id}.nc"))
            })

          # # ********

          # # with a given dom/gcm/rcm/tile, this section verifies that the new
          # # code (v4) produces the same results as v3

          # output_vars_v3 <-
          #   output_vars |>
          #   str_replace_all("_", "-") |>
          #   str_replace_all("wb", "wetbulb") |>
          #   str_replace_all("(\\d{2})c", "\\1C") |>

          #   str_replace("total-annual-precipitation", "change-total-annual-precipitation") |>
          #   str_replace("wettest-90", "change-90-wettest") |>
          #   str_replace("snowy-days", "change-snowy-days") |>
          #   str_replace("dry-hot-days", "change-dry-hot-days")

          # source("scripts_v3/tb_vars_all.R")

          # foo <-
          #   seq_along(output_vars) |>
          #   set_names(output_vars) %>%
          #   .[-13] |>
          #   map(\(iv) {

          #     message(output_vars[iv])

          #     if (output_vars_v3[iv] == "wettest-day") {
          #       ov_v3 <- "one-day-max-precip"
          #     } else {
          #       ov_v3 <-
          #         tb_vars_all |>
          #         filter(var_final == output_vars_v3[iv]) |>
          #         pull(var_derived)
          #     }

          #     f_v3 <-
          #       "gs://clim_data_reg_useast1/results/global_heat_pf/01_derived" |>
          #       rt_gs_list_files() |>
          #       str_subset(str_glue("01_derived/{dom}_")) |>
          #       str_subset(str_glue("_{ov_v3}_")) |>
          #       str_subset("REMO2015_MOHC")

          #     f_v3 <-
          #       f_v3 |>
          #       rt_gs_download_files(dir_temp, quiet = T)

          #     s_v3 <-
          #       f_v3 |>
          #       read_ncdf(
          #         ncsub = cbind(
          #           start = c(start_x, start_y, 1),
          #           count = c(count_x, count_y, NA)
          #         ),
          #         make_time = F,
          #       ) |>
          #       suppressMessages() |>
          #       setNames("v")

          #     if (output_vars[iv] == "total_annual_precipitation" |
          #         output_vars[iv] == "wettest_90_days" |
          #         output_vars[iv] == "wettest_day"){

          #       s_v3 <-
          #         s_v3 |>
          #         mutate(v = units::set_units(v, kg/m^2/d))

          #     }

          #     s_v3 <-
          #       s_v3 |>
          #       units::drop_units()

          #     fs::file_delete(f_v3)

          #     s_v4 <-
          #       str_glue("{dir_tiles}/{output_vars[iv]}/tile_{tile_id}.nc") |>
          #       read_ncdf() |>
          #       suppressMessages() |>
          #       units::drop_units() |>
          #       setNames("v")

          #     d <- pull(s_v3) - pull(s_v4)
          #     quantile(d, c(0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1)) |> round(1)

          #   })

          # foo

          # # dry hot days differs!
          # dir_cat <- str_glue("{dir_temp}/cat")
          # fs::dir_create(dir_cat)

          # s_tile$tasmax |>
          #   rt_write_nc(str_glue("{dir_cat}/tasmax.nc"))

          # s_tile$precip |>
          #   rt_write_nc(str_glue("{dir_cat}/precip.nc"))

          # walk(c("precip", "tasmax"), \(v) {
          #   # params
          #   if (v == "tasmax") {
          #     thresh <- -1080 # time dimension / 90 ; negative: starts from the end
          #     command <- "gec"
          #   } else if (v == "precip") {
          #     thresh <- 1080
          #     command <- "ltc"
          #   }

          #   # subset baseline
          #   str_glue(
          #     "cdo selyear,1971/2000 {dir_cat}/{v}.nc {dir_cat}/{v}_step1.nc"
          #   ) %>%
          #     system(ignore.stdout = T, ignore.stderr = T)

          #   # calculate percentile
          #   # timpctl produces different results!
          #   str_glue(
          #     "cdo -seltimestep,{thresh} -timsort {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc"
          #   ) %>%
          #     system(ignore.stdout = T, ignore.stderr = T)

          #   # obtain no. days under/above baseline percentile
          #   str_glue(
          #     "cdo -{command},0 -sub {dir_cat}/{v}.nc {dir_cat}/{v}_step2.nc {dir_cat}/{v}_step3.nc"
          #   ) %>%
          #     system(ignore.stdout = T, ignore.stderr = T)
          # })

          # ff <-
          #   dir_cat %>%
          #   list.files(full.names = T) %>%
          #   str_subset("_cat", negate = T) %>%
          #   str_subset("step3") %>%
          #   str_flatten(" ")

          # str_glue("cdo -yearsum -gec,2 -add {ff} {dir_cat}/result.nc") %>%
          #   system()

          # bar <- str_glue("{dir_cat}/result.nc") |> read_ncdf() |> units::drop_units()

          # s_v4 # from above (foo loop)

          # d <- pull(bar) - pull(s_v4)
          # quantile(d, c(0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1)) |> round(1)

          # fs::dir_delete(dir_cat)

          # # ********
        }
      )

      # mosaic

      output_vars_units <-
        var_info_list[output_vars] |>
        map(pluck, "units") |>
        unname() |>
        unlist()

      # loop variables

      walk2(output_vars, output_vars_units, \(ov, ov_un) {
        # some doms/rcms/gcms don't have all years (EAS RegCM4 MPI starts in 1980)

        # ov = output_vars[1]
        # ov_un = output_vars_units[1]

        years_in_tiles <-
          str_glue("{dir_tiles}/{ov}") |>
          fs::dir_ls() |>
          first() |>
          read_ncdf(proxy = T) |>
          suppressMessages() |>
          st_get_dimension_values("time") |>
          str_sub(end = 4) |>
          as.numeric()

        yrs_f_mosaic <- seq(max(first(years_in_tiles), 1971), min(last(years_in_tiles), 2099))

        mosaic(
          output_var = ov,
          output_var_unit = ov_un,
          years = yrs_f_mosaic, #yrs |> tail(-1), #|> tail(-1),
          prefix = str_glue("{rcm}_{gcm}_{dom}"),
          dir_dest_cloud = str_glue(
            "gs://clim_data_reg_useast1/cordex/annual_aggregates/{ov}/{dom}/{rcm}_{gcm}/"
          )
        )
      })

      # clean up

      fs::dir_delete(dir_rawdata)
      fs::dir_delete(dir_tiles)
      fs::dir_delete(dir_res)
    } # end of gcm loop
  } # end of rcm loop
} # end of domain loop


fs::dir_delete(dir_temp)

daemons(0)
