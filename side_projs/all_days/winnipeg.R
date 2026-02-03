# download files in parallel
library(tidyverse)
library(stars)
library(furrr)
# library(units)

plan(multicore, workers = parallelly::availableCores() - 1)

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
  arrange(rcm, gcm) |>
  mutate(
    gcm_short = c(
      "HadGEM2-ES",
      "MPI-ESM-LR",
      "NorESM1-M",
      "HadGEM2-ES",
      "MPI-ESM-LR",
      "GFDL-ESM2M"
    )
  )

# tb_files %>%
#   filter(str_detect(file, "historical")) |>
#   future_pwalk(function(loc, file, ...) {
#     loc_ <-
#       loc %>%
#       str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")

#     str_glue("{loc_}/{file}") %>%
#       {
#         system(
#           str_glue("gcloud storage cp {.} {dir_raw_data}"),
#           ignore.stdout = T,
#           ignore.stderr = T
#         )
#       }
#   })

extract_data_cordex <- function(lon, lat) {
  r <-
    seq(6) |>
    future_map(\(i) {
      gcm = tb_models$gcm[i]
      rcm = tb_models$rcm[i]

      ff <-
        fs::dir_ls(dir_raw_data) |>
        str_subset(rcm) |>
        str_subset(gcm)

      proxy <-
        ff[1] |>
        read_mdim(proxy = T)

      lon_i <- which.min(abs(lon + 360 - st_get_dimension_values(proxy, 1)))
      lat_i <- which.min(abs(lat - st_get_dimension_values(proxy, 2)))

      r <-
        ff |>
        map(\(f) {
          f |>
            read_mdim(offset = c(lon_i - 1, lat_i - 1, 0), count = c(1, 1, NA))
        })

      do.call(c, c(r, along = "time")) |>
        setNames("tasmax") |>
        filter(year(time) <= 2000, year(time) >= 1971) |>
        # filter(month(time) %in% c(6, 7, 8)) |>
        units::drop_units() |>
        as_tibble() |>
        mutate(time = str_sub(time, end = 10)) |>
        mutate(model = str_glue("{rcm}-{tb_models$gcm_short[i]}"))
    })

  r <-
    r |>
    set_names(
      tb_models |>
        transmute(m = str_glue("{rcm}-{tb_models$gcm_short}")) |>
        pull(m)
    )

  return(r)
}


# DRIVING GCM
dir_raw_data_gcm <- "/mnt/pd-pf/all_days/raw_gcm"
fs::dir_create(dir_raw_data_gcm)

models <- c("HadGEM2-ES", "MPI-ESM-LR")

ff_gcm <-
  map(models |> set_names(), \(m) {
    str_glue(
      "gcloud storage ls gs://cmip5_data/CMIP5_raw_data/daily_data/maximum_temperature/*{m}_historical*"
    ) |>
      system(intern = T) |>
      tail(4)
  })


ff_gcm <-
  ff_gcm |>
  map(\(ff) {
    ff |>
      map_chr(\(f) {
        # str_glue("gcloud storage cp {f} {dir_raw_data_gcm}") |>
        #   system(ignore.stdout = T, ignore.stderr = T)

        fs::path(dir_raw_data_gcm, fs::path_file(f))
      })
  })

extract_data_cmip <- function(lon, lat) {
  r <-
    seq(2) |>
    future_map(\(i) {
      ff <-
        ff_gcm[[i]]

      proxy <-
        ff[1] |>
        read_mdim(proxy = T)

      lon_i <- which.min(abs(lon + 360 - st_get_dimension_values(proxy, 1)))
      lat_i <- which.min(abs(lat - st_get_dimension_values(proxy, 2)))

      r <-
        ff |>
        map(\(f) {
          f |>
            read_mdim(offset = c(lon_i, lat_i, 0), count = c(1, 1, NA))
        })

      do.call(c, c(r, along = "time")) |>
        setNames("tasmax") |>
        filter(year(time) <= 2000, year(time) >= 1971) |>
        # filter(month(time) %in% c(6, 7, 8)) |>
        units::drop_units() |>
        as_tibble() |>
        mutate(time = str_sub(time, end = 10)) |>
        mutate(model = str_glue("CMIP6-{models[i]}"))
    })
  r <-
    r |>
    set_names(models)
  return(r)
}


# ERA 5

dir_raw_data_era <- "/mnt/pd-pf/all_days/raw_era"
fs::dir_create(dir_raw_data_era)

ff_era <-
  str_glue(
    "gcloud storage ls gs://clim_data_reg_useast1/era5/daily_aggregates/2m_maximum_temperature/"
  ) |>
  system(intern = T) |>
  str_subset(str_flatten(
    seq(as_date("1971-01-01"), as_date("2000-12-31"), by = "1 day"),
    "|"
  ))


ff_era <-
  ff_era |>
  future_map_chr(\(f) {
    # str_glue("gcloud storage cp {f} {dir_raw_data_era}") |>
    #   system(ignore.stdout = T, ignore.stderr = T)

    fs::path(dir_raw_data_era, fs::path_file(f))
  })

extract_data_era <- function(lon, lat) {
  proxy <-
    ff_era[1] |>
    read_mdim(proxy = T)

  lon_i <- which.min(abs(lon + 360 - st_get_dimension_values(proxy, 1)))
  lat_i <- which.min(abs(lat - st_get_dimension_values(proxy, 2)))

  r <-
    ff_era |>
    future_map(\(f) {
      f |>
        read_mdim(offset = c(lon_i, lat_i, 0), count = c(1, 1, NA))
    })

  do.call(c, c(r, along = "time")) |>
    setNames("tasmax") |>
    filter(year(time) <= 2000, year(time) >= 1971) |>
    # filter(month(time) %in% c(6, 7, 8)) |>
    units::drop_units() |>
    as_tibble() |>
    mutate(time = str_sub(time, end = 10)) |>
    mutate(model = str_glue("ERA5")) |>
    rename(lon = longitude, lat = latitude)
}


# *******************************

plot_annual <- function(data, city_name) {
  data |>
    # bind_rows() |>

    ggplot(aes(x = tasmax)) +
    geom_histogram(bins = 100) +
    geom_vline(xintercept = 273, linetype = "dashed") +
    labs(x = "tasmax (K)", title = city_name, subtitle = "Annual") +
    facet_wrap(~model, scales = "free_y", ncol = 3) #+
  # ggview::canvas(10, 5)
}

seas <-
  list(
    DJF = c("12", "01", "02"),
    MAM = c("03", "04", "05"),
    JJA = c("06", "07", "08"),
    SON = c("09", "10", "11")
  )

plot_seasonal <- function(data, city_name) {
  data |>
    group_by(
      season = case_when(
        str_sub(time, 6, 7) %in% seas$DJF ~ "DJF",
        str_sub(time, 6, 7) %in% seas$MAM ~ "MAM",
        str_sub(time, 6, 7) %in% seas$JJA ~ "JJA",
        str_sub(time, 6, 7) %in% seas$SON ~ "SON"
      )
    ) |>
    nest() |>
    mutate(
      p = map2(season, data, \(seas, tb) {
        tb |>
          ggplot(aes(x = tasmax)) +
          geom_histogram(bins = 100) +
          geom_vline(xintercept = 273, linetype = "dashed") +
          labs(x = "tasmax (K)", title = city_name, subtitle = seas) +
          facet_wrap(~model, scales = "free_y", ncol = 3)
      })
    ) |>
    pull(p)
}


fn_plot <- function(city, lon, lat) {
  d <- c(
    era = list(extract_data_era(lon, lat)),
    extract_data_cordex(lon, lat),
    extract_data_cmip(lon, lat)
  )

  d <-
    list(
      d$`HadGEM2-ES`,
      d$`REMO2015-HadGEM2-ES`,
      d$`RegCM4-HadGEM2-ES`,
      d$`MPI-ESM-LR`,
      d$`REMO2015-MPI-ESM-LR`,
      d$`RegCM4-MPI-ESM-LR`,
      d$era
    ) |>
    bind_rows() |>
    mutate(model = fct_inorder(model))

  p1 <-
    d |>
    plot_annual(city)

  p <-
    d |>
    plot_seasonal(city)

  r <- c(p1, p)
  return(r)
}


# ****************************************************

# winnipeg <- fn_plot("Winnipeg", -97, 50)
winnipeg[[1]] + ggview::canvas(10, 6)
winnipeg[[2]] + ggview::canvas(10, 6)
winnipeg[[3]] + ggview::canvas(10, 6)
winnipeg[[4]] + ggview::canvas(10, 6)
winnipeg[[5]] + ggview::canvas(10, 6)

# havre <- fn_plot("Havre, MT", -109, 48)
havre[[1]] + ggview::canvas(10, 6)
havre[[2]] + ggview::canvas(10, 6)
havre[[3]] + ggview::canvas(10, 6)
havre[[4]] + ggview::canvas(10, 6)
havre[[5]] + ggview::canvas(10, 6)

# portland <- fn_plot("Portland", -122, 45)
portland[[1]] + ggview::canvas(10, 6)
portland[[2]] + ggview::canvas(10, 6)
portland[[3]] + ggview::canvas(10, 6)
portland[[4]] + ggview::canvas(10, 6)
portland[[5]] + ggview::canvas(10, 6)

# dallas <- fn_plot("Dallas", -97, 33)
dallas[[1]] + ggview::canvas(10, 6)
dallas[[2]] + ggview::canvas(10, 6)
dallas[[3]] + ggview::canvas(10, 6)
dallas[[4]] + ggview::canvas(10, 6)
dallas[[5]] + ggview::canvas(10, 6)
