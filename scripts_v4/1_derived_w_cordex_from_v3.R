
dir_data <- "/mnt/pers_disk/v3_transfer"


library(tidyverse)
library(stars)
library(PCICt)

box::use(functions/general_tools[...])

fs::dir_create(dir_data)




source("scripts_v4/0_output_vars.R")
source("scripts_v4/var_info_list.R")

output_vars <- 
  output_vars[-c(13,22)] |> # no winter temps in v3, dry hot days v3 is wrong
  str_subset("wb", negate = T)

output_vars_v3 <- 
  output_vars |>
  str_replace_all("_", "-") |> 
  str_replace_all("wb", "wetbulb") |> 
  str_replace_all("(\\d{2})c", "\\1C") |> 

  str_replace("total-annual-precipitation", "change-total-annual-precipitation") |> 
  str_replace("wettest-90", "change-90-wettest") |> 
  str_replace("snowy-days", "change-snowy-days") |> 
  str_replace("dry-hot-days", "change-dry-hot-days")
 

source("scripts_v3/tb_vars_all.R")






dom <- "EUR"

for(iv in seq(length(output_vars))) {

  ov_v3 <- output_vars_v3[iv]
  ov_v4 <- 
    var_info_list |> 
    pluck(output_vars[iv])
    
  message(str_glue("PROCESSING VAR {iv} / {length(output_vars)} ({output_vars[iv]})"))

  if (ov_v3 == "wettest-day") {

    ov_v3 <- "one-day-max-precip"

  } else {

    ov_v3 <-
      tb_vars_all |>
      filter(var_final == ov_v3) |>
      pull(var_derived)

  }

  ff_v3 <-
    "gs://clim_data_reg_useast1/results/global_heat_pf/01_derived" |>
    rt_gs_list_files() |>
    str_subset(str_glue("01_derived/{dom}_")) |>
    str_subset(str_glue("_{ov_v3}_"))

  ff_v3 <-
    ff_v3 |>
    rt_gs_download_files(dir_data, quiet = T)


  for(rcm in c("REMO2015", "RegCM4")) {
    
    message(str_glue("   {rcm}"))

    gcms <-
      ff_v3 |>
      str_subset(rcm) |>
      str_extract(str_glue("(?<={rcm}_)[^.]+(?=\\.nc)")) |> 
      str_remove("_v[:digit:].")

    for(gcm in gcms) {

      message(str_glue("      {gcm}"))

      f_v3 <-
        ff_v3 |>
        str_subset(rcm) |>
        str_subset(gcm)

      s_v3 <-
        read_mdim(f_v3)

      dim_1 <- st_get_dimension_values(s_v3, 1)
      dim_2 <- st_get_dimension_values(s_v3, 2)
      dim_time <- 
        st_get_dimension_values(s_v3, "time") |> 
        str_sub(end = 4) |> 
        as.numeric()

      s_v3 <-
        s_v3 |>
        
        st_set_dimensions(
          1,
          values = seq(
            round(first(dim_1)-0.1, 1),
            round(last(dim_1)-0.1, 1),
            length.out = length(dim_1)
          )
        ) |>
        
        st_set_dimensions(
          2,
          values = seq(
            round(first(dim_2)-0.1, 1),
            round(last(dim_2)-0.1, 1),
            length.out = length(dim_2)
          )
        ) |>
        
        st_set_dimensions(
          3,
          values = dim_time
        ) |> 
        
        st_set_crs(4326) |>
        suppressWarnings() |>
        setNames(output_vars[iv])

      un_v3 <-
        s_v3 |> pull() |> units::deparse_unit()

      un_v4 <-
        ov_v4 |> pluck("units")

      if ((un_v3 == "K" | un_v3 == "kg m-2 s-1") & un_v4 == "d") {

        s_v3 <-
          s_v3 |>
          units::drop_units() |>
          mutate(
            !!sym(names(s_v3)) := units::set_units(!!sym(names(s_v3)), !!un_v4)
          )
        
      } else if (un_v3 == "kg m-2 s-1" & un_v4 == "mm") {

        s_v3 <-
          s_v3 |>
          mutate(!!sym(names(s_v3)) := units::set_units(!!sym(names(s_v3)), kg/m^2/d)) |> 
          units::drop_units() |> 
          mutate(!!sym(names(s_v3)) := units::set_units(!!sym(names(s_v3)), mm))

      }

      walk(seq(1971, 2099), \(yr) {
        
        f <- 
          str_glue("{dir_data}/{rcm}_{gcm}_{dom}_{str_replace_all(output_vars[iv], '_', '-')}_yr_{yr}-01-01_v3.nc")

        s_v3 |>
          filter(time == yr) |>
          adrop() |>
          rt_write_nc(f)

        str_glue(
          "gcloud storage mv {f} gs://clim_data_reg_useast1/cordex/annual_aggregates/{output_vars[iv]}/{dom}/{rcm}_{gcm}/"
        ) |>
          system(ignore.stdout = T, ignore.stderr = T)
      })

      fs::file_delete(c(f_v3))

    }

  }

}

fs::dir_delete(dir_data)





# *************

mod <- "REMO2015_MOHC-HadGEM2-ES"


ff <- 
  str_glue("gs://clim_data_reg_useast1/cordex/annual_aggregates/{output_vars[-13]}") |> 
  map(\(f){

    print(fs::path_file(f))

    str_glue("{f}/NAM/{mod}") |>
      rt_gs_list_files() |>
      str_subset("2070-01-01") |> 
      rt_gs_download_files(dir_data)

  })

a <- 
  ff |> 
  map(\(f){

    ss <-
      f |>
      map(read_ncdf) |> 
      suppressMessages()

    dif <- ss[[1]] - ss[[2]]

    q <- quantile(pull(dif), c(0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1), na.rm = T)
    
    p <-
      dif |>
      setNames("v") |>
      units::drop_units() |>
      as_tibble() |>
      ggplot(aes(lon, lat, fill = v)) +
      geom_raster() +
      colorspace::scale_fill_binned_diverging(na.value = "transparent")

    return(list(q = q, p = p))

  })


# 15