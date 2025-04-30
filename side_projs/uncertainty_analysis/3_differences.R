
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")


library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

source("side_projs/uncertainty_analysis/0_output_vars.R")

output_vars <- 
  output_vars |> 
  str_replace_all("_", "-")

output_vars_cordex <- 
  output_vars |> 
  str_replace_all("wb", "wetbulb") |> 
  str_replace_all("(\\d{2})c", "\\1C") |> 
  str_replace("wettest-90", "90-wettest")


dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_res <- "/mnt/pers_disk_300/dif_era_cordex"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_res)



ff_era <- 
  rt_gs_list_files("gs://clim_data_reg_useast1/era5/warming_level_aggregates/") |> 
  str_subset(".nc")

ff_cordex <- 
  c(
    rt_gs_list_files("gs://clim_data_reg_useast1/results/global_heat_pf/03_mosaicked/heat/v3/") |> 
      str_subset(".nc"),
    rt_gs_list_files("gs://clim_data_reg_useast1/results/global_heat_pf/03_mosaicked/water/v3/") |> 
      str_subset(".nc"),
    rt_gs_list_files("gs://clim_data_reg_useast1/results/misc/winter_temps_pf/") |> 
      str_subset(".nc")
  )
  

walk(seq_along(output_vars), \(i){
  
  ov <- output_vars[i]
  ovc <- output_vars_cordex[i]
  
  
  if(ov != "average-winter-temperature") {
    f_cordex <- 
      ff_cordex |> 
      str_subset(str_glue("{ovc}_v03"))|> 
      str_subset("100perc", negate = T)
    
  } else {
    f_cordex <- 
      ff_cordex |> 
      str_subset(ovc)
    
  } 
  
  change <- case_when(str_detect(f_cordex, "/water/") ~ T,
                      TRUE ~ F)
  
  f_cordex <- 
    f_cordex |> 
    rt_gs_download_files(dir_rawdata)
  
  s_cordex <- 
    f_cordex |> 
    read_ncdf(proxy = F) |> 
    suppressMessages() |> 
    slice(wl, 1:2)
  
  
  f_era <- 
    ff_era |> 
    str_subset(str_glue("{ov}_wls")) |> 
    str_subset("100perc", negate = T) |> 
    rt_gs_download_files(dir_rawdata)
  
  s_era <- 
    f_era |> 
    read_ncdf(proxy = F) |> 
    suppressMessages() |> 
    st_warp(s_cordex)
  
  un <- 
    s_era |> 
    pull() |> 
    units::deparse_unit()
  
  if (un == "K") {
    
    # un_cordex <- case_when(un == "K" ~ "degC")
    un <- "degC"
    
    s_era <- 
      names(s_era) |> 
      map(\(s){
        s_era |> 
          select(s) |> 
          mutate(!!sym(s) := units::set_units(!!sym(s), !!un))
      }) %>%
      do.call(c, .)
    
  }
  
  s_era <- 
    units::drop_units(s_era)
  
  print(str_glue("ERA: {fs::path_file(f_era)}
               CORDEX: {fs::path_file(f_cordex)}
               "))
  
  if(!change){
    dif <- s_cordex - s_era
    
  } else {
    # difference of baselines
    dif <- slice(s_cordex, wl, 1) - slice(s_era, wl, 1)
    
  }
  
  dif <- 
    names(dif) |> 
    map(\(s){
      dif |> 
        select(all_of(s)) |> 
        mutate(!!sym(s) := units::set_units(!!sym(s), !!un))
    }) %>% 
    do.call(c, .)
  
  f_dif <- str_glue("{dir_res}/era-cordex-dif_{output_vars[i]}.nc")
  
  rt_write_nc_notime(dif, f_dif)
  
  
  
  
  if(!change){
    dif_delta <- 
      list(cordex = slice(s_cordex, wl, 2) - slice(s_cordex, wl, 1),
           era = slice(s_era, wl, 2) - slice(s_era, wl, 1)
      )
    
  } else {
    dif_delta <- 
      list(cordex = slice(s_cordex, wl, 2), # delta ready
           era = slice(s_era, wl, 2) - slice(s_era, wl, 1))

  }
  
  dif_delta <- 
    dif_delta$cordex - dif_delta$era
 
  
  dif_delta <- 
    names(dif_delta) |> 
    map(\(s){
      dif_delta |> 
        select(all_of(s)) |> 
        mutate(!!sym(s) := units::set_units(!!sym(s), !!un))
    }) %>%
    do.call(c, .)
  
  f_dif_delta <- str_glue("{dir_res}/era-cordex-dif-delta_{output_vars[i]}.nc")
  
  rt_write_nc_notime(dif_delta, f_dif_delta)
  
  
  c(f_cordex, f_era) |> 
    walk(fs::file_delete)
  
  
})

fs::dir_delete(dir_rawdata)
















