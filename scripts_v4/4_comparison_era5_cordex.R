
dir_data <- "/mnt/pers_disk/dif_era_cordex"




library(tidyverse)
library(stars)
library(furrr)

box::use(functions/general_tools[...])

options(future.fork.enable = T,
        future.rng.onMisuse = "ignore")

plan(multicore)

source("scripts_v4/0_output_vars.R")

output_vars <- 
  output_vars |> 
  str_replace_all("_", "-")

output_vars_cordex <- 
  output_vars |> 
  str_replace_all("wb", "wetbulb") |> 
  str_replace_all("(\\d{2})c", "\\1C") |> 
  str_replace("wettest-90", "90-wettest")


fs::dir_create(dir_data)
dir_rawdata <- str_glue("{dir_data}/rawdata")
dir_res <- str_glue("{dir_data}/dif")

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
      str_subset(".nc"),
    rt_gs_list_files("gs://clim_data_reg_useast1/cordex/warming_level_aggregates/**GLOBAL**") |> 
      str_subset(".nc")
  )
  

output_vars <- output_vars[22]
output_vars_cordex <- output_vars_cordex[22]
walk(seq_along(output_vars), \(i){
  
  ov <- output_vars[i]
  ovc <- output_vars_cordex[i]
  
  
  if(ov == "wettest-day"){
    f_cordex <- 
      ff_cordex |> 
      str_subset(ov) |> # no need for ovc (same name)
      str_subset("0.5_stats|1.0_stats")
    
  } else if(ov != "average-winter-temperature") {
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
    rt_gs_download_files(dir_rawdata, quiet = T)
  
  if(length(f_cordex) > 1) {
    
    s_cordex <- 
      f_cordex |> 
      map(read_ncdf, proxy = F) |> 
      suppressMessages()
    
    s_cordex <- 
      do.call(c, c(s_cordex, along = "wl"))
    
  } else {
    
    s_cordex <- 
      f_cordex |> 
      read_ncdf(proxy = F) |> 
      suppressMessages() |> 
      slice(wl, 1:2)
    
  }
  
  
  
  
  f_era <- 
    ff_era |> 
    str_subset(str_glue("{ov}_wls")) |> 
    str_subset("100perc", negate = T) |> 
    rt_gs_download_files(dir_rawdata, quiet = T)
  
  s_era <- 
    f_era |> 
    read_ncdf(proxy = F) |> 
    suppressMessages() |> 
    st_warp(s_cordex)
  
  if(ov == "wettest-day"){
    
    s_cordex <- 
      s_cordex |> 
      select(1,3,5,7,8)
    
    s_era <- 
      s_era |> 
      select(1,3,5,7,8)
    
  } else {
    
    s_era <- 
      s_era |> 
      select(1,3,5,7)
    
  }
  
  
  
  
  
  
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
  
  
  if(ov != "wettest-day"){
    
    s_era <- 
      units::drop_units(s_era)
    
    if(change){
      s_wl_1 <- slice(s_cordex, wl, 1)
      s_cordex <- c(s_wl_1, s_wl_1 + slice(s_cordex, wl, 2), along = "wl")
      # st_dimensions(s_cordex)[3] <- st_dimensions(s_era)[3]
    }
    
    s_cordex <- 
      s_cordex |> 
      setNames(names(s_era))
    
  }

  if(ov == "wettest-day"){

    s_cordex |> 
      rt_write_nc(str_glue("{dir_res}/wd_cordex.nc"))

    s_era |> 
      rt_write_nc(str_glue("{dir_res}/wd_era.nc"))

  }
  
  
  st_dimensions(s_cordex)[3] <- st_dimensions(s_era)[3]
  
  print(str_glue("ERA: {fs::path_file(f_era)}
               CORDEX: {fs::path_file(f_cordex)}
               "))
  
  # if(!change){
    
    dif_abs <- s_cordex - s_era
    dif_rel <- s_cordex/s_era
    
  # } else {
  #   
  #   # difference of baselines
  #   dif_abs <- slice(s_cordex, wl, 1) - slice(s_era, wl, 1)
  #   dif_rel <- slice(s_cordex, wl, 1)/slice(s_era, wl, 1)
  # }
    
  
    
  if (ov == "wettest-day"){
    
    # annual max
    dif <- 
      list(abs = dif_abs |> select(-5),
           rel = 
             dif_rel |>
             select(-5) |> 
             units::drop_units() |> 
             merge() |> 
             setNames("a") |> 
             mutate(a = if_else(is.infinite(a), NA, a)) |> 
             split("attributes")
             )
    
    # wl max
    dif_2 <- 
      list(abs = dif_abs |> select(5),
           rel = 
             dif_rel |>
             select(5) |> 
             units::drop_units() |> 
             mutate(perc_100 = if_else(is.infinite(perc_100), NA, perc_100)) 
             )
  
    
      
  } else {
    
    dif <- 
      map(list(dif_abs, dif_rel), \(dif){
        
        names(dif) |> 
          map(\(s){
            dif |> 
              select(all_of(s)) |> 
              mutate(!!sym(s) := if_else(is.infinite(!!sym(s)), NA, !!sym(s))) |> 
              mutate(!!sym(s) := units::set_units(!!sym(s), !!un))
          })  |>  
          do.call(c, args = _)
      }) |> 
      set_names(c("abs", "rel"))
    
  }

  
  iwalk(dif, \(s, d){
    
    f_dif <- str_glue("{dir_res}/era-cordex-dif_{d}_{output_vars[i]}.nc")
    
    rt_write_nc(s, f_dif)
    
  })
  
  
  if(ov == "wettest-day"){
    
    iwalk(dif_2, \(s, d){
      
      f_dif <- str_glue("{dir_res}/era-cordex-dif_{d}_max-{output_vars[i]}.nc")
      
      rt_write_nc(s, f_dif)
      
    })
    
  }
  
  
  # 
  # if(!change){
  #   dif_delta <- 
  #     list(cordex = slice(s_cordex, wl, 2) - slice(s_cordex, wl, 1),
  #          era = slice(s_era, wl, 2) - slice(s_era, wl, 1)
  #     )
  #   
  # } else {
  #   dif_delta <- 
  #     list(cordex = slice(s_cordex, wl, 2), # delta ready
  #          era = slice(s_era, wl, 2) - slice(s_era, wl, 1))
  # 
  # }
  # 
  # dif_delta_abs <- 
  #   dif_delta$cordex - dif_delta$era
  # 
  # dif_delta_rel <- 
  #   dif_delta$cordex/dif_delta$era
  # 
  # 
  # dif_delta <- 
  #   map(list(dif_delta_abs, dif_delta_rel), \(dif){
  #     
  #     names(dif) |> 
  #       map(\(s){
  #         dif |> 
  #           select(all_of(s)) |> 
  #           mutate(!!sym(s) := if_else(is.infinite(!!sym(s)), NA, !!sym(s))) |> 
  #           mutate(!!sym(s) := units::set_units(!!sym(s), !!un))
  #       })  |>  
  #       do.call(c, args = _)
  #   }) |> 
  #   set_names(c("abs", "rel"))
  # 
  # iwalk(dif_delta, \(s, d){
  #   
  #   f_dif <- str_glue("{dir_res}/era-cordex-dif-delta_{d}_{output_vars[i]}.nc")
  #   message(f_dif)
  #   
  #   rt_write_nc(s, f_dif)
  #   
  # })
  
  c(f_cordex, f_era) |> 
    walk(fs::file_delete)
  
  
})

fs::dir_delete(dir_rawdata)
















