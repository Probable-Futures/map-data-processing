
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")


library(tidyverse)
library(stars)
library(furrr)

box::use(../functions/general_tools[...])

options(future.fork.enable = T,
        future.rng.onMisuse = "ignore")

plan(multicore)

source("scripts_v4/0_output_vars.R")
source("scripts_v4/processing_functions.R")

dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_res <- "/mnt/pers_disk_300/results"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_res)


wl_yrs <- 
  list(wl_0p5 = c(1971, 2000),
       wl_1p0 = c(2000, 2020))



# loop output_vars
output_vars |> 
  walk(\(v){
    
    message(str_glue("PROCESSING VAR {which(v == output_vars)} / {length(output_vars)} ({v})"))
    
    ff <- 
      rt_gs_list_files(str_glue("gs://clim_data_reg_useast1/era5/annual_aggregates/{v}/"))
    
    ff <- 
      rt_gs_download_files(ff, dir_rawdata, quiet = T)
    
    date_vector <- 
      ff |> 
      str_sub(-13,-4) |> 
      as_date()
    
    
    s <- 
      ff |> 
      future_map(read_ncdf) |> 
      suppressMessages()
    
    s <- 
      do.call(c, c(s, along = "time")) |> 
      st_set_dimensions("time", values = date_vector)
    
    un_s <- 
      s |> 
      pull() |> 
      units::deparse_unit()
    
    s_wl_stats <- 
      wl_yrs |> 
      map(\(yrs){
        
        s |> 
          filter(year(time) >= yrs[1],
                 year(time) <= yrs[2]) |> 
          
          # function to calculate mean and percentiles
          # via st_apply (see processing_functions.R)
          calc_stats_wl(un_s)
        
      })
    
    
    s_wl_stats <- 
      do.call(c, c(s_wl_stats, along = "wl")) |> 
      st_set_dimensions("wl", values = names(s_wl_stats) |> str_sub(-3) |> str_replace("p", ".") |> as.numeric())
    
    f_res <- str_glue("{dir_res}/era5_{str_replace_all(v, '_', '-')}_wls.nc")
    
    rt_write_nc(s_wl_stats, 
                f_res,
                gatt_name = "source code",
                gatt_val = "https://github.com/Probable-Futures/map-data-processing")
    
    system(str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/era5/warming_level_aggregates/"),
           ignore.stdout = T, ignore.stderr = T)
    
    ff |> future_walk(fs::file_delete)
    
    
  })

fs::dir_delete(dir_rawdata)
fs::dir_delete(dir_res)





