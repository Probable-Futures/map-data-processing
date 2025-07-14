
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")


library(tidyverse)
library(stars)
library(furrr)
library(PCICt)
box::use(../functions/general_tools[...],
         ../functions/tile[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
options(future.globals.maxSize = 1000 * 1024^2)

plan(multicore)

source("scripts_v4/0_output_vars.R")
source("scripts_v4/var_info_list.R")
source("scripts_v4/fn_derived.R")
source("scripts_v4/processing_functions.R")

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")
rcms <- c("REMO2015", "RegCM4")



dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_tiles <- "/mnt/pers_disk_300/tiles"
dir_res <- "/mnt/pers_disk_300/results"




yrs <- seq(1970, 2099) # add 1 year at each end


# get all file names

input_vars_all <- 
  var_info_list[output_vars] |> 
  map(pluck, "input_vars") |>
  unname() |> 
  unlist()

input_vars <- 
  unique(names(input_vars_all)) |> 
  set_names(unique(input_vars_all))

# CHANGE!!!

input_vars <- 
  c("precipitation") |> set_names("precip")


df_files <- 
  input_vars |> 
  map(\(var){
    
    str_glue("data_tables/df-files_{var}.csv") |> 
      read_csv() |> 
      filter(!(str_detect(rcm, "RegCM") & domain == "SAM-22")) |>
      filter(!(str_detect(rcm, "RegCM") & domain == "AUS-22")) |> 
      mutate(domain = str_sub(domain, end = 3))
    
  })
    
    


for(dom in doms) {
  
  message(str_glue("PROCESSING domain {which(dom == doms)} / {length(doms)} ({dom})"))
  
  
  for(rcm in rcms) {
    
    df_files_rcm <- 
      df_files |>
      map(\(df){
        
        df |> 
          filter(domain == dom,
                 str_detect(rcm, {{rcm}}))
      })
    
    if(nrow(df_files_rcm[[1]]) == 0) next
    
    
    gcms <- 
      df_files_rcm |> 
      pluck(1) |>  
      pull(gcm) |> 
      unique()
    
    for(gcm in gcms) {
      
      fs::dir_create(dir_rawdata)
      fs::dir_create(dir_tiles)
      fs::dir_create(dir_res)
      
      message(str_glue("  RCM {which(rcm == rcms)} / {length(rcms)} ({rcm})  |  GCM {which(gcm == gcms)} / {length(gcms)} ({gcm})"))
      
      df_files_rcm_gcm <- 
        df_files_rcm |> 
        map(\(df){
          
          df |> 
            filter(gcm == {{gcm}})
          
        })
      
      
      ff_sub <- 
        df_files_rcm_gcm |> 
        map(\(df){
          
          df |> 
            filter(year(end_date) >= first(yrs),
                   year(start_date) <= last(yrs))
          
        }) |> 
        map(\(df){
          
          df |> 
            pull(file)
          
        })
      
      
      
      # reference grid
      f_proxy <- 
        rt_gs_download_files(ff_sub[[1]][1], dir_rawdata, quiet = T)
      
      s_proxy <- 
        f_proxy |> 
        read_ncdf(ncsub = cbind(start = c(1,1,1), count = c(NA,NA,1))) |> 
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
      
      while(count_xy < 2){
        
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
      
      pwalk(df_tiles_land, function(tile_id, start_x, start_y, end_x, end_y, count_x, count_y, ...){
        
        
        # tile_id = "262"
        # start_x = 320
        # count_x = 20
        # start_y = 100
        # count_y = 20
        
        # tile_id = "216"
        # start_x = 264
        # end_x = 283
        # count_x = 20
        # start_y = 143
        # end_y = 162
        # count_y = 20
        

                
        
        message(str_glue("      {which(df_tiles_land$tile_id == tile_id)} / {nrow(df_tiles_land)}"))
        
        
        s_tile <- 
          prepare_tile(start_x, start_y, count_x, count_y)
        
        s_tile <- 
          s_tile |> 
          map(\(s){
            s |> 
              st_set_dimensions(1, values = st_get_dimension_values(s_proxy, 1)[start_x:end_x]) |> 
              st_set_dimensions(2, values = st_get_dimension_values(s_proxy, 2)[start_y:end_y]) |> 
              st_set_crs(4326)
          })
        
        # plan(multicore, workers = 8)
        # run functions
        output_vars |> 
          walk(\(ov) {
            
            message(str_glue("          processing {ov}"))
            
            fun_list[[ov]](s_tile) |>   
              rt_write_nc(str_glue("{dir_tiles}/{ov}/tile_{tile_id}.nc"))
            
          })
        
      })
      
      
      
      
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
        
        mosaic(output_var = ov,
               output_var_unit = ov_un,
               years = yrs_f_mosaic,  #yrs |> tail(-1), #|> tail(-1),
               prefix = str_glue("{rcm}_{gcm}_{dom}"),
               dir_dest_cloud = str_glue("gs://clim_data_reg_useast1/cordex/annual_aggregates/{ov}/{dom}/{rcm}_{gcm}/"))
        
      })
      
      
      # clean up
      
      fs::dir_delete(dir_rawdata)
      fs::dir_delete(dir_tiles)
      fs::dir_delete(dir_res)
      
      
      
    } # end of gcm loop
    
  } # end of rcm loop
  
} # end of domain loop



