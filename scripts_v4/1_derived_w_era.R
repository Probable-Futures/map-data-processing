
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")


library(tidyverse)
library(stars)
library(furrr)
library(PCICt)
box::use(../functions/general_tools[...],
         ../functions/tile[...])

options(future.fork.enable = T,
        future.rng.onMisuse = "ignore")

plan(multicore)

source("scripts_v4/0_output_vars.R")
source("scripts_v4/var_info_list.R")
source("scripts_v4/fn_derived.R")
source("scripts_v4/processing_functions.R")


dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_tiles <- "/mnt/pers_disk_300/tiles"
dir_res <- "/mnt/pers_disk_300/results"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_tiles)
fs::dir_create(dir_res)


yrs <- seq(1970, 2021) # add 1 year at each end


# get all file names

input_vars_all <- 
  var_info_list[output_vars] |> 
  map(pluck, "input_vars") |>
  unname() |> 
  unlist()

input_vars <- 
  unique(names(input_vars_all)) |> 
  set_names(unique(input_vars_all))

ff <- 
  input_vars |> 
  map(\(v) rt_gs_list_files(str_glue("gs://clim_data_reg_useast1/era5/daily_aggregates/{v}")))

# subset files based on yrs
ff_sub <- 
  ff |>
  map(\(f) str_subset(f, str_flatten(yrs, "|")))


# reference grid
f_proxy <- 
  rt_gs_download_files(ff[[1]][1], dir_rawdata, quiet = T)

s_proxy <- 
  f_proxy |> 
  read_ncdf(ncsub = cbind(start = c(1,1,1), count = c(NA,NA,1))) |> 
  adrop()


# land
land_r <- land()


# TILE *****

df_tiles <- 
  rt_tile_table(s_proxy, 50, land_r)

df_tiles_land <- 
  df_tiles |> 
  filter(land == T)



# download all files
ff_sub <-
  ff_sub |>
  map(\(f) rt_gs_download_files(f, dir_rawdata))

# ff_sub <-
#   ff_sub |>
#   map(\(ff) str_glue("{dir_rawdata}/{fs::path_file(ff)}"))




output_vars |> 
  walk(\(dir_v) fs::dir_create(str_glue("{dir_tiles}/{dir_v}")))




# loop through tiles
pwalk(df_tiles_land, function(tile_id, start_x, start_y, count_x, count_y, ...){
  
  # arctic
  # tile_id = "071"
  # start_x = 249
  # count_x = 49
  # start_y = 1
  # count_y = 51
  
  # colombia
  # tile_id = "330"
  # start_x = 1143
  # count_x = 49
  # start_y = 361
  # count_y = 52
  
  # north america
  # tile_id = "285"
  # start_x = 994
  # count_x = 49
  # start_y = 207
  # count_y = 51
  
  message(str_glue("importing tile {which(df_tiles_land$tile_id == tile_id)} / {nrow(df_tiles_land)}"))
  
  s_tile <- 
    prepare_tile(start_x, start_y, count_x, count_y)
  
  # run functions
  output_vars |> 
    walk(\(ov) {
      
      message(str_glue("   processing {ov}"))
      
      fun_list[[ov]](s_tile) |> 
        rt_write_nc(str_glue("{dir_tiles}/{ov}/tile_{tile_id}.nc"))
      
    })
  
})



# ********


# mosaic 

output_vars_units <- 
  var_info_list[output_vars] |> 
  map(pluck, "units") |>
  unname() |> 
  unlist()



# loop variables
walk2(output_vars, output_vars_units, \(ov, ov_un) {
  
  # ov = output_vars
  # ov_un = output_vars_units
  
  
  mosaic(output_var = ov,
         output_var_unit = ov_un,
         years = yrs |> head(-1) |> tail(-1),
         prefix = "era5",
         dir_dest_cloud = str_glue("gs://clim_data_reg_useast1/era5/annual_aggregates/{ov}/"))
  
})


# clean up

fs::dir_delete(dir_rawdata)
fs::dir_delete(dir_tiles)
fs::dir_delete(dir_res)


