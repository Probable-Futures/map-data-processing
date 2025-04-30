
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")


library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")
source("https://raw.github.com/carlosdobler/spatial-routines/master/tile.R")

source("side_projs/uncertainty_analysis/0_output_vars.R")
source("side_projs/uncertainty_analysis/var_info_list.R")
source("side_projs/uncertainty_analysis/fn_derived.R")



dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_tiles <- "/mnt/pers_disk_300/tiles"
dir_res <- "/mnt/pers_disk_300/results"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_tiles)
fs::dir_create(dir_res)



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
  map(input_vars, \(v) rt_gs_list_files(str_glue("gs://clim_data_reg_useast1/era5/daily_aggregates/{v}")))


# reference grid
f_proxy <- 
  rt_gs_download_files(ff[[1]][1], dir_rawdata)

s_proxy <- 
  f_proxy |> 
  read_ncdf(ncsub = cbind(start = c(1,1,1), count = c(NA,NA,1))) |> 
  adrop()


# land
land_p <- 
  "/mnt/bucket_mine/misc_data/physical/ne_50m_land/ne_50m_land.shp" |> 
  st_read()

land_centr <- 
  land_p |> 
  st_centroid() |> 
  st_coordinates()

north_ant_centr <- 
  which(land_centr[,2] > -60)

land_r <- 
  land_p |> 
  slice(north_ant_centr) |> 
  mutate(a = 1) |>  
  select(a) |> 
  st_rasterize(st_as_stars(st_bbox(),
                           dx = 0.1,
                           values = 0))

land_r <- 
  land_r %>% 
  st_warp(s_proxy,
          method = "max",
          use_gdal = T) %>% 
  setNames("land")

land_r[land_r == 0] <- NA

st_dimensions(land_r) <- st_dimensions(s_proxy)[1:2]


# TILE *****

df_tiles <- rt_tile_table(s_proxy, 50, land_r)

df_tiles_land <- 
  df_tiles |> 
  filter(land == T)




yrs <- seq(1970, 2021) # add 1 year at each end




# subset files based on yrs
ff_sub <- 
  ff |>
  map(\(f) str_subset(f, str_flatten(yrs, "|")))

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
  
  print(str_glue("importing {tile_id}"))
  
  # load all data within the tile
  s_tile <-
    ff_sub |> 
    map(\(f) rt_tile_load(start_x, start_y, count_x, count_y, f))
    
  
  # run functions
  output_vars |> 
    walk(\(ov) {
      
      print(str_glue("   processing {ov}"))
      
      fun_list[[ov]](s_tile) |> 
        rt_write_nc(str_glue("{dir_tiles}/{ov}/tile_{tile_id}.nc"), daily = F)
      
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
  
  print(ov)
  
  # loop yrs
  yrs |> 
    tail(-1) |> 
    head(-1) |> 
    
    walk(\(yr) { # future?
      
      print(yr)
      
      mos <- 
        rt_tile_mosaic(df_tiles, 
                       str_glue("{dir_tiles}/{ov}"), 
                       st_dimensions(s_proxy), 
                       as_date(str_glue("{yr}-01-01"))) |> 
        adrop()
      
      mos <- 
        mos |> 
        setNames(ov) |> 
        mutate(!!sym(ov) := units::set_units(!!sym(ov), !!ov_un))
      
      
      f_res <- str_glue("{dir_res}/era5_{str_replace_all(ov, '_', '-')}_yr_{yr}-01-01.nc")
      
      rt_write_nc(mos,
                  f_res,
                  daily = F,
                  gatt_name = "source code",
                  gatt_val = "https://github.com/Probable-Futures/map-data-processing")
      
      system(str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/era5/annual_aggregates/{ov}/"),
             ignore.stdout = T, ignore.stderr = T)
      
      
    })
  
})


# clean up

fs::dir_delete(dir_rawdata)
fs::dir_delete(dir_tiles)
fs::dir_delete(dir_res)


