

land <- function() {
  
  land_p <- 
    "/mnt/bucket_mine/misc_data/physical/ne_50m_land/ne_50m_land.shp" |> 
    st_read(quiet = T)
  
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
  
  land_r[is.na(s_proxy)] <- 0

  land_r[land_r == 0] <- NA
  
  st_dimensions(land_r) <- st_dimensions(s_proxy)[1:2]
  
  return(land_r)
  
}



prepare_tile <- function(start_x, start_y, count_x, count_y){
  
  # load all data within the tile
  s_tile <-
    ff_sub |> 
    map(\(f) rt_tile_load(start_x, start_y, count_x, count_y, f, parallel = T))
  
  # convert units
  s_tile <- 
    s_tile |>
    map(\(s){
      
      un <- 
        s |> 
        pull() |> 
        units::deparse_unit()
      
      v <- names(s)
      
      if(un == "kg m-2 s-1") {
        s <- 
          s |> 
          mutate(!!sym(v) := units::set_units(!!sym(v), kg/m^2/d))
      } else if(un == "m d-1") {
        s <- 
          s |> 
          mutate(!!sym(v) := units::set_units(!!sym(v), mm/d))
      }
      
      return(s)
      
    })
  
  return(s_tile)
  
}



mosaic <- function(output_var, output_var_unit, years, prefix, dir_dest_cloud) {
  
  message(str_glue("    mosaicking var {which(output_var == output_vars)} / {length(output_vars)} ({output_var})"))
  
  ff_tiles <- 
    str_glue("{dir_tiles}/{output_var}") |>
    fs::dir_ls() |> 
    str_subset("tile_[:digit:]{3}.nc")
  
  full_time_vector <- 
    ff_tiles |> 
    first() |> 
    read_ncdf(proxy = F) |> 
    suppressMessages() |> 
    st_get_dimension_values("time")
  
  
  # loop yrs
  years |> 
    walk(\(yr) { # future?
      
      message(str_glue("      {yr}"))
      
      # mos <- 
      #   rt_tile_mosaic(df_tiles = df_tiles, 
      #                  dir_tiles = str_glue("{dir_tiles}/{output_var}"), 
      #                  spatial_dims = st_dimensions(s_proxy), 
      #                  time_dim = as_date(str_glue("{yr}-01-01"))) |> 
      #   adrop()
      
      mos <- 
        rt_tile_mosaic_gdal(ff_tiles,
                            dir_res,
                            spatial_dims = st_dimensions(s_proxy),
                            time_dim = as_date(str_glue("{yr}-01-01")),
                            time_full = full_time_vector)
      
      mos <- 
        mos |> 
        setNames(output_var) |> 
        mutate(!!sym(output_var) := units::set_units(!!sym(output_var), !!output_var_unit))
      
      
      f_res <- str_glue("{dir_res}/{prefix}_{str_replace_all(output_var, '_', '-')}_yr_{yr}-01-01.nc")
      
      rt_write_nc(mos,
                  f_res,
                  gatt_name = "source code",
                  gatt_val = "https://github.com/Probable-Futures/map-data-processing")
      
      system(str_glue("gcloud storage mv {f_res} {dir_dest_cloud}"),
             ignore.stdout = T, ignore.stderr = T)
      
    })
  
}


fix_coords <- function(s){
  
  dim_1 <- st_get_dimension_values(s, 1)
  dim_2 <- st_get_dimension_values(s, 2)
  
  s <- 
    s |> 
    st_set_dimensions(1, values = seq(round(first(dim_1),1), round(last(dim_1),1)-0.2, length.out = length(dim_1))) |> 
    st_set_dimensions(2, values = seq(round(first(dim_2),1), round(last(dim_2),1)-0.2, length.out = length(dim_2))) |> 
    st_set_crs(4326) |> 
    suppressWarnings()
  
  return(s)
}


calc_stats_wl <- function(s, un){
  
  s |> 
    st_apply(c(1,2), \(x){
      
      if (any(is.na(x))) {
        
        r <- rep(NA, 8)
        
      } else {
        
        r <- 
          c(mean(x),
            quantile(x, c(0, 0.05, 0.25, 0.5, 0.75, 0.95, 1))
          )
        
      }
      
      r |> 
        set_names(c("mean", 
                    str_glue("perc_{c(0, 5, 25, 50, 75, 95, 100)}")))
      
    },
    FUTURE = T,
    .fname = "stats") |> 
    setNames("a") |> 
    mutate(a = units::set_units(a, !!sym(un))) |> 
    split("stats")
}
