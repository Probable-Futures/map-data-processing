
# system("sudo mount -o discard,defaults /dev/sdc /mnt/pers_disk_300/")



library(tidyverse)
library(stars)
library(furrr)
library(PCICt)
box::use(../functions/general_tools[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
options(future.globals.maxSize = 1000 * 1024^2)

plan(multicore)

source("scripts_v4/0_output_vars.R")

# doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")
wls <- sprintf("%.1f", seq(0.5, 3.0, 0.5))

dir_gs_cordex <- "gs://clim_data_reg_useast1/cordex"

dir_rawdata <- "/mnt/pers_disk_300/rawdata"
dir_res <- "/mnt/pers_disk_300/results"

fs::dir_create(dir_rawdata)
fs::dir_create(dir_res)



# TEMPLATE DOMAIN MAPS

tb_files <- 
  str_glue("data_tables/df-files_precipitation.csv") |> 
  read_csv() |> 
  mutate(domain = str_sub(domain, end = 3)) |> 
  filter(str_detect(rcm, "REMO2015"),
         str_detect(gcm, "Had")) |> 
  group_by(domain) |> 
  slice(1) |>
  ungroup()


l_s_valid <- 
  
  tb_files |> 
  future_pmap(\(file, domain, ...){
    
    # load map
    f <- 
      file |> 
      rt_gs_download_files(tempdir(), quiet = T)
    
    s <- 
      f |> 
      read_ncdf(ncsub = cbind(start = c(1,1,1),
                              count = c(NA,NA,1))) |>
      suppressMessages() |> 
      adrop()
    
    fs::file_delete(f)
    
    # fix coordinates
    s <- 
      s |> 
      fix_coords()
    
    
    
    # fix domains trespassing the 360 meridian  
    if(domain == "EAS"){
      
      s <- 
        s |> 
        filter(lon < 180)
      
    } else if(domain == "AUS"){
      
      s1 <- 
        s |> 
        filter(lon < 180)
      
      s2 <- 
        s |> 
        filter(lon >= 180)
      
      s2 <- 
        st_set_dimensions(s2, 
                          which = "lon", 
                          values = st_get_dimension_values(s2, 
                                                           "lon", 
                                                           center = F)-360) |> 
        st_set_crs(4326)
      
      # keep AUS split
      s <- list(AUS1 = s1, 
                AUS2 = s2)
      
    }
    
    return(s)
    
  }) |> 
  set_names(tb_files$domain)
    

# append AUS parts separately
l_s_valid <- 
  c(l_s_valid[which(names(l_s_valid) != "AUS")], l_s_valid[["AUS"]])


# assign 1 to non NA grid cells
l_s_valid <- 
  l_s_valid |> 
  map(function(s){
    
    s |>
      setNames("v") |>
      mutate(v = if_else(is.na(v), NA, 1))
    
  })



# GLOBAL GRID

global <- 
  # c(
  #   st_point(c(-179.9, -89.9)),
  #   st_point(c(179.9, 89.9))
  # ) |> 
  c(
    st_point(c(-180, -90)),
    st_point(c(180, 90))
  ) |> 
  st_bbox() |> 
  st_set_crs(4326) |> 
  st_as_stars(dx = 0.2, values = NA) |>  
  st_set_dimensions(c(1,2), names = c("lon", "lat"))



# INVERSE DISTANCES

l_s_dist <-
  
  future_map(names(l_s_valid) |> set_names(), function(dom){
    
    if(dom != "AUS2"){
      
      s_valid <-
        l_s_valid |>
        pluck(dom)
      
      pt_valid <-
        s_valid |>
        st_as_sf(as_points = T)
      
      domain_bound <- 
        s_valid |> 
        st_as_sf(as.points = F, merge = T) |>
        st_cast("LINESTRING") |> 
        suppressWarnings()
      
      s_dist <-
        pt_valid |>
        mutate(dist = st_distance(pt_valid, domain_bound),
               dist = units::set_units(dist, NULL),
               dist = scales::rescale(dist, to = c(1e-10, 1))
        ) |>
        select(dist) |>
        st_rasterize(s_valid)
      
    } else {
      
      s_dist <- 
        l_s_valid |>
        pluck(dom) |> 
        setNames("dist")
      
    }
    
    s_dist |> 
      st_warp(global)
    
  })



# SUMMED DISTANCES 
# denominator; only in overlapping areas

s_intersections <- 
  
  do.call(c, c(l_s_dist, along = "dom")) |> 
  st_apply(c(1,2), function(foo){
    
    bar <- ifelse(is.na(foo), 0, 1)
    
    if(sum(bar) > 1){
      sum(foo, na.rm = T)
    } else {
      NA
    }
    
  }, 
  FUTURE = T,
  .fname = "sum_intersect")



# WEIGHTS PER DOMAIN

l_s_weights <- 
  map(l_s_dist, function(s){
    
    c(s, s_intersections) |> 
      
      # 1 if no intersection; domain's distance / summed distance otherwise
      mutate(weights = ifelse(is.na(sum_intersect) & !is.na(dist), 1, dist/sum_intersect)) |>
      select(weights)
    
  })




# MOSAIC ----------------------------------------------------------------------

# loop through variables

walk(output_vars[24], function(ov){
  
  # ov = output_vars[1]
  
  print(str_glue(" "))
  print(str_glue("Mosaicking {ov}"))
  
  
  l_s <- 
    map(tb_files$domain |> set_names(), function(dom){
      
      message(dom)
      
      # load ensembled map 
      ff <- 
        "gs://clim_data_reg_useast1/cordex/warming_level_aggregates/{ov}/{dom}/CORDEX_CORE_ensemble/" |> 
        str_glue() |> 
        rt_gs_list_files() |> 
        rt_gs_download_files(dir_rawdata, quiet = T)
      
      ss <- 
        ff |> 
        map(read_ncdf) |>
        suppressMessages()
      
      fs::file_delete(ff)
      
      s <- 
        do.call(c, c(ss, along = "wl"))
      
      # fix domains trespassing the 360 meridian 
      if(dom == "EAS"){
        
        s <- 
          s |> 
          filter(lon < 180)
          
        
      } else if(dom == "AUS"){
        
        s1 <- 
          s |> 
          filter(lon < 180)
        
        s2 <- 
          s |> 
          filter(lon >= 180)
        
        s2 <- 
          st_set_dimensions(s2, 
                            which = "lon", 
                            values = st_get_dimension_values(s2, 
                                                             "lon", 
                                                             center = F)-360) |> 
          st_set_crs(4326)
        
        s <- list(AUS1 = s1, 
                  AUS2 = s2)
        
      }
      
      return(s)
      
    })
  
  l_s <- 
    c(l_s[which(names(l_s) != "AUS")], l_s[["AUS"]])
  
  
  
  # loop through warming levels
  iwalk(wls, function(wl, wl_pos){
    
    message(str_glue("    {wl}"))
    
    
    l_s_wl <-
      l_s |> 
      map(slice, wl, wl_pos) |> 
      map(st_warp, global)
    
    # APPLY WEIGHTS
    l_s_weighted <- 
      
      map2(l_s_wl, l_s_weights, function(s, w){
        
        s*w
        
      }) 
    
    
    # MOSAIC
    un <- 
      l_s_weighted[[1]] |> 
      select(1) |> 
      pull() |> 
      units::deparse_unit()
    
    mos <- 
      l_s_weighted |>
      map(merge, name = "stats") |>
      imap(~setNames(.x, .y)) |>
      unname() #|> 
    
    mos <- 
      do.call(c, c(mos, along = "dom"))
    
    mos <- 
      mos |> 
      st_apply(c(1,2,3), function(foo){
        
        if(all(is.na(foo))){
          NA
        } else {
          sum(foo, na.rm = T)
        }
        
      },
      FUTURE = F)
    
    mos <- 
      mos |> 
      setNames("v") |> 
      mutate(v = units::set_units(v, !!sym(un))) |> 
      split("stats")
    
    
    
    f_res <- 
      str_glue("{dir_res}/CORDEX-CORE-ens_GLOBAL_{str_replace_all(ov, '_','-')}_wl-{wl}_stats.nc")
    
    rt_write_nc(mos,
                f_res,
                gatt_name = "source_code",
                gatt_val = "https://github.com/Probable-Futures/map-data-processing")
    
    
    str_glue("gcloud storage mv {f_res} {dir_gs_cordex}/warming_level_aggregates/{ov}/GLOBAL/CORDEX_CORE_ensemble/") |> 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
  })
  
  
  # if(str_detect(final_name, "change")){
  #   
  #   print(str_glue("Calculating differences"))
  #   
  #   #   
  #   #   if(str_detect(final_name, "freq")){
  #   #     
  #   #     l_mos_wl <- 
  #   #       l_mos_wl |> 
  #   #       map(function(s){
  #   #         
  #   #         (1-s) / 0.01
  #   #         
  #   #       })
  #   #     
  #   #   } else {
  #   #     
  #   l_mos_wl <-
  #     l_mos_wl[2:6] |>
  #     map(function(s){
  #       
  #       s - l_mos_wl[[1]]
  #       
  #     }) |>
  #     {append(list(l_mos_wl[[1]]), .)}
  #   #     
  #   #   }
  # }
  # 
  # 
  # # round
  # l_mos_wl <-
  #   l_mos_wl |>
  #   map(function(s){
  #     
  #     wl <- names(s)
  #     
  #     if(final_name == "change-water-balance"){
  #       
  #       s |>
  #         rename(a = 1) |>
  #         mutate(a = round(a, 1)) |>
  #         setNames(wl)
  #       
  #       # } else if(final_name == "intensity-heat-wave") {                            # ************** intensity !!!!
  #       #   
  #       #   s |>     
  #       #     rename(a = 1) |>
  #       #     mutate(a = round(a, 2)) |>
  #       #     setNames(wl)
  #       
  #     } else if(str_detect(final_name, "drought") | str_detect(final_name, "heatwave")){
  #       
  #       s |>
  #         rename(a = 1) |>
  #         mutate(a = a * 100,
  #                a = as.integer(round(a))) |>
  #         setNames(wl)
  #       
  #     } else {
  #       
  #       s |>
  #         rename(a = 1) |>
  #         mutate(a = as.integer(round(a))) |>
  #         setNames(wl)
  #       
  #     }
  #     
  #   })
  # 
  # s <- 
  #   l_mos_wl |> 
  #   do.call(c, .) |> 
  #   merge(name = "wl") |> 
  #   split("stats") |> 
  #   st_set_dimensions(3, values = as.numeric(wls))
  # 
  # 
  # if(str_detect(derived_var, "spei|fwi")){
  #   
  #   print(str_glue("Removing deserts"))
  #   
  #   s[barren == 1] <- -88888
  #   
  # }
  # 
  # s[is.na(land)] <- NA_integer_
  # 
  # 
  # if(str_detect(final_name, "drought")){
  #   print("removing metrics") # **********
  #   s <- 
  #     s |> 
  #     select(mean, perc50)
  #   
  # }
  # 
  # 
  # # save as nc
  # print(str_glue("  Saving"))
  # 
  # file_name <- str_glue("{dir_mosaicked}/{vol}/v3/{final_name}_v03.nc") # *******************
  # fn_write_nc(s, file_name, "wl")
  
  
})


