
var_input <- 
  
  c(# heat module
    "maximum_temperature",
    "minimum_temperature",
    "maximum_wetbulb_temperature",
    "average_temperature",
    
    # water module
    "precipitation",
    "precipitation+average_temperature",
    "precipitation+maximum_temperature",
    
    # land module
    "spei",
    "fwi")[8] # choose input variable to process


# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)

source("scripts/functions.R")

dir_ensembled <- "/mnt/bucket_mine/results/global_heat_pf/02_ensembled"
dir_mosaicked <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/"

wl <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# domain centroids
tb_dom <- 
  tibble(
    dom = c("NAM", "CAM", "SAM", "EUR", "AFR", "WAS", "CAS", "SEA", "EAS", "AUS"),
    centr_lon = c(263.0-360, 287.29-360, 299.70-360, 9.75, 17.60, 67.18, 74.64, 118.04, 116.57, 147.63),
    centr_lat = c(47.28, 10.20, -21.11, 49.68, -1.32, 16.93, 47.82, 6.5, 34.40, -24.26)
  )

# load table of variables
tb_vars <-
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()


# subset vars based on input var
tb <- 
  tb_vars %>% 
  filter(var_input == {{var_input}})




# PRE-PROCESS -----------------------------------------------------------------
# setup grid and weights


# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(tb_dom$dom), function(dom){
    
    # load map
    s <- 
      dir_ensembled %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset(tb$var_derived[1]) %>% # any var works
      # str_subset("total-precip") %>% 
      read_ncdf(ncsub = cbind(start = c(1, 1, 1), 
                              count = c(NA,NA,1))) %>% # only 1 timestep
      suppressMessages() %>% 
      select(1) %>% 
      adrop()
    
    # fix domains trespassing the 360 meridian  
    if(dom == "EAS"){
      
      s <- 
        s %>% 
        filter(lon < 180)
      
    } else if(dom == "AUS"){
      
      s1 <- 
        s %>% 
        filter(lon < 180)
      
      s2 <- 
        s %>% 
        filter(lon > 180)
      
      s2 <- 
        st_set_dimensions(s2, 
                          which = "lon", 
                          values = st_get_dimension_values(s2, 
                                                           "lon", 
                                                           center = F)-360) %>% 
        st_set_crs(4326)
      
      s <- 
        list(s1, s2) %>% 
        map(as, "SpatRaster") %>% 
        do.call(terra::merge, .) %>% #plot()
        st_as_stars(proxy = F)
      
      s <- 
        s %>%
        st_set_dimensions(c(1,2), names = c("lon", "lat"))
      
      rm(s1, s2)
      
    }
    
    return(s)
    
  }) %>% 
  
  map(function(s){
    
    s %>% 
      setNames("v") %>% 
      mutate(v = ifelse(is.na(v), NA, 1))
    
  })





# GLOBAL TEMPLATE

global <- 
  c(
    st_point(c(-179.9, -89.9)),
    st_point(c(179.9, 89.9))
  ) %>% 
  st_bbox() %>% 
  st_set_crs(4326) %>% 
  st_as_stars(dx = 0.2, values = NA) %>%  
  st_set_dimensions(c(1,2), names = c("lon", "lat"))





# INVERSE DISTANCES

l_s_dist <-

  pmap(tb_dom, function(dom, centr_lon, centr_lat){

    s_valid <-
      l_s_valid %>%
      pluck(dom)

    pt_valid <-
      s_valid %>%
      st_as_sf(as_points = T)

    centr <-
      st_point(c(centr_lon, centr_lat)) %>%
      st_sfc() %>%
      st_set_crs(4326)

    s_dist <-
      pt_valid %>%
      mutate(dist = st_distance(., centr),
             dist = set_units(dist, NULL),
             dist = max(dist)-dist,
             # dist = 1/dist,
             dist = scales::rescale(dist),
             # dist = dist^2
             dist = dist^(1/2)
      ) %>%
      select(dist) %>%
      st_rasterize(s_valid) %>%
      st_warp(global)

    return(s_dist)

  }) %>%
  set_names(tb_dom$dom)

# l_s_dist <-
#   
#   pmap(tb_dom, function(dom, centr_lon, centr_lat){
#     
#     s_valid <-
#       l_s_valid %>%
#       pluck(dom)
#     
#     pt_valid <-
#       s_valid %>%
#       st_as_sf(as_points = T)
#     
#     centr <-
#       st_point(c(centr_lon, centr_lat)) %>%
#       st_sfc() %>%
#       st_set_crs(4326)
#     
#     s_dist <-
#       pt_valid %>%
#       mutate(dist = st_distance(., centr),
#              dist = set_units(dist, NULL),
#              dist = max(dist)-dist) %>% 
#       select(dist) %>% 
#       st_rasterize(s_valid)
#     
#     if(dom != "AUS"){
#       
#       min_dim <- which.min(dim(s_dist))
#       
#       if(min_dim == 2){
#         
#         min_dist <- 
#           s_dist %>% 
#           slice(x, round(dim(s_dist)[1]/2)) %>% 
#           pull() %>% 
#           min(na.rm = T)
#         
#       } else {
#         
#         min_dist <- 
#           s_dist %>% 
#           slice(x, round(dim(s_dist)[2]/2)) %>% 
#           pull() %>% 
#           min(na.rm = T)
#         
#       }
#       
#       s_dist <- 
#         s_dist %>% 
#         mutate(dist = ifelse(dist < min_dist, min_dist, dist),
#                dist = scales::rescale(dist),
#                dist = dist^2) %>% 
#         st_warp(global)
#       
#       
#       
#     } else {
#       
#       s_dist <- 
#         s_dist %>% 
#         mutate(dist = scales::rescale(dist),
#                dist = dist^2) %>% 
#         st_warp(global)
#       
#     }
#     
#     
#     return(s_dist)
#     
#   }) %>%
#   set_names(tb_dom$dom)




# SUMMED DISTANCES 
# only in overlapping areas

s_intersections <- 
  
  l_s_dist %>% 
  do.call(c, .) %>% 
  merge() %>% 
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
    
    c(s, s_intersections) %>% 
      
      mutate(sum_intersect = ifelse(sum_intersect == 0, 1e-10, sum_intersect)) %>% 
      
      # 1 if no intersection; domain's distance / summed distance otherwise
      mutate(weights = ifelse(is.na(sum_intersect) & !is.na(dist), 1, dist/sum_intersect)) %>%
      select(weights) #%>% 
      
      # st_warp(global)
    
  })




# LAND MASK

# rast_reference_0.05 <- 
#   st_as_stars(st_bbox(global), dx = 0.05, dy = 0.05, values = -9999) 
# 
# land <- 
#   "/mnt/bucket_mine/misc_data/ne_50m_land/ne_50m_land.shp" %>% 
#   st_read() %>% 
#   mutate(a = 1) %>%
#   select(a) %>% 
#   st_rasterize(rast_reference_0.05)
# 
# land <- 
#   land %>%
#   st_warp(global, use_gdal = T, method = "max") %>%
#   suppressWarnings() %>% 
#   setNames("a") %>%
#   mutate(a = ifelse(a == -9999, NA, 1))
# 
# land %>% 
#   as("SpatRaster") %>% 
#   terra::buffer(1000) -> foo

land <- 
  "/mnt/bucket_cmip5/Probable_futures/irunde_scripts/create_a_dataset/04_rcm_buffered_ocean_mask.nc" %>% 
  read_ncdf() %>%
  st_warp(global) %>% 
  setNames("a")


# DESERT MAKS
if(var_input %in% c("spei", "fwi")){
  
  precip <- 
    read_stars("/mnt/bucket_mine/results/global_heat_pf/global_mean-annual-precip_wl0p5.tif")
  
  desert <- 
    precip %>%
    split("band") %>% 
    mutate(v = ifelse(mean <= 90, NA, 1)) %>% 
    select(v)
  
}



# MOSAIC

# loop through variables


pwalk(tb[2,], function(var_derived, final_name, ...){
  
  print(str_glue(" "))
  # print(str_glue("Mosaicking {derived_vars_}"))
  
  l_s <- 
    map(tb_dom$dom %>% set_names(), function(dom){
      
      # load ensembled map 
      s <- 
        dir_ensembled %>%
        list.files(full.names = T) %>%
        str_subset(dom) %>%
        str_subset(str_glue("{var_derived}_ensemble")) %>%
        read_ncdf %>%
        suppressMessages()
      
      # # fix domains trespassing the 360 meridian 
      if(dom == "EAS"){
        
        s <- 
          s %>% 
          filter(lon < 180)
        
      } else if(dom == "AUS"){
        
        s1 <- 
          s %>% 
          filter(lon < 180)
        
        s2 <- 
          s %>% 
          filter(lon > 180)
        
        s2 <- 
          st_set_dimensions(s2, 
                            which = "lon", 
                            values = st_get_dimension_values(s2, 
                                                             "lon", 
                                                             center = F)-360) %>% 
          st_set_crs(4326)
        
        s <- 
          map(names(s), function(v){
            
            list(
              s1 %>% select(v),
              s2 %>% select(v)
            ) %>% 
              map(as, "SpatRaster") %>% 
              do.call(terra::merge, .) %>%
              st_as_stars(proxy = F) %>% 
              setNames(v) %>% 
              st_set_dimensions(c(1,2,3), names = c("lon", "lat", "warming_levels")) %>% 
              st_set_dimensions(3, values = st_get_dimension_values(s, "warming_levels"))
            
          }) %>% 
          do.call(c, .)
        
      }
      
      
      # s_valid <- 
      #   l_s_valid %>% 
      #   pluck(dom)
      # 
      # s <- 
      #   s %>% 
      #   st_warp(s_valid)
      # 
      # s[is.na(s_valid)] <- NA
      
      return(s)
    })
  
  
  if(str_detect(final_name, "freq")){
    wl <- wl[-1]
  }
  
  
  
  l_mos_wl <- 
    
    # loop through warming levels
    imap(wl, function(wl_, wl_pos){
      
      print(str_glue("    {wl_}"))
      
      l_s_weighted <- 
        
        map(tb_dom$dom %>% set_names(), function(dom){
          
          s <- 
            l_s %>% 
            pluck(dom)
          
          r <- 
            s %>% 
            st_warp(global) %>%
            slice(warming_levels, wl_pos) # only 1 layer
          
          orig_names <- names(s)
          
          map(orig_names, function(v_){
            
            c(r %>% select(v_) %>% setNames("v"),
              l_s_weights %>% pluck(dom)) %>% 
              
              # apply weights
              mutate(v = v*weights) %>% 
              select(-weights) %>% 
              setNames(v_)
            
          }) %>% 
            do.call(c, .)
          
        })
      
      mos <- 
        l_s_weighted %>%
        map(merge, name = "stats") %>%
        imap(~setNames(.x, .y)) %>%
        unname() %>% 
        do.call(c, .) %>% 
        merge(name = "doms") %>%
        
        st_apply(c(1,2,3), function(foo){
          
          if(all(is.na(foo))){
            NA
          } else {
            sum(foo, na.rm = T)
          }
          
        },
        FUTURE = F) %>% 
        setNames(wl_)
      
      return(mos)
      
    })
  
  
  
  if(str_detect(final_name, "change")){
    
    print(str_glue("Calculating differences"))
    
    
    if(str_detect(final_name, "freq")){
      
      l_mos_wl <- 
        l_mos_wl %>% 
        map(function(s){
          
          (1-s) / 0.01
          
        })
      
    } else {
      
      l_mos_wl <-
        l_mos_wl[2:6] %>%
        map(function(s){
          
          s - l_mos_wl[[1]]
          
        }) %>%
        {append(list(l_mos_wl[[1]]), .)}
      
    }
  }
  
  
  s <- 
    l_mos_wl %>% 
    do.call(c, .) %>% 
    merge(name = "warming_level") %>% 
    split("stats")
  
  
  if(var_input %in% c("spei", "fwi")){
    
    print(str_glue("Removing deserts"))
    
    s[is.na(desert)] <- NA
    
  }
  
  
  s <- 
    s %>% 
    round(1)
  
  s[is.na(land)] <- NA
  
  
  # save as nc
  print(str_glue("  Saving"))
  
  file_name <- str_glue("{dir_mosaicked}/{final_name}_v01.nc")
  fn_save_nc(file_name, s)
  
  
})





# 
# 
# 
# 
# 
# 
# 
# a <- seq(1,0, by = -0.05)[6]
# b <- seq(1,0, by = -0.05)[2]
# 
# d <- sum(c(a,b)^2)
# 
# a^2/d
# b^2/d
# 
# (a^2/d) + (b^2/d)
# 
# 
# 
# a <- (seq(1,0, by = -0.05)^2)[6]
# b <- (seq(1,0, by = -0.05)^2)[2]
# 
# d <- sum(a,b)
# 
# (a/d) + (b/d)
# 
# 
# 
# wl <- 1
# v <- "mean"
# 
# s %>% 
#   select(v) %>% 
#   slice(warming_level, wl) %>% 
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal() +
#   theme(axis.title = element_blank())
# 
# # NAM
# s %>% 
#   select(v) %>% 
#   slice(warming_level, wl) %>% 
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal(xlim = c(-130, -70), ylim = c(10,55)) +
#   theme(axis.title = element_blank())
# 
# # SAM
# s %>% 
#   select(v) %>% 
#   slice(warming_level, wl) %>%   
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal(xlim = c(-100, -30), ylim = c(-30,25)) +
#   theme(axis.title = element_blank())
# 
# # AFR
# s %>% 
#   select(v) %>% 
#   slice(warming_level, wl) %>% 
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal(xlim = c(-20, 55), ylim = c(0,50)) +
#   theme(axis.title = element_blank())
# 
# # CAS
# mos %>% 
#   slice(stats, 1) %>% 
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal(xlim = c(25, 85), ylim = c(15,70)) +
#   theme(axis.title = element_blank())
# 
# # AUS
# mos %>% 
#   slice(stats, 1) %>% 
#   as_tibble() %>% 
#   rename("v" = 3) %>% 
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
#   coord_equal(xlim = c(90, 170), ylim = c(-50,10)) +
#   theme(axis.title = element_blank())
