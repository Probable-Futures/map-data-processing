
derived_vars <- 
  
  c("days-gte-32C-tasmax",
    "days-gte-35C-tasmax",
    "days-gte-38C-tasmax",
    "ten-hottest-days-tasmax",
    "mean-tasmax",
    "days-lt-0C-tasmax", # 6
    
    "days-gte-20C-tasmin",
    "days-gte-25C-tasmin",
    "mean-tasmin",
    "days-lt-0C-tasmin", # 10
    
    "days-gte-26C-wetbulb",
    "days-gte-28C-wetbulb",
    "days-gte-30C-wetbulb",
    "days-gte-32C-wetbulb",
    "ten-hottest-days-wetbulb", # 15
    
    "mean-tasmean",
    
    "total-precip",
    "ninety-wettest-days",
    # "100yr-storm-precip",
    # "100yr-storm-freq",
    "days-gte-1mm-precip-lt-0C-tasmean",
    "days-gte-b90perc-tasmax-lt-b10perc-precip", #20
    
    "mean-spei",
    "prop-months-lte-neg0.8-spei",
    "prop-months-lte-neg1.6-spei",
    "days-gte-b95perc-fwi",
    
    # "prop-days-gte-b85perc-3dayrunmeantasmin"
    
    "prop-days-gte-b90perc-3dayrunmeantasmax",
    "mean-cont-days-gte-b90perc-3dayrunmeantasmax",
    "mean-tas-gte-b90perc-3dayrunmeantasmax"
    
  )[27] # choose derived variable(s) to assemble



# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

source("scripts/functions.R")

dir_ensembled <- "/mnt/bucket_mine/results/global_heat_pf/02_ensembled"
dir_mosaicked <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/"

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# # domain centroids
# tb_dom <- 
#   tibble(
#     dom = c("NAM", "CAM", "SAM", "EUR", "AFR", "WAS", "CAS", "SEA", "EAS", "AUS"),
#     centr_lon = c(263.0-360, 287.29-360, 299.70-360, 9.75, 17.60, 67.18, 74.64, 118.04, 116.57, 147.63),
#     centr_lat = c(47.28, 10.20, -21.11, 49.68, -1.32, 16.93, 47.82, 6.5, 34.40, -24.26)
#   )

# load table of variables
tb_vars <-
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()


# subset vars based on input var
# tb <- 
#   tb_vars %>% 
#   filter(var_input == {{var_input}})


# ***************** HEAT WAVES ADDITION!

# tb_vars %>% 
#   rbind(c("minimum_temperature",
#           "prop-days-gte-b85perc-3dayrunmeantasmin",
#           "nothing",
#           "likelihood-heat-wave",
#           "n"
#   )) -> tb_vars

tb_vars %>% 
  rbind(c("maximum_temperature",
          "prop-days-gte-b90perc-3dayrunmeantasmax",
          "nothing",
          "likelihood-heat-wave",
          "n")) %>% 
  rbind(c("maximum_temperature",
          "mean-cont-days-gte-b90perc-3dayrunmeantasmax",
          "nothing",
          "duration-heat-wave",
          "n")) %>% 
  rbind(c("maximum_temperature",
          "mean-tas-gte-b90perc-3dayrunmeantasmax",
          "nothing",
          "intensity-heat-wave",
          "n")) -> tb_vars


# *****************



# PRE-PROCESS -----------------------------------------------------------------
# setup grid and weights


# spei and fwi have gaps (tiles not processed; ocean)
if(any(str_detect(derived_vars, "spei|fwi"))){
  template_var <- "total-precip"
} else {
  template_var <- derived_vars[1]
}



# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(doms), function(dom){
    
    # load map
    s <- 
      dir_ensembled %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset(template_var) %>%
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
        filter(lon >= 180)
      
      s2 <- 
        st_set_dimensions(s2, 
                          which = "lon", 
                          values = st_get_dimension_values(s2, 
                                                           "lon", 
                                                           center = F)-360) %>% 
        st_set_crs(4326)
      
      # keep AUS split
      s <- list(AUS1 = s1, 
                AUS2 = s2)
      
    }
    
    return(s)
    
  })

# append AUS parts separately
l_s_valid <- 
  append(l_s_valid[1:9], l_s_valid[[10]])

# assign 1 to non NA grid cells
l_s_valid <- 
  l_s_valid %>% 
  map(function(s){
    
    s %>%
      setNames("v") %>%
      mutate(v = ifelse(is.na(v), NA, 1))
    
  })

doms_2aus <- c(doms[1:9], "AUS1", "AUS2")


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
  
  future_map(doms_2aus, function(dom){
    
    if(dom != "AUS2"){
      
      s_valid <-
        l_s_valid %>%
        pluck(dom)
      
      pt_valid <-
        s_valid %>%
        st_as_sf(as_points = T)
      
      domain_bound <- 
        s_valid %>% 
        st_as_sf(as.points = F, merge = T) %>%
        st_cast("LINESTRING") %>% 
        suppressWarnings()
      
      s_dist <-
        pt_valid %>%
        mutate(dist = st_distance(., domain_bound),
               dist = set_units(dist, NULL),
               dist = scales::rescale(dist, to = c(1e-10, 1))
        ) %>%
        select(dist) %>%
        st_rasterize(s_valid)
      
    } else {
      
      s_dist <- 
        l_s_valid %>%
        pluck(dom) %>% 
        setNames("dist")
      
    }
    
    s_dist %>% 
      st_warp(global)
    
  }) %>%
  set_names(doms_2aus)




# SUMMED DISTANCES 
# denominator; only in overlapping areas

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
      
      # 1 if no intersection; domain's distance / summed distance otherwise
      mutate(weights = ifelse(is.na(sum_intersect) & !is.na(dist), 1, dist/sum_intersect)) %>%
      select(weights)
    
  })




# LAND MASK

land <- 
  "/mnt/bucket_cmip5/Probable_futures/irunde_scripts/create_a_dataset/04_rcm_buffered_ocean_mask.nc" %>% 
  read_ncdf() %>%
  st_warp(global) %>% 
  setNames("a")


# BARREN MASK

if(any(str_detect(derived_vars, "spei|fwi"))){
  
  barren <- 
    "/mnt/bucket_cmip5/Probable_futures/land_module/maps/mask_layers/modis_barren_mask_ge90perc_regridto22kmwmean.tif" %>% 
    read_stars() %>% 
    st_warp(global) %>% 
    setNames("barren")
}



# MOSAIC

# loop through variables

walk(derived_vars, function(derived_var){
  
  print(str_glue(" "))
  print(str_glue("Mosaicking {derived_var}"))
  
  final_name <- 
    tb_vars %>% 
    filter(var_derived == derived_var) %>% 
    pull(final_name)
  
  l_s <- 
    map(doms %>% set_names(), function(dom){
      
      print(dom)
      
      # load ensembled map 
      s <- 
        dir_ensembled %>%
        list.files(full.names = T) %>%
        str_subset(dom) %>%
        str_subset(str_glue("{derived_var}_ensemble")) %>%
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
          filter(lon >= 180)
        
        s2 <- 
          st_set_dimensions(s2, 
                            which = "lon", 
                            values = st_get_dimension_values(s2, 
                                                             "lon", 
                                                             center = F)-360) %>% 
          st_set_crs(4326)
        
        s <- list(AUS1 = s1, 
                  AUS2 = s2)
        
      }
      
      return(s)
    })
  
  l_s <- append(l_s[1:9], l_s[[10]])
  
  
  # if(str_detect(final_name, "freq")){
  #   wl <- wl[-1]
  # }
  
  
  l_mos_wl <- 
    
    # loop through warming levels
    imap(wls, function(wl, wl_pos){
      
      print(str_glue("    {wl}"))
      
      
      l_s_wl <-
        l_s %>% 
        map(slice, wl, wl_pos) %>% 
        map(st_warp, global)
      
      l_s_weighted <- 
        
        map2(l_s_wl, l_s_weights, function(s, w){
          
          orig_names <- names(s)
          
          map(orig_names, function(v_){
            
            c(s %>% select(all_of(v_)) %>% setNames("v"),
              w) %>% 
              
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
        setNames(wl)
      
      return(mos)
      
    })
  
  
  # if(str_detect(final_name, "change") | final_name == "intensity-heat-wave"){            # ***** "intensity-heat-wave"
  if(str_detect(final_name, "change")){

    print(str_glue("Calculating differences"))

  #   
  #   if(str_detect(final_name, "freq")){
  #     
  #     l_mos_wl <- 
  #       l_mos_wl %>% 
  #       map(function(s){
  #         
  #         (1-s) / 0.01
  #         
  #       })
  #     
  #   } else {
  #     
      l_mos_wl <-
        l_mos_wl[2:6] %>%
        map(function(s){

          s - l_mos_wl[[1]]

        }) %>%
        {append(list(l_mos_wl[[1]]), .)}
  #     
  #   }
  }
  
  
  # round
  l_mos_wl <-
    l_mos_wl %>%
    map(function(s){

      wl <- names(s)
      
      if(final_name == "change-water-balance"){
        
        s %>%
          rename(a = 1) %>%
          mutate(a = round(a, 1)) %>%
          setNames(wl)
        
      # } else if(final_name == "intensity-heat-wave") {                            # ************** intensity !!!!
      #   
      #   s %>%     
      #     rename(a = 1) %>%
      #     mutate(a = round(a, 2)) %>%
      #     setNames(wl)
        
      } else if(str_detect(final_name, "drought") | str_detect(final_name, "likelihood-heat-wave")){
        
        s %>%
          rename(a = 1) %>%
          mutate(a = a * 100,
                 a = as.integer(round(a))) %>%
          setNames(wl)
        
      } else {
        
        s %>%
          rename(a = 1) %>%
          mutate(a = as.integer(round(a))) %>%
          setNames(wl)
        
      }

    })
  
  s <- 
    l_mos_wl %>% 
    do.call(c, .) %>% 
    merge(name = "wl") %>% 
    split("stats") %>% 
    st_set_dimensions(3, values = as.numeric(wls))
  
  
  if(str_detect(derived_var, "spei|fwi")){

    print(str_glue("Removing deserts"))

    s[barren == 1] <- -88888

  }
  
  s[is.na(land)] <- NA_integer_
  
  
  if(str_detect(final_name, "drought")){
    print("removing metrics") # **********
    s <- 
      s %>% 
      select(mean, perc50)
    
  }
  
  
  # save as nc
  print(str_glue("  Saving"))
  
  file_name <- str_glue("{dir_mosaicked}/heat/v3/{final_name}_v03.nc") # *******************
  fn_write_nc(s, file_name, "wl")
  
})



# s2_orig <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/land/" %>% read_ncdf()
# s2_round_b <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/average-daytime-temperature_v02_1.nc" %>% read_ncdf()
# s2_round_o <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/average-daytime-temperature_v02_2.nc" %>% read_ncdf()
# s3 <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/land/likelihood-yearplus-extreme-drought_v03.nc" %>% read_ncdf()
# 
# a <- s2_orig - s3
# a %>% slice(wl, 6) %>% select(1) %>% plot(breaks = "equal")
# a %>% pull() %>% .[!is.na(.)] %>% .[. > 1]
# 
# mean(c(5/12, 2/12, 7/12, 9/12, 3/12, 8/12))
# mean(c((5+2+7)/(12*3), (9+3+8)/(12*3)))




