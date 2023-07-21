

# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


# load main function to calculate derived vars
# source("scripts/fn_derived.R") 
source("scripts/functions.R") # other functions


dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_tmp <- "/mnt/pers_disk"

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# load thresholds table
thresholds <- 
  str_glue("/mnt/bucket_mine/misc_data/CMIP5_model_temp_thresholds.csv") %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  
  mutate(wl = str_sub(wl, 3)) %>% 
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  # add institutes
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))







# MOSAIC SETUP ----------------------------------------------------------------

dir_ensembled <- "/mnt/bucket_mine/results/global_heat_pf/02_ensembled"


# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(doms), function(dom){
    
    # load map
    s <- 
      dir_ensembled %>%
      list.files(full.names = T) %>%
      str_subset("total-precip") %>%
      str_subset(dom) %>%
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






# NORMALS CALCULATION ---------------------------------------------------------


# VARIABLE LOOP
for(v_long in c("maximum_temperature", "minimum_temperature", "average_temperature", "precipitation")){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {v_long}"))
  
  
  dir_normals <- str_glue("{dir_tmp}/normals")
  
  if(dir.exists(dir_normals)){
    print(str_glue("      (previous dir_normals deleted)"))
    unlink(dir_normals, recursive = T)
  } 
  
  dir.create(dir_normals)
  
  
  # DOMAIN LOOP
  for(dom in doms){
    
    print(str_glue(" "))
    print(str_glue("   Processing {dom}"))
    
    # assemble table of needed files for calculation
    tb_files <-
      fn_data_table(v_long) %>%
      filter(str_detect(file, "MISSING", negate = T))
    
    # extract models
    tb_models <-
      unique(tb_files[, c("gcm", "rcm")]) %>% 
      arrange(rcm, gcm)
    
    # ignore RegCM in these domains  
    if(dom %in% c("SAM", "AUS", "CAS")){
      tb_models <- 
        tb_models %>% 
        filter(str_detect(rcm, "RegCM", negate = T))
    }
    
    
    # MODEL LOOP
    walk(seq_len(nrow(tb_models)), function(i){
      
      gcm_ <- tb_models$gcm[i]
      rcm_ <- tb_models$rcm[i]
      
      print(str_glue(" "))
      print(str_glue("      Processing model {i} / {nrow(tb_models)} [{rcm_}] [{gcm_}]"))
      
      
      ## DOWNLOAD RAW DATA -----------------------------------------------------
      
      print(str_glue("      Downloading raw data"))
      
      dir_raw_data <- str_glue("{dir_tmp}/raw_data")
      
      if(dir.exists(dir_raw_data)){
        print(str_glue("      (previous dir_raw_data deleted)"))
        unlink(dir_raw_data, recursive = T)
      } 
      
      dir.create(dir_raw_data)
      
      # table of files for 1 model
      tb_files_mod <- 
        tb_files %>% 
        filter(gcm == gcm_,
               rcm == rcm_) 
      
      # download files in parallel  
      tb_files_mod %>% 
        future_pwalk(function(loc, file, ...){
          
          loc_ <-
            loc %>% 
            str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
          
          str_glue("{loc_}/{file}") %>%
            {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                    ignore.stdout = T, ignore.stderr = T)}
          
        })
      
      "         Done: {length(list.files(dir_raw_data))} / {nrow(tb_files_mod)} files downloaded" %>% 
        str_glue() %>% 
        print()
      
    
      
      ## CDO PRE-PROCESS -------------------------------------------------------
      
      print(str_glue("      Pre-processing with CDO"))
      
      tb_files_mod %>%
        future_pwalk(function(file, t_i, t_f, ...){

          # extract first and last year included in the file
          time_i <- as_date(t_i)
          time_f <- as_date(t_f)

          f <- str_glue("{dir_raw_data}/{file}")

          # extract variable's (short) name
          v <- str_split(file, "_") %>% .[[1]] %>% .[1]

          # split annually
          system(str_glue("cdo splityear {f} {dir_raw_data}/{v}_yrsplit_"),
                 ignore.stdout = T, ignore.stderr = T)

          # fix time and aggregate monthly
          # (only of the files that came from the file above)

          if(v == "pr"){
            stat <- "sum"
          } else {
            stat <- "mean"
          }

          dir_raw_data %>%
            list.files(full.names = T) %>%
            str_subset(v) %>%
            str_subset("yrsplit") %>%
            str_subset(str_flatten((year(time_i)):(year(time_f)), "|")) %>%

            walk2(seq(year(time_i), year(time_f)), function(f2, yr){

              f_new <- str_glue("{dir_raw_data}/{v}_yrfix_{yr}.nc")

              str_glue("cdo -mon{stat} -settaxis,{year(time_i)}-{month(time_i)}-01,12:00:00,1day {f2} {f_new}") %>% 
                system(ignore.stdout = T, ignore.stderr = T)

              file.remove(f2)

            })

          file.remove(f)

        })

      # check if some files could not be year-split/time-fixed
      bad_remnants <-
        dir_raw_data %>%
        list.files(full.names = T) %>%
        str_subset("yrsplit")

      if(length(bad_remnants) > 0){
        print(str_glue("   ({length(bad_remnants)} bad file(s) - deleted)"))

        bad_remnants %>%
          walk(file.remove)
      }
      
      # concatenate
      
      dir_cat <- str_glue("{dir_tmp}/cat")
      
      if(dir.exists(dir_cat)){
        print(str_glue("      (previous dir_cat deleted)"))
        unlink(dir_cat, recursive = T)
      }
      
      dir.create(dir_cat)
      
      ff <-
        dir_raw_data %>%
        list.files(full.names = T) %>% 
        str_flatten(" ")
      
      str_glue("cdo cat {ff} {dir_cat}/cat.nc") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      
      ## CALCULATE NORMALS --------------------------------------------------
      
      print(str_glue("      Calculating normals"))
      
      
      # loop through warming levels
      walk(wls, function(wl){
        
        # baseline:
        if(wl == "0.5"){
          
          y1 <- 1971
          y2 <- 2000
          
          print(str_glue("         [{wl}] {gcm_}: baseline..."))
          
          # other warming levels:
        } else {
          
          thres_val <-
            thresholds %>%
            filter(str_detect(Model, str_glue("{gcm_}$"))) %>% 
            filter(wl == {{wl}})
          
          y1 <- thres_val$value - 10
          y2 <- thres_val$value + 10
          
          print(str_glue("         [{wl}] {gcm_}: {thres_val$Model}: {thres_val$value}"))
          
        }
        
        str_glue("cdo setday,1 -setyear,1970 -ymonmean -selyear,{y1}/{y2} {dir_cat}/cat.nc {dir_normals}/{dom}_{wl}_normals-1.nc") %>% 
          system(ignore.stdout = T, ignore.stderr = T)
        
        
      })
      
      ff <- 
        dir_normals %>% 
        list.files(full.names = T) %>% 
        str_subset("normals-1")
      
      str_glue("cdo merge {str_flatten(ff, ' ')} {dir_normals}/{dom}_{gcm_}_{rcm_}_normals-2.nc") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      walk(ff, file.remove)
      
      # delete concatenated files
      unlink(dir_cat, recursive = T)
      
      # delete raw files
      unlink(dir_raw_data, recursive = T)
      
    }) # end of model loop
    
    
    
    # ENSEMBLE MODELS ---------------------------------------------------------
    
    ff <- 
      dir_normals %>% 
      list.files(full.names = T) %>% 
      str_subset("normals-2")
    
    str_glue("cdo ensmean {str_flatten(ff, ' ')} {dir_normals}/{dom}_normals-ens.nc") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    walk(ff, file.remove)
    
  } # end of domain loop
  
  
  
  # MOSAIC --------------------------------------------------------------------
  
  
  # dir_normals %>% 
  #   list.files(full.names = T) %>% 
  #   str_subset("normals-ens")
  
  print(str_glue("      Mosaicking"))
  
  
  l_s <- 
    map(doms %>% set_names(), function(dom){
      
      # load ensembled map 
      s <- 
        dir_normals %>%
        list.files(full.names = T) %>%
        str_subset(dom) %>%
        read_ncdf %>%
        suppressMessages() %>% 
        setNames("v") %>% 
        st_set_dimensions(3, names = "wl")
      
      if(v_long == "precipitation"){
        
        s <- 
          s %>% 
          mutate(v = set_units(v, kg/m^2/d)) %>% 
          drop_units() %>% 
          mutate(v = round(v) %>% as.integer())
        
      } else {
        
        s <- 
          s %>% 
          mutate(v = set_units(v, degC)) %>% 
          drop_units()
        
      }
      
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
  
  
  # l_s <- 
  #   l_s %>% 
  #   map(st_set_dimensions, "time", values = month.abb) #%>% 
  #   # map(split, "time")
  
  l_s <- 
    l_s %>% 
    map(function(s){
      
      s %>% 
        st_set_dimensions("lon", values = st_get_dimension_values(s, "lon") %>% round(1) %>% {.-0.1}) %>% 
        st_set_dimensions("lat", values = st_get_dimension_values(s, "lat") %>% round(1) %>% {.-0.1}) %>% 
        st_set_crs(4326) %>% 
        
        st_set_dimensions("wl", values = wls)
      
    })
  
  
  l_mos_wl <- 
    
    # loop through wl
    imap(wls, function(wl, wl_i){
      
      print(str_glue("         wl: {wl}"))
      
      l_s_wl <-
        l_s %>% 
        map(slice, wl, wl_i) %>% 
        map(st_warp, global) %>%
        map(st_set_dimensions, "time", values = month.abb) %>% 
        map(split, "time")
      
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
      
      print(str_glue("         summing..."))
      
      mos <- 
        names(l_s_weighted[[1]]) %>% 
        map(function(mon){
          
          # print(mon)
          
          l_s_weighted %>% 
            map(select, mon) %>% 
            {do.call(c, c(., along = "doms"))} %>% 
            st_apply(c(1,2), function(foo){
              
              if(all(is.na(foo))){
                NA
              } else {
                sum(foo, na.rm = T)
              }
              
            },
            FUTURE = F)
          
        }) %>% 
        do.call(c, .)
      
      return(mos)
      
    })
  
  
  
  s <- 
    l_mos_wl %>% 
    {do.call(c, c(., along = "wl"))} %>% 
    st_set_dimensions(3, values = as.numeric(wls))
  
  s[is.na(land)] <- NA
  
  
  # save as nc
  print(str_glue("  Saving"))
  
  file_name <- str_glue("{dir_tmp}/normals_mos/{v_long}_mosaic.nc")
  fn_write_nc(s, file_name, "wl")
  
  unlink(dir_normals, recursive = T)
  
  
} # end of variable loop


rm(l_s_valid, l_s_dist, l_s_weights, s_intersections,
   l_s, l_mos_wl, s)




# CLASSIFICATION --------------------------------------------------------------

vari_names <- 
  str_glue("{dir_tmp}/normals_mos") %>% 
  list.files() %>% 
  str_remove("_mosaic.nc")

vari_names_sh <- 
  c("Tm", "Tx", "Tn", "P") %>% 
  set_names(vari_names)


l_s <- 
  str_glue("{dir_tmp}/normals_mos") %>% 
  list.files(full.names = T) %>%
  map(read_ncdf, proxy = F) %>% 
  suppressMessages()

s_template <- 
  l_s[[1]] %>% 
  select(1) %>% 
  slice(wl, 1)

tb_coords <- 
  s_template %>% 
  as_tibble() %>% 
  select(1,2)
    


clim_types <- tibble(climate = c("Af", "Am", "Aw",
                                 "BSh", "BSk",
                                 "BWh", "BWk",
                                 "Cfa", "Cfb", "Cfc",
                                 "Csa", "Csb", "Csc",
                                 "Cwa", "Cwb", "Cwc",
                                 "Dfa", "Dfb", "Dfc",
                                 "Dsa", "Dsb", "Dsc",
                                 "Dwa", "Dwb", "Dwc",
                                 "EF", "ET"),
                     clim_num = 1:27)




# loop through wl
l_clim_wl <- 
  imap(wls, function(wl, wl_i){
    
    print(str_glue("Classifying {wl}"))
    
    tb <- 
      l_s %>% 
      map(slice, wl, wl_i) %>% 
      map(merge, name = "month") %>% 
      do.call(c, .) %>% 
      setNames(vari_names_sh) %>% 
      st_set_dimensions("month", values = seq(1,12)) %>% 
      as_tibble() %>% 
      filter(!is.na(Tm)) #%>% 
      
      # select(-Tx, -Tn)
    
    tb_nested <-
      tb %>%
      group_by(lon, lat) %>%
      nest()
    
    tb_nested <- 
      tb_nested %>% 
      mutate(climate = map_chr(data, function(df){
        
        ClimClass::koeppen_geiger(df, A_B_C_special_sub.classes = F, class.nr = F)$class
        
      })) %>% 
      select(-data) %>% 
      ungroup()
    
    
    # tb_nested <- 
    #   tb_nested %>% 
    #   mutate(climate = ifelse(str_detect(climate, "^A."), str_sub(climate, end = 2), climate),
    #          climate_num_1 = climate %>% str_sub(end = 1) %>% match(LETTERS),
    #          climate_num_2 = climate %>% str_sub(start = 2, end = 2) %>% toupper() %>% match(LETTERS) %>% str_pad(2, "left", "0"),
    #          climate_num_3 = climate %>% str_sub(start = 3, end = 3) %>% toupper() %>% match(LETTERS) %>% str_pad(2, "left", "0"),
    #          climate_num_3 = ifelse(is.na(climate_num_3), "00", climate_num_3),
    #          climate = str_glue("{climate_num_1}{climate_num_2}{climate_num_3}") %>% as.numeric()
    #   ) %>% 
    #   select(lon, lat, climate)
    
    tb_nested <- 
      tb_nested %>%
      mutate(climate = case_when(str_detect(climate, "^A.") ~ str_sub(climate, end = 2), 
                                 TRUE ~ str_sub(climate, end = 3))) %>%
      left_join(clim_types, by = "climate") %>%
      select(lon, lat, clim_num)
    
    
    s <- 
      tb_coords %>% 
      left_join(tb_nested, by = c("lon", "lat")) %>% 
      # pull(climate) %>%
      pull(clim_num) %>% 
      matrix(nrow = dim(s_template)[1], ncol = dim(s_template)[2], byrow = F) %>% 
      st_as_stars() %>% 
      setNames(wl) 
    
    st_dimensions(s) <- st_dimensions(s_template)
    
    return(s)
    
  })
   
     

# l_clim_wl %>% saveRDS("res.rds")
 
res <- 
  l_clim_wl %>% 
  {do.call(c, c(., along = "wl"))} %>%
  st_set_dimensions("wl", values = as.numeric(wls)) %>% 
  setNames("climzone")
      
fn_write_nc(res,
            "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/climate_zones_v03.nc",
            "wl")



# write_stars(res, "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/climate_zones_2ndlev_v02.tif")
# 
# walk(seq(1,6), function(i){
#   
#   res_pol <- 
#     res %>% 
#     slice(wl, i) %>% 
#     st_as_sf(as_points = F, merge = T)
#   
#   st_write(res_pol, str_glue("/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/climate_zones_2ndlev_band{i}_v02.gpkg"))
#   
# })






