
library(tidyverse)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

source("scripts/functions.R")


varr <- "average_temperature"


dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_disk <- "/mnt/pers_disk"
dir_winter_doms <- "/mnt/pers_disk/winter_doms"
# dir.create(dir_winter_doms)

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")



for (dom in doms) {
  
  tb_files <- fn_data_table(varr)
  
  tb_models <-
    unique(tb_files[, c("gcm", "rcm")]) %>% 
    arrange(rcm, gcm)
  
  # ignore RegCM in these domains  
  if(dom %in% c("SAM", "AUS", "CAS")){
    tb_models <- 
      tb_models %>% 
      filter(str_detect(rcm, "RegCM", negate = T))
  }
  
  
  # MODEL LOOP ------
  
  for(i in seq_len(nrow(tb_models))){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING model {i} / {nrow(tb_models)}"))
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    ## DOWNLOAD RAW DATA -----------------------------------------------------
    
    print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
    
    dir_raw_data <- str_glue("{dir_disk}/raw_data")
    
    if(dir.exists(dir_raw_data)){
      print(str_glue("   (previous dir_raw_data deleted)"))
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
    
    "   Done: {length(list.files(dir_raw_data))} / {nrow(tb_files_mod)} files downloaded" %>% 
      str_glue() %>% 
      print()
    
    
    
    
    ## CDO PRE-PROCESS -------------------------------------------------------
    
    # CDO is used here to split files annually and fix their time dimension.
    # This process solves some inconsistencies present in RegCM4 models.
    # Then files are concatenated again into one single file with the complete
    # record of years (~ 1970-2100). Concatenating everything into one file 
    # eases successive calculations to obtain derived variables.
    
    
    print(str_glue("Pre-processing with CDO [{rcm_}] [{gcm_}]"))
    
    tb_files_mod %>%
      future_pwalk(function(file, t_i, t_f, ...){
        
        # extract first and last year included in the file
        yr_i <- year(as_date(t_i))
        yr_f <- year(as_date(t_f))
        
        f <- str_glue("{dir_raw_data}/{file}")
        
        
        # split annually
        system(str_glue("cdo splityear {f} {dir_raw_data}/tas_yrsplit_"),
               ignore.stdout = T, ignore.stderr = T)
        
        
        # fix time (only of the files that came from the file above)
        dir_raw_data %>% 
          list.files(full.names = T) %>% 
          str_subset("yrsplit") %>%
          str_subset(str_flatten(yr_i:yr_f, "|")) %>% 
          
          walk2(seq(yr_i, yr_f), function(f2, yr) {
            
            f_north <- str_glue("{dir_raw_data}/tas_winter_north_{yr}.nc")
            system(str_glue("cdo -a -setdate,{yr}-01-01 -timmean -selseason,DJF {f2} {f_north}"),
                   ignore.stdout = T, ignore.stderr = T)
            
            f_south <- str_glue("{dir_raw_data}/tas_winter_south_{yr}.nc")
            system(str_glue("cdo -a -setdate,{yr}-01-01 -timmean -selseason,JJA {f2} {f_south}"),
                   ignore.stdout = T, ignore.stderr = T)
            
            # file.remove(f2)
            
          })
        
        file.remove(f)
        
      })
    
    
    # check if some files could not be year-split/time-fixed
    bad_remnants <- 
      dir_raw_data %>% 
      list.files(full.names = T) %>% 
      str_subset("yrsplit")
    
    if (length(bad_remnants) > 0) {
      print(str_glue("   ({length(bad_remnants)} bad file(s) - deleted)"))
      
      bad_remnants %>% 
        walk(file.remove)
      
    }
    
    
    # concatenate
    
    # loop through variable(s)
    c("north", "south") %>% 
      future_walk(function(vv){
        
        ff <-
          dir_raw_data %>%
          list.files(full.names = T) %>%
          str_subset(vv) %>% 
          str_flatten(" ")
        
        system(str_glue("cdo -settunits,d  -cat {ff} {dir_winter_doms}/{vv}_{dom}_{rcm_}_{gcm_}.nc"),
               ignore.stdout = T, ignore.stderr = T)
        
      })
    
    
    # delete raw files
    unlink(dir_raw_data, recursive = T)
    
  }
  
} 




# ****************************************************************************


library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


dir_winter_doms <- "/mnt/pers_disk/winter_doms"

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")



# load thresholds table
thresholds <- 
  "../map-data-processing/cmip5_model_temp_thresholds.csv" %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  
  mutate(wl = str_sub(wl, 3)) %>% 
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  # add institutes to model names (for joins to work)
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))



for (dom in doms) {
  
  ff <- 
    dir_winter_doms %>% 
    fs::dir_ls() %>% 
    str_subset(dom)
  
  
  # import files into a list
  l_s <- 
    
    future_map(ff, function(f){
      
      # apply changes 
      read_ncdf(f, 
                proxy = F) %>% 
        suppressMessages() %>% 
        suppressWarnings() %>% 
        mutate(tas = set_units(tas, degC))
      
      
    },
    .options = furrr_options(seed = NULL)) %>% 
    
    # fix time dim
    map(function(s){
      
      s %>%
        st_set_dimensions("time",
                          values = st_get_dimension_values(s, "time") %>%
                            as.character() %>%
                            str_sub(end = 4) %>% 
                            as.integer()) %>%
        
        mutate(tas = set_units(tas, NULL))
    })
  
  
  
  # slice by wl
  
  l_s_wl <- 
    
    # loop through warming levels
    map(wls, function(wl){
      
      print(str_glue("Slicing WL {wl}"))
      
      # loop through models
      map2(ff, l_s, function(f, s){
        
        # rcm_ <- f %>% str_split("_", simplify = T) %>% .[,4]
        
        # extract GCM to identify threshold year
        gcm_ <-
          f %>%
          fs::path_file() %>% 
          fs::path_ext_remove() %>% 
          str_split("_", simplify = T) %>% 
          .[,4]
        
        # baseline:
        if(wl == "0.5"){
          
          s %>% 
            filter(time >= 1971,
                   time <= 2000)
          
          # other warming levels:
        } else {
          
          thres_val <-
            thresholds %>%
            filter(str_detect(Model, str_glue("{gcm_}$"))) %>% 
            filter(wl == {{wl}})
          
          s <- 
            s %>% 
            filter(time >= thres_val$value - 10,
                   time <= thres_val$value + 10)
          
          # verify correct slicing:
          print(str_glue("   {gcm_}: {thres_val$Model}: {thres_val$value}"))
          
          return(s)
        }
        
      }) #%>% 
      # 
      # # concatenate all models along time (3rd) dimension
      # {do.call(c, c(., along = "time"))}
      
    })
  
  
  
  l_s_wl_hemi <- 
    map(c("north", "south") %>% set_names(), function(hemi) {
      
      l_s_wl %>% 
        map(function(s){
          
          s[str_detect(ff, hemi)] %>% 
            {do.call(c, c(., along = "time"))}
          
        })
    })
  
  
  
  # Calculate stats
  
  l_s_wl_hemi_stats <-
    
    map(c("north", "south") %>% set_names(), function(hemi) {
      
      # loop through warming levels
      imap(wls, function(wl, iwl){
        
        print(str_glue("Calculating stats WL {wl}"))
        
        l_s_wl_hemi %>%
          pluck(hemi) %>% 
          pluck(iwl) %>%
          
          st_apply(c(1,2), function(ts){
            
            # if a given grid cell is empty, propagate NAs
            if(any(is.na(ts))){
              
              # c(mean = NA,
              #   perc05 = NA, 
              #   perc50 = NA,
              #   perc95 = NA)
              
              rep(NA, 101) %>%
                set_names(str_glue("perc_{seq(0, 100)}"))
              
            } else {
              
              # c(mean = mean(ts),
              #   quantile(ts, c(0.05, 0.5, 0.95)) %>%
              #     setNames(c("perc05", "perc50", "perc95")))
              
              quantile(ts, c(seq(0,1, by = 0.01))) %>%
                set_names(str_glue("perc_{seq(0, 100)}"))
              
            }
            
          },
          FUTURE = T,
          .fname = "stats") %>%
          aperm(c(2,3,1)) %>%
          split("stats")
        
      })
      
    })
  
  
  
  
  
  # concatenate warming levels
  l_s_result <-
    
    map(c("north", "south") %>% set_names(), function(hemi) {
      
      l_s_wl_hemi_stats %>%
        pluck(hemi) %>% 
        {do.call(c, c(., along = "wl"))} %>%
        st_set_dimensions(3, values = as.numeric(wls))
      
      
    })
  
  
  # save results
  write_rds(l_s_result, str_glue("{dir_winter_doms}/f_{dom}_winter_tas_wl.rds"))
  
  
}



# *****************************************************************************


library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

dir_winter_doms <- "/mnt/pers_disk/winter_doms"

doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")


# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(doms), function(dom){
    
    # load map
    s <- 
      dir_winter_doms %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset("winter_tas") %>%
      read_rds() %>% 
      pluck(1) %>% 
      slice(wl, 1) %>% 
      select(1)
    
    s <- 
      s %>% 
      st_set_dimensions(1, st_get_dimension_values(s, 1) %>% round(1) %>% {.-0.1}) %>% 
      st_set_dimensions(2, st_get_dimension_values(s, 2) %>% round(1) %>% {.-0.1}) %>% 
      st_set_crs(4326)
    
    
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
  "../map-data-processing/buffered_ocean_mask.nc" %>% 
  read_ncdf() %>%
  st_warp(global) %>% 
  setNames("a")


# MOSAIC *******************************************
# to mosaic 100 perc maps, go to next section


l_s <- 
  
  map(c("north", "south") %>% set_names(), function(hemi) {
    
    l_s <- 
      map(doms %>% set_names(), function(dom){
        
        print(dom)
        
        # load ensembled map 
        s <- 
          dir_winter_doms %>%
          list.files(full.names = T) %>%
          str_subset(dom) %>%
          str_subset("winter_tas") %>%
          read_rds() %>% 
          pluck(hemi)
        
        s <- 
          s %>% 
          st_set_dimensions(1, st_get_dimension_values(s, 1) %>% round(1) %>% {.-0.1}) %>% 
          st_set_dimensions(2, st_get_dimension_values(s, 2) %>% round(1) %>% {.-0.1}) %>% 
          st_set_crs(4326)
        
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
    
    
    
    l_mos_wl <- 
      
      # loop through warming levels
      imap(wls, function(wl, wl_pos){
        
        print(str_glue("    {wl}"))
        
        
        l_s_wl <-
          l_s %>% 
          map(slice, wl, wl_pos) %>% 
          map(st_warp, global)
        
        # APPLY WEIGHTS
        l_s_weighted <- 
          
          map2(l_s_wl, l_s_weights, function(s, w){
            
            orig_names <- names(s)
            
            map(orig_names, function(v_){
              
              c(s %>% select(all_of(v_)) %>% setNames("v"),
                w) %>% 
                
                mutate(v = v*weights) %>% 
                select(-weights) %>% 
                setNames(v_)
              
            }) %>% 
              do.call(c, .)
            
          })
        
        # MOSAIC
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
    
    
    # round
    l_mos_wl <-
      l_mos_wl %>%
      map(function(s){
        
        wl <- names(s)
        
        s %>%
          rename(a = 1) %>%
          mutate(a = as.integer(round(a))) %>%
          setNames(wl)
        
        
      })
    
    s <- 
      l_mos_wl %>% 
      do.call(c, .) %>% 
      merge(name = "wl") %>% 
      split("stats") %>% 
      st_set_dimensions(3, values = as.numeric(wls))
    
    
  })


s_north <- 
  l_s$north %>% 
  filter(lat >= 0)

s_south <- 
  l_s$south %>% 
  filter(lat < 0)

s <-
  names(s_north) %>% 
  map(function(i) {
    
    st_mosaic(
      s_north %>% 
        select(i),
      s_south %>% 
        select(i)
    ) %>% 
      setNames(i)
    
  }) %>% 
  do.call(c, .)


s[is.na(land)] <- NA_integer_
"/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/days-above-32C_v03.nc" %>% read_ncdf() -> a  
st_dimensions(s) <- st_dimensions(a)
s <- st_set_dimensions(s, 3, values = seq(0.5, 3.0, 0.5))





fn_write_nc(s %>% st_set_dimensions(3, values = seq(0.5, 3.0, 0.5)),
            "/mnt/bucket_mine/results/misc/winter_temps_pf/global_avg_winter_tas.nc",
            "wl")




# MOSAIC 100 perc maps *******************************************

source("scripts/functions.R")

dir_tmp <- "/mnt/pers_disk/tmp"

l_s <- 
  
  map(c("north", "south") %>% set_names(), function(hemi) {
    
    l_s <- 
      map(doms %>% set_names(), function(dom){
        
        print(dom)
        
        # load ensembled map 
        s <- 
          dir_winter_doms %>%
          list.files(full.names = T) %>%
          str_subset(dom) %>%
          str_subset("winter_tas") %>%
          read_rds() %>% 
          pluck(hemi)
        
        s <- 
          s %>% 
          st_set_dimensions(1, st_get_dimension_values(s, 1) %>% round(1) %>% {.-0.1}) %>% 
          st_set_dimensions(2, st_get_dimension_values(s, 2) %>% round(1) %>% {.-0.1}) %>% 
          st_set_crs(4326)
        
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
    
    
    
    l_mos_wl <- 
      
      # loop through warming levels
      imap(wls, function(wl, wl_pos){
        
        print(str_glue("    {wl}"))
        
        
        l_s_wl <-
          l_s %>% 
          map(slice, wl, wl_pos) %>% 
          map(st_warp, global)
        
        # APPLY WEIGHTS
        l_s_weighted <- 
          
          map2(l_s_wl, l_s_weights, function(s, w){
            
            orig_names <- names(s)
            
            map(orig_names, function(v_){
              
              c(s %>% select(all_of(v_)) %>% setNames("v"),
                w) %>% 
                
                mutate(v = v*weights) %>% 
                select(-weights) %>% 
                setNames(v_)
              
            })
            
          })
        
        
        l_s_weighted <- 
          transpose(l_s_weighted)
        
        rm(l_s_wl)
        
        
        
        # MOSAIC
        
        dir.create(dir_tmp)
        
        iwalk(l_s_weighted, function(l, i) {
          
          do.call(c, c(l, along = "a")) %>% 
            st_apply(c(1,2), function(foo){
              
              if(all(is.na(foo))){
                NA
              } else {
                sum(foo, na.rm = T)
              }
            }) %>% 
            write_rds(str_glue("{dir_tmp}/s_{str_pad(i, 3, 'left', '0')}.rds"))
          
        })
        
        rm(l_s_weighted)
        
        mos <- 
          dir_tmp %>% 
          fs::dir_ls() %>% 
          map(read_rds) %>% 
          unname() %>% 
          do.call(c, .)
        
        unlink(dir_tmp, recursive = T)
        
        return(mos)
        
      })
    
    
    rm(l_s)
    
    
    # round
    l_mos_wl <-
      l_mos_wl %>%
      map(function(s){
        
        s %>% 
          round()
        
        
      })
    
    
    s <- 
      l_mos_wl %>% 
      {do.call(c, c(., along = "wl"))} %>% 
      st_set_dimensions(3, values = as.numeric(wls))
    
    
    write_rds(s, str_glue("/mnt/pers_disk/s_{hemi}.rds"))
    
    
  })


s_north <- 
  "/mnt/pers_disk/s_north.rds" %>% 
  read_rds() %>% 
  filter(lat >= 0)

s_south <- 
  "/mnt/pers_disk/s_south.rds" %>% 
  read_rds() %>% 
  filter(lat < 0)

s <-
  names(s_north) %>% 
  map(function(i) {
    
    print(i)
    
    st_mosaic(
      s_north %>% 
        select(i),
      s_south %>% 
        select(i)
    ) %>% 
      setNames(i)
    
  }) %>% 
  do.call(c, .)


s[is.na(land)] <- NA_integer_
"/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/days-above-32C_v03.nc" %>% read_ncdf() -> a
st_dimensions(s) <- st_dimensions(a)
s <- st_set_dimensions(s, 3, values = seq(0.5, 3.0, 0.5))


final_name <- "average-winter-temperature"
vol <- "heat"

file_name <- str_glue("/mnt/pers_disk/{final_name}_v03_cdf_v02.nc")

fn_write_nc(s, file_name, "wl")

"gsutil mv {file_name} gs://clim_data_reg_useast1/results/global_heat_pf/all-stats/{vol}" %>% 
  str_glue() %>% 
  system()

c("/mnt/pers_disk/s_north.rds",
  "/mnt/pers_disk/s_south.rds") %>% 
  walk(fs::file_delete)

