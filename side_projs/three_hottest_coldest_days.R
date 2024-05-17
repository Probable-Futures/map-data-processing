
# this file is now the one used for 3 hottest/coldest days


# CHOOSE VARIABLE(S) TO PROCESS
var_index <- c(11)

# 1 - days-above-32C
# 2 - days-above-35C
# 3 - days-above-38C
# 4 - days-above-45C *****
# 4 - ten-hottest-days
# 5 - average-daytime-temperature
# 6 - freezing-days
# 7 - likelihood-daytime-heatwave
# 8 - nights-above-20C
# 9 - nights-above-25C
# 10 - ten-hottest-nights
# 11 - average-nighttime-temperature
# 12 - frost-nights                       
# 13 - likelihood-nighttime-heatwave
# 14 - days-above-26C-wetbulb
# 15 - days-above-28C-wetbulb
# 16 - days-above-30C-wetbulb
# 17 - days-above-32C-wetbulb
# 18 - ten-hottest-wetbulb-days
# 19 - average-temperature
# 20 - change-total-annual-precipitation  
# 21 - change-90-wettest-days
# 22 - change-100yr-storm-precip
# 23 - change-100yr-storm-freq
# 24 - change-snowy-days                  
# 25 - change-dry-hot-days
# 26 - change-water-balance
# 27 - likelihood-yearplus-drought
# 28 - likelihood-yearplus-extreme-drought
# 29 - change-wildfire-days               




# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

# load main directory routes 
source("scripts/setup.R")

# load main function to calculate derived vars
source("scripts/fn_derived.R") 
source("scripts/functions.R") # other functions


dir_derived <- str_glue("{dir_results}/01_derived")

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

# load table of all variables
tb_vars_all <-
  read_csv("pf_variable_table.csv") %>% 
  suppressMessages()

# subset those that will be processed
tb_vars <- 
  tb_vars_all[var_index, ]




wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# load thresholds table
thresholds <- 
  str_glue("cmip5_model_temp_thresholds.csv") %>% 
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



# PROCESS DERIVED VARIABLES ---------------------------------------------------


# INPUT VARIABLE LOOP -----

vars_input <- 
  tb_vars %>% 
  pull(var_input) %>% 
  unique()

for (var_input in vars_input) {
  
  print(str_glue(" "))
  print(str_glue("PROCESSING VARS BASED ON {var_input}"))
  
  
  # DOMAIN LOOP -----
  
  for(dom in doms){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING {dom}"))
    
    # assemble table of needed files for calculation
    tb_files <-
      str_split(var_input, "[+]") %>% .[[1]] %>% # in case > 1 variable 
      map_dfr(fn_data_table) %>% 
      
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
      
      
      
      # FWI raw layers from REMO need the time dimension to be properly formatted 
      
      # if(str_detect(var_input, "fire") & str_detect(rcm_, "REMO")){
      #   
      #   future_pwalk(tb_files_mod, function(file, t_i, ...){
      #     
      #     infile <- str_glue("{dir_raw_data}/{file}")
      #     outfile <- infile %>% str_replace(".nc", "_fixed_time.nc")
      #     
      #     "cdo -setcalendar,365_day -settaxis,{as.character(ymd(t_i))},12:00:00,1day {infile} {outfile}" %>% 
      #       str_glue %>% 
      #       system(ignore.stdout = T, ignore.stderr = T)
      #     
      #     file.remove(infile)
      #     
      #     file.rename(outfile, infile)
      #     
      #   })
      #   
      # }
      
      
      
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
          
          
          if(str_detect(var_input, "wetbulb")){ # already split annually
            
            f_new <- str_glue("{dir_raw_data}/yrfix_{yr_i}.nc")
            
            # fix time
            system(str_glue("cdo -a setdate,{yr_i}-01-01 {f} {f_new}"),
                   ignore.stdout = T, ignore.stderr = T)
            
            file.remove(f)
            
            
          } else {
            
            # extract variable's (short) name
            v <- str_split(file, "_") %>% .[[1]] %>% .[1]
            
            # split annually
            system(str_glue("cdo splityear {f} {dir_raw_data}/{v}_yrsplit_"),
                   ignore.stdout = T, ignore.stderr = T)
            
            # fix time (only of the files that came from the file above)
            dir_raw_data %>% 
              list.files(full.names = T) %>% 
              str_subset(v) %>% 
              str_subset("yrsplit") %>%
              str_subset(str_flatten(yr_i:yr_f, "|")) %>% 
              
              walk2(seq(yr_i, yr_f), function(f2, yr) {
                
                f_new <- str_glue("{dir_raw_data}/{v}_yrfix_{yr}.nc")
                
                system(str_glue("cdo -a setdate,{yr}-01-01 {f2} {f_new}"),
                       ignore.stdout = T, ignore.stderr = T)
                
                file.remove(f2)
                
              })
            
            file.remove(f)
            
          }
          
          
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
      
      dir_cat <- str_glue("{dir_disk}/cat")
      
      if (dir.exists(dir_cat)) {
        print(str_glue("   (previous dir_cat deleted)"))
        unlink(dir_cat, recursive = T)
      }
      
      dir.create(dir_cat)
      
      # extract variable(s)'s (short) name
      v <- 
        dir_raw_data %>%
        list.files() %>% 
        str_split("_", simplify = T) %>% 
        .[,1] %>% 
        unique()
      
      # loop through variable(s)
      future_walk(v, function(vv){
        
        ff <-
          dir_raw_data %>%
          list.files(full.names = T) %>%
          str_subset(str_glue("{vv}_")) %>% 
          str_flatten(" ")
        
        system(str_glue("cdo -settunits,d  -cat {ff} {dir_cat}/{vv}_cat.nc"),
               ignore.stdout = T, ignore.stderr = T)
        
        
        
        # delete raw files
        unlink(dir_raw_data, recursive = T)
        
        # running mean
        system(str_glue("cdo runmean,3 {dir_cat}/{vv}_cat.nc {dir_cat}/{vv}_cat_rm3.nc"),
               ignore.stdout = T, ignore.stderr = T)
        
      })
      
      
      
      # ## CALCULATE DERIVED VARIABLES ------------------------------------------
      # 
      # # This section calculates derived variables from the concatenated file
      # # (some vars use the annual files at dir_raw_data).
      # # Derived variables that use the same input variable are calculated 
      # # at once. This step uses function "fn_derived" from the "fn_derived.R" 
      # # script loaded in the SETUP section above. Refer to it to see how each 
      # # derived variable is obtained.
      # 
      # 
      # print(str_glue("Calculating derived variables [{rcm_}] [{gcm_}]"))
      # 
      # # identify all vars to derive from input var
      # tb_derived_vars <-
      #   tb_vars %>%
      #   filter(var_input == {{var_input}})
      # 
      # # process variables not eligible for parallelization
      # tb_derived_vars_np <-
      #   tb_derived_vars %>%
      #   filter(parallel == "n")
      # 
      # if (nrow(tb_derived_vars_np) > 0) {
      #   pwalk(tb_derived_vars_np, function(var_derived, ...){
      #     fn_derived(var_derived)
      #   })
      # }
      # 
      # # process variables eligible for parallelization
      # tb_derived_vars_p <- 
      #   tb_derived_vars %>% 
      #   filter(parallel == "y")
      # 
      # if (nrow(tb_derived_vars_p) > 0) {
      #   future_pwalk(tb_derived_vars_p, function(var_derived, ...){
      #     fn_derived(var_derived)
      #   })
      # }
      
      
      
      for (wl in wls) {
        
        if(wl == "0.5"){
          
          yr_i <- 1971
          yr_f <- 2000
          
          print(str_glue("   {gcm_}: 0.5 deg WL"))
          
          # other warming levels:
        } else {
          
          thres_val <-
            thresholds %>%
            filter(str_detect(Model, str_glue("{gcm_}$"))) %>% 
            filter(wl == {{wl}})
          
          yr_i <- thres_val$value - 10
          yr_f <- thres_val$value + 10
          
          # verify correct slicing:
          print(str_glue("   {gcm_}: {thres_val$Model}: {thres_val$value}"))
          
        }
        
        
        if (v == "tasmax") {
          tsteps <- "-3/-1"
          timstat <- "timmax"
        } else if (v == "tasmin") {
          tsteps <- "1/3"
          timstat <- "timmin"
        }
        
        
        
        # 1 : 3 hottests/coldest days within the WL period
        
        f <- str_glue("{dir_cat}/{v}_cat.nc")
        
        str_glue("cdo -selyear,{yr_i}/{yr_f} {f} /mnt/pers_disk/pf_3_hc/{v}_pre.nc") %>% 
          system(ignore.stdout = T, ignore.stderr = T) 
        
        str_glue("cdo -seltimestep,{tsteps} -timsort /mnt/pers_disk/pf_3_hc/{v}_pre.nc /mnt/pers_disk/pf_3_hc/{dom}_{v}_3days_{wl}_{rcm_}_{gcm_}.nc") %>% 
          system(ignore.stdout = T, ignore.stderr = T)
        
        fs::file_delete(str_glue("/mnt/pers_disk/pf_3_hc/{v}_pre.nc"))
        
        
        # *******
        
        # 2 : mean of 3 *consecutive* hottest/coldest days within the WL period
        
        f <- str_glue("{dir_cat}/{v}_cat_rm3.nc")
        
        str_glue("cdo -{timstat} -selyear,{yr_i}/{yr_f} {f} /mnt/pers_disk/pf_3_hc/{dom}_{v}_3drm-day_{wl}_{rcm_}_{gcm_}.nc") %>% 
          system(ignore.stdout = T, ignore.stderr = T)
        
      }
      
      
      
      # delete concatenated file
      unlink(dir_cat, recursive = T)
      
      # # delete raw files
      # unlink(dir_raw_data, recursive = T)
      
      
    }
    
  }
  
}



for (var in c("tasmax", "tasmin")) {
  
  for (dom in doms) {
    
    s <- 
      map(wls, function(wl) {
        
        if (var == "tasmax") {
          vname <- "3_hottest_"
          func <- "max"
        } else if (var == "tasmin") {
          vname <- "3_coldest"
          func <- "min"
        }
        
        
        three_nonconsecutive <- 
          "/mnt/pers_disk/pf_3_hc/" %>% 
          fs::dir_ls() %>% 
          str_subset(var) %>% 
          str_subset("3days") %>% 
          str_subset(dom) %>% 
          str_subset(str_glue("_{wl}_")) %>% 
          map(read_ncdf, make_time = F) %>% 
          suppressMessages() %>% 
          {do.call(c, c(., along = "time"))} %>% 
          st_apply(c(1,2), 
                   function(x) {mean(head(sort(x), 3))},
                   FUTURE = T)
        
        
        s <- 
          "/mnt/pers_disk/pf_3_hc/" %>% 
          fs::dir_ls() %>% 
          str_subset(var) %>% 
          str_subset("3drm") %>% 
          str_subset(dom) %>% 
          str_subset(str_glue("_{wl}_")) %>% 
          map(read_ncdf, make_time = F) %>% 
          suppressMessages() %>% 
          {do.call(c, c(., along = "time"))}
        
        
        three_consecutive_ensmean <- 
          s %>% 
          st_apply(c(1,2), 
                   mean,
                   FUTURE = T)
        
        three_consecutive_ensmxmn <- 
          s %>% 
          st_apply(c(1,2), 
                   {{func}},
                   FUTURE = T)
        
        list(three_nonconsecutive = three_nonconsecutive,
             three_consecutive_ensmean = three_consecutive_ensmean,
             three_consecutive_ensmxmn = three_consecutive_ensmxmn)
        
      })
    
    
    s %>% 
      transpose() %>% 
      map(~do.call(c, c(.x, along = "wl")) %>% st_set_dimensions(3, values = wls)) %>% 
      iwalk(function(ss, i) {
        
        ss %>% 
          st_set_dimensions(1, values = st_get_dimension_values(ss, 1) %>% round(1)) %>% 
          st_set_dimensions(2, values = st_get_dimension_values(ss, 2) %>% round(1)) %>% 
          st_set_crs(4326) %>%
          write_rds(str_glue("/mnt/pers_disk/pf_3_hc/{dom}_{var}_{i}.rds"))
        
      })
    
    
    
  }
  
}








doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(doms), function(dom){
    
    # load map
    s <- 
      "/mnt/pers_disk/pf_3_hc/" %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset("tasmax_three_consecutive_ensmean") %>% 
      read_rds() %>% 
      slice(wl, 1)
    
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
  # "/mnt/bucket_cmip5/Probable_futures/irunde_scripts/create_a_dataset/04_rcm_buffered_ocean_mask.nc" %>% 
  "buffered_ocean_mask.nc" %>% 
  read_ncdf() %>%
  st_warp(global) %>% 
  setNames("a")



# MOSAIC ----------------------------------------------------------------------

# loop through variables

derived_vars <- 
  "/mnt/pers_disk/pf_3_hc/" %>% 
  list.files() %>% 
  str_subset("three") %>% 
  str_subset("CAM") %>% 
  str_sub(start = 5)

walk(derived_vars, function(derived_var){
  
  print(str_glue(" "))
  print(str_glue("Mosaicking {derived_var}"))
  
  # final_name <- 
  #   tb_vars %>% 
  #   filter(var_derived == derived_var) %>% 
  #   pull(var_final)
  
  # vol <- 
  #   tb_vars %>% 
  #   filter(var_derived == derived_var) %>% 
  #   pull(volume)
  
  
  l_s <- 
    map(doms %>% set_names(), function(dom){
      
      print(dom)
      
      # load ensembled map 
      s <- 
        "/mnt/pers_disk/pf_3_hc" %>%
        list.files(full.names = T) %>%
        str_subset(dom) %>%
        str_subset(derived_var) %>%
        read_rds()
      
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
        imap(~setNames(.x, .y)) %>%
        unname() %>% 
        do.call(c, .) %>% 
        merge(name = "doms") %>%
        
        st_apply(c(1,2), function(foo){
          
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
        mutate(a = a %>% units::set_units(K) %>% units::set_units(degC),
               a = as.integer(round(a))) %>%
        setNames(wl)
      
    })
  
  s <- 
    l_mos_wl %>% 
    do.call(c, .) %>% 
    merge(name = "wl") %>% 
    st_set_dimensions(3, values = as.numeric(wls)) %>% 
    setNames("a")
  
  
  s[is.na(land)] <- NA_integer_
  
  
  
  # save as nc
  print(str_glue("  Saving"))
  
  suf <- 
    fs::path_ext_remove(derived_var) %>% 
    str_sub(start = 14)
  
  
  if(str_detect(derived_var, "tasmax")) {
    vname = str_glue("three_hottest_days_{suf}.nc")
  } else if (str_detect(derived_var, "tasmin")) {
    vname = str_glue("three_coldest_days_{suf}.nc")
  }
  
  file_name <- str_glue("/mnt/bucket_mine/results/misc/{vname}") # *******************
  fn_write_nc(s, file_name, "wl")
  
})
