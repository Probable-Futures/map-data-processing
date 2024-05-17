



# SETUP -----------------------------------------------------------------------

setwd("../../map-data-processing/")

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
# source("scripts/fn_derived.R")
source("scripts/functions.R") # other functions


dir_derived <- str_glue("{dir_results}/01_derived")


doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# # load table of all variables
# tb_vars_all <-
#   read_csv("pf_variable_table.csv") %>% 
#   suppressMessages()
# 
# # subset those that will be processed
# tb_vars <- 
#   tb_vars_all[var_index, ]


# var_input <- "precipitation"
# derived_var <- "one-day-max-precip"
# change_import <- "convert-mm"

var_input <- "maximum_temperature"
derived_var <- "one-day-max-tasmax"
change_import <- "convert-C"
dir_res <- "/mnt/pers_disk/pf_max_tasmax/"



# *****************************************************************************


# 1 PROCESS DERIVED VARIABLES -------------------------------------------------

for(dom in doms){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  # assemble table of needed files for calculation
  tb_files <-
    fn_data_table(var_input) %>% 
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
      
    })
    
    
    
    ## CALCULATE DERIVED VARIABLES ------------------------------------------
    
    # This section calculates derived variables from the concatenated file
    # (some vars use the annual files at dir_raw_data).
    # Derived variables that use the same input variable are calculated 
    # at once. This step uses function "fn_derived" from the "fn_derived.R" 
    # script loaded in the SETUP section above. Refer to it to see how each 
    # derived variable is obtained.
    
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc")
    
    if(file.exists(outfile)){
      file.remove(outfile)
      print(str_glue("      (old derived file removed)"))
    }
    
    
    print(str_glue("Calculating derived variables [{rcm_}] [{gcm_}]"))
    
    str_glue("cdo yearmax {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    # delete concatenated file
    unlink(dir_cat, recursive = T)
    
    # delete raw files
    unlink(dir_raw_data, recursive = T)
    
  }
  
}
  


# *****************************************************************************


# 2 WARMING LEVELS ------------------------------------------------------------


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


for(dom in doms){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
    
    ## IMPORT DERIVED VAR FILES --------
    
    # vector of files to import
    ff <- 
      dir_derived %>% 
      list.files() %>% 
      str_subset(dom) %>% 
      str_subset(derived_var)
    
    
    # import files into a list
    l_s <- 
      
      future_map(ff, function(f){
        
        # apply changes 
        if(change_import == "fix-time+convert-C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "fix-time+convert-mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "convert-C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "convert-mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "fix-time"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        } else if(change_import == "nothing"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        }
        
        
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
          
          mutate(v = set_units(v, NULL))
      })
    
    
    # Verify correct import
    print(str_glue("Imported:"))
    
    walk2(l_s, ff, function(s, f){
      
      yrs <-
        s %>% 
        st_get_dimension_values("time")
      
      range_time <- 
        yrs %>% 
        range()
      
      time_steps <- 
        yrs %>% 
        length()
      
      mod <- 
        f %>% 
        str_extract("(?<=yr_)[:alnum:]*_[:graph:]*(?=\\.nc)")
      
      print(str_glue("   {mod}: \t{range_time[1]} - {range_time[2]} ({time_steps} timesteps)"))
      
    })
    
    
    # If model rasters have different dimensions, warp to the smallest 
    size_rasters <- l_s %>% map(dim) %>% map_int(~.x[1]*.x[2])
    
    if(size_rasters %>% unique() %>% length() %>% {. > 1}){
      
      print("   ...warping...")
      
      smallest <- size_rasters %>% which.min()
      
      larger <- which(size_rasters != size_rasters[smallest])
      
      l_s[larger] <- 
        
        l_s[larger] %>% 
        map(function(s){
          
          st_warp(s, l_s[[smallest]] %>% slice(time, 1))
          
        })
      
    }
    
    
    ## SLICE BY WARMING LEVELS -------
    
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
            str_split("_", simplify = T) %>% 
            .[,5] %>% 
            str_remove(".nc")
          
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
          
        })
        
      })
    
    
    ## CALCULATE STATISTICS ---------------------------------------------------
    
    l_s_wl_stats <-
      
      # loop through warming levels
      imap(wls, function(wl, iwl){
        
        print(str_glue("Calculating stats WL {wl}"))
        
        s_max <- 
          l_s_wl %>%
          pluck(iwl) %>%
          map(st_apply, c(1,2), max) %>% 
          {do.call(c, c(., along = "model"))}
          
        s_model_max <- 
          s_max %>% 
          st_apply(c(1,2), function(x) {
            
            if (any(is.na(x))) {
              NA
            } else {
              which.max(x)
            }
            
          }, 
          .fname = "model_max") #%>%
          # mutate(model_max = case_when(model_max == 1 ~ s_gcm_rcm[1],
          #                              model_max == 2 ~ s_gcm_rcm[2],
          #                              model_max == 3 ~ s_gcm_rcm[3],
          #                              model_max == 4 ~ s_gcm_rcm[4],
          #                              model_max == 5 ~ s_gcm_rcm[5],
          #                              model_max == 6 ~ s_gcm_rcm[6]) %>% factor())
        
        
        s_stats_max <- 
          s_max %>% 
          st_apply(c(1,2), function(x) {
            
            if (any(is.na(x))) {
              
              a <- c(NA,NA,NA)
              
            } else {
              
              a <- c(max(x), mean(x), diff(range(x)))
              
            }
            
            res <- c(max_max = a[1],
                     max_mean = a[2],
                     max_range = a[3])
            
            return(res)
            
          },
          .fname = "band") %>% 
          aperm(c(2,3,1))
        
        
        res <- list(s_model_max, s_stats_max)
        
        return(res)
        
      })
    
    
    s_gcms <- 
      ff %>% 
      str_split("_", simplify = T) %>% 
      .[,5] %>% 
      str_remove(".nc") 
    
    s_gcms <- 
      case_when(str_detect(s_gcms, "HadGEM") ~ str_remove(s_gcms, "MOHC-"),
                str_detect(s_gcms, "MPI") ~ str_remove(s_gcms, "MPI-M-"),
                str_detect(s_gcms, "NorESM") ~ str_remove(s_gcms, "NCC-"),
                str_detect(s_gcms, "GFDL") ~ str_remove(s_gcms, "NOAA-GFDL-"),
                str_detect(s_gcms, "MIROC") ~ str_remove(s_gcms, "MIROC-")) 
    
    s_rcms <- 
      ff %>% 
      str_split("_", simplify = T) %>% 
      .[,4] %>% 
      str_remove(".nc")
    
    s_gcm_rcm <- str_c(s_rcms, "_", s_gcms)
    
    write_lines(s_gcm_rcm, str_glue("{dir_res}/{dom}_order_models.txt"))
    
    
    
    l_s_wl_stats %>% 
      transpose() %>% 
      pluck(1) %>% 
      {do.call(c, c(., along = "wls"))} %>% 
      fn_write_nc(str_glue("{dir_res}/{dom}_{derived_var}_max-model.nc"), "wls")
      
    
    walk2(1:3, c("max-max", "max-mean", "max-range"), function(i, nam) {
      l_s_wl_stats %>% 
        transpose() %>% 
        pluck(2) %>%
        map(slice, band, i) %>% 
        {do.call(c, c(., along = "wls"))} %>%
        fn_write_nc(str_glue("{dir_res}/{dom}_{derived_var}_{nam}.nc"), "wls")
    })
}





# *****************************************************************************


# 3 MOSAIC -------------------------------------------------------------------- 


# TEMPLATE DOMAIN MAPS

l_s_valid <-
  
  map(set_names(doms), function(dom){
    
    # load map
    s <-
      dir_res %>% 
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      
      str_subset(derived_var) %>%
      str_subset("max-max") %>%
      
      read_ncdf(ncsub = cbind(start = c(1, 1, 1),
                              count = c(NA,NA,1))) %>% # only 1 timestep
    
      suppressMessages() %>%
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

# land <- 
#   # "/mnt/bucket_cmip5/Probable_futures/irunde_scripts/create_a_dataset/04_rcm_buffered_ocean_mask.nc" %>% 
#   "buffered_ocean_mask.nc" %>% 
#   read_ncdf() %>%
#   st_warp(global) %>% 
#   setNames("a")


land <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/ne_110m_land.shp" %>% 
  st_read(quiet = T) %>% 
  mutate(a = 1) %>% 
  select(a) %>% 
  st_rasterize(global)



## MOSAIC --------


print(str_glue(" "))
print(str_glue("Mosaicking {derived_var}"))

final_name <- 
  # tb_vars %>% 
  # filter(var_derived == derived_var) %>% 
  # pull(var_final)
  derived_var

for (max_var in c("max-max", "max-mean")) {
  
  l_s <- 
    map(doms %>% set_names(), function(dom){
      
      print(dom)
      
      # load ensembled map 
      s <- 
        dir_res %>%
        list.files(full.names = T) %>%
        str_subset(dom) %>%
        str_subset(derived_var) %>%
        str_subset(max_var) %>% 
        read_ncdf() %>%
        suppressMessages()
      
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
        map(slice, wls, wl_pos) %>% 
        map(st_warp, global)
      
      # APPLY WEIGHTS
      l_s_weighted <- 
        
        map2(l_s_wl, l_s_weights, function(s, w){
          
          c(s %>% setNames("v"),
            w) %>% 
            
            mutate(v = v*weights) %>% 
            select(-weights)
          
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
  
  
  
  # Differences
  l_mos_wl_dif <- l_mos_wl
  
  l_mos_wl_dif[2:6] <-
    l_mos_wl[2:6] %>%
    map(function(s){
      
      s - l_mos_wl[[1]]
      
    })
  

  
  # Merge
  l_mos_wl <-
    do.call(c, c(l_mos_wl, along = "wls")) %>% 
    st_set_dimensions(3, values = as.numeric(wls)) %>% 
    setNames(max_var)
  
  # mask
  l_mos_wl[is.na(land)] <- NA_integer_
  
  # save
  fn_write_nc(l_mos_wl, 
              str_glue("{dir_res}/GLOBAL_{derived_var}_{max_var}.nc"),
              "wls")
  
  
  
  # Merge
  l_mos_wl_dif <-
    do.call(c, c(l_mos_wl_dif, along = "wls")) %>% 
    st_set_dimensions(3, values = as.numeric(wls)) %>% 
    setNames(max_var)
  
  # mask
  l_mos_wl_dif[is.na(land)] <- NA_integer_
  
  # save
  fn_write_nc(l_mos_wl_dif, 
              str_glue("{dir_res}/GLOBAL_{derived_var}_{max_var}_dif.nc"),
              "wls")
  
  
}


# 4 SAVE WT MAPS --------------------------------------------------------------

dir_res %>% 
  list.files(full.names = T) %>% 
  str_subset("GLOBAL") %>% 
  str_subset("nc") %>% 
  walk(function(s) {
    
    s_wt <- str_replace(s, ".nc", "_wt.tif")
    
    s %>% 
      read_ncdf(proxy = F) %>% 
      slice(wls, c(2,4,6)) %>% 
      st_set_dimensions(3, values = c(1,2,3)) %>% 
      st_warp(crs = "+proj=wintri +datum=WGS84 +no_defs +over") %>% 
      write_stars(s_wt)
    
  })



  

# max_max_wt %>%
#   slice(wls, 1) %>% 
#   setNames("v") %>% 
#   as_tibble() %>%
#   mutate(v = if_else(x < -1.1e7 | (x > 1.1e7 & (y > 0.7e7 | y < -0.7e7)), 
#                      NA, 
#                      v)) %>% 
#   
#   ggplot(aes(x,y,fill = v)) +
#   geom_raster() +
#   coord_equal(xlim = c(-1e7, 1.3e7),
#               ylim = c(-0.6e7, 0.8e7))




land <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/ne_110m_land.shp" %>% 
  st_read(quiet = T) %>% 
  mutate(a = 1) %>% 
  select(a)




# lats <- c(90:-90, -90:90, 90)
# longs <- c(rep(c(180, -180), each = 181), 180)
# 
# list(cbind(longs, lats)) %>%
#   st_polygon() %>%
#   st_sfc( # create sf geometry list column
#     crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
#   ) %>% 
#   st_sf() %>% 
#   mutate(a = 2) %>% 
#   bind_rows(land) %>% 
#   st_intersection() %>% 
#   select(1) %>% 
#   .[1,] %>% 
#   st_transform_proj(crs = crs_wintri) %>% 
#   plot()
