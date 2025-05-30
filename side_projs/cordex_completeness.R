
library(tidyverse)
library(stars)
library(furrr)
box::use(../functions/general_tools[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


dir_temp <- "/mnt/pers_disk/tmp2"
fs::dir_create(dir_temp)


# bucket directories
dir_gs_remo <- "gs://cmip5_data/RCM_regridded_data/REMO2015"
dir_gs_regcm <- "gs://cmip5_data/RCM_regridded_data/CORDEX_22"

# minimum num of years expected in data
years_min <- seq(1970, 2099)






var <- "precipitation" |> set_names("pr")


# loop through RCMs 
walk2(c(dir_gs_remo, dir_gs_regcm), c("REMO2015", "RegCM4"), \(dir, rcm){
  
  # dir = dir_gs_remo
  # rcm = "REMO2015"
  
  print(str_glue("  RCM: {rcm}"))
  
  
  df_1rcm_1var <-
    
    # loop through domains
    map_dfr(c("NAM", "CAM", "SAM", "EUR", "AFR", "CAS", "WAS", "EAS", "SEA", "AUS"), \(dom){
      
      # dom = "NAM"
      
      print(str_glue("    domain: {dom}")) 
      
      dir_gs <- str_glue("{dir}/{dom}/daily/{var}")
      
      # get all files
      ff_all <- 
        rt_gs_list_files(dir_gs) |> 
        fs::path_file() |> 
        str_subset("historical|rcp85") |> 
        str_subset("nc..*$", negate = T) |> 
        str_subset("CNRM", negate = T) |> 
        str_subset("ICHEC", negate = T)
      
      
      # 1st pass
      # subset based on XXXXXXX-XXXXXX date format
      ff_1 <- 
        ff_all |> 
        str_subset(str_glue('{names(var)}_{dom}-11|22_[^_]*_[:alnum:]*_r1i1p1_[^_]*_v.*_day_\\d{{8}}-\\d{{8}}.nc')) 
        
      
      
      if (length(ff_1) > 0) {
        
        # as df
        df_ff_1 <- 
          tibble(file = ff_1) |> 
          separate(file, 
                   into = c("var", "domain", "gcm", "scenario", "ensemble", "rcm", "version", "freq", "dates"), 
                   sep = "_", 
                   remove = FALSE
          ) |> 
          separate(dates, into = c("start_date", "end_date"), sep = "-") |> 
          mutate(across(c(start_date, end_date), \(x) x |> str_remove(".nc") |> as_date())) |> 
          filter(year(start_date) >= 1970)
        
        
        # extract gcm names
        gcms <- 
          unique(df_ff_1$gcm)
        
        print(gcms)
        
        
        df_1rcm_1var_1dom <-
          
          # loop through gcms
          map(gcms, \(gcm){
            
            # gcm = gcms[1]
            
            print(str_glue("      gcm: {gcm}"))  
            
            # subset gcm
            df_1rcm_1var_1dom_1gcm <- 
              df_ff_1 |> 
              filter(gcm == {{gcm}}) |>  
              arrange(start_date)
            
            # extract all years based on file names
            years_gcm <- 
              df_1rcm_1var_1dom_1gcm |> 
              pmap(\(start_date, end_date, ...){
                
                seq(year(start_date), year(end_date))
                
              }) |> 
              unlist()
            
            
            # what years are not in min years expected
            years_missing <- 
              years_min[which(!years_min %in% years_gcm)]
            
            
            # if there are years missing, run 2nd pass
            # look for files with only one year
            if (length(years_missing) > 0) {
              
              
              # 2nd pass
              ff_2 <- 
                years_missing |> 
                map_chr(\(yr){
                  
                  x <- 
                    ff_all |> 
                    str_subset(str_glue('{names(var)}_{dom}-11|22_{gcm}_[:alnum:]*_r1i1p1_[^_]*_v.*_day_{yr}.nc')) 
                    
                  # if there are no files, missing label
                  if (length(x) < 1) x <- "missing"
                  
                  return(x)
                  
                })
              
              
              # if there are no files to fill the missing years, 
              # return the table from the first pass
              if (all(ff_2 == "missing")) {
                
                df_f <- df_1rcm_1var_1dom_1gcm
              
              # if there are files to fill the missing years,
              # format as df
              } else {
                
                df_f <- 
                  tibble(file = ff_2[ff_2 != "missing"]) |> 
                  separate(file, 
                           into = c("var", "domain", "gcm", "scenario", "ensemble", "rcm", "version", "freq", "start_date"), 
                           sep = "_", 
                           remove = FALSE
                  ) |> 
                  mutate(start_date = str_glue("{str_remove(start_date, '.nc')}-01-01") |> as_date()) |> 
                  mutate(end_date = str_glue("{year(start_date)}-12-30") |> as_date()) |> 
                  
                  bind_rows(df_1rcm_1var_1dom_1gcm) |> 
                  arrange(start_date)
                
              }
              
              
              
              
            } else {
              
              df_f <- df_1rcm_1var_1dom_1gcm
              
            }
            
            
            # add gs directory to file column
            df_f <- 
              df_f |>
              mutate(file = str_glue("{dir_gs}/{file}"))
            
            # identify file from a february from a leap year
            leap_yr <- 
              df_f |> 
              filter(year(end_date) >= 2000,
                     year(start_date) <= 2000) |> 
              pull(file)
            
            # download 
            leap_yr <- 
              rt_gs_download_files(leap_yr, dir_temp, quiet = T)
            
            # get time dimension
            time_dim <- 
              read_ncdf(leap_yr, proxy = T) |> 
              suppressMessages() |> 
              st_get_dimension_values("time")
            
            # clean up
            fs::file_delete(leap_yr)
            
            # get the maximum day to identify calendar
            max_feb <- 
              time_dim[year(time_dim) == 2000 & month(time_dim) == 2] |> 
              day() |> 
              max() |> 
              suppressWarnings()
            
            cal_spec <-
              case_when(max_feb == 30 ~ "360_day",
                        max_feb == 29 ~ "gregorian",
                        max_feb == 28 ~ "noleap")
            
            
            # edit dates columns with actual dates from the time dimensions
            df_f <- 
              df_f |> 
              mutate(new_dates = future_pmap(df_f, \(file, ...){
                
                f <- rt_gs_download_files(file, dir_temp, quiet = T)
                
                time_dim <- 
                  read_ncdf(f, proxy = T) |> 
                  suppressMessages() |> 
                  st_get_dimension_values("time") |> 
                  as.character() |> 
                  str_sub(end = 10)
                
                fs::file_delete(f)
                
                return(list(start_date = first(time_dim),
                            end_date = last(time_dim)))
                
              })) |> 
              select(-start_date, -end_date) |> 
              unnest_wider(new_dates) |> 
              mutate(calendar = cal_spec)
            
            
            # verify completeness:
            # all dates are included
            
            start_date <- PCICt::as.PCICt(first(df_f$start_date), cal = cal_spec)
            end_date <- PCICt::as.PCICt(last(df_f$end_date), cal = cal_spec)
            
            dates_reference <-
              seq(start_date, end_date, by = "1 day") |> 
              as.character()
            
            
            dates_gcm <- 
              df_f |> 
              pmap(\(start_date, end_date, ...){
                
                start_date <- PCICt::as.PCICt(start_date, cal = cal_spec)
                end_date <- PCICt::as.PCICt(end_date, cal = cal_spec)
                
                seq(start_date, end_date, by = "1 day") |> 
                  as.character()
                
              }) |> 
              unlist()
            
            dates_missing <- list(which(!dates_gcm %in% dates_reference))
            same_length <- length(dates_gcm) == length(dates_reference)
            
            df_check <- 
              tibble(var = names(var),
                     domain = dom,
                     gcm = gcm,
                     rcm = rcm,
                     missing_dates = dates_missing,
                     repeated_dates = !same_length)
            
            
            
            return(list(df_f, df_check))
            
          })
        
        
        df_1rcm_1var_1dom <- 
          df_1rcm_1var_1dom |>
          transpose() |> 
          map(bind_rows)
        
        df_1rcm_1var_1dom |> 
          pluck(1) |> 
          write_csv(str_glue("{dir_temp}/df-files_{names(var)}_{dom}_{rcm}.csv"))
        
        df_1rcm_1var_1dom |> 
          pluck(2) |> 
          write_csv(str_glue("{dir_temp}/df-check_{names(var)}_{dom}_{rcm}.csv"))
        
      }
      
    })
  
})


dir_temp |> 
  fs::dir_ls() |> 
  str_subset("df-check") |> 
  map_dfr(read_csv) |> 
  write_csv(str_glue("data_tables/df-check_{var}.csv"))

df_files <- 
  dir_temp |> 
  fs::dir_ls() |> 
  str_subset("df-files") |> 
  map_dfr(read_csv) |> 
  write_csv(str_glue("data_tables/df-files_{var}.csv"))

fs::dir_delete(dir_temp)











