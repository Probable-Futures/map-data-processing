
library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore, workers = 10)

dir_data <- "/mnt/pers_disk/data_uncertainty_pf/"
# fs::dir_create(dir_data)



# *****************************************************************************

cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/average-daytime-temperature_v03.nc" %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download and aggregate ERA5 data

fs::dir_ls("/mnt/bucket_cmip5/Reanalysis_data/ERA5_raw_data/daily/maximum_temperature/") %>%
  str_subset(str_flatten(1971:2020, "|")) %>%
  future_walk(function(f) {

    f_gs <-
      str_replace(f, "/mnt/bucket_mine", "gs://cmip5_data")

    str_glue("gsutil cp {f_gs} {dir_data}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    str_glue("cdo yearmean {dir_data}{fs::path_file(f)} {dir_data}d_yr_{str_extract(f, '\\\\d{4}')}.nc") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    fs::file_delete(str_glue("{dir_data}{fs::path_file(f)}"))
    
  })
  



# Load

era <- 
  dir_data %>% 
  fs::dir_ls() %>%
  str_subset("_yr_") %>% 
  future_map(read_ncdf, proxy = F) %>% 
  suppressMessages() %>%
  do.call(c, .) %>% 
  setNames("era") %>% 
  mutate(era = era %>% units::set_units(degC)) %>% 
  units::drop_units()

era <- 
  era %>% 
  st_warp(cordex)

mask <- 
  cordex %>% 
  slice(wl, 1) %>% 
  slice(stats, 1)

era[is.na(mask)] <- NA


# Stats

era_wl05 <- era %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

era_wl10 <- era %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(era_wl05, era_wl10) %>%  
  future_map(function(s) {
    
    s %>% 
      st_apply(c(1,2), function(x) {
        
        if(any(is.na(x))){
          
          c(mean = NA,
            perc05 = NA, 
            perc50 = NA,
            perc95 = NA)
          
        } else {
          
          c(mean = mean(x),
            quantile(x, c(0.05, 0.5, 0.95)) %>%
              setNames(c("perc05", "perc50", "perc95")))
          
        }
        
      },
      .fname = "stats") %>% 
      aperm(c(2,3,1))
    
  })


dif <- 
  stats %>% 
  map2(list(slice(cordex, wl, 1), 
            slice(cordex, wl, 2)), 
       
       function(s, cor) {
         
         c(s, cor) %>% 
           mutate(dif = cordex - era) %>% 
           select(dif)
         
       })


dif_dif <- 
  c(stats[[2]] - stats[[1]],
    slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
  mutate(dif = cordex - era) %>% 
  select(dif)
  

dif[[1]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6),
                                          show.limits = T) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average daytime temperature: 0.5°C WL")


dif[[2]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6)) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average daytime temperature: 1.0°C WL")


dif_dif %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-3,3),
                                          # show.limits = T
                                          ) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average daytime temperature: 1.0°C - 0.5°C WL")


dir_data %>% 
  fs::dir_ls() %>% 
  fs::file_delete()




# ******************************************************************************


cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/average-nighttime-temperature_v03.nc" %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download and aggregate ERA5 data

fs::dir_ls("/mnt/bucket_cmip5/Reanalysis_data/ERA5_raw_data/daily/minimum_temperature/") %>%
  str_subset(str_flatten(1971:2020, "|")) %>%
  future_walk(function(f) {
    
    f_gs <-
      str_replace(f, "/mnt/bucket_mine", "gs://cmip5_data")
    
    str_glue("gsutil cp {f_gs} {dir_data}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    str_glue("cdo yearmean {dir_data}{fs::path_file(f)} {dir_data}d_yr_{str_extract(f, '\\\\d{4}')}.nc") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    fs::file_delete(str_glue("{dir_data}{fs::path_file(f)}"))
    
  })





# Load

era <- 
  dir_data %>% 
  fs::dir_ls() %>%
  str_subset("_yr_") %>% 
  future_map(read_ncdf, proxy = F) %>% 
  suppressMessages() %>%
  do.call(c, .) %>% 
  setNames("era") %>% 
  mutate(era = era %>% units::set_units(degC)) %>% 
  units::drop_units()

era <- 
  era %>% 
  st_warp(cordex)

mask <- 
  cordex %>% 
  slice(wl, 1) %>% 
  slice(stats, 1)

era[is.na(mask)] <- NA


# Stats

era_wl05 <- era %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

era_wl10 <- era %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(era_wl05, era_wl10) %>%  
  future_map(function(s) {
    
    s %>% 
      st_apply(c(1,2), function(x) {
        
        if(any(is.na(x))){
          
          c(mean = NA,
            perc05 = NA, 
            perc50 = NA,
            perc95 = NA)
          
        } else {
          
          c(mean = mean(x),
            quantile(x, c(0.05, 0.5, 0.95)) %>%
              setNames(c("perc05", "perc50", "perc95")))
          
        }
        
      },
      .fname = "stats") %>% 
      aperm(c(2,3,1))
    
  })


dif <- 
  stats %>% 
  map2(list(slice(cordex, wl, 1), 
            slice(cordex, wl, 2)), 
       
       function(s, cor) {
         
         c(s, cor) %>% 
           mutate(dif = cordex - era) %>% 
           select(dif)
         
       })


dif_dif <- 
  c(stats[[2]] - stats[[1]],
    slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
  mutate(dif = cordex - era) %>% 
  select(dif)


dif[[1]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6),
                                          show.limits = T) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average nighttime temperature: 0.5°C WL")


dif[[2]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6)) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average nighttime temperature: 1.0°C WL")


dif_dif %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-3,3),
                                          show.limits = T) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average nighttime temperature: 1.0°C - 0.5°C WL")


dir_data %>% 
  fs::dir_ls() %>% 
  fs::file_delete()



# *****************************************************************************

cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/average-temperature_v03.nc" %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download and aggregate ERA5 data

fs::dir_ls("/mnt/bucket_mine/era/monthly/mean-tasmean/") %>%
  str_subset(str_flatten(1971:2020, "|")) %>%
  future_walk(function(f) {
    
    f_gs <-
      str_replace(f, "/mnt/bucket_mine", "gs://clim_data_reg_useast1")
    
    str_glue("gsutil cp {f_gs} {dir_data}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    str_glue("cdo timmean {dir_data}{fs::path_file(f)} {dir_data}{str_replace(fs::path_file(f), '_mon_', '_yr_')}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
  })




# Load

era <- 
  dir_data %>% 
  fs::dir_ls() %>%
  str_subset("_yr_") %>% 
  future_map(read_ncdf, proxy = F) %>% 
  suppressMessages() %>%
  do.call(c, .) %>% 
  setNames("era") %>% 
  mutate(era = era %>% units::set_units(degC)) %>% 
  units::drop_units()

era <- 
  era %>% 
  st_warp(cordex)

mask <- 
  cordex %>% 
  slice(wl, 1) %>% 
  slice(stats, 1)

era[is.na(mask)] <- NA


# Stats

era_wl05 <- era %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

era_wl10 <- era %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(era_wl05, era_wl10) %>%  
  future_map(function(s) {
    
    s %>% 
      st_apply(c(1,2), function(x) {
        
        if(any(is.na(x))){
          
          c(mean = NA,
            perc05 = NA, 
            perc50 = NA,
            perc95 = NA)
          
        } else {
          
          c(mean = mean(x),
            quantile(x, c(0.05, 0.5, 0.95)) %>%
              setNames(c("perc05", "perc50", "perc95")))
          
        }
        
      },
      .fname = "stats") %>% 
      aperm(c(2,3,1))
    
  })



dif <- 
  stats %>% 
  map2(list(slice(cordex, wl, 1), 
            slice(cordex, wl, 2)), 
       
       function(s, cor) {
         
         c(s, cor) %>% 
           mutate(dif = cordex - era) %>% 
           select(dif)
         
       })


dif_dif <- 
  c(stats[[2]] - stats[[1]],
    slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
  mutate(dif = cordex - era) %>% 
  select(dif)


dif[[1]] %>% 
  # slice(stats, )
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6),
                                          show.limits = T) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average temperature: 0.5°C WL")


dif[[2]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-6,6)) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average temperature: 1.0°C WL")


dif_dif %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = c(-3,3),
                                          # show.limits = T
  ) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Average temperature: 1.0°C - 0.5°C WL")


dir_data %>% 
  fs::dir_ls() %>% 
  fs::file_delete()






