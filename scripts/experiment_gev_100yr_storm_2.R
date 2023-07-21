
library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)
library(supercells)
library(lmomRFA)

options(future.fork.enable = T)
plan(multicore)

dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_tmp <- "/mnt/pers_disk"

dom <- "AFR"

precip <- 
  "/mnt/bucket_mine/results/global_heat_pf/01_derived/AFR_total-precip_yr_REMO2015_MPI-M-MPI-ESM-LR.nc" %>% 
  read_ncdf() %>% 
  mutate(pr = set_units(pr, kg/m^2/d))

precip <- 
  precip %>% 
  st_set_dimensions(1, values = st_get_dimension_values(precip, 1) %>% round(1)) %>% 
  st_set_dimensions(2, values = st_get_dimension_values(precip, 2) %>% round(1)) %>% 
  st_set_crs(4326)

land <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/ne_110m_land.shp" %>% 
  st_read() %>% 
  mutate(a = 1) %>% 
  select(a)

land <- 
  land %>% 
  st_rasterize(st_as_stars(st_bbox(precip), dx = 0.2, values = NA)) %>%
  st_warp(precip %>% slice(time, 1))

precip[is.na(land)] <- NA



# initial regionalization

sp <- 
  supercells(precip %>% 
               filter(year(time) >= 1971,
                      year(time) <= 2000),
             step = 25,
             compactness = 30,
             dist_fun = "dtw",
             future = T)

sp <- 
  sp %>% 
  select(1:3)


# block max
block_max <- 
  "/mnt/pers_disk/block_max/AFR_1day-max-precip_yr_REMO2015_MPI-M-MPI-ESM-LR.nc" %>% 
  read_ncdf() %>% 
  st_set_dimensions(1, values = st_get_dimension_values(., 1) %>% round(1)) %>% 
  st_set_dimensions(2, values = st_get_dimension_values(., 2) %>% round(1)) %>% 
  st_set_crs(4326) %>% 
  mutate(pr = set_units(pr, kg/m^2/d))

# test re-assembling
block_max %>% 
  slice(time, 1) %>% #plot
  as_tibble() %>% # arrange(lat, lon) # sort by lat, then lon 
  pull(pr) %>% 
  matrix(nrow = dim(block_max)[1], ncol = dim(block_max)[2]) %>% 
  st_as_stars() %>% 
  plot()



l_sp <- split(sp, seq_len(nrow(sp)))


# BASELINE

baseline_block_max <- 
  block_max %>% 
  filter(year(time) >= 1971,
         year(time) <= 2000)

# loop through regions

levels_baseline <- 
  future_map_dfr(l_sp, function(r){
    
    print(str_glue("Processing region {r$supercells}"))
    
    reg_block_max <- 
      baseline_block_max %>% 
      st_crop(r)
      
    l_sites <-
      reg_block_max %>% 
      as_tibble() %>% 
      na.omit() %>% 
      group_by(lon, lat) %>%
      nest() 
    
    l_sites_split <- 
      l_sites %>% 
      pmap(function(data, ...){
        data %>% pull(pr) %>% drop_units()
      })
    
    reg_lmom <- 
      regsamlmu(l_sites_split) # obtains l-moments for each cell
    
    reg_gev <- 
      regfit(reg_lmom, "gev") # regional gev parameters
    
    reg_precip <- 
      sitequant(0.99, reg_gev) %>% as.vector()
    
    reg_precip <- 
      l_sites %>% 
      ungroup() %>% 
      select(-data) %>% 
      mutate(lev = reg_precip)
    
    return(reg_precip)
    
  })


levels_baseline <- 
  levels_baseline %>% 
  mutate(lon = round(lon, 1),
         lat = round(lat, 1))


levels_baseline %>%
  mutate(lev = raster::clamp(lev, upper = quantile(lev, 0.98))) %>% 
  ggplot(aes(lon, lat, fill = lev)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("viridis", rev = F) +
  coord_equal() +
  theme(axis.title = element_blank())


levels_baseline <- 
  block_max %>% 
  slice(time, 1) %>%
  as_tibble() %>% 
  select(-pr) %>% 
  mutate(lon = round(lon, 1),
         lat = round(lat, 1)) %>% 
 
  left_join(levels_baseline, by = c("lon", "lat")) %>% 
  
  pull(lev) %>% 
  matrix(nrow = dim(block_max)[1], ncol = dim(block_max)[2]) %>% 
  st_as_stars()
  
st_dimensions(levels_baseline) <- st_dimensions(block_max %>% slice(time, 1))



# WARMING LEVELS

wl_block_max <- 
  block_max %>% 
  filter(year(time) >= 2062-10,
         year(time) <= 2062+10)


# loop through regions

levels_wl3p0 <- 
  future_map_dfr(l_sp, function(r){
    
    # print(str_glue("Processing region {r$supercells}"))
    
    reg_block_max <- 
      wl_block_max %>% 
      st_crop(r)
    
    l_sites <-
      reg_block_max %>% 
      as_tibble() %>% 
      na.omit() %>% 
      group_by(lon, lat) %>%
      nest() 
    
    l_sites_split <- 
      l_sites %>% 
      pmap(function(data, ...){
        data %>% pull(pr) %>% drop_units()
      })
    
    reg_lmom <- 
      regsamlmu(l_sites_split) # obtains l-moments for each cell
    
    reg_gev <- 
      regfit(reg_lmom, "gev") # regional gev parameters
    
    reg_precip <- 
      sitequant(0.99, reg_gev) %>% as.vector()
    
    l_sites <- 
      l_sites %>% 
      ungroup() %>% 
      select(-data) %>% 
      mutate(lon = round(lon,1),
             lat = round(lat,1))
    
    tb_baseline_lev <- 
      l_sites %>% 
      left_join(levels_baseline %>% 
                  as_tibble() %>% 
                  mutate(lon = round(lon,1),
                         lat = round(lat,1)),
                by = c("lon", "lat"))
      
    reg_prob <- 
      map_dbl(seq_len(nrow(tb_baseline_lev)), function(i){
        
        sitequant(seq(0, 1, by = 0.001), reg_gev, i) %>% 
          {which.min(abs(. - tb_baseline_lev$A1[i]))} %>% 
          names() %>% 
          as.numeric() %>% 
          {1-.}
        
      })
    
    l_sites %>% 
      mutate(lev = reg_precip,
             prob = reg_prob)
    
    
  })


# levels_wl3p0 <- 
#   levels_wl3p0 %>% 
#   mutate(lon = round(lon, 1),
#          lat = round(lat, 1))


levels_wl3p0 %>%
  mutate(lev = raster::clamp(lev, upper = quantile(lev, 0.98))) %>% 
  ggplot(aes(lon, lat, fill = lev)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("viridis", rev = F) +
  coord_equal() +
  theme(axis.title = element_blank())

levels_wl3p0 %>%
  mutate(prob = raster::clamp(prob, upper = quantile(prob, 0.98))) %>% 
  ggplot(aes(lon, lat, fill = prob)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("viridis", rev = F) +
  coord_equal() +
  theme(axis.title = element_blank())


levels_baseline <- 
  block_max %>% 
  slice(time, 1) %>%
  as_tibble() %>% 
  select(-pr) %>% 
  mutate(lon = round(lon, 1),
         lat = round(lat, 1)) %>% 
  
  left_join(levels_baseline, by = c("lon", "lat")) %>% 
  
  pull(lev) %>% 
  matrix(nrow = dim(block_max)[1], ncol = dim(block_max)[2]) %>% 
  st_as_stars()

st_dimensions(levels_baseline) <- st_dimensions(block_max %>% slice(time, 1))


























# same
lmom::quagev(0.99, reg_gev$para)
regquant(0.99, reg_gev)
# this means reg_gev$para are the regional parameters
















  

# regional l moments
reg_12_lmom <- regsamlmu(l_precip_12_split) # obtains l moments for each cell

# fit gev
reg_12_gev <- regfit(reg_12_lmom, "gev") 
# basically an average of l moments across cells
# Corroborate:
reg_lmom %>% apply(2, mean) %>% .[-(1:2)]
reg_12_gev$rmom

# site amounts
reg_12_lev <- sitequant(0.99, reg_12_gev) %>% as.vector()

# regquant(0.99, reg_12_gev) + mean(l_precip_12_split[[1]])


sitequant(0.99, reg_gev, 1) -> q
sitequant(seq(0.005,0.995, 0.005), reg_gev, 1) %>% {which.min(abs(. - q))} %>% names() %>% as.numeric()


reg_12_lev <- raster::clamp(reg_12_lev, quantile(reg_12_lev, 0.02), quantile(reg_12_lev, 0.98))


reg_12_lev <- 
  l_precip_12 %>% 
  ungroup() %>% 
  mutate(lev = reg_12_lev)

reg_12_lev %>% 
  ggplot(aes(lon, lat, fill = lev)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("viridis", rev = T) +
  coord_equal()




