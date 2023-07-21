
"/mnt/bucket_cmip5/Probable_futures/water_module/rcm_data/annual_sum_pr/annual_by_time_span/" %>% 
  list.files(full.names = T) %>% 
  str_subset("AFR") %>% 
  str_subset("19712000") %>% 
  
  map(read_ncdf) -> foo_1

foo_1_2 <- 
  foo_1 %>% 
  {do.call(c, c(., along = 3))}

foo_1_2 %>% 
  st_apply(c(1,2), quantile, prob = 0.5, na.rm = T, FUTURE = T) -> bar_1



"/mnt/bucket_mine/results/global_heat_pf/02_ensembled/AFR_total-precip_ensemble.nc" %>% 
  read_ncdf() -> foo_2

foo_2 %>% 
  select(perc50) %>% 
  slice(warming_levels, 1) -> bar_2

bar_1b <- 
  bar_1 %>% 
  st_set_dimensions("lon", values = st_get_dimension_values(bar_1, "lon") %>% round(1) %>% {.- 0.1}) %>% 
  st_set_dimensions("lat", values = st_get_dimension_values(bar_1, "lat") %>% round(1) %>% {. - 0.1}) %>% 
  st_set_crs(4326)

dif <- bar_1b - bar_2

land <- "/mnt/bucket_mine/misc_data/ne_110m_land/ne_110m_land.shp" %>% 
  st_read() %>% 
  mutate(a = 1) %>% 
  select(a)


ggplot() +
  geom_raster(data = as_tibble(dif), aes(lon, lat, fill = quantile)) +
  geom_sf(data = land, fill = NA) +
  coord_sf(xlim = c(-10,30), ylim = c(-20,20)) +
  # coord_sf(xlim = c(-20,55), ylim = c(-30,40)) +
  colorspace::scale_fill_continuous_diverging()
