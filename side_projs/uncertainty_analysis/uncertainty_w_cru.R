

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore, workers = 10)


dir_data <- "/mnt/pers_disk/data_uncertainty_pf/"
# fs::dir_create(dir_data)

vol <- "water"
var_cordex <- "change-total-annual-precipitation_v03"
var_cru <- "pre"
fn_agg <- function(f) sum(f)
lim_1 <- c(-500,500)
lim_2 <- c(-300,300)
revv <- T
title_1 <- "Total annual precipitation"
title_2 <- "Change in total annual precip. rel. to 0.5 WL"


vol <- "heat"
var_cordex <- "average-temperature_v03"
var_cru <- "tmp"
fn_agg <- function(f) mean(f)
lim_1 <- c(-6,6)
lim_2 <- lim_1
lim_3 <- c(-3,3)
revv <- F
title_1 <- "Average temperature"
title_2 <- title_1
title_3 <- title_1


# "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/days-above-38C_v03.nc"
vol <- "heat"
var_cordex <- "days-above-38C_v03"
var_cru <- "tmx"
fn_agg <- function(f) sum(f >= 38)
lim_1 <- c(-6,6)
lim_2 <- lim_1
lim_3 <- c(-3,3)
revv <- F
title_1 <- "Average temperature"
title_2 <- title_1
title_3 <- title_1




# *****************************************************************************

cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/{vol}/v3/{var_cordex}.nc" %>% 
  str_glue() %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download/load and aggregate CRU data

ss <- 
  map(c(1971,1981,1991,2001,2011), function(i) {
  
  print(i)
  
  s <- 
    read_mdim(
      str_glue("/vsigzip//vsicurl/https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.07/cruts.2304141047.v4.07/{var_cru}/cru_ts4.07.{i}.{i+9}.{var_cru}.dat.nc.gz"),
      variable = var_cru) %>% 
    units::drop_units()
  
  s_annual <- 
    aggregate(s, by = "1 year", fn_agg) %>%
    aperm(c(2,3,1))
  
  s_warped <- 
    s_annual %>% 
    st_warp(cordex)
  
  return(s_warped)
  
})


cru <- 
  do.call(c, ss) %>% 
  setNames("cru")


# Stats

cru_wl05 <- cru %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

cru_wl10 <- cru %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(cru_wl05, cru_wl10) %>%  
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




if (str_detect(var_cordex, "change")) {
  
  stats <-
    list(stats[[1]],
         stats[[2]] - stats[[1]])
    
}


dif <- 
  stats %>% 
  map2(list(slice(cordex, wl, 1), 
            slice(cordex, wl, 2)), 
       
       function(s, cor) {
         
         c(s, cor) %>% 
           mutate(dif = cordex - cru) %>% 
           select(dif)
         
       })


if (!str_detect(var_cordex, "change")) {
  
  dif_dif <- 
    c(stats[[2]] - stats[[1]],
      slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
    mutate(dif = cordex - cru) %>% 
    select(dif)
  
}


lim_1 <- 
  dif[[1]] %>% 
  slice(stats, 1) %>% 
  pull() %>% 
  quantile(c(0.25, 0.75), na.rm = T) %>% 
  round() %>% 
  abs() %>% 
  max()

dif[[1]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          # limits = c((-1*lim_1), lim_1),
                                          limits = c(-200,200),
                                          show.limits = T,
                                          rev = revv
                                          ) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = str_glue("{title_1}: 0.5°C WL"))


dif[[2]] %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = dif)) +
  geom_raster() + 
  colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                          n.breaks = 9,
                                          guide = guide_colorsteps(barheight = 12,
                                                                   barwidth = 0.6),
                                          name = NULL,
                                          limits = lim_2,
                                          rev = revv
                                          ) +
  facet_wrap(~stats, ncol = 2) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = str_glue("{title_2}: 1.0°C WL"))


if (!str_detect(var_cordex, "change")) {
  
  dif_dif %>% 
    as_tibble() %>% 
    ggplot(aes(lon, lat, fill = dif)) +
    geom_raster() + 
    colorspace::scale_fill_binned_diverging(na.value = "transparent",
                                            n.breaks = 9,
                                            guide = guide_colorsteps(barheight = 12,
                                                                     barwidth = 0.6),
                                            name = NULL,
                                            limits = lim_3,
                                            rev = revv
                                            # show.limits = T
                                            ) +
    facet_wrap(~stats, ncol = 2) +
    coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
    theme(axis.title = element_blank()) +
    labs(title = str_glue("{title_3}: 1.0°C - 0.5°C WL"))

}





# ******************************************************************************


cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/average-nighttime-temperature_v03.nc" %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download/load and aggregate CRU data

ss <- 
  map(c(1971,1981,1991,2001,2011), function(i) {
    
    print(i)
    
    s <- 
      read_mdim(
        str_glue("/vsigzip//vsicurl/https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.07/cruts.2304141047.v4.07/tmn/cru_ts4.07.{i}.{i+9}.tmn.dat.nc.gz"),
        variable = "tmn") %>% 
      units::drop_units()
    
    s_annual <- 
      aggregate(s, by = "1 year", mean) %>% 
      aperm(c(2,3,1))
    
    s_warped <- 
      s_annual %>% 
      st_warp(cordex)
    
    return(s_warped)
    
  })


cru <- 
  do.call(c, ss) %>% 
  setNames("cru")


# Stats

cru_wl05 <- cru %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

cru_wl10 <- cru %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(cru_wl05, cru_wl10) %>%  
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
           mutate(dif = cordex - cru) %>% 
           select(dif)
         
       })


dif_dif <- 
  c(stats[[2]] - stats[[1]],
    slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
  mutate(dif = cordex - cru) %>% 
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





# *****************************************************************************

cordex <- 
  "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/heat/v3/average-temperature_v03.nc" %>% 
  read_ncdf() %>%
  slice(wl, 1:2) %>% 
  merge(name = "stats") %>% 
  setNames("cordex")


# Download/load and aggregate CRU data

ss <- 
  map(c(1971,1981,1991,2001,2011), function(i) {
    
    print(i)
    
    s <- 
      read_mdim(
        str_glue("/vsigzip//vsicurl/https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.07/cruts.2304141047.v4.07/tmp/cru_ts4.07.{i}.{i+9}.tmp.dat.nc.gz"),
        variable = "tmp") %>% 
      units::drop_units()
    
    s_annual <- 
      aggregate(s, by = "1 year", mean) %>% 
      aperm(c(2,3,1))
    
    s_warped <- 
      s_annual %>% 
      st_warp(cordex)
    
    return(s_warped)
    
  })


cru <- 
  do.call(c, ss) %>% 
  setNames("cru")


# Stats

cru_wl05 <- cru %>% filter(year(time) >= 1971,
                           year(time) <= 2000)

cru_wl10 <- cru %>% filter(year(time) >= 2000,
                           year(time) <= 2020)


stats <- 
  list(cru_wl05, cru_wl10) %>%  
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
           mutate(dif = cordex - cru) %>% 
           select(dif)
         
       })


dif_dif <- 
  c(stats[[2]] - stats[[1]],
    slice(cordex, wl, 2) - slice(cordex, wl, 1)) %>% 
  mutate(dif = cordex - cru) %>% 
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










# ****************************************************************************

i = 1971
s <- 
  read_mdim(
    str_glue("/vsigzip//vsicurl/https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.07/cruts.2304141047.v4.07/tmx/cru_ts4.07.{i}.{i+9}.tmx.dat.nc.gz"),
    variable = "stn")


s %>% 
  slice(time, 1) %>% 
  as_tibble() %>% 
  ggplot(aes(lon, lat, fill = stn)) +
  geom_raster() +
  colorspace::scale_fill_binned_sequential("viridis",
                                           na.value = "transparent",
                                           guide = guide_colorsteps(barheight = 12,
                                                                    barwidth = 0.6),
                                           n.breaks = 9) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank()) +
  labs(title = "Number of stations contributing to each interpolation")
 