# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


source("scripts_v3/setup.R") # load main directory routes
source("scripts_v3/functions.R") # load functions

# directory where ensembles are stored
dir_ensembled <- str_glue("{dir_results}/02_ensembled")

# directory where resulting mosaics with be stored
dir_mosaicked <- str_glue("{dir_results}/03_mosaicked")


doms <- c("SEA", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM", "AUS")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")


# load table of all variables
source("scripts_v3/tb_vars_all.R")

# subset those that will be processed
tb_vars <-
  tb_vars_all[var_index, ]


derived_vars <- tb_vars$var_derived


# PRE-PROCESS -----------------------------------------------------------------
# setup grid and weights

# spei and fwi have gaps (tiles not processed; ocean)
if (any(str_detect(derived_vars, "spei|fwi"))) {
  template_var <- "total-precip"
} else {
  template_var <- derived_vars[1]
}


dir_pr_perc <- str_glue("{dir_disk}/pr_perc")

# TEMPLATE DOMAIN MAPS

l_s_valid <-
  map(set_names(doms), function(dom) {
    # load map
    s <-
      dir_pr_perc %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset(".tif") |>
      read_stars() %>%
      suppressMessages() %>%
      select(1) %>%
      adrop()

    # fix domains trespassing the 360 meridian
    if (dom == "EAS") {
      s <-
        s %>%
        filter(x < 180)
    } else if (dom == "AUS") {
      s1 <-
        s %>%
        filter(x < 180)

      s2 <-
        s %>%
        filter(x >= 180)

      s2 <-
        st_set_dimensions(
          s2,
          which = "x",
          values = st_get_dimension_values(s2, "x", center = F) - 360
        ) %>%
        st_set_crs(4326)

      # keep AUS split
      s <- list(AUS1 = s1, AUS2 = s2)
    }

    return(s)
  })

# append AUS parts separately
l_s_valid <-
  append(l_s_valid[1:9], l_s_valid[[10]])

# assign 1 to non NA grid cells
l_s_valid <-
  l_s_valid %>%
  map(function(s) {
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
  st_set_dimensions(c(1, 2), names = c("lon", "lat"))


# INVERSE DISTANCES

l_s_dist <-
  future_map(doms_2aus, function(dom) {
    if (dom != "AUS2") {
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
        mutate(
          dist = st_distance(., domain_bound),
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
  st_apply(
    c(1, 2),
    function(foo) {
      bar <- ifelse(is.na(foo), 0, 1)

      if (sum(bar) > 1) {
        sum(foo, na.rm = T)
      } else {
        NA
      }
    },
    FUTURE = T,
    .fname = "sum_intersect"
  )


# WEIGHTS PER DOMAIN

l_s_weights <-
  map(l_s_dist, function(s) {
    c(s, s_intersections) %>%

      # 1 if no intersection; domain's distance / summed distance otherwise
      mutate(weights = ifelse(is.na(sum_intersect) & !is.na(dist), 1, dist / sum_intersect)) %>%
      select(weights)
  })


# LAND MASK

land <-
  # "/mnt/bucket_cmip5/Probable_futures/irunde_scripts/create_a_dataset/04_rcm_buffered_ocean_mask.nc" %>%
  "buffered_ocean_mask.nc" %>%
  read_ncdf() %>%
  st_set_crs(4326) |>
  st_warp(global) %>%
  setNames("a")


# BARREN MASK

if (any(str_detect(derived_vars, "spei|fwi"))) {
  barren <-
    # "/mnt/bucket_cmip5/Probable_futures/land_module/maps/mask_layers/modis_barren_mask_ge90perc_regridto22kmwmean.tif" %>%
    "modis_barren_mask_ge90perc_regridto22kmwmean.tif" %>%
    read_stars() %>%
    st_warp(global) %>%
    setNames("barren")
}


# MOSAIC ----------------------------------------------------------------------

final_name <-
  tb_vars %>%
  filter(var_derived == derived_var) %>%
  pull(var_final)

l_s <-
  map(doms %>% set_names(), function(dom) {
    print(dom)

    # load ensembled map
    s <-
      dir_pr_perc %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset("tif") %>%
      read_stars %>%
      suppressMessages() |>
      adrop()

    # # fix domains trespassing the 360 meridian
    if (dom == "EAS") {
      s <-
        s %>%
        filter(x < 180)
    } else if (dom == "AUS") {
      s1 <-
        s %>%
        filter(x < 180)

      s2 <-
        s %>%
        filter(x >= 180)

      s2 <-
        st_set_dimensions(
          s2,
          which = "x",
          values = st_get_dimension_values(s2, "x", center = F) - 360
        ) %>%
        st_set_crs(4326)

      s <- list(AUS1 = s1, AUS2 = s2)
    }

    return(s)
  })

l_s <- append(l_s[1:9], l_s[[10]])

l_s_wl <-
  l_s %>%
  map(st_warp, global)

# APPLY WEIGHTS
l_s_weighted <-
  map2(l_s_wl, l_s_weights, function(s, w) {
    orig_names <- names(s)

    map(orig_names, function(v_) {
      c(s %>% select(all_of(v_)) %>% setNames("v"), w) %>%

        mutate(v = v * weights) %>%
        select(-weights) %>%
        setNames(v_)
    }) %>%
      do.call(c, .)
  })

# MOSAIC
mos <-
  l_s_weighted %>%
  imap(~ setNames(.x, .y)) %>%
  unname() %>%
  do.call(c, .) %>%
  merge(name = "doms") %>%

  st_apply(
    c(1, 2),
    function(foo) {
      if (all(is.na(foo))) {
        NA
      } else {
        sum(foo, na.rm = T)
      }
    },
    FUTURE = F
  ) |>
  setNames("perc_10")

write_stars(mos, str_glue("{dir_pr_perc}/mos.tif"))


# plot

land_pol <-
  "/mnt/bucket_mine/misc_data/physical/ne_110m_land/" |>
  st_read(quiet = T)

land_df <-
  land_pol %>%
  st_coordinates %>%
  as_tibble %>%
  mutate(L = paste0(L1, "_", L2))

tb <-
  mos |>
  as_tibble()


ggplot() +
  geom_raster(data = tb, aes(lon, lat, fill = perc_10)) +
  geom_path(data = land_df, aes(X, Y, group = L), linewidth = 0.25) +
  coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
  theme(axis.title = element_blank(), legend.position = "bottom") +
  colorspace::scale_fill_binned_sequential(
    "plasma",
    na.value = "transparent",
    guide = guide_colorsteps(barheight = 0.5, barwidth = 24, title.position = "left", even.steps = T),
    name = "mm",
    rev = T,
    limits = c(-1, 10),
    # trans = "exp",
    breaks = c(-Inf, 0, 1, 2, 4, 8, Inf),
    oob = scales::squish
  ) +
  labs(title = "10th percentile 1-day precip [WL: 0.5°C]")
