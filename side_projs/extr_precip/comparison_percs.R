library(tidyverse)
library(stars)
sf_use_s2(F)

dir_data <- "/mnt/pers_disk/results/"

land_pol <-
  "/mnt/bucket_mine/misc_data/physical/ne_110m_land/" |>
  st_read(quiet = T)

land_df <-
  land_pol %>%
  st_coordinates %>%
  as_tibble %>%
  mutate(L = paste0(L1, "_", L2))

ff <-
  dir_data |>
  fs::dir_ls(regexp = "GLOBAL")


s_0p5 <-
  ff[1] |>
  read_mdim() |>
  # st_downsample(c(1, 1)) |>
  units::drop_units() |>
  st_crop(land_pol)

tb_0p5 <-
  s_0p5 |>
  as_tibble()

pp_0p5 <-
  map(c("perc_98", "perc_99", "perc_100"), \(perc) {
    ggplot() +
      geom_raster(
        data = tb_0p5,
        aes(lon, lat, fill = .data[[perc]])
      ) +
      geom_path(data = land_df, aes(X, Y, group = L), linewidth = 0.25) +
      scale_fill_viridis_c(
        limits = quantile(tb_0p5 |> pull(.data[[perc]]), c(0.02, 0.98), na.rm = TRUE),
        oob = scales::squish,
        na.value = "transparent",
        name = "mm",
        guide = guide_colorbar(
          barheight = 0.5,
          barwidth = 24,
          title.position = "left"
        )
      ) +
      coord_cartesian(ylim = c(-55, 75), xlim = c(-155, 160)) +
      theme(legend.position = "bottom", axis.title = element_blank())
  })

pp_0p5[[3]]

tb_0p5_sub <-
  tb_0p5 |>
  filter(lon <= -75, lon >= -101) |>
  filter(lat <= 50, lat >= 24)

ggplot() +
  geom_raster(
    data = tb_0p5_sub,
    aes(lon, lat, fill = .data[[perc]])
  ) +
  geom_path(data = land_df, aes(X, Y, group = L), linewidth = 0.25) +
  scale_fill_viridis_c(
    limits = quantile(tb_0p5_sub |> pull(.data[[perc]]), c(0.02, 0.98), na.rm = TRUE),
    oob = scales::squish,
    na.value = "transparent",
    name = "mm",
    guide = guide_colorbar(
      barheight = 0.5,
      barwidth = 24,
      title.position = "left"
    )
  ) +
  coord_equal(expand = F, xlim = c(-101, -75), ylim = c(24, 50)) +
  theme(legend.position = "bottom", axis.title = element_blank())


s_3p0 <-
  ff[6] |>
  read_mdim() |>
  units::drop_units() |>
  st_crop(land_pol)

tb_dif <-
  as_tibble(s_3p0 - s_0p5)

tb_dif_sub <-
  tb_dif |>
  filter(lon <= -75, lon >= -101) |>
  filter(lat <= 50, lat >= 24)

ggplot() +
  geom_raster(
    data = tb_dif_sub,
    aes(lon, lat, fill = .data[[perc]])
  ) +
  geom_path(data = land_df, aes(X, Y, group = L), linewidth = 0.25) +
  colorspace::scale_fill_continuous_diverging(
    limits = quantile(tb_dif_sub |> pull(.data[[perc]]), c(0.02, 0.98), na.rm = TRUE),
    oob = scales::squish,
    na.value = "transparent",
    name = "mm",
    rev = T,
    guide = guide_colorbar(
      barheight = 0.5,
      barwidth = 24,
      title.position = "left"
    )
  ) +
  coord_equal(expand = F, xlim = c(-101, -75), ylim = c(24, 50)) +
  theme(legend.position = "bottom", axis.title = element_blank())
