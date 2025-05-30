
yr <- 2012



"/mnt/bucket_mine/cordex/annual_aggregates/wettest_day/REMO2015_MOHC-HadGEM2-ES_SEA_wettest-day_yr_2012-01-01.nc" |> 
  str_glue() |> 
  read_ncdf() |> 
  units::drop_units() -> a

"/mnt/bucket_mine/results/global_heat_pf/01_derived/SEA_one-day-max-precip_yr_REMO2015_MOHC-HadGEM2-ES.nc" |> 
  read_ncdf() |> 
  filter(year(time) == yr) |> 
  adrop() |> 
  mutate(pr = pr |> units::set_units(kg/m^2/d)) |> 
  units::drop_units() |> 
  st_warp(a) -> b

b - a -> foo

plot(foo)
