fn_derived <- function(derived_var) {
  outfile <-
    str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc") # v32 for new

  if (file.exists(outfile)) {
    file.remove(outfile)
    print(str_glue("      (old derived file removed)"))
  }

  # HEAT VOLUME: TASMAX -------------------------------------------------------

  if (derived_var == "days-gte-32C-tasmax") {
    set_units(32, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-35C-tasmax") {
    set_units(35, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-38C-tasmax") {
    set_units(38, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-45C-tasmax") {
    set_units(45, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "ten-hottest-days-tasmax") {
    dir_temp <- str_glue("{dir_disk}/dir_temp")
    dir.create(dir_temp)

    # loop through annual files
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(f) {
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]

        # sort across time >> slice last 10 days >> calculate the mean
        str_glue(
          "cdo -timmean -seltimestep,{time_length-9}/{time_length} -timsort {dir_raw_data}/{f} {dir_temp}/mean_sel_{f}"
        ) %>%
          system(ignore.stdout = T, ignore.stderr = T)
      })

    # concatenate and save
    dir_temp %>%
      list.files(full.names = T) %>%
      str_flatten(" ") %>%

      {
        system(str_glue("cdo cat {.} {outfile}"), ignore.stdout = T, ignore.stderr = T)
      }

    # delete intermediate files
    unlink(dir_temp, recursive = T)

    # *************
  } else if (derived_var == "mean-tasmax") {
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-lt-0C-tasmax") {
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -ltc,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "prop-days-gte-b90perc-3dayrunmean-tasmax") {
    # select 90 hottest days per year within baseline
    dir_raw_data %>%
      list.files() %>%
      str_subset(seq(1971, 2000) %>% str_flatten("|")) %>%
      future_walk(function(f) {
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]

        # sort across time >> slice last 90 days
        str_glue(
          "cdo -seltimestep,{time_length-89}/{time_length} -timsort {dir_raw_data}/{f} {dir_cat}/mean_sel_{f}"
        ) %>%
          system(ignore.stdout = T, ignore.stderr = T)
      })

    # concatenate
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>%
      str_flatten(" ") %>%

      {
        system(
          str_glue("cdo cat {.} {dir_cat}/ninety_hottest_days.nc"),
          ignore.stdout = T,
          ignore.stderr = T
        )
      }

    # remove intermediate files
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>%
      walk(file.remove)

    # obtain threshold (90th percentile)
    "cdo timpctl,90 {dir_cat}/ninety_hottest_days.nc -timmin {dir_cat}/ninety_hottest_days.nc -timmax {dir_cat}/ninety_hottest_days.nc {dir_cat}/threshold.nc" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # 3-day running mean
    "cdo runmean,3 {dir_cat}/{v}_cat.nc {dir_cat}/run_mean.nc" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)
    # The time of outfile is determined by the time in the middle of all contributing timesteps of infile.
    # This can be change with the CDO option --timestat_date <first|middle|last>.

    # obtain prop of days above threshold
    "cdo yearmean -gec,0 -sub {dir_cat}/run_mean.nc {dir_cat}/threshold.nc {outfile}" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # remove additional intermediate files
    file.remove(
      str_glue("{dir_cat}/ninety_hottest_days.nc"),
      str_glue("{dir_cat}/run_mean.nc"),
      str_glue("{dir_cat}/threshold.nc")
    )

    # HEAT VOLUME: TAS --------------------------------------------------------
  } else if (derived_var == "mean-tasmean") {
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # HEAT VOLUME: TASMIN -----------------------------------------------------
  } else if (derived_var == "days-gte-20C-tasmin") {
    set_units(20, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-25C-tasmin") {
    set_units(25, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "ten-hottest-days-tasmin") {
    dir_temp <- str_glue("{dir_disk}/dir_temp")
    dir.create(dir_temp)

    # loop through annual files
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(f) {
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]

        # sort across time >> slice last 10 days >> calculate the mean
        str_glue(
          "cdo -timmean -seltimestep,{time_length-9}/{time_length} -timsort {dir_raw_data}/{f} {dir_temp}/mean_sel_{f}"
        ) %>%
          system(ignore.stdout = T, ignore.stderr = T)
      })

    # concatenate and save
    dir_temp %>%
      list.files(full.names = T) %>%
      str_flatten(" ") %>%

      {
        system(str_glue("cdo cat {.} {outfile}"), ignore.stdout = T, ignore.stderr = T)
      }

    # delete intermediate files
    unlink(dir_temp, recursive = T)

    # *************
  } else if (derived_var == "mean-tasmin") {
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-lt-0C-tasmin") {
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k

    str_glue("cdo -yearsum -ltc,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "prop-days-gte-b90perc-3dayrunmean-tasmin") {
    # select 90 hottest days per year within baseline
    dir_raw_data %>%
      list.files() %>%
      str_subset(seq(1971, 2000) %>% str_flatten("|")) %>%
      future_walk(function(f) {
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]

        # sort across time >> slice last 90 days
        str_glue(
          "cdo -seltimestep,{time_length-89}/{time_length} -timsort {dir_raw_data}/{f} {dir_cat}/mean_sel_{f}"
        ) %>%
          system(ignore.stdout = T, ignore.stderr = T)
      })

    # concatenate
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>%
      str_flatten(" ") %>%

      {
        system(
          str_glue("cdo cat {.} {dir_cat}/ninety_hottest_days.nc"),
          ignore.stdout = T,
          ignore.stderr = T
        )
      }

    # remove intermediate files
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>%
      walk(file.remove)

    # obtain threshold (90th percentile)
    "cdo timpctl,90 {dir_cat}/ninety_hottest_days.nc -timmin {dir_cat}/ninety_hottest_days.nc -timmax {dir_cat}/ninety_hottest_days.nc {dir_cat}/threshold.nc" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # 3-day running mean
    "cdo runmean,3 {dir_cat}/{v}_cat.nc {dir_cat}/run_mean.nc" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)
    # The time of outfile is determined by the time in the middle of all contributing timesteps of infile.
    # This can be change with the CDO option --timestat_date <first|middle|last>.

    # obtain prop of days above threshold
    "cdo yearmean -gec,0 -sub {dir_cat}/run_mean.nc {dir_cat}/threshold.nc {outfile}" %>%
      str_glue() %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # remove additional intermediate files
    file.remove(
      str_glue("{dir_cat}/ninety_hottest_days.nc"),
      str_glue("{dir_cat}/run_mean.nc"),
      str_glue("{dir_cat}/threshold.nc")
    )

    # HEAT VOLUME: WETBULB TEMPERATURE ----------------------------------------
  } else if (derived_var == "days-gte-26C-wetbulb") {
    lim_k <- 26

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-28C-wetbulb") {
    lim_k <- 28

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-30C-wetbulb") {
    lim_k <- 30

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "days-gte-32C-wetbulb") {
    lim_k <- 32

    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "ten-hottest-days-wetbulb") {
    dir_temp <- str_glue("{dir_disk}/dir_temp")
    dir.create(dir_temp)

    # loop through annual files
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(f) {
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]

        # sort across time >> slice last 10 days >> calculate the mean
        str_glue(
          "cdo -timmean -seltimestep,{time_length-9}/{time_length} -timsort {dir_raw_data}/{f} {dir_temp}/mean_sel_{f}"
        ) %>%
          system(ignore.stdout = T, ignore.stderr = T)
      })

    # concatenate and save
    dir_temp %>%
      list.files(full.names = T) %>%
      str_flatten(" ") %>%

      {
        system(str_glue("cdo cat {.} {outfile}"), ignore.stdout = T, ignore.stderr = T)
      }

    # delete intermediate files
    unlink(dir_temp, recursive = T)

    # WATER VOLUME: PRECIP -----------------------------------------------------
  } else if (derived_var == "total-precip") {
    str_glue("cdo yearsum {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "ninety-wettest-days") {
    # load a proxy object to obtain dimensions
    # to tile and get time

    s_proxy <-
      str_glue("{dir_cat}/{v}_cat.nc") %>%
      read_ncdf(proxy = T) %>%
      suppressMessages()

    # obtain tiles

    chunks_index <- fn_tiling(s_proxy)
    lon_chunks <- chunks_index$lon_chunks
    lat_chunks <- chunks_index$lat_chunks

    # extract years

    time_dim <-
      s_proxy %>%
      st_get_dimension_values("time") %>%
      year()

    all_years <- time_dim %>% unique()

    # create temporary directory to save tiles

    dir_tmp <- str_glue("{dir_disk}/tmp")
    dir.create(dir_tmp)

    # loop through chunks
    # calculate precip of 90 wettest days for each

    iwalk(lon_chunks, function(lon_, i_lon) {
      iwalk(lat_chunks, function(lat_, i_lat) {
        print(str_glue("      processing chunk {i_lon} - {i_lat}"))

        s_proxy[, lon_[1]:lon_[2], lat_[1]:lat_[2], ] %>%

          st_apply(
            c(1, 2),

            # function to identify annual maximas
            # while preventing their overlap:

            function(x) {
              if (any(is.na(x))) {
                pr <- rep(NA, length(all_years))
              } else {
                # running sum of 90 days
                # value assigned to last obs of the window
                runsum <-
                  x %>%
                  slider::slide_dbl(sum, .before = 89, .complete = T, .step = 2)

                # initialize vector
                pr <- rep(NA_real_, length(all_years))

                dy <- 0 # first iteration; no day

                # loop through years
                for (i in 1:length(all_years)) {
                  time_range <- which(time_dim == all_years[i]) %>% range()

                  # if max is within the last 90 days of the year
                  # shorten the range of time to look for max
                  # so that windows do not overlap
                  if (i != 1 & dy >= time_range[1] - 90) {
                    dy <-
                      which.max(runsum[(dy + 90):time_range[2]]) + (dy + 90) - 1
                  } else {
                    dy <-
                      which.max(runsum[time_range[1]:time_range[2]]) + time_range[1] - 1
                  }

                  pr[i] <- runsum[dy]
                }
              }

              return(pr)
            }, # end of function

            FUTURE = T,
            .fname = "time"
          ) %>%

          st_as_stars(proxy = F) %>%
          aperm(c(2, 3, 1)) %>%

          # save chunk
          write_stars(str_glue("{dir_tmp}/{dom}_tmpfile_{i_lon}_{i_lat}.tif"))
      })
    })

    # mosaic chunks row-wise
    rows_ <-
      map(seq_along(lat_chunks), function(i_lat) {
        # build a table to sort tiles
        # and ensure they are imported in order
        tibble(
          file = dir_tmp %>%
            list.files(full.names = T) %>%
            str_subset(str_glue("_{i_lat}.tif"))
        ) %>%
          mutate(col = str_extract(file, "_[:digit:]*_"), col = parse_number(col)) %>%
          arrange(col) %>%
          pull(file) %>%

          # import
          read_stars(along = 1)
      })

    # mosaic rows
    mos <-
      rows_ %>%
      {
        do.call(c, c(., along = 2))
      } %>%
      st_set_dimensions(1, names = "lon", values = st_get_dimension_values(s_proxy, "lon")) %>%
      st_set_dimensions(2, names = "lat", values = st_get_dimension_values(s_proxy, "lat")) %>%
      st_set_crs(4326) %>%
      st_set_dimensions(
        3,
        names = "time",
        values = str_glue("{all_years}0101") %>%
          as_date() %>%
          as.numeric()
      ) %>%
      setNames("pr")

    fn_write_nc(mos, outfile, "time", "days since 1970-01-01", un = "kg/m^2/s")

    unlink(dir_tmp, recursive = T)

    # WATER VOLUME: PRECIP + AVG. TEMPERATURE ---------------------------------
  } else if (derived_var == "days-gte-1mm-precip-lt-0C-tasmean") {
    c("pr", "tas") %>%
      future_walk(function(v) {
        if (v == "pr") {
          set_units(1, kg / m^2 / d) %>%
            set_units(kg / m^2 / s) %>%
            drop_units() -> lim_v

          str_glue("cdo gec,{lim_v} {dir_cat}/pr_cat.nc {dir_cat}/pr_step1.nc") %>%
            system(ignore.stdout = T, ignore.stderr = T)
        } else {
          set_units(0, degC) %>%
            set_units(K) %>%
            drop_units() -> lim_v

          str_glue("cdo ltc,{lim_v} {dir_cat}/tas_cat.nc {dir_cat}/tas_step1.nc") %>%
            system(ignore.stdout = T, ignore.stderr = T)
        }
      })

    # joint condition
    str_glue("cdo -yearsum -gec,2 -add {dir_cat}/pr_step1.nc {dir_cat}/tas_step1.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # WATER VOLUME: PRECIP + MAX. TEMPERATURE ---------------------------------
  } else if (derived_var == "days-gte-b90perc-tasmax-lte-b10perc-precip") {
    future_walk(c("pr", "tasmax"), function(v) {
      # params
      if (v == "tasmax") {
        thresh <- 90
        command <- "gec"
        f <- "tasmax_cat.nc"
      } else if (v == "pr") {
        thresh <- 10
        command <- "lec"
        str_glue("cdo mulc,864000 {dir_cat}/pr_cat.nc {dir_cat}/pr_cat2.nc") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        fs::file_delete(str_glue("{dir_cat}/pr_cat.nc"))
        str_glue("cdo expr,'rounded_pr=floor(pr)' {dir_cat}/pr_cat2.nc {dir_cat}/pr_cat3.nc") |>
          system(ignore.stdout = T, ignore.stderr = T)
        fs::file_delete(str_glue("{dir_cat}/pr_cat2.nc"))
        # we consider anything below 0.1 mm/day as 0, so we
        # multiply by 86400 then by 10 then round with floor
        f <- str_glue("pr_cat3.nc")
      }

      # subset baseline
      str_glue("cdo selyear,1971/2000 {dir_cat}/{f} {dir_cat}/{v}_step1.nc") %>%
        system(ignore.stdout = T, ignore.stderr = T)

      # calculate percentile
      # str_glue("cdo timpctl,{thresh} {dir_cat}/{v}_step1.nc -timmin {dir_cat}/{v}_step1.nc -timmax {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc") %>%
      #   system(ignore.stdout = T, ignore.stderr = T)
      length_time_bl <-
        str_glue("{dir_cat}/{v}_step1.nc") |>
        read_ncdf(proxy = T) %>%
        suppressMessages() %>%
        {
          dim(.)[3]
        }
      str_glue(
        "cdo seltimestep,{thresh*length_time_bl/100} -timsort {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc"
      ) |>
        system(ignore.stdout = T, ignore.stderr = T)

      # obtain no. days under/above baseline percentile
      str_glue(
        "cdo -{command},0 -sub {dir_cat}/{f} {dir_cat}/{v}_step2.nc {dir_cat}/{v}_step3.nc"
      ) %>%
        system(ignore.stdout = T, ignore.stderr = T)
    })

    # **********

    # "/mnt/pers_disk/cat/pr_step1.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(1, 1, 1), count = c(NA, NA, 1))) -> miau

    # rt_from_coord_to_ind(miau, 10, 32) # tunisia
    # rt_from_coord_to_ind(miau, 18, -1) # drc

    # xx <- 173
    # yy <- 390 # tunisia

    # miau[, xx, yy, 1] |> as_tibble()

    # tictoc::tic()
    # "/mnt/pers_disk/cat/pr_step1.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> pr_bl
    # tictoc::toc()

    # pr_bl |>
    #   pull() |>
    #   as.vector() -> pr_bl

    # q <- quantile(pr_bl, 0.1, type = 3) # 3.117334e-17 # 0

    # "/mnt/pers_disk/cat/pr_step2.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA)), make_time = F) |>
    #   pull() |>
    #   as.vector() # 3.117334e-17 # 0

    # sum(pr_bl < q) # 1080

    # "/mnt/pers_disk/cat/pr_step3.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> pr_th_count

    # pr_th_count |>
    #   as_tibble() |>
    #   filter(year(time) >= 1971, year(time) <= 2000) |>
    #   pull(pr) |>
    #   sum() # 1079 # 0

    # tictoc::tic()
    # "/mnt/pers_disk/cat/tasmax_step1.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> tmax_bl
    # tictoc::toc()

    # tmax_bl |>
    #   pull() |>
    #   as.vector() -> tmax_bl

    # q <- quantile(tmax_bl, 0.9, type = 3) # 310.3214  # 309.89

    # "/mnt/pers_disk/cat/tasmax_step2.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA)), make_time = F) |>
    #   pull() |>
    #   as.vector() # 310.3214 # 309.89

    # sum(tmax_bl >= q) # 1080 # 1097

    # "/mnt/pers_disk/cat/tasmax_step3.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> tmax_th_count

    # tmax_th_count |>
    #   as_tibble() |>
    #   filter(year(time) >= 1971, year(time) <= 2000) |>
    #   pull(tasmax) |>
    #   sum() # 1081 # 1097

    # bind_cols(
    #   pr_th_count |> as_tibble(),
    #   tmax_th_count |> as_tibble() |> select(tasmax)
    # ) -> tb_join

    # tb_join |>
    #   units::drop_units() |>
    #   mutate(j = if_else(pr == 1 & tasmax == 1, 1, 0)) |>
    #   group_by(yr = year(time)) |>
    #   summarize(j = sum(j)) -> tb_yr

    # tb_yr |>
    #   filter(yr >= 1971, yr <= 2000) |>
    #   pull(j) |>
    #   mean()

    # "/mnt/bucket_mine/results/global_heat_pf/01_derived/AFR_days-gte-b90perc-tasmax-lt-b10perc-precip_yr_RegCM4_MPI-M-MPI-ESM-MR_v32.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> outfile

    # outfile |>
    #   as_tibble()

    # # drc
    # xx <- 213
    # yy <- 225 # drc

    # tictoc::tic()
    # "/mnt/pers_disk/cat/pr_step1.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA))) -> pr_bl
    # tictoc::toc()

    # pr_bl |>
    #   pull() |>
    #   as.vector() -> pr_bl

    # q <- quantile(pr_bl, 0.1, type = 3) # 2.921593e-07 # 3.415071e-06

    # "/mnt/pers_disk/cat/pr_step2.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA)), make_time = F) |>
    #   pull() |>
    #   as.vector() # 2.921593e-07 # 3.411919e-06

    # sum(pr_bl < q) # 1080 # 1095

    # "/mnt/pers_disk/cat/pr_step3.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA))) -> pr_th_count

    # pr_th_count |>
    #   as_tibble() |>
    #   filter(year(time) >= 1971, year(time) <= 2000) |>
    #   pull(pr) |>
    #   sum() # 1079 # 1094

    # tictoc::tic()
    # "/mnt/pers_disk/cat/tasmax_step1.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA))) -> tmax_bl
    # tictoc::toc()

    # tmax_bl |>
    #   pull() |>
    #   as.vector() -> tmax_bl

    # q <- quantile(tmax_bl, 0.9, type = 3) # 307.076  # 307.4936

    # "/mnt/pers_disk/cat/tasmax_step2.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA)), make_time = F) |>
    #   pull() |>
    #   as.vector() # 307.076 # 307.4936

    # sum(tmax_bl >= q) # 1080 # 1097

    # "/mnt/pers_disk/cat/tasmax_step3.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA))) -> tmax_th_count

    # tmax_th_count |>
    #   as_tibble() |>
    #   filter(year(time) >= 1971, year(time) <= 2000) |>
    #   pull(tasmax) |>
    #   sum() # 1081 # 1097

    # bind_cols(
    #   pr_th_count |> as_tibble(),
    #   tmax_th_count |> as_tibble() |> select(tasmax)
    # ) -> tb_join

    # tb_join |>
    #   units::drop_units() |>
    #   mutate(j = if_else(pr == 1 & tasmax == 1, 1, 0)) |>
    #   group_by(yr = year(time)) |>
    #   summarize(j = sum(j)) -> tb_yr

    # tb_yr |>
    #   filter(yr >= 1971, yr <= 2000) |>
    #   pull(j) |>
    #   mean()

    # "/mnt/bucket_mine/results/global_heat_pf/01_derived/AFR_days-gte-b90perc-tasmax-lt-b10perc-precip_yr_REMO2015_MOHC-HadGEM2-ES_v32.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(213, 225, 1), count = c(1, 1, NA))) -> outfile

    # outfile |>
    #   as_tibble()

    # xx <- 173
    # yy <- 390 # algeria
    # xx <- 213
    # yy <- 225 # guinea

    # fs::dir_ls("/mnt/bucket_mine/results/global_heat_pf/01_derived/") |>
    #   str_subset("AFR_days-gte-b90perc-tasmax-lt-b10perc-precip_yr") |>
    #   str_subset("v32") |>
    #   map(~ read_ncdf(.x, ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, NA)))) -> outfiles

    # outfiles |>
    #   map(\(s) {
    #     s |>
    #       as_tibble() |>
    #       filter(year(time) >= 1971, year(time) <= 2000) |>
    #       pull(pr) #|>
    #     # mean()
    #   }) -> tbs_bl

    # tbs_bl |>
    #   unlist() |>
    #   quantile(c(0.05, 0.5, 0.95))

    # "/mnt/bucket_mine/results/global_heat_pf/02_ensembled/AFR_days-gte-b90perc-tasmax-lt-b10perc-precip_ensemble.nc" |>
    #   read_ncdf(ncsub = cbind(start = c(xx, yy, 1), count = c(1, 1, 1)))

    # **********

    # joint condition

    ff <-
      dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("_cat", negate = T) %>%
      str_subset("step3") %>%
      str_flatten(" ")

    str_glue("cdo -yearsum -gec,2 -add {ff} {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # LAND VOLUME: SPEI --------------------------------------------------------
  } else if (derived_var == "mean-spei") {
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "prop-months-lte-neg0.8-spei") {
    str_glue("cdo yearmean -lec,-0.8 {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # *************
  } else if (derived_var == "prop-months-lte-neg1.6-spei") {
    str_glue("cdo yearmean -lec,-1.6 {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # LAND VOLUME: FWI ---------------------------------------------------------
  } else if (derived_var == "days-gte-b95perc-fwi") {
    # subset baseline
    str_glue("cdo selyear,1972/2000 {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step1.nc") %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # calculate percentile
    str_glue(
      "cdo timpctl,95 {dir_cat}/{v}_step1.nc -timmin {dir_cat}/{v}_step1.nc -timmax {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc"
    ) %>%
      system(ignore.stdout = T, ignore.stderr = T)

    # obtain no. days above baseline percentile; then sum per year
    str_glue("cdo -yearsum -gec,0 -sub {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step2.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
  }

  # ***************************************************************************

  # verify correct time dimension
  time_steps <-
    outfile %>%
    read_ncdf(proxy = T, make_time = F) %>%
    suppressMessages() %>%
    suppressWarnings() %>%
    st_get_dimension_values("time") %>%
    length()

  print(str_glue("      Done: new file with {time_steps} timesteps ({derived_var})"))
}
