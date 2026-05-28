# Probable Futures Map Data Processing

Climate data processing pipeline for preparing data before it reaches the web stack.

## Overview

This repository contains a three-stage processing pipeline that transforms raw CORDEX-CORE climate model data into analysis-ready global climate datasets. The pipeline is organized in the `scripts_v3` directory and processes climate variables across multiple geographic domains and warming level scenarios.

## Processing Pipeline

`01_derived.R` calculates derived climate variables from CORDEX-CORE model output. This script:
- Downloads raw climate data from multiple climate models (GCM/RCM combinations)
- Pre-processes files using CDO (Climate Data Operators) to fix temporal dimensions and split data annually
- Calculates derived variables such as temperature indices, precipitation-based metrics, and fire weather indicators
- Processes data in parallel across multiple domains (SEA, AUS, CAS, WAS, EAS, AFR, EUR, NAM, CAM, SAM)
- Outputs NetCDF files with processed derived variables

`02_ensemble.R`creates ensemble statistics from derived variables across multiple climate models. This script:
- Imports derived variable files from all available climate models
- Slices data by warming levels (0.5°C to 3.0°C above baseline)
- Calculates statistical summaries per grid cell: mean, median, 5th and 95th percentiles
- Outputs ensemble NetCDF files with statistics for each warming level

`03_mosaic.R` creates global mosaic datasets by combining regional domain ensembles with inverse distance weighting. This script:
- Loads regional ensemble files from each domain
- Computes inverse distance weights based on domain boundaries to create seamless transitions
- Applies weights and mosaics domains into a global grid (0.2° resolution)
- Calculates differences from baseline for change metrics
- Applies land and barren land masks
- Outputs final global NetCDF datasets ready for web services
- Syncs final outputs to cloud storage (Google Cloud Storage and AWS S3)

