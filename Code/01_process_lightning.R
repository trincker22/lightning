#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(terra)
  library(sf)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(stringr)
  library(geobr)
  library(arrow)
})

root_dir <- here::here()

lightning_dir <- here::here("data", "Lightning", "WGLC")
nc_files <- list.files(lightning_dir, pattern = "\\.nc$", full.names = TRUE)
if (length(nc_files) == 0) {
  stop("No NetCDF file found in ", lightning_dir)
}
preferred_nc <- nc_files[grepl("05m", basename(nc_files), ignore.case = TRUE)]
if (length(preferred_nc) > 0) {
  input_nc <- preferred_nc[which.max(file.info(preferred_nc)$mtime)]
} else {
  input_nc <- nc_files[which.max(file.info(nc_files)$mtime)]
}
output_dir <- here::here("data", "powerIV", "lightning")
raster_dir <- here::here(output_dir, "rasters")
panel_dir <- here::here(output_dir, "panels")
shape_dir <- here::here(output_dir, "shapes")

dir.create(raster_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(shape_dir, recursive = TRUE, showWarnings = FALSE)

masked_raster_path <- here::here(raster_dir, "wglc_density_monthly_total_brazil_2010_2021.tif")
intermediate_panel_path <- here::here(panel_dir, "wglc_density_monthly_intermediate_region_2010_2021.parquet")
intermediate_shape_path <- here::here(shape_dir, "ibge_intermediate_regions_2020.gpkg")

message("Reading WGLC density raster.")
message("Using NetCDF file: ", input_nc)
density <- terra::rast(input_nc)

raw_time <- terra::time(density)
if (!all(is.na(raw_time))) {
  dates <- as.Date(raw_time)
} else {
  start_date <- as.Date("2010-01-01")
  dates <- seq.Date(from = start_date, by = "month", length.out = terra::nlyr(density))
}

if (all(file.exists(c(masked_raster_path, intermediate_panel_path, intermediate_shape_path)))) {
  existing_panel_dates <- arrow::read_parquet(intermediate_panel_path, col_select = "date")$date
  panel_is_current <- length(existing_panel_dates) > 0 &&
    min(existing_panel_dates, na.rm = TRUE) <= min(dates, na.rm = TRUE) &&
    max(existing_panel_dates, na.rm = TRUE) >= max(dates, na.rm = TRUE)
  if (panel_is_current) {
    message("All lightning outputs already exist in data/powerIV and cover current source dates. Skipping rebuild.")
    quit(save = "no", status = 0)
  }
  message("Existing lightning outputs are stale; rebuilding with expanded source coverage.")
}

message("Reading Brazil geometry and intermediate regions.")
brazil <- geobr::read_country(year = 2020, simplified = FALSE, showProgress = FALSE) |>
  sf::st_transform(4326)

intermediate_regions <- geobr::read_intermediate_region(
  year = 2020,
  simplified = FALSE,
  showProgress = FALSE
) |>
  sf::st_transform(4326)

days_per_month <- lubridate::days_in_month(dates)

message("Cropping and masking to Brazil.")
brazil_vect <- terra::vect(brazil)
density_brazil <- density |>
  terra::crop(brazil_vect) |>
  terra::mask(brazil_vect)

message("Converting daily density to monthly totals.")
for (i in seq_len(terra::nlyr(density_brazil))) {
  density_brazil[[i]] <- density_brazil[[i]] * days_per_month[[i]]
}
names(density_brazil) <- format(dates, "%Y_%m")

message("Writing masked monthly raster.")
terra::writeRaster(density_brazil, masked_raster_path, overwrite = TRUE)

message("Extracting monthly mean density by IBGE intermediate region.")
intermediate_vect <- terra::vect(intermediate_regions)
extract_df <- terra::extract(
  density_brazil,
  intermediate_vect,
  fun = mean,
  na.rm = TRUE
)

intermediate_meta <- intermediate_regions |>
  sf::st_drop_geometry() |>
  mutate(ID = row_number()) |>
  select(code_intermediate, name_intermediate, abbrev_state) |>
  mutate(ID = row_number())

lightning_intermediate <- extract_df |>
  left_join(intermediate_meta, by = "ID") |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "year_month",
    values_to = "lightning_density_monthly_total"
  ) |>
  mutate(
    date = lubridate::ym(str_replace_all(year_month, "_", "-")),
    year = lubridate::year(date),
    month = lubridate::month(date)
  ) |>
  select(
    code_intermediate,
    name_intermediate,
    abbrev_state,
    date,
    year,
    month,
    lightning_density_monthly_total
  ) |>
  arrange(code_intermediate, date)

message("Writing panel outputs.")
arrow::write_parquet(lightning_intermediate, intermediate_panel_path)
sf::write_sf(intermediate_regions, intermediate_shape_path, delete_dsn = TRUE, quiet = TRUE)

message("Done.")
message("Raster: ", masked_raster_path)
message("Panel:  ", intermediate_panel_path)
message("Shapes: ", intermediate_shape_path)
