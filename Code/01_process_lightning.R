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

input_nc <- file.path(root_dir, "data", "Lightning", "WGLC", "wglc_timeseries_05m.nc")
output_dir <- file.path(root_dir, "data", "powerIV", "lightning")
raster_dir <- file.path(output_dir, "rasters")
panel_dir <- file.path(output_dir, "panels")
shape_dir <- file.path(output_dir, "shapes")

dir.create(raster_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(shape_dir, recursive = TRUE, showWarnings = FALSE)

masked_raster_path <- file.path(raster_dir, "wglc_density_monthly_total_brazil_2010_2021.tif")
intermediate_panel_path <- file.path(panel_dir, "wglc_density_monthly_intermediate_region_2010_2021.parquet")
intermediate_shape_path <- file.path(shape_dir, "ibge_intermediate_regions_2020.gpkg")

if (all(file.exists(c(masked_raster_path, intermediate_panel_path, intermediate_shape_path)))) {
  message("All lightning outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

if (!file.exists(input_nc)) {
  stop("Missing input NetCDF: ", input_nc)
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

message("Reading WGLC density raster.")
density <- terra::rast(input_nc)

start_date <- as.Date("2010-01-01")
dates <- seq.Date(from = start_date, by = "month", length.out = terra::nlyr(density))
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
