#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(sf)
  library(terra)
  library(ggplot2)
  library(lubridate)
  library(scales)
  library(readr)
})

sf::sf_use_s2(FALSE)

root_dir <- here::here()

pnadc_panel_path <- file.path(root_dir, "data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")
pnadc_geom_path <- file.path(root_dir, "data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")
raw_event_path <- file.path(root_dir, "data", "powerIV", "outages", "panels", "raw_event_conj_month.parquet")
continuity_path <- file.path(root_dir, "data", "powerIV", "outages", "panels", "aneel_conj_monthly_outages.parquet")
conj_geom_path <- file.path(root_dir, "data", "powerIV", "outages", "shapes", "aneel_conj_all_vintages.gpkg")
match_path <- file.path(root_dir, "data", "powerIV", "outages", "qa", "aneel_conj_outage_nearest_geometry_match.parquet")
light_raster_path <- file.path(root_dir, "data", "powerIV", "lightning", "rasters", "wglc_density_monthly_total_brazil_2010_2021.tif")

out_dir <- file.path(root_dir, "data", "powerIV", "pnadc_power")
panel_dir <- file.path(out_dir, "panels")
qa_dir <- file.path(out_dir, "qa")
fig_dir <- file.path(root_dir, "Figures", "powerIV", "pnadc_power")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

weights_path <- file.path(panel_dir, "conj_snapshot_to_estrato4_weights.parquet")
light_month_path <- file.path(panel_dir, "lightning_estrato4_month.parquet")
light_quarter_path <- file.path(panel_dir, "lightning_estrato4_quarter.parquet")
raw_month_path <- file.path(panel_dir, "raw_event_estrato4_month.parquet")
raw_quarter_path <- file.path(panel_dir, "raw_event_estrato4_quarter.parquet")
cont_month_path <- file.path(panel_dir, "continuity_estrato4_month.parquet")
cont_quarter_path <- file.path(panel_dir, "continuity_estrato4_quarter.parquet")
merged_path <- file.path(panel_dir, "pnadc_estrato4_quarter_with_power.parquet")
qa_path <- file.path(qa_dir, "pnadc_power_merge_qa.csv")

required_outputs <- c(
  weights_path,
  light_month_path,
  light_quarter_path,
  raw_month_path,
  raw_quarter_path,
  cont_month_path,
  cont_quarter_path,
  merged_path,
  qa_path
)

if (all(file.exists(required_outputs))) {
  message("All PNAD-C power merge outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

message("Reading PNAD-C panel and estrato geometry.")
pnadc_panel <- read_parquet(pnadc_panel_path) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  )

estrato_sf <- st_read(pnadc_geom_path, quiet = TRUE) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  st_transform(5880) %>%
  st_make_valid() %>%
  suppressWarnings(st_buffer(0))

message("Preparing CONJ to estrato overlap weights.")
if (file.exists(weights_path)) {
  conj_estrato_weights <- read_parquet(weights_path)
} else {
  raw_dates <- read_parquet(raw_event_path) %>% transmute(conj_code = as.character(conj_code), date = as.Date(date))
  cont_dates <- read_parquet(continuity_path) %>% transmute(conj_code = as.character(conj_code), date = as.Date(date))
  needed_matches <- bind_rows(raw_dates, cont_dates) %>%
    distinct(conj_code, date) %>%
    inner_join(read_parquet(match_path), by = c("conj_code", "date")) %>%
    filter(matched_geometry) %>%
    distinct(conj_code, geometry_snapshot_date, source_file)

  conj_geom <- st_read(conj_geom_path, quiet = TRUE) %>%
    mutate(conj_code = as.character(conj_code)) %>%
    select(conj_code, geometry_snapshot_date, source_file, geom) %>%
    inner_join(needed_matches, by = c("conj_code", "geometry_snapshot_date", "source_file")) %>%
    st_transform(5880) %>%
    st_make_valid() %>%
    suppressWarnings(st_buffer(0))

  conj_estrato_weights <- st_intersection(conj_geom, estrato_sf %>% select(estrato4)) %>%
    mutate(overlap_area_m2 = as.numeric(st_area(geom))) %>%
    st_drop_geometry() %>%
    group_by(conj_code, geometry_snapshot_date, source_file) %>%
    mutate(overlap_weight = overlap_area_m2 / sum(overlap_area_m2, na.rm = TRUE)) %>%
    ungroup() %>%
    select(conj_code, geometry_snapshot_date, source_file, estrato4, overlap_area_m2, overlap_weight)

  write_parquet(conj_estrato_weights, weights_path)
}

message("Aggregating lightning to estrato-month and estrato-quarter.")
if (file.exists(light_month_path) && file.exists(light_quarter_path)) {
  light_month <- read_parquet(light_month_path)
  light_quarter <- read_parquet(light_quarter_path)
} else {
  light_rast <- rast(light_raster_path)
  estrato_wgs84 <- st_transform(estrato_sf, 4326)
  light_extract <- terra::extract(light_rast, terra::vect(estrato_wgs84), fun = mean, na.rm = TRUE)
  light_month <- as_tibble(light_extract) %>%
    bind_cols(estrato4 = estrato_wgs84$estrato4, .before = 1) %>%
    select(-ID) %>%
    pivot_longer(cols = -estrato4, names_to = "layer_name", values_to = "lightning_monthly_total") %>%
    mutate(
      date = as.Date(paste0(gsub("_", "-", layer_name), "-01")),
      year = year(date),
      quarter = quarter(date)
    ) %>%
    select(estrato4, date, year, quarter, lightning_monthly_total)

  light_quarter <- light_month %>%
    group_by(estrato4, year, quarter) %>%
    summarise(lightning_quarterly_total = sum(lightning_monthly_total, na.rm = TRUE), .groups = "drop")

  write_parquet(light_month, light_month_path)
  write_parquet(light_quarter, light_quarter_path)
}

message("Aggregating raw-event outages to estrato-month and estrato-quarter.")
if (file.exists(raw_month_path) && file.exists(raw_quarter_path)) {
  raw_month <- read_parquet(raw_month_path)
  raw_quarter <- read_parquet(raw_quarter_path)
} else {
  raw_month <- read_parquet(raw_event_path) %>%
    mutate(conj_code = as.character(conj_code), date = as.Date(date)) %>%
    inner_join(read_parquet(match_path), by = c("conj_code", "date")) %>%
    filter(matched_geometry) %>%
    inner_join(conj_estrato_weights, by = c("conj_code", "geometry_snapshot_date", "source_file"), relationship = "many-to-many") %>%
    mutate(
      estimated_consumers = group_consumer_count_avg * overlap_weight,
      outage_hours_component = total_customer_outage_hrs * overlap_weight,
      events_component = n_events * overlap_weight
    ) %>%
    group_by(estrato4, date) %>%
    summarise(
      year = first(year(date)),
      month = first(month(date)),
      total_customer_outage_hrs = sum(outage_hours_component, na.rm = TRUE),
      total_events = sum(events_component, na.rm = TRUE),
      estimated_consumers = sum(estimated_consumers, na.rm = TRUE),
      outage_hrs_per_1000 = ifelse(sum(estimated_consumers, na.rm = TRUE) > 0, sum(outage_hours_component, na.rm = TRUE) * 1000 / sum(estimated_consumers, na.rm = TRUE), NA_real_),
      events_per_1000 = ifelse(sum(estimated_consumers, na.rm = TRUE) > 0, sum(events_component, na.rm = TRUE) * 1000 / sum(estimated_consumers, na.rm = TRUE), NA_real_),
      n_conj = n_distinct(conj_code),
      .groups = "drop"
    )

  raw_quarter <- raw_month %>%
    mutate(quarter = quarter(date)) %>%
    group_by(estrato4, year, quarter) %>%
    summarise(
      raw_quarterly_outage_hrs_per_1000 = sum(total_customer_outage_hrs, na.rm = TRUE) * 1000 / mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_events_per_1000 = sum(total_events, na.rm = TRUE) * 1000 / mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_total_customer_outage_hrs = sum(total_customer_outage_hrs, na.rm = TRUE),
      raw_quarterly_total_events = sum(total_events, na.rm = TRUE),
      raw_quarterly_estimated_consumers = mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_conj_mean = mean(n_conj, na.rm = TRUE),
      .groups = "drop"
    )

  write_parquet(raw_month, raw_month_path)
  write_parquet(raw_quarter, raw_quarter_path)
}

message("Aggregating continuity outages to estrato-month and estrato-quarter.")
if (file.exists(cont_month_path) && file.exists(cont_quarter_path)) {
  cont_month <- read_parquet(cont_month_path)
  cont_quarter <- read_parquet(cont_quarter_path)
} else {
  cont_month <- read_parquet(continuity_path) %>%
    mutate(conj_code = as.character(conj_code), date = as.Date(date)) %>%
    inner_join(read_parquet(match_path), by = c("conj_code", "date")) %>%
    filter(matched_geometry) %>%
    inner_join(conj_estrato_weights, by = c("conj_code", "geometry_snapshot_date", "source_file"), relationship = "many-to-many") %>%
    group_by(estrato4, date) %>%
    summarise(
      year = first(year(date)),
      month = first(month(date)),
      dec_monthly = weighted_mean_or_na(dec_monthly, overlap_weight),
      fec_monthly = weighted_mean_or_na(fec_monthly, overlap_weight),
      n_conj = n_distinct(conj_code),
      .groups = "drop"
    )

  cont_quarter <- cont_month %>%
    mutate(quarter = quarter(date)) %>%
    group_by(estrato4, year, quarter) %>%
    summarise(
      dec_quarterly = sum(dec_monthly, na.rm = TRUE),
      fec_quarterly = sum(fec_monthly, na.rm = TRUE),
      continuity_quarterly_conj_mean = mean(n_conj, na.rm = TRUE),
      .groups = "drop"
    )

  write_parquet(cont_month, cont_month_path)
  write_parquet(cont_quarter, cont_quarter_path)
}

message("Merging PNAD-C and power panels.")
merged_panel <- pnadc_panel %>%
  left_join(light_quarter, by = c("estrato4", "year", "quarter")) %>%
  left_join(raw_quarter, by = c("estrato4", "year", "quarter")) %>%
  left_join(cont_quarter, by = c("estrato4", "year", "quarter"))

write_parquet(merged_panel, merged_path)

latest_panel <- pnadc_panel %>%
  filter(year == max(year), quarter == max(quarter[year == max(year)]))

map_sf <- estrato_sf %>% left_join(latest_panel, by = "estrato4")

make_map(
  map_sf,
  "share_washing_machine",
  "PNAD-C strata: washing machine ownership",
  "2024 Q4",
  file.path(fig_dir, "map_1_washing_machine_2024q4.png")
)
make_map(
  map_sf,
  "share_cooking_wood_charcoal",
  "PNAD-C strata: wood or charcoal used for cooking",
  "2024 Q4",
  file.path(fig_dir, "map_2_cooking_wood_charcoal_2024q4.png")
)
make_map(
  map_sf,
  "share_cooking_electricity",
  "PNAD-C strata: electricity used for cooking",
  "2024 Q4",
  file.path(fig_dir, "map_3_cooking_electricity_2024q4.png")
)
make_map(
  map_sf,
  "share_grid_full_time",
  "PNAD-C strata: full-time grid availability",
  "2024 Q4",
  file.path(fig_dir, "map_4_grid_full_time_2024q4.png")
)

qa <- tibble(
  metric = c(
    "pnadc_rows",
    "merged_rows",
    "unique_estrato4",
    "lightning_nonmissing",
    "raw_event_nonmissing",
    "continuity_nonmissing",
    "raw_event_years",
    "lightning_years",
    "continuity_years"
  ),
  value = c(
    as.character(nrow(pnadc_panel)),
    as.character(nrow(merged_panel)),
    as.character(n_distinct(merged_panel$estrato4)),
    as.character(sum(!is.na(merged_panel$lightning_quarterly_total))),
    as.character(sum(!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000))),
    as.character(sum(!is.na(merged_panel$dec_quarterly))),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$lightning_quarterly_total)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$dec_quarterly)])), collapse = ",")
  )
)
write_csv(qa, qa_path)

message("Done.")
message("Merged panel: ", merged_path)
message("QA: ", qa_path)
message("Figures: ", fig_dir)
