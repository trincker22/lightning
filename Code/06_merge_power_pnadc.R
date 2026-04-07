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

pnadc_panel_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")
pnadc_geom_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")
raw_event_path <- here::here("data", "powerIV", "outages", "panels", "raw_event_conj_month.parquet")
continuity_path <- here::here("data", "powerIV", "outages", "panels", "aneel_conj_monthly_outages.parquet")
conj_geom_path <- here::here("data", "powerIV", "outages", "shapes", "aneel_conj_all_vintages.gpkg")
match_path <- here::here("data", "powerIV", "outages", "qa", "aneel_conj_outage_nearest_geometry_match.parquet")
light_raster_path <- here::here("data", "powerIV", "lightning", "rasters", "wglc_density_monthly_total_brazil_2010_2021.tif")

out_dir <- here::here("data", "powerIV", "pnadc_power")
panel_dir <- here::here(out_dir, "panels")
qa_dir <- here::here(out_dir, "qa")
fig_dir <- here::here("Figures", "powerIV", "pnadc_power")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

weights_path <- here::here(panel_dir, "conj_snapshot_to_estrato4_weights.parquet")
light_month_path <- here::here(panel_dir, "lightning_estrato4_month.parquet")
light_quarter_path <- here::here(panel_dir, "lightning_estrato4_quarter.parquet")
raw_month_path <- here::here(panel_dir, "raw_event_estrato4_month.parquet")
raw_quarter_path <- here::here(panel_dir, "raw_event_estrato4_quarter.parquet")
raw_month_no_overlap_path <- here::here(panel_dir, "raw_event_estrato4_month_no_overlap.parquet")
raw_quarter_no_overlap_path <- here::here(panel_dir, "raw_event_estrato4_quarter_no_overlap.parquet")
cont_month_path <- here::here(panel_dir, "continuity_estrato4_month.parquet")
cont_quarter_path <- here::here(panel_dir, "continuity_estrato4_quarter.parquet")
cont_month_no_overlap_path <- here::here(panel_dir, "continuity_estrato4_month_no_overlap.parquet")
cont_quarter_no_overlap_path <- here::here(panel_dir, "continuity_estrato4_quarter_no_overlap.parquet")
merged_path <- here::here(panel_dir, "pnadc_estrato4_quarter_with_power.parquet")
qa_path <- here::here(qa_dir, "pnadc_power_merge_qa.csv")

required_outputs <- c(
  weights_path,
  light_month_path,
  light_quarter_path,
  raw_month_path,
  raw_quarter_path,
  raw_month_no_overlap_path,
  raw_quarter_no_overlap_path,
  cont_month_path,
  cont_quarter_path,
  cont_month_no_overlap_path,
  cont_quarter_no_overlap_path,
  merged_path,
  qa_path
)

weighted_mean_or_na <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  w_sum <- sum(w[keep], na.rm = TRUE)
  if (w_sum <= 0) {
    return(NA_real_)
  }
  sum(x[keep] * w[keep], na.rm = TRUE) / w_sum
}

make_map <- function(data_sf, value_col, title_text, subtitle_text, out_path) {
  p <- ggplot(data_sf) +
    geom_sf(aes(fill = .data[[value_col]]), color = "grey60", linewidth = 0.1) +
    scale_fill_viridis_c(labels = scales::percent_format(accuracy = 1), na.value = "grey90") +
    labs(title = title_text, subtitle = subtitle_text, fill = "Share") +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )
  ggsave(out_path, p, width = 10, height = 7, dpi = 300)
}

if (all(file.exists(required_outputs))) {
  pnadc_years <- read_parquet(pnadc_panel_path, col_select = "year") %>%
    distinct(year) %>%
    pull(year)
  light_source_max_year <- {
    light_source_time <- terra::time(terra::rast(light_raster_path))
    max(lubridate::year(as.Date(light_source_time)), na.rm = TRUE)
  }
  raw_source_max_year <- read_parquet(raw_event_path, col_select = "year") %>%
    summarise(max_year = max(year, na.rm = TRUE)) %>%
    pull(max_year)
  dec_source_max_year <- read_parquet(continuity_path, col_select = "year") %>%
    summarise(max_year = max(year, na.rm = TRUE)) %>%
    pull(max_year)
  merged_existing <- read_parquet(
    merged_path,
    col_select = c("year", "lightning_quarterly_total", "raw_quarterly_outage_hrs_per_1000", "dec_quarterly")
  )
  light_max_year <- merged_existing %>%
    filter(!is.na(lightning_quarterly_total)) %>%
    summarise(max_year = max(year, na.rm = TRUE)) %>%
    pull(max_year)
  raw_max_year <- merged_existing %>%
    filter(!is.na(raw_quarterly_outage_hrs_per_1000)) %>%
    summarise(max_year = max(year, na.rm = TRUE)) %>%
    pull(max_year)
  dec_max_year <- merged_existing %>%
    filter(!is.na(dec_quarterly)) %>%
    summarise(max_year = max(year, na.rm = TRUE)) %>%
    pull(max_year)
  expected_light_years <- pnadc_years[pnadc_years <= light_source_max_year]
  expected_raw_years <- pnadc_years[pnadc_years <= raw_source_max_year]
  expected_dec_years <- pnadc_years[pnadc_years <= dec_source_max_year]
  expected_light_max <- if (length(expected_light_years) > 0) max(expected_light_years, na.rm = TRUE) else NA_real_
  expected_raw_max <- if (length(expected_raw_years) > 0) max(expected_raw_years, na.rm = TRUE) else NA_real_
  expected_dec_max <- if (length(expected_dec_years) > 0) max(expected_dec_years, na.rm = TRUE) else NA_real_
  outputs_are_current <- !is.na(light_max_year) && !is.na(dec_max_year) &&
    light_max_year >= expected_light_max &&
    dec_max_year >= expected_dec_max &&
    !is.na(raw_max_year) &&
    raw_max_year >= expected_raw_max
  if (outputs_are_current) {
    message("All PNAD-C power merge outputs already exist in data/powerIV and cover current source dates. Skipping rebuild.")
    quit(save = "no", status = 0)
  }
  message("Existing PNAD-C power outputs are stale; rebuilding with expanded source coverage.")
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

no_overlap_keys <- conj_estrato_weights %>%
  group_by(conj_code, geometry_snapshot_date, source_file) %>%
  summarise(n_strata = n(), .groups = "drop") %>%
  filter(n_strata == 1L) %>%
  select(conj_code, geometry_snapshot_date, source_file)

message("Aggregating lightning to estrato-month and estrato-quarter.")
rebuild_light <- TRUE
if (file.exists(light_month_path) && file.exists(light_quarter_path)) {
  light_month <- read_parquet(light_month_path)
  light_source_max_date <- max(as.Date(terra::time(rast(light_raster_path))), na.rm = TRUE)
  existing_light_max_date <- max(light_month$date, na.rm = TRUE)
  rebuild_light <- is.na(existing_light_max_date) || existing_light_max_date < light_source_max_date
}
if (!rebuild_light) {
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
rebuild_raw <- TRUE
if (file.exists(raw_month_path) && file.exists(raw_quarter_path) && file.exists(raw_month_no_overlap_path) && file.exists(raw_quarter_no_overlap_path)) {
  raw_month <- read_parquet(raw_month_path)
  raw_source_max_date <- read_parquet(raw_event_path, col_select = "date") %>%
    summarise(max_date = max(date, na.rm = TRUE)) %>%
    pull(max_date)
  existing_raw_max_date <- max(raw_month$date, na.rm = TRUE)
  rebuild_raw <- is.na(existing_raw_max_date) || existing_raw_max_date < raw_source_max_date
}
if (!rebuild_raw) {
  raw_quarter <- read_parquet(raw_quarter_path)
  raw_month_no_overlap <- read_parquet(raw_month_no_overlap_path)
  raw_quarter_no_overlap <- read_parquet(raw_quarter_no_overlap_path)
} else {
  raw_components <- read_parquet(raw_event_path) %>%
    mutate(conj_code = as.character(conj_code), date = as.Date(date)) %>%
    inner_join(read_parquet(match_path), by = c("conj_code", "date")) %>%
    filter(matched_geometry) %>%
    inner_join(conj_estrato_weights, by = c("conj_code", "geometry_snapshot_date", "source_file"), relationship = "many-to-many") %>%
    mutate(
      estimated_consumers = group_consumer_count_avg * overlap_weight,
      outage_hours_component = total_customer_outage_hrs * overlap_weight,
      events_component = n_events * overlap_weight
    )

  raw_month <- raw_components %>%
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

  raw_month_no_overlap <- raw_components %>%
    semi_join(no_overlap_keys, by = c("conj_code", "geometry_snapshot_date", "source_file")) %>%
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

  raw_quarter_no_overlap <- raw_month_no_overlap %>%
    mutate(quarter = quarter(date)) %>%
    group_by(estrato4, year, quarter) %>%
    summarise(
      raw_quarterly_outage_hrs_per_1000_no_overlap = sum(total_customer_outage_hrs, na.rm = TRUE) * 1000 / mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_events_per_1000_no_overlap = sum(total_events, na.rm = TRUE) * 1000 / mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_total_customer_outage_hrs_no_overlap = sum(total_customer_outage_hrs, na.rm = TRUE),
      raw_quarterly_total_events_no_overlap = sum(total_events, na.rm = TRUE),
      raw_quarterly_estimated_consumers_no_overlap = mean(estimated_consumers, na.rm = TRUE),
      raw_quarterly_conj_mean_no_overlap = mean(n_conj, na.rm = TRUE),
      .groups = "drop"
    )

  write_parquet(raw_month, raw_month_path)
  write_parquet(raw_quarter, raw_quarter_path)
  write_parquet(raw_month_no_overlap, raw_month_no_overlap_path)
  write_parquet(raw_quarter_no_overlap, raw_quarter_no_overlap_path)
}

message("Aggregating continuity outages to estrato-month and estrato-quarter.")
rebuild_cont <- TRUE
if (file.exists(cont_month_path) && file.exists(cont_quarter_path) && file.exists(cont_month_no_overlap_path) && file.exists(cont_quarter_no_overlap_path)) {
  cont_month <- read_parquet(cont_month_path)
  cont_source_max_date <- read_parquet(continuity_path, col_select = "date") %>%
    summarise(max_date = max(date, na.rm = TRUE)) %>%
    pull(max_date)
  existing_cont_max_date <- max(cont_month$date, na.rm = TRUE)
  rebuild_cont <- is.na(existing_cont_max_date) || existing_cont_max_date < cont_source_max_date
}
if (!rebuild_cont) {
  cont_quarter <- read_parquet(cont_quarter_path)
  cont_month_no_overlap <- read_parquet(cont_month_no_overlap_path)
  cont_quarter_no_overlap <- read_parquet(cont_quarter_no_overlap_path)
} else {
  cont_components <- read_parquet(continuity_path) %>%
    mutate(conj_code = as.character(conj_code), date = as.Date(date)) %>%
    inner_join(read_parquet(match_path), by = c("conj_code", "date")) %>%
    filter(matched_geometry) %>%
    inner_join(conj_estrato_weights, by = c("conj_code", "geometry_snapshot_date", "source_file"), relationship = "many-to-many") %>%
    select(estrato4, date, conj_code, dec_monthly, fec_monthly, overlap_weight, geometry_snapshot_date, source_file)

  cont_month <- cont_components %>%
    group_by(estrato4, date) %>%
    summarise(
      year = first(year(date)),
      month = first(month(date)),
      dec_monthly = weighted_mean_or_na(dec_monthly, overlap_weight),
      fec_monthly = weighted_mean_or_na(fec_monthly, overlap_weight),
      n_conj = n_distinct(conj_code),
      .groups = "drop"
    )

  cont_month_no_overlap <- cont_components %>%
    semi_join(no_overlap_keys, by = c("conj_code", "geometry_snapshot_date", "source_file")) %>%
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

  cont_quarter_no_overlap <- cont_month_no_overlap %>%
    mutate(quarter = quarter(date)) %>%
    group_by(estrato4, year, quarter) %>%
    summarise(
      dec_quarterly_no_overlap = sum(dec_monthly, na.rm = TRUE),
      fec_quarterly_no_overlap = sum(fec_monthly, na.rm = TRUE),
      continuity_quarterly_conj_mean_no_overlap = mean(n_conj, na.rm = TRUE),
      .groups = "drop"
    )

  write_parquet(cont_month, cont_month_path)
  write_parquet(cont_quarter, cont_quarter_path)
  write_parquet(cont_month_no_overlap, cont_month_no_overlap_path)
  write_parquet(cont_quarter_no_overlap, cont_quarter_no_overlap_path)
}

message("Merging PNAD-C and power panels.")
merged_panel <- pnadc_panel %>%
  left_join(light_quarter, by = c("estrato4", "year", "quarter")) %>%
  left_join(raw_quarter, by = c("estrato4", "year", "quarter")) %>%
  left_join(cont_quarter, by = c("estrato4", "year", "quarter")) %>%
  left_join(raw_quarter_no_overlap, by = c("estrato4", "year", "quarter")) %>%
  left_join(cont_quarter_no_overlap, by = c("estrato4", "year", "quarter"))

write_parquet(merged_panel, merged_path)

latest_panel <- pnadc_panel %>%
  filter(year == max(year), quarter == max(quarter[year == max(year)]))

map_sf <- estrato_sf %>% left_join(latest_panel, by = "estrato4")

make_map(
  map_sf,
  "share_washing_machine",
  "PNAD-C strata: washing machine ownership",
  "2024 Q4",
  here::here(fig_dir, "map_1_washing_machine_2024q4.png")
)
make_map(
  map_sf,
  "share_cooking_wood_charcoal",
  "PNAD-C strata: wood or charcoal used for cooking",
  "2024 Q4",
  here::here(fig_dir, "map_2_cooking_wood_charcoal_2024q4.png")
)
make_map(
  map_sf,
  "share_cooking_electricity",
  "PNAD-C strata: electricity used for cooking",
  "2024 Q4",
  here::here(fig_dir, "map_3_cooking_electricity_2024q4.png")
)
make_map(
  map_sf,
  "share_grid_full_time",
  "PNAD-C strata: full-time grid availability",
  "2024 Q4",
  here::here(fig_dir, "map_4_grid_full_time_2024q4.png")
)

qa <- tibble(
  metric = c(
    "pnadc_rows",
    "merged_rows",
    "unique_estrato4",
    "lightning_nonmissing",
    "raw_event_nonmissing",
    "raw_event_no_overlap_nonmissing",
    "continuity_nonmissing",
    "continuity_no_overlap_nonmissing",
    "raw_event_years",
    "raw_event_no_overlap_years",
    "lightning_years",
    "continuity_years",
    "continuity_no_overlap_years"
  ),
  value = c(
    as.character(nrow(pnadc_panel)),
    as.character(nrow(merged_panel)),
    as.character(n_distinct(merged_panel$estrato4)),
    as.character(sum(!is.na(merged_panel$lightning_quarterly_total))),
    as.character(sum(!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000))),
    as.character(sum(!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000_no_overlap))),
    as.character(sum(!is.na(merged_panel$dec_quarterly))),
    as.character(sum(!is.na(merged_panel$dec_quarterly_no_overlap))),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$raw_quarterly_outage_hrs_per_1000_no_overlap)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$lightning_quarterly_total)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$dec_quarterly)])), collapse = ","),
    paste(sort(unique(merged_panel$year[!is.na(merged_panel$dec_quarterly_no_overlap)])), collapse = ",")
  )
)
write_csv(qa, qa_path)

message("Done.")
message("Merged panel: ", merged_path)
message("QA: ", qa_path)
message("Figures: ", fig_dir)
