#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(sf)
  library(dplyr)
  library(stringr)
  library(data.table)
  library(arrow)
})

root_dir <- here::here()

bdgd_gpkg_dir <- here::here("data", "ANEEL", "BDGD_CONJ", "gpkg")
out_dir <- here::here("data", "powerIV", "outages")
shape_dir <- here::here(out_dir, "shapes")
qa_dir <- here::here(out_dir, "qa")
panel_path <- here::here(out_dir, "panels", "aneel_conj_monthly_outages.parquet")

dir.create(shape_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

all_vintages_gpkg_path <- here::here(shape_dir, "aneel_conj_all_vintages.gpkg")
match_path <- here::here(qa_dir, "aneel_conj_outage_nearest_geometry_match.parquet")
coverage_path <- here::here(qa_dir, "aneel_conj_vintage_match_coverage.parquet")

if (all(file.exists(c(all_vintages_gpkg_path, match_path, coverage_path)))) {
  message("All CONJ geometry outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

gpkg_files <- list.files(bdgd_gpkg_dir, pattern = "\\.gpkg$", full.names = TRUE)
if (length(gpkg_files) == 0) {
  stop("No BDGD CONJ GeoPackages found in ", bdgd_gpkg_dir)
}

message("Reading all cached BDGD CONJ vintages.")
read_conj_gpkg <- function(path) {
  snapshot_date <- as.Date(str_extract(basename(path), "\\d{4}-\\d{2}-\\d{2}"))
  publish_stamp <- extract_publish_stamp(basename(path))
  x <- suppressMessages(st_read(path, layer = "CONJ", quiet = TRUE))
  name_col <- if ("NOME" %in% names(x)) "NOME" else if ("NOM" %in% names(x)) "NOM" else NA_character_
  if (is.na(name_col)) {
    stop("No expected name field found in ", basename(path))
  }
  if (nrow(x) == 0) {
    return(st_sf(
      conj_code = character(),
      distributor_code_geom = character(),
      conj_name_geom = character(),
      geometry_snapshot_date = as.Date(character()),
      publish_stamp = character(),
      source_file = character(),
      geometry = st_sfc(crs = st_crs(x))
    ))
  }
  st_sf(
    conj_code = as.character(x$COD_ID),
    distributor_code_geom = as.character(x$DIST),
    conj_name_geom = as.character(x[[name_col]]),
    geometry_snapshot_date = snapshot_date,
    publish_stamp = publish_stamp,
    source_file = basename(path),
    geometry = st_geometry(x),
    crs = st_crs(x)
  )
}

conj_all <- bind_rows(lapply(gpkg_files, read_conj_gpkg)) |>
  st_as_sf()

message("Deduplicating repeated CONJ + snapshot-date geometries.")
conj_all <- conj_all |>
  mutate(source_rank = rank(publish_stamp, ties.method = "first")) |>
  arrange(conj_code, geometry_snapshot_date, desc(publish_stamp), source_file) |>
  group_by(conj_code, geometry_snapshot_date) |>
  slice(1) |>
  ungroup() |>
  select(-source_rank)

if (requireNamespace("lwgeom", quietly = TRUE)) {
  invalid <- !sf::st_is_valid(conj_all)
  if (any(invalid)) {
    conj_all[invalid, ] <- sf::st_make_valid(conj_all[invalid, ])
  }
}

message("Writing all-vintage CONJ geometry layer.")
sf::write_sf(conj_all, all_vintages_gpkg_path, delete_dsn = TRUE, quiet = TRUE)

if (!file.exists(panel_path)) {
  stop("Missing outage panel: ", panel_path)
}

message("Matching outage months to nearest available geometry snapshot by CONJ code.")
panel_dt <- as.data.table(arrow::read_parquet(panel_path))[
  ,
  .(
    conj_code = as.character(conj_code),
    outage_date = as.Date(date)
  )
][
  ,
  .(conj_code, outage_date)
][
  order(conj_code, outage_date)
]

geom_idx <- as.data.table(st_drop_geometry(conj_all))[
  ,
  .(
    conj_code = as.character(conj_code),
    snapshot_date = as.Date(geometry_snapshot_date),
    snapshot_date_value = as.Date(geometry_snapshot_date),
    distributor_code_geom,
    conj_name_geom,
    source_file
  )
][
  order(conj_code, snapshot_date)
]

setkey(geom_idx, conj_code, snapshot_date)

prev_match <- geom_idx[panel_dt, on = .(conj_code, snapshot_date = outage_date), roll = Inf]
next_match <- geom_idx[panel_dt, on = .(conj_code, snapshot_date = outage_date), roll = -Inf]

match_dt <- copy(panel_dt)
match_dt[, prev_snapshot_date := prev_match$snapshot_date_value]
match_dt[, next_snapshot_date := next_match$snapshot_date_value]
match_dt[, prev_source_file := prev_match$source_file]
match_dt[, next_source_file := next_match$source_file]
match_dt[, prev_distributor_code_geom := prev_match$distributor_code_geom]
match_dt[, next_distributor_code_geom := next_match$distributor_code_geom]
match_dt[, prev_conj_name_geom := prev_match$conj_name_geom]
match_dt[, next_conj_name_geom := next_match$conj_name_geom]

match_dt[is.na(prev_source_file), prev_snapshot_date := as.Date(NA)]
match_dt[is.na(next_source_file), next_snapshot_date := as.Date(NA)]

match_dt[, prev_diff_days := fifelse(is.na(prev_snapshot_date), Inf, abs(as.integer(outage_date - prev_snapshot_date)))]
match_dt[, next_diff_days := fifelse(is.na(next_snapshot_date), Inf, abs(as.integer(next_snapshot_date - outage_date)))]

match_dt[, use_prev := prev_diff_days <= next_diff_days]
match_dt[is.infinite(prev_diff_days) & !is.infinite(next_diff_days), use_prev := FALSE]

match_dt[, geometry_snapshot_date := fifelse(use_prev, prev_snapshot_date, next_snapshot_date)]
match_dt[, source_file := fifelse(use_prev, prev_source_file, next_source_file)]
match_dt[, distributor_code_geom := fifelse(use_prev, prev_distributor_code_geom, next_distributor_code_geom)]
match_dt[, conj_name_geom := fifelse(use_prev, prev_conj_name_geom, next_conj_name_geom)]
match_dt[, snapshot_distance_days := pmin(prev_diff_days, next_diff_days)]
match_dt[, matched_geometry := !is.na(source_file)]
match_dt[matched_geometry == FALSE, geometry_snapshot_date := as.Date(NA)]
match_dt[matched_geometry == FALSE, snapshot_distance_days := NA_real_]

match_out <- match_dt[
  ,
  .(
    conj_code,
    date = outage_date,
    geometry_snapshot_date,
    snapshot_distance_days,
    distributor_code_geom,
    conj_name_geom,
    source_file,
    matched_geometry
  )
][order(conj_code, date)]

coverage_dt <- rbindlist(
  list(
    data.table(metric = "rows", value = as.character(nrow(match_out))),
    data.table(metric = "matched_rows", value = as.character(sum(match_out$matched_geometry, na.rm = TRUE))),
    data.table(metric = "distinct_conj_with_any_match", value = as.character(uniqueN(match_out[matched_geometry == TRUE, conj_code]))),
    data.table(metric = "first_snapshot_date", value = as.character(min(conj_all$geometry_snapshot_date, na.rm = TRUE))),
    data.table(metric = "last_snapshot_date", value = as.character(max(conj_all$geometry_snapshot_date, na.rm = TRUE)))
  ),
  use.names = TRUE
)

message("Writing nearest-vintage match outputs.")
arrow::write_parquet(as.data.frame(match_out), match_path)
arrow::write_parquet(as.data.frame(coverage_dt), coverage_path)

message("Done.")
message("All vintages: ", all_vintages_gpkg_path)
message("Match table: ", match_path)
message("Coverage:    ", coverage_path)
