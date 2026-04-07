#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(httr)
  library(jsonlite)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(readr)
  library(lubridate)
})

options(timeout = max(3600, getOption("timeout")))

root_dir <- here::here()
out_dir <- here::here("data", "powerIV", "outages")
raw_dir <- here::here(out_dir, "raw", "raw_events")
panel_dir <- here::here(out_dir, "panels")
qa_dir <- here::here(out_dir, "qa")
legacy_csv_dir <- here::here("data", "ANEEL", "CSVs")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

conj_month_out <- here::here(panel_dir, "raw_event_conj_month.parquet")
qa_out <- here::here(qa_dir, "raw_event_conj_month_qa.csv")

years_env <- Sys.getenv("ANEEL_TARGET_YEARS", unset = "")
target_years <- if (nzchar(years_env)) {
  years <- suppressWarnings(as.integer(trimws(strsplit(years_env, ",", fixed = TRUE)[[1]])))
  years <- years[!is.na(years)]
  if (length(years) == 0) stop("ANEEL_TARGET_YEARS did not contain valid years.")
  sort(unique(years))
} else {
  2017:2024
}
expected_max_month <- as.Date(sprintf("%04d-12-01", max(target_years)))

package_slug <- "interrupcoes-de-energia-eletrica-nas-redes-de-distribuicao"
package_url <- "https://dadosabertos.aneel.gov.br/api/3/action/package_show"

normalize_text <- function(x) {
  x <- as.character(x)
  x_utf8 <- suppressWarnings(iconv(x, to = "UTF-8", sub = ""))
  x_utf8[is.na(x_utf8)] <- x[is.na(x_utf8)]
  x_ascii <- suppressWarnings(iconv(x_utf8, to = "ASCII//TRANSLIT", sub = ""))
  x_ascii[is.na(x_ascii)] <- x_utf8[is.na(x_ascii)]
  x_ascii <- tolower(trimws(x_ascii))
  x_ascii <- gsub("[^a-z ]", "", x_ascii)
  gsub("\\s+", " ", x_ascii)
}

get_resource_table <- function(target_years) {
  response <- httr::GET(package_url, query = list(id = package_slug))
  httr::stop_for_status(response)
  payload <- jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"), flatten = TRUE)
  as.data.table(payload$result$resources)[
    grepl("^interrupcoes-energia-eletrica-[0-9]{4}$", name) &
      grepl("\\.csv$", url, ignore.case = TRUE),
    .(name, url, size)
  ][
    , year := as.integer(sub(".*-([0-9]{4})$", "\\1", name))
  ][
    year %in% target_years
  ][order(year)]
}

download_if_needed <- function(url, dest_path, expected_size = NA_real_, legacy_path = NULL) {
  if (!is.null(legacy_path) && file.exists(legacy_path) && file.info(legacy_path)$size > 0) {
    return(legacy_path)
  }
  if (file.exists(dest_path) && file.info(dest_path)$size > 0) {
    if (!is.na(expected_size) && file.info(dest_path)$size >= expected_size * 0.995) {
      return(dest_path)
    }
  }
  cmd <- sprintf(
    "wget -c -O %s %s",
    shQuote(dest_path),
    shQuote(url)
  )
  status <- system(cmd)
  if (status != 0) {
    stop("Download failed for ", basename(dest_path))
  }
  if (!is.na(expected_size) && file.info(dest_path)$size < expected_size * 0.995) {
    stop("Downloaded file appears incomplete for ", basename(dest_path))
  }
  dest_path
}

read_and_aggregate <- function(path) {
  dt <- data.table::fread(
    path,
    sep = ";",
    encoding = "Latin-1",
    colClasses = "character",
    select = c(
      "IdeConjuntoUnidadeConsumidora",
      "DscTipoInterrupcao",
      "DatInicioInterrupcao",
      "DatFimInterrupcao",
      "NumUnidadeConsumidora",
      "NumConsumidorConjunto"
    ),
    showProgress = TRUE
  )

  data.table::setnames(
    dt,
    c(
      "IdeConjuntoUnidadeConsumidora",
      "DscTipoInterrupcao",
      "DatInicioInterrupcao",
      "DatFimInterrupcao",
      "NumUnidadeConsumidora",
      "NumConsumidorConjunto"
    ),
    c(
      "conj_code",
      "interruption_type",
      "interruption_start",
      "interruption_end",
      "affected_units",
      "group_consumer_count"
    )
  )

  dt[, interruption_type_norm := normalize_text(interruption_type)]
  dt <- dt[interruption_type_norm == "nao programada"]

  dt[, start_ts := as.POSIXct(interruption_start, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
  dt[, end_ts := as.POSIXct(interruption_end, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
  dt <- dt[!is.na(start_ts) & !is.na(end_ts)]

  dt[, outage_duration_min := as.numeric(difftime(end_ts, start_ts, units = "mins"))]
  dt <- dt[is.finite(outage_duration_min) & outage_duration_min >= 0]

  dt[, affected_units := suppressWarnings(as.numeric(affected_units))]
  dt[, group_consumer_count := suppressWarnings(as.numeric(group_consumer_count))]
  dt[, affected_units := fifelse(is.na(affected_units) | affected_units < 0, 0, affected_units)]
  dt[, customer_outage_min := outage_duration_min * affected_units]
  dt[, year := lubridate::year(start_ts)]
  dt[, month := lubridate::month(start_ts)]

  dt[
    ,
    .(
      total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
      n_events = .N,
      group_consumer_count_avg = mean(group_consumer_count, na.rm = TRUE)
    ),
    by = .(conj_code, year, month)
  ][
    , date := as.Date(sprintf("%04d-%02d-01", year, month))
  ][]
}

message("Retrieving ANEEL interruption resources.")
resources <- get_resource_table(target_years)
if (nrow(resources) == 0) {
  stop("No ANEEL interruption resources found for requested years.")
}

existing_panel <- NULL
existing_years <- integer(0)
if (file.exists(conj_month_out)) {
  existing_panel <- read_parquet(conj_month_out) %>%
    mutate(date = as.Date(date))
  existing_years <- sort(unique(existing_panel$year))
}

years_to_process <- setdiff(target_years, existing_years)
if (length(years_to_process) == 0 && !is.null(existing_panel)) {
  existing_max_month <- max(existing_panel$date, na.rm = TRUE)
  if (!is.na(existing_max_month) && existing_max_month >= expected_max_month) {
    message("Raw outage panel already covers target years through 2024. Skipping rebuild.")
    if (!file.exists(qa_out)) {
      qa <- tibble(
        metric = c("rows", "distinct_conj", "min_date", "max_date", "years_covered"),
        value = c(
          as.character(nrow(existing_panel)),
          as.character(dplyr::n_distinct(existing_panel$conj_code)),
          as.character(min(existing_panel$date, na.rm = TRUE)),
          as.character(max(existing_panel$date, na.rm = TRUE)),
          paste(sort(unique(existing_panel$year)), collapse = ",")
        )
      )
      write_csv(qa, qa_out)
    }
    quit(save = "no", status = 0)
  }
}

if (length(years_to_process) == 0) {
  years_to_process <- target_years
  existing_panel <- NULL
}

resources_to_process <- resources[year %in% years_to_process][order(year)]
if (nrow(resources_to_process) == 0) {
  stop("Could not locate ANEEL resources for years that still need processing.")
}

local_files <- resources_to_process[
  ,
  {
    dest <- here::here(raw_dir, paste0(name, ".csv"))
    legacy <- here::here(legacy_csv_dir, paste0(name, ".csv"))
    path <- download_if_needed(url, dest, expected_size = as.numeric(size), legacy_path = legacy)
    .(path = path)
  },
  by = .(year, name, url)
][order(year)]

agg_list <- vector("list", length = nrow(local_files))
for (i in seq_len(nrow(local_files))) {
  message("Processing ANEEL interruptions for year ", local_files$year[i])
  agg_list[[i]] <- read_and_aggregate(local_files$path[i])
  gc()
}

new_panel <- rbindlist(agg_list, use.names = TRUE, fill = TRUE)[
  year %in% target_years
]

if (!is.null(existing_panel)) {
  combined <- bind_rows(
    as_tibble(existing_panel) %>% filter(!(year %in% years_to_process)),
    as_tibble(new_panel)
  )
} else {
  combined <- as_tibble(new_panel)
}

conj_month <- combined %>%
  group_by(conj_code, year, month, date) %>%
  summarise(
    total_customer_outage_hrs = sum(total_customer_outage_hrs, na.rm = TRUE),
    n_events = sum(n_events, na.rm = TRUE),
    group_consumer_count_avg = mean(group_consumer_count_avg, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(conj_code, date)

write_parquet(conj_month, conj_month_out)

qa <- tibble(
  metric = c("rows", "distinct_conj", "min_date", "max_date", "years_covered", "source_years_processed"),
  value = c(
    as.character(nrow(conj_month)),
    as.character(dplyr::n_distinct(conj_month$conj_code)),
    as.character(min(conj_month$date, na.rm = TRUE)),
    as.character(max(conj_month$date, na.rm = TRUE)),
    paste(sort(unique(conj_month$year)), collapse = ","),
    paste(sort(unique(local_files$year)), collapse = ",")
  )
)
write_csv(qa, qa_out)

message("Wrote: ", conj_month_out)
message("QA: ", qa_out)
