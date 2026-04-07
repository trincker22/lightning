#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(httr)
  library(jsonlite)
  library(data.table)
  library(arrow)
})

options(timeout = max(3600, getOption("timeout")))

root_dir <- here::here()

out_dir <- here::here("data", "powerIV", "outages")
raw_dir <- here::here(out_dir, "raw")
panel_dir <- here::here(out_dir, "panels")
qa_dir <- here::here(out_dir, "qa")
legacy_raw_dir <- here::here("data", "BrazilPowerPipeline", "outages", "raw")

dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

panel_path <- here::here(panel_dir, "aneel_conj_monthly_outages.parquet")
qa_path <- here::here(qa_dir, "aneel_conj_monthly_outages_qa.parquet")

if (all(file.exists(c(panel_path, qa_path)))) {
  message("All ANEEL continuity outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

package_slug <- "indicadores-coletivos-de-continuidade-dec-e-fec"
package_url <- "https://dadosabertos.aneel.gov.br/api/3/action/package_show"

get_resource_table <- function() {
  response <- httr::GET(package_url, query = list(id = package_slug))
  httr::stop_for_status(response)
  payload <- jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"), flatten = TRUE)
  as.data.table(payload$result$resources)[
    grepl("^indicadores-continuidade-coletivos-(2010-2019|2020-2029)$", name) &
      !grepl("compensacao|limite|atributos", name, ignore.case = TRUE) &
      grepl("\\.csv$", url, ignore.case = TRUE),
    .(name, url)
  ][order(name)]
}

download_if_needed <- function(url, dest_path, legacy_path = NULL) {
  if (!is.null(legacy_path) && file.exists(legacy_path) && file.info(legacy_path)$size > 0) {
    message("Using existing cached file ", basename(legacy_path))
    return(legacy_path)
  }
  remote_size <- suppressWarnings(as.numeric(httr::headers(httr::HEAD(url))[["content-length"]]))
  if (file.exists(dest_path) && file.info(dest_path)$size > 0) {
    local_size <- file.info(dest_path)$size
    if (!is.na(remote_size) && identical(local_size, remote_size)) {
      return(dest_path)
    }
    message("Re-downloading incomplete or stale file ", basename(dest_path))
  }
  message("Downloading ", basename(dest_path))
  utils::download.file(url, destfile = dest_path, mode = "wb", quiet = FALSE)
  if (!is.na(remote_size) && file.info(dest_path)$size != remote_size) {
    stop("Downloaded file size does not match expected size for ", basename(dest_path))
  }
  dest_path
}

parse_numeric_br <- function(x) {
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x <- sub("^,", "0,", x)
  x <- gsub("\\.", "", x, fixed = FALSE)
  x <- sub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

safe_text <- function(x) {
  trimws(iconv(x, from = "Latin1", to = "UTF-8", sub = ""))
}

read_continuity_csv <- function(path) {
  dt <- data.table::fread(
    path,
    sep = ";",
    encoding = "Latin-1",
    colClasses = "character",
    showProgress = TRUE
  )

  needed <- c(
    "DatGeracaoConjuntoDados",
    "SigAgente",
    "NumCNPJ",
    "IdeConjUndConsumidoras",
    "DscConjUndConsumidoras",
    "SigIndicador",
    "AnoIndice",
    "NumPeriodoIndice",
    "VlrIndiceEnviado"
  )
  missing_cols <- setdiff(needed, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing expected columns in ", path, ": ", paste(missing_cols, collapse = ", "))
  }

  dt <- dt[
    SigIndicador %chin% c("DEC", "FEC")
  ][
    ,
    .(
      data_generated = as.Date(DatGeracaoConjuntoDados),
      distributor_code = safe_text(SigAgente),
      distributor_cnpj = safe_text(NumCNPJ),
      conj_code = safe_text(IdeConjUndConsumidoras),
      conj_name = safe_text(DscConjUndConsumidoras),
      indicator = safe_text(SigIndicador),
      year = suppressWarnings(as.integer(AnoIndice)),
      month = suppressWarnings(as.integer(NumPeriodoIndice)),
      value = parse_numeric_br(VlrIndiceEnviado)
    )
  ][
    year >= 2010 & !is.na(year) & month >= 1 & month <= 12
  ][
    ,
    date := as.Date(sprintf("%04d-%02d-01", year, month))
  ]

  dt[]
}

message("Retrieving ANEEL continuity resources.")
resources <- get_resource_table()
if (nrow(resources) == 0) {
  stop("No ANEEL continuity CSV resources were found.")
}

local_files <- resources[
  ,
  {
    dest <- here::here(raw_dir, name)
    legacy <- here::here(legacy_raw_dir, name)
    download_if_needed(url, dest, legacy_path = legacy)
  },
  by = .(name, url)
]

message("Reading and combining ANEEL continuity files.")
dt_list <- lapply(local_files$V1, read_continuity_csv)
outages_long <- rbindlist(dt_list, use.names = TRUE, fill = TRUE)

dup_summary <- outages_long[
  ,
  .(
    n_rows = .N,
    n_distinct_values = uniqueN(value)
  ),
  by = .(conj_code, date, indicator)
][n_rows > 1]

if (nrow(dup_summary) > 0) {
  message("Found duplicated CONJ-date-indicator rows; collapsing to unique values.")
}

outages_long <- unique(outages_long)

outages_meta <- outages_long[
  ,
  .(
    conj_name = first(na.omit(conj_name)),
    distributor_code = first(na.omit(distributor_code)),
    distributor_cnpj = first(na.omit(distributor_cnpj)),
    data_generated = max(data_generated, na.rm = TRUE)
  ),
  by = .(conj_code, date, year, month)
]

outages_wide <- dcast(
  outages_long,
  conj_code + date + year + month ~ indicator,
  value.var = "value",
  fun.aggregate = function(x) {
    vals <- unique(na.omit(x))
    if (length(vals) == 0) {
      return(NA_real_)
    }
    vals[[1]]
  }
)

setnames(outages_wide, old = c("DEC", "FEC"), new = c("dec_monthly", "fec_monthly"), skip_absent = TRUE)

outages_panel <- outages_meta[outages_wide, on = .(conj_code, date, year, month)][
  order(conj_code, date)
]

qa_table <- rbindlist(
  list(
    data.table(metric = "rows_panel", value = as.character(nrow(outages_panel))),
    data.table(metric = "conj_codes", value = as.character(uniqueN(outages_panel$conj_code))),
    data.table(metric = "first_month", value = as.character(min(outages_panel$date, na.rm = TRUE))),
    data.table(metric = "last_month", value = as.character(max(outages_panel$date, na.rm = TRUE))),
    data.table(metric = "duplicate_conj_date_indicator_rows", value = as.character(nrow(dup_summary)))
  ),
  use.names = TRUE
)

message("Writing outage panel and QA outputs.")
arrow::write_parquet(as.data.frame(outages_panel), panel_path)
arrow::write_parquet(as.data.frame(qa_table), qa_path)

message("Done.")
message("Panel: ", panel_path)
message("QA:    ", qa_path)
