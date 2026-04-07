#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(tidyr)
})

root_dir <- here::here()

lfp_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_national_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_national_2016_2024.parquet")
lpg_uf_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet")
lpg_estrato_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "coverage")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
var_year_out <- here::here(out_dir, "power_variable_coverage_by_year.csv")
sample_out <- here::here(out_dir, "power_sample_sizes.csv")
sample_year_out <- here::here(out_dir, "power_sample_sizes_by_year.csv")
qa_out <- here::here(out_dir, "power_coverage_qa.csv")

if (!file.exists(lfp_path)) stop("Missing LFP panel: ", lfp_path)
if (!file.exists(power_path)) stop("Missing PNAD-C power panel: ", power_path)

lfp <- read_parquet(lfp_path) %>%
  mutate(estrato4 = as.character(estrato4))

power <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_cooking_wood_charcoal,
    share_vehicle,
    share_internet_home,
    share_electricity_from_grid,
    share_grid_full_time
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter"), suffix = c("", "_visit1"))

lpg_national <- if (file.exists(lpg_national_path)) {
  read_parquet(lpg_national_path) %>%
    transmute(year, quarter, has_lpg_national = 1L)
} else {
  tibble(year = integer(), quarter = integer(), has_lpg_national = integer())
}

lpg_uf <- if (file.exists(lpg_uf_path)) {
  read_parquet(lpg_uf_path) %>%
    transmute(uf_sigla, year, quarter, has_lpg_uf = 1L)
} else {
  tibble(uf_sigla = character(), year = integer(), quarter = integer(), has_lpg_uf = integer())
}

lpg_estrato <- if (file.exists(lpg_estrato_path)) {
  read_parquet(lpg_estrato_path) %>%
    transmute(estrato4 = as.character(estrato4), year, quarter, has_lpg_estrato = 1L)
} else {
  tibble(estrato4 = character(), year = integer(), quarter = integer(), has_lpg_estrato = integer())
}

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP",
  "Tocantins" = "TO", "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN",
  "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS", "Mato Grosso do Sul" = "MS",
  "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

panel <- panel %>%
  mutate(uf_sigla = unname(uf_to_sigla[uf])) %>%
  left_join(lpg_national, by = c("year", "quarter")) %>%
  left_join(lpg_uf, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(lpg_estrato, by = c("estrato4", "year", "quarter")) %>%
  mutate(
    has_lpg_national = ifelse(is.na(has_lpg_national), 0L, has_lpg_national),
    has_lpg_uf = ifelse(is.na(has_lpg_uf), 0L, has_lpg_uf),
    has_lpg_estrato = ifelse(is.na(has_lpg_estrato), 0L, has_lpg_estrato)
  )

vars <- c(
  "female_lfp",
  "female_hours_effective_employed",
  "female_hours_effective_population",
  "lightning_quarterly_total",
  "raw_quarterly_outage_hrs_per_1000",
  "dec_quarterly",
  "share_cooking_wood_charcoal",
  "share_vehicle",
  "share_internet_home",
  "share_electricity_from_grid",
  "share_grid_full_time",
  "has_lpg_national",
  "has_lpg_uf",
  "has_lpg_estrato"
)

var_by_year <- bind_rows(lapply(vars, function(v) {
  panel %>%
    group_by(year) %>%
    summarise(
      non_missing = if (grepl("^has_lpg_", v)) sum(.data[[v]] == 1L, na.rm = TRUE) else sum(!is.na(.data[[v]])),
      non_na = sum(!is.na(.data[[v]])),
      total = n(),
      .groups = "drop"
    ) %>%
    mutate(variable = v)
})) %>%
  select(variable, year, total, non_na, non_missing) %>%
  arrange(variable, year)

sample_defs <- list(
  raw_light = c("raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total"),
  raw_light_controls = c("raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time"),
  dec_light = c("dec_quarterly", "lightning_quarterly_total"),
  dec_light_controls = c("dec_quarterly", "lightning_quarterly_total", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time"),
  raw_charcoal_controls = c("raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total", "share_cooking_wood_charcoal", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time"),
  dec_charcoal_controls = c("dec_quarterly", "lightning_quarterly_total", "share_cooking_wood_charcoal", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time"),
  shift_share_national = c("share_cooking_wood_charcoal", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time", "has_lpg_national"),
  shift_share_uf = c("share_cooking_wood_charcoal", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time", "has_lpg_uf"),
  shift_share_estrato = c("share_cooking_wood_charcoal", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time", "has_lpg_estrato")
)

sample_sizes <- bind_rows(lapply(names(sample_defs), function(nm) {
  needed <- sample_defs[[nm]]
  ok_vec <- Reduce(`&`, lapply(needed, function(v) {
    if (grepl("^has_lpg_", v)) {
      panel[[v]] == 1L
    } else {
      !is.na(panel[[v]])
    }
  }))
  ok <- panel[ok_vec, , drop = FALSE]
  tibble(
    sample_id = nm,
    nobs = nrow(ok),
    years = paste(sort(unique(ok$year)), collapse = ",")
  )
}))

sample_sizes_by_year <- bind_rows(lapply(names(sample_defs), function(nm) {
  needed <- sample_defs[[nm]]
  ok_vec <- Reduce(`&`, lapply(needed, function(v) {
    if (grepl("^has_lpg_", v)) {
      panel[[v]] == 1L
    } else {
      !is.na(panel[[v]])
    }
  }))
  panel %>%
    mutate(ok = ok_vec) %>%
    group_by(year) %>%
    summarise(nobs = sum(ok), total = n(), .groups = "drop") %>%
    mutate(sample_id = nm)
})) %>%
  select(sample_id, year, total, nobs) %>%
  arrange(sample_id, year)

write_csv(var_by_year, var_year_out)
write_csv(sample_sizes, sample_out)
write_csv(sample_sizes_by_year, sample_year_out)

qa <- tibble(
  metric = c(
    "panel_rows",
    "panel_years",
    "raw_non_na",
    "dec_non_na",
    "light_non_na",
    "charcoal_non_na",
    "controls_complete_rows"
  ),
  value = c(
    as.character(nrow(panel)),
    paste(sort(unique(panel$year)), collapse = ","),
    as.character(sum(!is.na(panel$raw_quarterly_outage_hrs_per_1000))),
    as.character(sum(!is.na(panel$dec_quarterly))),
    as.character(sum(!is.na(panel$lightning_quarterly_total))),
    as.character(sum(!is.na(panel$share_cooking_wood_charcoal))),
    as.character(sum(!is.na(panel$share_vehicle) & !is.na(panel$share_internet_home) & !is.na(panel$share_electricity_from_grid) & !is.na(panel$share_grid_full_time)))
  )
)
write_csv(qa, qa_out)

message("Wrote: ", var_year_out)
message("Wrote: ", sample_out)
message("Wrote: ", sample_year_out)
message("Wrote: ", qa_out)
