#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

root_dir <- here::here()

hh_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_household_micro", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(out_dir, "lightning_iv_household_micro_results.csv")

if (!file.exists(hh_path)) stop("Missing file: ", hh_path, call. = FALSE)
if (!file.exists(power_path)) stop("Missing file: ", power_path, call. = FALSE)
if (!file.exists(macro_path)) stop("Missing file: ", macro_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

hh <- read_parquet(hh_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    uf = as.character(uf),
    uf_sigla = unname(uf_to_sigla[uf]),
    weight = as.numeric(weight_calibrated),
    n_rooms = as.numeric(n_rooms),
    n_bedrooms = as.numeric(n_bedrooms),
    rural = if_else(area_situation == "Rural", 1, 0, missing = 0),
    washing_machine = as.numeric(washing_machine),
    computer = as.numeric(computer),
    vehicle = as.numeric(vehicle)
  )

power <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000)
  ) %>%
  distinct(estrato4, year, quarter, .keep_all = TRUE)

macro <- read_parquet(macro_path) %>%
  transmute(
    uf_sigla = as.character(uf_sigla),
    year = as.integer(year),
    quarter = as.integer(quarter),
    uf_unemployment_rate = as.numeric(uf_unemployment_rate),
    uf_labor_demand_proxy = as.numeric(uf_labor_demand_proxy),
    uf_real_wage_proxy = as.numeric(uf_real_wage_proxy),
    uf_share_secondaryplus = as.numeric(uf_share_secondaryplus),
    uf_share_tertiary = as.numeric(uf_share_tertiary)
  ) %>%
  distinct(uf_sigla, year, quarter, .keep_all = TRUE)

panel <- hh %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter"))

outcomes <- c("washing_machine", "computer", "vehicle")

controls <- c(
  "n_rooms",
  "n_bedrooms",
  "rural",
  "uf_unemployment_rate",
  "uf_labor_demand_proxy",
  "uf_real_wage_proxy",
  "uf_share_secondaryplus",
  "uf_share_tertiary"
)
controls_txt <- paste(controls, collapse = " + ")

samples <- tibble::tribble(
  ~sample_name, ~year_min, ~year_max,
  "full_2016_2024", 2016L, 2024L,
  "prepandemic_2016_2019", 2016L, 2019L
)

rows <- list()
k <- 1L

for (ii in seq_len(nrow(samples))) {
  sample_name <- samples$sample_name[[ii]]
  year_min <- samples$year_min[[ii]]
  year_max <- samples$year_max[[ii]]

  panel_s <- panel %>% filter(year >= year_min, year <= year_max)

  for (jj in seq_along(outcomes)) {
    y <- outcomes[[jj]]
    needed <- c(
      "weight",
      "estrato4",
      "quarter",
      "raw_quarterly_outage_hrs_per_1000",
      "lightning_quarterly_total",
      y,
      controls
    )

    dat <- panel_s %>%
      filter(if_all(all_of(needed), ~ !is.na(.x)), weight > 0)

    if (nrow(dat) == 0) next
    if (is.na(sd(dat[[y]], na.rm = TRUE)) || sd(dat[[y]], na.rm = TRUE) == 0) next

    fs <- feols(
      as.formula(paste0("raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter")),
      data = dat,
      weights = ~weight,
      cluster = ~estrato4
    )

    rf <- feols(
      as.formula(paste0(y, " ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter")),
      data = dat,
      weights = ~weight,
      cluster = ~estrato4
    )

    iv <- feols(
      as.formula(paste0(y, " ~ ", controls_txt, " | estrato4 + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total")),
      data = dat,
      weights = ~weight,
      cluster = ~estrato4
    )

    fs_t <- tidy(fs) %>% filter(term == "lightning_quarterly_total")
    rf_t <- tidy(rf) %>% filter(term == "lightning_quarterly_total")
    iv_t <- tidy(iv) %>% filter(grepl("^fit_", term))

    iv_coef <- ifelse(nrow(iv_t) == 0, NA_real_, iv_t$estimate[[1]])
    iv_se <- ifelse(nrow(iv_t) == 0, NA_real_, iv_t$std.error[[1]])

    rows[[k]] <- tibble(
      sample = sample_name,
      outcome = y,
      nobs = nobs(iv),
      fs_coef = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$estimate[[1]]),
      fs_p = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$p.value[[1]]),
      rf_coef = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$estimate[[1]]),
      rf_p = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$p.value[[1]]),
      iv_coef = iv_coef,
      iv_se = iv_se,
      iv_p = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$p.value[[1]]),
      iv_coef_per_100_outage_hours = iv_coef * 100,
      iv_coef_pp_per_100_outage_hours = iv_coef * 10000,
      iv_se_pp_per_100_outage_hours = iv_se * 10000,
      ivf1 = safe_fitstat(iv, "ivf1"),
      ivfall = safe_fitstat(iv, "ivfall")
    )
    k <- k + 1L
  }
}

results <- bind_rows(rows) %>% arrange(sample, outcome)
write_csv(results, results_path)

message("Wrote: ", results_path)
print(results)
