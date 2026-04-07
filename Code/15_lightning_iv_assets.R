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

power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_assets", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(out_dir, "lightning_iv_assets_results.csv")

if (!file.exists(power_panel_path)) stop("Missing file: ", power_panel_path, call. = FALSE)
if (!file.exists(macro_path)) stop("Missing file: ", macro_path, call. = FALSE)
if (!file.exists(baseline_path)) stop("Missing file: ", baseline_path, call. = FALSE)

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

power <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    uf,
    uf_sigla = unname(uf_to_sigla[uf]),
    year,
    quarter,
    weight_sum,
    lightning_quarterly_total,
    raw_quarterly_outage_hrs_per_1000,
    raw_quarterly_events_per_1000,
    raw_quarterly_outage_hrs_per_1000_no_overlap,
    raw_quarterly_events_per_1000_no_overlap,
    share_refrigerator,
    share_washing_machine,
    share_satellite_dish_tv,
    share_computer,
    share_vehicle,
    mean_rooms,
    mean_bedrooms
  )

macro <- read_parquet(macro_path) %>%
  transmute(
    uf_sigla = as.character(uf_sigla),
    year,
    quarter,
    uf_unemployment_rate,
    uf_labor_demand_proxy,
    uf_real_wage_proxy,
    uf_share_secondaryplus,
    uf_share_tertiary
  )

baseline <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_female_age_25_34,
    baseline_female_age_35_54,
    baseline_female_age_55plus,
    baseline_child_dependency,
    baseline_female_share_secondaryplus,
    baseline_female_share_tertiary,
    baseline_log_income_p25,
    baseline_log_income_p50,
    baseline_log_income_p75,
    baseline_rural_share
  )

panel <- power %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  mutate(
    trend = year - 2016,
    b_age2534_t = baseline_female_age_25_34 * trend,
    b_age3554_t = baseline_female_age_35_54 * trend,
    b_age55plus_t = baseline_female_age_55plus * trend,
    b_dep_t = baseline_child_dependency * trend,
    b_secplus_t = baseline_female_share_secondaryplus * trend,
    b_tertiary_t = baseline_female_share_tertiary * trend,
    b_inc_p25_t = baseline_log_income_p25 * trend,
    b_inc_p50_t = baseline_log_income_p50 * trend,
    b_inc_p75_t = baseline_log_income_p75 * trend,
    b_rural_t = baseline_rural_share * trend
  )

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "share_refrigerator", "Refrigerator ownership",
  "share_washing_machine", "Washing machine ownership",
  "share_satellite_dish_tv", "TV ownership (panel variable)",
  "share_computer", "Computer ownership",
  "share_vehicle", "Vehicle ownership"
)

endog_specs <- tibble::tribble(
  ~endog_var, ~endog_label,
  "raw_quarterly_outage_hrs_per_1000", "Outage hours per 1,000",
  "raw_quarterly_events_per_1000", "Outage events per 1,000",
  "raw_quarterly_outage_hrs_per_1000_no_overlap", "Outage hours per 1,000 (no-overlap)",
  "raw_quarterly_events_per_1000_no_overlap", "Outage events per 1,000 (no-overlap)"
)

base_controls <- c(
  "mean_rooms",
  "mean_bedrooms",
  "uf_unemployment_rate",
  "uf_labor_demand_proxy",
  "uf_real_wage_proxy",
  "uf_share_secondaryplus",
  "uf_share_tertiary",
  "b_age2534_t",
  "b_age3554_t",
  "b_age55plus_t",
  "b_dep_t",
  "b_secplus_t",
  "b_tertiary_t",
  "b_inc_p25_t",
  "b_inc_p50_t",
  "b_inc_p75_t",
  "b_rural_t"
)

rows <- list()
k <- 1L

for (ii in seq_len(nrow(endog_specs))) {
  endog_var <- endog_specs$endog_var[[ii]]
  endog_label <- endog_specs$endog_label[[ii]]

  for (jj in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[jj]]
    outcome_label <- outcomes$outcome_label[[jj]]

    model_controls <- if (outcome == "share_vehicle") base_controls else c("share_vehicle", base_controls)
    required_vars <- unique(c("weight_sum", "estrato4", "quarter", "lightning_quarterly_total", endog_var, outcome, model_controls))

    dat <- panel %>%
      filter(
        if_all(all_of(required_vars), ~ !is.na(.x)),
        weight_sum > 0
      )

    if (nrow(dat) == 0) next

    controls_txt <- paste(model_controls, collapse = " + ")
    fs_formula <- as.formula(paste0(endog_var, " ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter"))
    rf_formula <- as.formula(paste0(outcome, " ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter"))
    iv_formula <- as.formula(paste0(outcome, " ~ ", controls_txt, " | estrato4 + quarter | ", endog_var, " ~ lightning_quarterly_total"))

    fs <- feols(fs_formula, data = dat, weights = ~weight_sum, cluster = ~estrato4)
    rf <- feols(rf_formula, data = dat, weights = ~weight_sum, cluster = ~estrato4)
    iv <- feols(iv_formula, data = dat, weights = ~weight_sum, cluster = ~estrato4)

    fs_term <- tidy(fs) %>% filter(term == "lightning_quarterly_total")
    rf_term <- tidy(rf) %>% filter(term == "lightning_quarterly_total")
    iv_term <- tidy(iv) %>% filter(grepl("^fit_", term))

    rows[[k]] <- tibble(
      outcome = outcome,
      outcome_label = outcome_label,
      endog_var = endog_var,
      endog_label = endog_label,
      nobs = nobs(iv),
      fs_coef = ifelse(nrow(fs_term) == 0, NA_real_, fs_term$estimate[[1]]),
      fs_p = ifelse(nrow(fs_term) == 0, NA_real_, fs_term$p.value[[1]]),
      fs_f = safe_fitstat(fs, "f"),
      rf_coef = ifelse(nrow(rf_term) == 0, NA_real_, rf_term$estimate[[1]]),
      rf_p = ifelse(nrow(rf_term) == 0, NA_real_, rf_term$p.value[[1]]),
      iv_coef = ifelse(nrow(iv_term) == 0, NA_real_, iv_term$estimate[[1]]),
      iv_se = ifelse(nrow(iv_term) == 0, NA_real_, iv_term$std.error[[1]]),
      iv_p = ifelse(nrow(iv_term) == 0, NA_real_, iv_term$p.value[[1]]),
      ivf1 = safe_fitstat(iv, "ivf1"),
      ivfall = safe_fitstat(iv, "ivfall")
    )
    k <- k + 1L
  }
}

results <- bind_rows(rows) %>%
  arrange(endog_var, outcome)

write_csv(results, results_path)

message("Wrote: ", results_path)
print(results)
