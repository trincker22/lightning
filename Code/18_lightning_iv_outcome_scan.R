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

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_scan", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(out_dir, "lightning_iv_outcome_scan_prepandemic.csv")
hits_path <- here::here(out_dir, "lightning_iv_outcome_scan_hits_prepandemic.csv")

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

panel <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    uf = as.character(uf),
    uf_sigla = unname(uf_to_sigla[uf]),
    year = as.integer(year),
    quarter = as.integer(quarter),
    weight_sum = as.numeric(weight_sum),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000),
    share_electricity_any = as.numeric(share_electricity_any),
    share_electricity_other_source = as.numeric(share_electricity_other_source),
    share_grid_full_time = as.numeric(share_grid_full_time),
    share_cooking_gas_any = as.numeric(share_cooking_gas_any),
    share_cooking_gas_bottled = as.numeric(share_cooking_gas_bottled),
    share_cooking_gas_piped = as.numeric(share_cooking_gas_piped),
    share_cooking_wood_charcoal = as.numeric(share_cooking_wood_charcoal),
    share_cooking_electricity = as.numeric(share_cooking_electricity),
    share_cooking_other_fuel = as.numeric(share_cooking_other_fuel),
    share_landline = as.numeric(share_landline),
    share_refrigerator = as.numeric(share_refrigerator),
    share_washing_machine = as.numeric(share_washing_machine),
    share_satellite_dish_tv = as.numeric(share_satellite_dish_tv),
    share_computer = as.numeric(share_computer),
    share_internet_home = as.numeric(share_internet_home),
    share_internet_by_mobile = as.numeric(share_internet_by_mobile),
    share_vehicle = as.numeric(share_vehicle),
    mean_rooms = as.numeric(mean_rooms),
    mean_bedrooms = as.numeric(mean_bedrooms),
    mean_mobile_phones = as.numeric(mean_mobile_phones),
    mean_appliance_count_basic = as.numeric(mean_appliance_count_basic),
    mean_appliance_count_connected = as.numeric(mean_appliance_count_connected)
  )

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
  )

baseline <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_female_age_25_34 = as.numeric(baseline_female_age_25_34),
    baseline_female_age_35_54 = as.numeric(baseline_female_age_35_54),
    baseline_female_age_55plus = as.numeric(baseline_female_age_55plus),
    baseline_child_dependency = as.numeric(baseline_child_dependency),
    baseline_female_share_secondaryplus = as.numeric(baseline_female_share_secondaryplus),
    baseline_female_share_tertiary = as.numeric(baseline_female_share_tertiary),
    baseline_log_income_p25 = as.numeric(baseline_log_income_p25),
    baseline_log_income_p50 = as.numeric(baseline_log_income_p50),
    baseline_log_income_p75 = as.numeric(baseline_log_income_p75),
    baseline_rural_share = as.numeric(baseline_rural_share)
  )

panel <- panel %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  filter(year <= 2019) %>%
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
  ~outcome, ~label,
  "share_electricity_any", "Any electricity source",
  "share_electricity_other_source", "Other electricity source",
  "share_grid_full_time", "Grid full-time availability",
  "share_cooking_gas_any", "Cooking gas any",
  "share_cooking_gas_bottled", "Cooking gas bottled",
  "share_cooking_gas_piped", "Cooking gas piped",
  "share_cooking_wood_charcoal", "Cooking wood/charcoal",
  "share_cooking_electricity", "Cooking electricity",
  "share_cooking_other_fuel", "Cooking other fuel",
  "share_landline", "Landline ownership",
  "share_refrigerator", "Refrigerator ownership",
  "share_washing_machine", "Washing machine ownership",
  "share_satellite_dish_tv", "TV ownership (panel variable)",
  "share_computer", "Computer ownership",
  "share_internet_home", "Internet home",
  "share_internet_by_mobile", "Internet by mobile",
  "share_vehicle", "Vehicle ownership",
  "mean_mobile_phones", "Mean mobile phones",
  "mean_appliance_count_basic", "Mean appliance count basic",
  "mean_appliance_count_connected", "Mean appliance count connected"
)

controls <- c(
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
controls_txt <- paste(controls, collapse = " + ")

rows <- list()
k <- 1L

for (ii in seq_len(nrow(outcomes))) {
  y <- outcomes$outcome[[ii]]
  y_label <- outcomes$label[[ii]]

  needed <- unique(c("weight_sum", "estrato4", "quarter", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total", y, controls))
  dat <- panel %>% filter(if_all(all_of(needed), ~ !is.na(.x)), weight_sum > 0)
  if (nrow(dat) == 0) next
  if (is.na(sd(dat[[y]], na.rm = TRUE)) || sd(dat[[y]], na.rm = TRUE) == 0) {
    rows[[k]] <- tibble(
      outcome = y,
      outcome_label = y_label,
      nobs = nrow(dat),
      fs_coef = NA_real_,
      fs_p = NA_real_,
      rf_coef = NA_real_,
      rf_p = NA_real_,
      iv_coef = NA_real_,
      iv_se = NA_real_,
      iv_p = NA_real_,
      ivf1 = NA_real_,
      ivfall = NA_real_,
      model_status = "skipped_constant_outcome"
    )
    k <- k + 1L
    next
  }

  fs <- tryCatch(feols(
    as.formula(paste0("raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  ), error = function(e) e)

  rf <- tryCatch(feols(
    as.formula(paste0(y, " ~ lightning_quarterly_total + ", controls_txt, " | estrato4 + quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  ), error = function(e) e)

  iv <- tryCatch(feols(
    as.formula(paste0(y, " ~ ", controls_txt, " | estrato4 + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  ), error = function(e) e)

  if (inherits(fs, "error") || inherits(rf, "error") || inherits(iv, "error")) {
    rows[[k]] <- tibble(
      outcome = y,
      outcome_label = y_label,
      nobs = nrow(dat),
      fs_coef = NA_real_,
      fs_p = NA_real_,
      rf_coef = NA_real_,
      rf_p = NA_real_,
      iv_coef = NA_real_,
      iv_se = NA_real_,
      iv_p = NA_real_,
      ivf1 = NA_real_,
      ivfall = NA_real_,
      model_status = "skipped_model_error"
    )
    k <- k + 1L
    next
  }

  fs_t <- tidy(fs) %>% filter(term == "lightning_quarterly_total")
  rf_t <- tidy(rf) %>% filter(term == "lightning_quarterly_total")
  iv_t <- tidy(iv) %>% filter(grepl("^fit_", term))

  rows[[k]] <- tibble(
    outcome = y,
    outcome_label = y_label,
    nobs = nobs(iv),
    fs_coef = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$estimate[[1]]),
    fs_p = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$p.value[[1]]),
    rf_coef = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$estimate[[1]]),
    rf_p = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$p.value[[1]]),
    iv_coef = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$estimate[[1]]),
    iv_se = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$std.error[[1]]),
    iv_p = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$p.value[[1]]),
    ivf1 = safe_fitstat(iv, "ivf1"),
    ivfall = safe_fitstat(iv, "ivfall"),
    model_status = "ok"
  )
  k <- k + 1L
}

results <- bind_rows(rows) %>%
  mutate(
    iv_p_bh = p.adjust(iv_p, method = "BH"),
    rf_p_bh = p.adjust(rf_p, method = "BH")
  ) %>%
  arrange(iv_p)

hits <- results %>%
  filter(!is.na(iv_p), iv_p < 0.10) %>%
  arrange(iv_p)

write_csv(results, results_path)
write_csv(hits, hits_path)

message("Wrote: ", results_path)
message("Wrote: ", hits_path)
print(results)
message("Number of IV p<0.10 hits: ", nrow(hits))
