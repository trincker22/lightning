#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(readr)
})

root_dir <- here::here()
coverage_script <- here::here("Code", "07a_power_coverage_audit.R")
if (file.exists(coverage_script)) {
  cov_status <- tryCatch(system2("Rscript", coverage_script), error = function(e) 1L)
  if (!identical(cov_status, 0L)) {
    warning("Coverage audit refresh failed in Script 07.")
  }
}

fitstat_num <- function(model, stat) {
  tryCatch({
    out <- fitstat(model, stat)
    vals <- suppressWarnings(as.numeric(unlist(out)))
    vals <- vals[!is.na(vals)]
    if (length(vals) == 0) NA_real_ else vals[[1]]
  }, error = function(e) NA_real_)
}

print_model <- function(label, model) {
  cat("\n", strrep("=", 80), "\n", sep = "")
  cat(label, "\n")
  cat(strrep("-", 80), "\n", sep = "")
  print(summary(model))
}

extract_term_row <- function(model, label, sample_name, outcome_name, term_name, treatment_family, model_family, lag_name) {
  td <- tidy(model, conf.int = TRUE)
  row <- td[td$term == term_name, , drop = FALSE]
  if (nrow(row) == 0 && grepl("^fit_", term_name)) {
    row <- td[grepl("^fit_", td$term), , drop = FALSE]
  }
  if (nrow(row) == 0) {
    return(tibble(
      label = label,
      sample = sample_name,
      outcome = outcome_name,
      treatment_family = treatment_family,
      model_family = model_family,
      lag = lag_name,
      term = term_name,
      estimate = NA_real_,
      std.error = NA_real_,
      statistic = NA_real_,
      p.value = NA_real_,
      conf.low = NA_real_,
      conf.high = NA_real_,
      nobs = nobs(model),
      wr2 = fitstat_num(model, "wr2"),
      ivf1 = fitstat_num(model, "ivf1")
    ))
  }
  tibble(
    label = label,
    sample = sample_name,
    outcome = outcome_name,
    treatment_family = treatment_family,
    model_family = model_family,
    lag = lag_name,
    term = row$term,
    estimate = row$estimate,
    std.error = row$std.error,
    statistic = row$statistic,
    p.value = row$p.value,
    conf.low = row$conf.low,
    conf.high = row$conf.high,
    nobs = nobs(model),
    wr2 = fitstat_num(model, "wr2"),
    ivf1 = fitstat_num(model, "ivf1")
  )
}

plot_coefficients <- function(df, title, out_path) {
  if (nrow(df) == 0) return(invisible(NULL))
  p <- ggplot(df, aes(x = estimate, y = label, color = model_family)) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "grey60") +
    geom_pointrange(aes(xmin = conf.low, xmax = conf.high), position = position_dodge(width = 0.4)) +
    facet_wrap(~outcome, scales = "free_y") +
    theme_minimal(base_size = 11) +
    labs(title = title, x = NULL, y = NULL, color = NULL)
  ggsave(out_path, p, width = 11, height = 7, dpi = 300)
}

panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
split_panel_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_urban_rural_2016_2024.parquet")
out_dir <- here::here("data", "powerIV", "regressions", "cooking")
fig_dir <- here::here("Figures", "powerIV", "cooking")
tab_dir <- here::here(out_dir, "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(tab_dir, "pnadc_power_regressions.csv")
first_stage_path <- here::here(tab_dir, "pnadc_power_first_stage_summary.csv")
clean_path <- here::here(tab_dir, "pnadc_cooking_fuel_clean_results.csv")
robustness_path <- here::here(tab_dir, "pnadc_cooking_overlap_robustness.csv")
wood_table_path <- here::here(tab_dir, "cooking_wood_charcoal_table.tex")
electricity_table_path <- here::here(tab_dir, "cooking_electricity_table.tex")

panel <- read_parquet(panel_path) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  )

split_panel <- read_parquet(split_panel_path) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  )

power_only <- panel %>%
  select(
    estrato4, year, quarter, quarter_start,
    lightning_quarterly_total,
    raw_quarterly_outage_hrs_per_1000,
    raw_quarterly_outage_hrs_per_1000_no_overlap,
    dec_quarterly,
    dec_quarterly_no_overlap
  ) %>%
  distinct() %>%
  arrange(estrato4, quarter_start) %>%
  group_by(estrato4) %>%
  mutate(
    lightning_quarterly_total_l4 = lag(lightning_quarterly_total, 4),
    raw_quarterly_outage_hrs_per_1000_l4 = lag(raw_quarterly_outage_hrs_per_1000, 4),
    raw_quarterly_outage_hrs_per_1000_no_overlap_l4 = lag(raw_quarterly_outage_hrs_per_1000_no_overlap, 4),
    dec_quarterly_l4 = lag(dec_quarterly, 4),
    dec_quarterly_no_overlap_l4 = lag(dec_quarterly_no_overlap, 4)
  ) %>%
  ungroup()

panel <- panel %>%
  select(-matches("_l4$")) %>%
  left_join(power_only, by = c("estrato4", "year", "quarter", "quarter_start", "lightning_quarterly_total", "raw_quarterly_outage_hrs_per_1000", "raw_quarterly_outage_hrs_per_1000_no_overlap", "dec_quarterly", "dec_quarterly_no_overlap"))

split_panel <- split_panel %>%
  left_join(power_only, by = c("estrato4", "year", "quarter", "quarter_start"))

controls <- c("mean_rooms", "mean_bedrooms", "share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time")

all_raw <- panel %>% filter(!is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000))
all_raw_no_overlap <- panel %>% filter(!is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000_no_overlap))
all_raw_l4 <- panel %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(raw_quarterly_outage_hrs_per_1000_l4))
all_raw_no_overlap_l4 <- panel %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(raw_quarterly_outage_hrs_per_1000_no_overlap_l4))
rural_raw <- split_panel %>% filter(urban_rural == "rural", !is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000))
rural_raw_l4 <- split_panel %>% filter(urban_rural == "rural", !is.na(lightning_quarterly_total_l4), !is.na(raw_quarterly_outage_hrs_per_1000_l4))
urban_raw <- split_panel %>% filter(urban_rural == "urban", !is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000))

all_dec <- panel %>% filter(!is.na(lightning_quarterly_total), !is.na(dec_quarterly))
all_dec_no_overlap <- panel %>% filter(!is.na(lightning_quarterly_total), !is.na(dec_quarterly_no_overlap))
all_dec_l4 <- panel %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_l4))
all_dec_no_overlap_l4 <- panel %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_no_overlap_l4))
rural_dec <- split_panel %>% filter(urban_rural == "rural", !is.na(lightning_quarterly_total), !is.na(dec_quarterly))
rural_dec_l4 <- split_panel %>% filter(urban_rural == "rural", !is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_l4))
urban_dec <- split_panel %>% filter(urban_rural == "urban", !is.na(lightning_quarterly_total), !is.na(dec_quarterly))

results <- list()
first_stage <- list()

raw_fs_all_current <- feols(raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: raw-event first stage, all, current", raw_fs_all_current)
first_stage[[length(first_stage) + 1]] <- tibble(label = "Raw FS all current", sample = "all", lag = "current", treatment_family = "raw_event", outcome = "raw_quarterly_outage_hrs_per_1000", nobs = nobs(raw_fs_all_current), fstat = fitstat_num(raw_fs_all_current, "f"), estimate = unname(coef(raw_fs_all_current)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(raw_fs_all_current, "Raw FS all current", "all", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total", "raw_event", "first_stage", "current")

raw_fs_all_l4 <- feols(raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: raw-event first stage, all, lag4", raw_fs_all_l4)
first_stage[[length(first_stage) + 1]] <- tibble(label = "Raw FS all lag4", sample = "all", lag = "lag4", treatment_family = "raw_event", outcome = "raw_quarterly_outage_hrs_per_1000", nobs = nobs(raw_fs_all_l4), fstat = fitstat_num(raw_fs_all_l4, "f"), estimate = unname(coef(raw_fs_all_l4)[["lightning_quarterly_total_l4"]]))
results[[length(results) + 1]] <- extract_term_row(raw_fs_all_l4, "Raw FS all lag4", "all", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total_l4", "raw_event", "first_stage", "lag4")

dec_fs_all_current <- feols(dec_quarterly ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: DEC first stage, all, current", dec_fs_all_current)
first_stage[[length(first_stage) + 1]] <- tibble(label = "DEC FS all current", sample = "all", lag = "current", treatment_family = "continuity", outcome = "dec_quarterly", nobs = nobs(dec_fs_all_current), fstat = fitstat_num(dec_fs_all_current, "f"), estimate = unname(coef(dec_fs_all_current)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(dec_fs_all_current, "DEC FS all current", "all", "dec_quarterly", "lightning_quarterly_total", "continuity", "first_stage", "current")

dec_fs_all_l4 <- feols(dec_quarterly_l4 ~ lightning_quarterly_total_l4 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: DEC first stage, all, lag4", dec_fs_all_l4)
first_stage[[length(first_stage) + 1]] <- tibble(label = "DEC FS all lag4", sample = "all", lag = "lag4", treatment_family = "continuity", outcome = "dec_quarterly", nobs = nobs(dec_fs_all_l4), fstat = fitstat_num(dec_fs_all_l4, "f"), estimate = unname(coef(dec_fs_all_l4)[["lightning_quarterly_total_l4"]]))
results[[length(results) + 1]] <- extract_term_row(dec_fs_all_l4, "DEC FS all lag4", "all", "dec_quarterly", "lightning_quarterly_total_l4", "continuity", "first_stage", "lag4")

wood_raw_rf_all_current <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw reduced form, all, current", wood_raw_rf_all_current)
results[[length(results) + 1]] <- extract_term_row(wood_raw_rf_all_current, "Wood raw RF all current", "all", "share_cooking_wood_charcoal", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

wood_raw_ols_all_current <- feols(share_cooking_wood_charcoal ~ raw_quarterly_outage_hrs_per_1000 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw OLS, all, current", wood_raw_ols_all_current)
results[[length(results) + 1]] <- extract_term_row(wood_raw_ols_all_current, "Wood raw OLS all current", "all", "share_cooking_wood_charcoal", "raw_quarterly_outage_hrs_per_1000", "raw_event", "ols", "current")

wood_raw_iv_all_current <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw IV, all, current", wood_raw_iv_all_current)
results[[length(results) + 1]] <- extract_term_row(wood_raw_iv_all_current, "Wood raw IV all current", "all", "share_cooking_wood_charcoal", "fit_raw_quarterly_outage_hrs_per_1000", "raw_event", "iv", "current")

wood_raw_rf_rural_current <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = rural_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw reduced form, rural, current", wood_raw_rf_rural_current)
results[[length(results) + 1]] <- extract_term_row(wood_raw_rf_rural_current, "Wood raw RF rural current", "rural", "share_cooking_wood_charcoal", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

wood_raw_iv_rural_current <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = rural_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw IV, rural, current", wood_raw_iv_rural_current)
results[[length(results) + 1]] <- extract_term_row(wood_raw_iv_rural_current, "Wood raw IV rural current", "rural", "share_cooking_wood_charcoal", "fit_raw_quarterly_outage_hrs_per_1000", "raw_event", "iv", "current")

wood_raw_rf_all_l4 <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total_l4 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw reduced form, all, lag4", wood_raw_rf_all_l4)
results[[length(results) + 1]] <- extract_term_row(wood_raw_rf_all_l4, "Wood raw RF all lag4", "all", "share_cooking_wood_charcoal", "lightning_quarterly_total_l4", "raw_event", "reduced_form", "lag4")

wood_raw_iv_all_l4 <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw IV, all, lag4", wood_raw_iv_all_l4)
results[[length(results) + 1]] <- extract_term_row(wood_raw_iv_all_l4, "Wood raw IV all lag4", "all", "share_cooking_wood_charcoal", "fit_raw_quarterly_outage_hrs_per_1000_l4", "raw_event", "iv", "lag4")

wood_dec_rf_all_current <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: DEC reduced form, all, current", wood_dec_rf_all_current)
results[[length(results) + 1]] <- extract_term_row(wood_dec_rf_all_current, "Wood DEC RF all current", "all", "share_cooking_wood_charcoal", "lightning_quarterly_total", "continuity", "reduced_form", "current")

wood_dec_iv_all_current <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly ~ lightning_quarterly_total, data = all_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: DEC IV, all, current", wood_dec_iv_all_current)
results[[length(results) + 1]] <- extract_term_row(wood_dec_iv_all_current, "Wood DEC IV all current", "all", "share_cooking_wood_charcoal", "fit_dec_quarterly", "continuity", "iv", "current")

wood_dec_rf_rural_l4 <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total_l4 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = rural_dec_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: DEC reduced form, rural, lag4", wood_dec_rf_rural_l4)
results[[length(results) + 1]] <- extract_term_row(wood_dec_rf_rural_l4, "Wood DEC RF rural lag4", "rural", "share_cooking_wood_charcoal", "lightning_quarterly_total_l4", "continuity", "reduced_form", "lag4")

wood_dec_iv_rural_l4 <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly_l4 ~ lightning_quarterly_total_l4, data = rural_dec_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: DEC IV, rural, lag4", wood_dec_iv_rural_l4)
results[[length(results) + 1]] <- extract_term_row(wood_dec_iv_rural_l4, "Wood DEC IV rural lag4", "rural", "share_cooking_wood_charcoal", "fit_dec_quarterly_l4", "continuity", "iv", "lag4")

elec_raw_rf_all_current <- feols(share_cooking_electricity ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: raw reduced form, all, current", elec_raw_rf_all_current)
results[[length(results) + 1]] <- extract_term_row(elec_raw_rf_all_current, "Elec raw RF all current", "all", "share_cooking_electricity", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

elec_raw_iv_all_current <- feols(share_cooking_electricity ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: raw IV, all, current", elec_raw_iv_all_current)
results[[length(results) + 1]] <- extract_term_row(elec_raw_iv_all_current, "Elec raw IV all current", "all", "share_cooking_electricity", "fit_raw_quarterly_outage_hrs_per_1000", "raw_event", "iv", "current")

elec_raw_rf_urban_current <- feols(share_cooking_electricity ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = urban_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: raw reduced form, urban, current", elec_raw_rf_urban_current)
results[[length(results) + 1]] <- extract_term_row(elec_raw_rf_urban_current, "Elec raw RF urban current", "urban", "share_cooking_electricity", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

elec_dec_rf_rural_current <- feols(share_cooking_electricity ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = rural_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: DEC reduced form, rural, current", elec_dec_rf_rural_current)
results[[length(results) + 1]] <- extract_term_row(elec_dec_rf_rural_current, "Elec DEC RF rural current", "rural", "share_cooking_electricity", "lightning_quarterly_total", "continuity", "reduced_form", "current")

elec_dec_iv_rural_current <- feols(share_cooking_electricity ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly ~ lightning_quarterly_total, data = rural_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: DEC IV, rural, current", elec_dec_iv_rural_current)
results[[length(results) + 1]] <- extract_term_row(elec_dec_iv_rural_current, "Elec DEC IV rural current", "rural", "share_cooking_electricity", "fit_dec_quarterly", "continuity", "iv", "current")

raw_fs_all_current_no_overlap <- feols(raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: raw-event first stage, all, current, no-overlap CONJ", raw_fs_all_current_no_overlap)
first_stage[[length(first_stage) + 1]] <- tibble(label = "Raw FS all current no-overlap", sample = "all", lag = "current", treatment_family = "raw_event_no_overlap", outcome = "raw_quarterly_outage_hrs_per_1000_no_overlap", nobs = nobs(raw_fs_all_current_no_overlap), fstat = fitstat_num(raw_fs_all_current_no_overlap, "f"), estimate = unname(coef(raw_fs_all_current_no_overlap)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(raw_fs_all_current_no_overlap, "Raw FS all current no-overlap", "all", "raw_quarterly_outage_hrs_per_1000_no_overlap", "lightning_quarterly_total", "raw_event_no_overlap", "first_stage", "current")

wood_raw_iv_all_current_no_overlap <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: raw IV, all, current, no-overlap CONJ", wood_raw_iv_all_current_no_overlap)
results[[length(results) + 1]] <- extract_term_row(wood_raw_iv_all_current_no_overlap, "Wood raw IV all current no-overlap", "all", "share_cooking_wood_charcoal", "fit_raw_quarterly_outage_hrs_per_1000_no_overlap", "raw_event_no_overlap", "iv", "current")

elec_raw_iv_all_current_no_overlap <- feols(share_cooking_electricity ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking electricity: raw IV, all, current, no-overlap CONJ", elec_raw_iv_all_current_no_overlap)
results[[length(results) + 1]] <- extract_term_row(elec_raw_iv_all_current_no_overlap, "Elec raw IV all current no-overlap", "all", "share_cooking_electricity", "fit_raw_quarterly_outage_hrs_per_1000_no_overlap", "raw_event_no_overlap", "iv", "current")

dec_fs_all_current_no_overlap <- feols(dec_quarterly_no_overlap ~ lightning_quarterly_total + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking: DEC first stage, all, current, no-overlap CONJ", dec_fs_all_current_no_overlap)
first_stage[[length(first_stage) + 1]] <- tibble(label = "DEC FS all current no-overlap", sample = "all", lag = "current", treatment_family = "continuity_no_overlap", outcome = "dec_quarterly_no_overlap", nobs = nobs(dec_fs_all_current_no_overlap), fstat = fitstat_num(dec_fs_all_current_no_overlap, "f"), estimate = unname(coef(dec_fs_all_current_no_overlap)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(dec_fs_all_current_no_overlap, "DEC FS all current no-overlap", "all", "dec_quarterly_no_overlap", "lightning_quarterly_total", "continuity_no_overlap", "first_stage", "current")

wood_dec_iv_all_current_no_overlap <- feols(share_cooking_wood_charcoal ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly_no_overlap ~ lightning_quarterly_total, data = all_dec_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("Cooking wood/charcoal: DEC IV, all, current, no-overlap CONJ", wood_dec_iv_all_current_no_overlap)
results[[length(results) + 1]] <- extract_term_row(wood_dec_iv_all_current_no_overlap, "Wood DEC IV all current no-overlap", "all", "share_cooking_wood_charcoal", "fit_dec_quarterly_no_overlap", "continuity_no_overlap", "iv", "current")

wood_table_rf_basic_current <- feols(share_cooking_wood_charcoal ~ lightning_quarterly_total + mean_rooms + mean_bedrooms | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
wood_table_rf_extended_current <- wood_raw_rf_all_current
wood_table_iv_extended_current <- wood_raw_iv_all_current
wood_table_rf_extended_l4 <- wood_raw_rf_all_l4
wood_table_iv_extended_l4 <- wood_raw_iv_all_l4

electricity_table_rf_basic_current <- feols(share_cooking_electricity ~ lightning_quarterly_total + mean_rooms + mean_bedrooms | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
electricity_table_rf_extended_current <- elec_raw_rf_all_current
electricity_table_iv_extended_current <- elec_raw_iv_all_current
electricity_table_rf_extended_l4 <- feols(share_cooking_electricity ~ lightning_quarterly_total_l4 + mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
electricity_table_iv_extended_l4 <- feols(share_cooking_electricity ~ mean_rooms + mean_bedrooms + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)

results_tbl <- bind_rows(results)
first_stage_tbl <- bind_rows(first_stage)
robustness_tbl <- results_tbl %>% filter(grepl("no-overlap", label, ignore.case = TRUE))
clean_tbl <- bind_rows(
  first_stage_tbl %>% mutate(model_family = "first_stage", term = "lightning", std.error = NA_real_, statistic = NA_real_, p.value = NA_real_, conf.low = NA_real_, conf.high = NA_real_, wr2 = NA_real_, ivf1 = NA_real_) %>% select(label, sample, lag, treatment_family, outcome, model_family, term, estimate, std.error, statistic, p.value, conf.low, conf.high, nobs, wr2, ivf1, fstat),
  results_tbl %>% mutate(fstat = NA_real_)
)

write_csv(results_tbl, results_path)
write_csv(first_stage_tbl, first_stage_path)
write_csv(clean_tbl, clean_path)
write_csv(robustness_tbl, robustness_path)

etable(
  wood_table_rf_basic_current,
  wood_table_rf_extended_current,
  wood_table_iv_extended_current,
  wood_table_rf_extended_l4,
  wood_table_iv_extended_l4,
  tex = TRUE,
  file = wood_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "mean_rooms" = "Mean rooms",
    "mean_bedrooms" = "Mean bedrooms",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Cooking with wood or charcoal"
)

etable(
  electricity_table_rf_basic_current,
  electricity_table_rf_extended_current,
  electricity_table_iv_extended_current,
  electricity_table_rf_extended_l4,
  electricity_table_iv_extended_l4,
  tex = TRUE,
  file = electricity_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "mean_rooms" = "Mean rooms",
    "mean_bedrooms" = "Mean bedrooms",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Cooking with electricity"
)

plot_coefficients(
  results_tbl %>% filter(outcome == "share_cooking_wood_charcoal", model_family %in% c("reduced_form", "iv"), !grepl("no-overlap", label, ignore.case = TRUE)),
  "Cooking with wood/charcoal",
  here::here(fig_dir, "figure_1_cooking_wood_charcoal.png")
)

plot_coefficients(
  results_tbl %>% filter(outcome == "share_cooking_electricity", model_family %in% c("reduced_form", "iv"), !grepl("no-overlap", label, ignore.case = TRUE)),
  "Cooking with electricity",
  here::here(fig_dir, "figure_2_cooking_electricity.png")
)

cat("Wrote\n")
cat(results_path, "\n")
cat(first_stage_path, "\n")
cat(clean_path, "\n")
cat(robustness_path, "\n")
cat(wood_table_path, "\n")
cat(electricity_table_path, "\n")
cat(here::here(fig_dir, "figure_1_cooking_wood_charcoal.png"), "\n")
cat(here::here(fig_dir, "figure_2_cooking_electricity.png"), "\n")
