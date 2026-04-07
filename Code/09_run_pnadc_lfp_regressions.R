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
    warning("Coverage audit refresh failed in Script 09.")
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

make_binned_plot <- function(df, xvar, yvar, title, out_path) {
  plot_df <- df %>%
    filter(!is.na(.data[[xvar]]), !is.na(.data[[yvar]]), !is.na(weight_sum), weight_sum > 0) %>%
    mutate(bin = ntile(.data[[xvar]], 20)) %>%
    group_by(bin) %>%
    summarise(
      x = weighted.mean(.data[[xvar]], weight_sum),
      y = weighted.mean(.data[[yvar]], weight_sum),
      .groups = "drop"
    )
  p <- ggplot(plot_df, aes(x = x, y = y)) +
    geom_point(size = 2.2) +
    geom_smooth(method = "lm", se = FALSE, color = "steelblue") +
    theme_minimal(base_size = 11) +
    labs(title = title, x = xvar, y = yvar)
  ggsave(out_path, p, width = 8.5, height = 6, dpi = 300)
}

pooled_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
split_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_urban_rural_with_power_2016_2024.parquet")
power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
out_dir <- here::here("data", "powerIV", "regressions", "lfp")
fig_dir <- here::here("Figures", "powerIV", "lfp")
tab_dir <- here::here(out_dir, "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(tab_dir, "pnadc_lfp_regressions.csv")
focus_path <- here::here(tab_dir, "pnadc_lfp_focus_results.csv")
first_stage_path <- here::here(tab_dir, "pnadc_lfp_first_stage_summary.csv")
story_path <- here::here(tab_dir, "pnadc_lfp_storytelling_results.csv")
robustness_path <- here::here(tab_dir, "pnadc_lfp_overlap_robustness.csv")
female_employment_table_path <- here::here(tab_dir, "female_employment_table.tex")
female_lfp_table_path <- here::here(tab_dir, "female_lfp_table.tex")
female_domcare_table_path <- here::here(tab_dir, "female_domestic_care_reason_table.tex")
female_outlf_table_path <- here::here(tab_dir, "female_outlf_domestic_care_table.tex")
female_lfp_story_table_path <- here::here(tab_dir, "female_lfp_story_table.tex")
female_domcare_story_table_path <- here::here(tab_dir, "female_domestic_care_story_table.tex")

pooled <- read_parquet(pooled_path) %>% mutate(estrato4 = as.character(estrato4))
split <- read_parquet(split_path) %>% mutate(estrato4 = as.character(estrato4))
power_controls <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4), year, quarter,
    share_vehicle,
    share_internet_home,
    share_electricity_from_grid,
    share_grid_full_time,
    share_cooking_wood_charcoal,
    share_cooking_electricity,
    raw_quarterly_outage_hrs_per_1000_no_overlap,
    dec_quarterly_no_overlap
  )

pooled <- pooled %>% left_join(power_controls, by = c("estrato4", "year", "quarter"), suffix = c("", "_power"))
split <- split %>% left_join(power_controls, by = c("estrato4", "year", "quarter"), suffix = c("", "_power"))

pooled <- pooled %>%
  mutate(quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))) %>%
  arrange(estrato4, quarter_start) %>%
  group_by(estrato4) %>%
  mutate(
    raw_quarterly_outage_hrs_per_1000_no_overlap_l4 = lag(raw_quarterly_outage_hrs_per_1000_no_overlap, 4),
    dec_quarterly_no_overlap_l4 = lag(dec_quarterly_no_overlap, 4)
  ) %>%
  ungroup() %>%
  select(-quarter_start)

controls <- c("share_vehicle", "share_internet_home", "share_electricity_from_grid", "share_grid_full_time")

all_raw <- pooled %>% filter(!is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000))
all_raw_no_overlap <- pooled %>% filter(!is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000_no_overlap))
all_raw_l4 <- pooled %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(raw_quarterly_outage_hrs_per_1000_l4))
urban_raw <- split %>% filter(urban_rural == "urban", !is.na(lightning_quarterly_total), !is.na(raw_quarterly_outage_hrs_per_1000))

all_dec <- pooled %>% filter(!is.na(lightning_quarterly_total), !is.na(dec_quarterly))
all_dec_no_overlap <- pooled %>% filter(!is.na(lightning_quarterly_total), !is.na(dec_quarterly_no_overlap))
all_dec_l4 <- pooled %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_l4))
urban_dec <- split %>% filter(urban_rural == "urban", !is.na(lightning_quarterly_total), !is.na(dec_quarterly))

results <- list()
first_stage <- list()
story_results <- list()
robustness_results <- list()

raw_fs_all_current <- feols(raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP: raw-event first stage, all, current", raw_fs_all_current)
first_stage[[length(first_stage) + 1]] <- tibble(label = "Raw FS all current", sample = "all", lag = "current", treatment_family = "raw_event", outcome = "raw_quarterly_outage_hrs_per_1000", nobs = nobs(raw_fs_all_current), fstat = fitstat_num(raw_fs_all_current, "f"), estimate = unname(coef(raw_fs_all_current)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(raw_fs_all_current, "Raw FS all current", "all", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total", "raw_event", "first_stage", "current")

dec_fs_all_current <- feols(dec_quarterly ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP: DEC first stage, all, current", dec_fs_all_current)
first_stage[[length(first_stage) + 1]] <- tibble(label = "DEC FS all current", sample = "all", lag = "current", treatment_family = "continuity", outcome = "dec_quarterly", nobs = nobs(dec_fs_all_current), fstat = fitstat_num(dec_fs_all_current, "f"), estimate = unname(coef(dec_fs_all_current)[["lightning_quarterly_total"]]))
results[[length(results) + 1]] <- extract_term_row(dec_fs_all_current, "DEC FS all current", "all", "dec_quarterly", "lightning_quarterly_total", "continuity", "first_stage", "current")

raw_fs_all_l4 <- feols(raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP: raw-event first stage, all, lag4", raw_fs_all_l4)
first_stage[[length(first_stage) + 1]] <- tibble(label = "Raw FS all lag4", sample = "all", lag = "lag4", treatment_family = "raw_event", outcome = "raw_quarterly_outage_hrs_per_1000", nobs = nobs(raw_fs_all_l4), fstat = fitstat_num(raw_fs_all_l4, "f"), estimate = unname(coef(raw_fs_all_l4)[["lightning_quarterly_total_l4"]]))
results[[length(results) + 1]] <- extract_term_row(raw_fs_all_l4, "Raw FS all lag4", "all", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total_l4", "raw_event", "first_stage", "lag4")

dec_fs_all_l4 <- feols(dec_quarterly_l4 ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP: DEC first stage, all, lag4", dec_fs_all_l4)
first_stage[[length(first_stage) + 1]] <- tibble(label = "DEC FS all lag4", sample = "all", lag = "lag4", treatment_family = "continuity", outcome = "dec_quarterly", nobs = nobs(dec_fs_all_l4), fstat = fitstat_num(dec_fs_all_l4, "f"), estimate = unname(coef(dec_fs_all_l4)[["lightning_quarterly_total_l4"]]))
results[[length(results) + 1]] <- extract_term_row(dec_fs_all_l4, "DEC FS all lag4", "all", "dec_quarterly", "lightning_quarterly_total_l4", "continuity", "first_stage", "lag4")

female_emp_raw_rf_all <- feols(female_employment ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female employment: raw reduced form, all, current", female_emp_raw_rf_all)
results[[length(results) + 1]] <- extract_term_row(female_emp_raw_rf_all, "Female employment raw RF all current", "all", "female_employment", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

female_emp_raw_iv_all <- feols(female_employment ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female employment: raw IV, all, current", female_emp_raw_iv_all)
results[[length(results) + 1]] <- extract_term_row(female_emp_raw_iv_all, "Female employment raw IV all current", "all", "female_employment", "fit_raw_quarterly_outage_hrs_per_1000", "raw_event", "iv", "current")

female_emp_dec_rf_urban <- feols(female_employment ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = urban_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female employment: DEC reduced form, urban, current", female_emp_dec_rf_urban)
results[[length(results) + 1]] <- extract_term_row(female_emp_dec_rf_urban, "Female employment DEC RF urban current", "urban", "female_employment", "lightning_quarterly_total", "continuity", "reduced_form", "current")

female_emp_dec_iv_urban <- feols(female_employment ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly ~ lightning_quarterly_total, data = urban_dec, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female employment: DEC IV, urban, current", female_emp_dec_iv_urban)
results[[length(results) + 1]] <- extract_term_row(female_emp_dec_iv_urban, "Female employment DEC IV urban current", "urban", "female_employment", "fit_dec_quarterly", "continuity", "iv", "current")

female_lfp_raw_rf_all <- feols(female_lfp ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female LFP: raw reduced form, all, current", female_lfp_raw_rf_all)
results[[length(results) + 1]] <- extract_term_row(female_lfp_raw_rf_all, "Female LFP raw RF all current", "all", "female_lfp", "lightning_quarterly_total", "raw_event", "reduced_form", "current")

female_lfp_raw_iv_all <- feols(female_lfp ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female LFP: raw IV, all, current", female_lfp_raw_iv_all)
results[[length(results) + 1]] <- extract_term_row(female_lfp_raw_iv_all, "Female LFP raw IV all current", "all", "female_lfp", "fit_raw_quarterly_outage_hrs_per_1000", "raw_event", "iv", "current")

female_domcare_dec_rf_urban_l4 <- feols(female_domestic_care_reason ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = urban_dec %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_l4)), weights = ~weight_sum, cluster = ~estrato4)
print_model("Female domestic care reason: DEC reduced form, urban, lag4", female_domcare_dec_rf_urban_l4)
results[[length(results) + 1]] <- extract_term_row(female_domcare_dec_rf_urban_l4, "Female domestic care DEC RF urban lag4", "urban", "female_domestic_care_reason", "lightning_quarterly_total_l4", "continuity", "reduced_form", "lag4")

female_domcare_dec_iv_urban_l4 <- feols(female_domestic_care_reason ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly_l4 ~ lightning_quarterly_total_l4, data = urban_dec %>% filter(!is.na(lightning_quarterly_total_l4), !is.na(dec_quarterly_l4)), weights = ~weight_sum, cluster = ~estrato4)
print_model("Female domestic care reason: DEC IV, urban, lag4", female_domcare_dec_iv_urban_l4)
results[[length(results) + 1]] <- extract_term_row(female_domcare_dec_iv_urban_l4, "Female domestic care DEC IV urban lag4", "urban", "female_domestic_care_reason", "fit_dec_quarterly_l4", "continuity", "iv", "lag4")

female_outlf_raw_rf_all_l4 <- feols(female_outlf_domestic_care ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female out of labor force for domestic care: raw reduced form, all, lag4", female_outlf_raw_rf_all_l4)
results[[length(results) + 1]] <- extract_term_row(female_outlf_raw_rf_all_l4, "Female outlf domestic care raw RF all lag4", "all", "female_outlf_domestic_care", "lightning_quarterly_total_l4", "raw_event", "reduced_form", "lag4")

female_outlf_raw_iv_all_l4 <- feols(female_outlf_domestic_care ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
print_model("Female out of labor force for domestic care: raw IV, all, lag4", female_outlf_raw_iv_all_l4)
results[[length(results) + 1]] <- extract_term_row(female_outlf_raw_iv_all_l4, "Female outlf domestic care raw IV all lag4", "all", "female_outlf_domestic_care", "fit_raw_quarterly_outage_hrs_per_1000_l4", "raw_event", "iv", "lag4")

raw_fs_no_overlap <- feols(raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP robustness: raw first stage, all, current, no-overlap CONJ", raw_fs_no_overlap)
robustness_results[[length(robustness_results) + 1]] <- extract_term_row(raw_fs_no_overlap, "Robustness raw FS no-overlap", "all", "raw_quarterly_outage_hrs_per_1000_no_overlap", "lightning_quarterly_total", "raw_event", "first_stage", "current")

female_lfp_raw_iv_no_overlap <- feols(female_lfp ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP robustness: female LFP raw IV, all, current, no-overlap CONJ", female_lfp_raw_iv_no_overlap)
robustness_results[[length(robustness_results) + 1]] <- extract_term_row(female_lfp_raw_iv_no_overlap, "Robustness female LFP raw IV no-overlap", "all", "female_lfp", "fit_raw_quarterly_outage_hrs_per_1000_no_overlap", "raw_event", "iv", "current")

female_domcare_raw_iv_no_overlap <- feols(female_domestic_care_reason ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_no_overlap ~ lightning_quarterly_total, data = all_raw_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP robustness: female domestic care raw IV, all, current, no-overlap CONJ", female_domcare_raw_iv_no_overlap)
robustness_results[[length(robustness_results) + 1]] <- extract_term_row(female_domcare_raw_iv_no_overlap, "Robustness female domcare raw IV no-overlap", "all", "female_domestic_care_reason", "fit_raw_quarterly_outage_hrs_per_1000_no_overlap", "raw_event", "iv", "current")

dec_fs_no_overlap <- feols(dec_quarterly_no_overlap ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_dec_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP robustness: DEC first stage, all, current, no-overlap CONJ", dec_fs_no_overlap)
robustness_results[[length(robustness_results) + 1]] <- extract_term_row(dec_fs_no_overlap, "Robustness DEC FS no-overlap", "all", "dec_quarterly_no_overlap", "lightning_quarterly_total", "continuity", "first_stage", "current")

female_lfp_dec_iv_no_overlap <- feols(female_lfp ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | dec_quarterly_no_overlap ~ lightning_quarterly_total, data = all_dec_no_overlap, weights = ~weight_sum, cluster = ~estrato4)
print_model("LFP robustness: female LFP DEC IV, all, current, no-overlap CONJ", female_lfp_dec_iv_no_overlap)
robustness_results[[length(robustness_results) + 1]] <- extract_term_row(female_lfp_dec_iv_no_overlap, "Robustness female LFP DEC IV no-overlap", "all", "female_lfp", "fit_dec_quarterly_no_overlap", "continuity", "iv", "current")

female_emp_table_rf_basic_current <- feols(female_employment ~ lightning_quarterly_total + share_vehicle + share_internet_home | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_emp_table_rf_extended_current <- female_emp_raw_rf_all
female_emp_table_iv_extended_current <- female_emp_raw_iv_all
female_emp_table_rf_extended_l4 <- feols(female_employment ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
female_emp_table_iv_extended_l4 <- feols(female_employment ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)

female_lfp_table_rf_basic_current <- feols(female_lfp ~ lightning_quarterly_total + share_vehicle + share_internet_home | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_lfp_table_rf_extended_current <- female_lfp_raw_rf_all
female_lfp_table_iv_extended_current <- female_lfp_raw_iv_all
female_lfp_table_rf_extended_l4 <- feols(female_lfp ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
female_lfp_table_iv_extended_l4 <- feols(female_lfp ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)

female_domcare_table_rf_basic_current <- feols(female_domestic_care_reason ~ lightning_quarterly_total + share_vehicle + share_internet_home | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_domcare_table_rf_extended_current <- feols(female_domestic_care_reason ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_domcare_table_iv_extended_current <- feols(female_domestic_care_reason ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_domcare_table_rf_extended_l4 <- feols(female_domestic_care_reason ~ lightning_quarterly_total_l4 + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)
female_domcare_table_iv_extended_l4 <- feols(female_domestic_care_reason ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000_l4 ~ lightning_quarterly_total_l4, data = all_raw_l4, weights = ~weight_sum, cluster = ~estrato4)

female_outlf_table_rf_basic_current <- feols(female_outlf_domestic_care ~ lightning_quarterly_total + share_vehicle + share_internet_home | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_outlf_table_rf_extended_current <- feols(female_outlf_domestic_care ~ lightning_quarterly_total + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_outlf_table_iv_extended_current <- feols(female_outlf_domestic_care ~ share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter | raw_quarterly_outage_hrs_per_1000 ~ lightning_quarterly_total, data = all_raw, weights = ~weight_sum, cluster = ~estrato4)
female_outlf_table_rf_extended_l4 <- female_outlf_raw_rf_all_l4
female_outlf_table_iv_extended_l4 <- female_outlf_raw_iv_all_l4

story_female_lfp <- feols(female_lfp ~ share_cooking_wood_charcoal + share_cooking_electricity + share_grid_full_time + share_electricity_from_grid | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
print_model("Story model: female LFP and cooking/electricity environment", story_female_lfp)
story_results[[length(story_results) + 1]] <- extract_term_row(story_female_lfp, "Story female LFP", "all", "female_lfp", "share_cooking_wood_charcoal", "story", "correlation", "current")
story_results[[length(story_results) + 1]] <- extract_term_row(story_female_lfp, "Story female LFP", "all", "female_lfp", "share_grid_full_time", "story", "correlation", "current")

story_domcare <- feols(female_domestic_care_reason ~ share_cooking_wood_charcoal + share_cooking_electricity + share_grid_full_time + share_electricity_from_grid | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
print_model("Story model: female domestic care and cooking/electricity environment", story_domcare)
story_results[[length(story_results) + 1]] <- extract_term_row(story_domcare, "Story domestic care", "all", "female_domestic_care_reason", "share_cooking_wood_charcoal", "story", "correlation", "current")
story_results[[length(story_results) + 1]] <- extract_term_row(story_domcare, "Story domestic care", "all", "female_domestic_care_reason", "share_grid_full_time", "story", "correlation", "current")

results_tbl <- bind_rows(results)
first_stage_tbl <- bind_rows(first_stage)
story_tbl <- bind_rows(story_results)
robustness_tbl <- bind_rows(robustness_results)
focus_tbl <- bind_rows(
  first_stage_tbl %>% mutate(model_family = "first_stage", term = "lightning", std.error = NA_real_, statistic = NA_real_, p.value = NA_real_, conf.low = NA_real_, conf.high = NA_real_, wr2 = NA_real_, ivf1 = NA_real_) %>% select(label, sample, lag, treatment_family, outcome, model_family, term, estimate, std.error, statistic, p.value, conf.low, conf.high, nobs, wr2, ivf1, fstat),
  results_tbl %>% mutate(fstat = NA_real_)
)

write_csv(results_tbl, results_path)
write_csv(first_stage_tbl, first_stage_path)
write_csv(focus_tbl, focus_path)
write_csv(story_tbl, story_path)
write_csv(robustness_tbl, robustness_path)

etable(
  female_emp_table_rf_basic_current,
  female_emp_table_rf_extended_current,
  female_emp_table_iv_extended_current,
  female_emp_table_rf_extended_l4,
  female_emp_table_iv_extended_l4,
  tex = TRUE,
  file = female_employment_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female employment"
)

etable(
  female_lfp_table_rf_basic_current,
  female_lfp_table_rf_extended_current,
  female_lfp_table_iv_extended_current,
  female_lfp_table_rf_extended_l4,
  female_lfp_table_iv_extended_l4,
  tex = TRUE,
  file = female_lfp_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female labor force participation"
)

etable(
  female_domcare_table_rf_basic_current,
  female_domcare_table_rf_extended_current,
  female_domcare_table_iv_extended_current,
  female_domcare_table_rf_extended_l4,
  female_domcare_table_iv_extended_l4,
  tex = TRUE,
  file = female_domcare_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female domestic-care-related nonparticipation reason"
)

etable(
  female_outlf_table_rf_basic_current,
  female_outlf_table_rf_extended_current,
  female_outlf_table_iv_extended_current,
  female_outlf_table_rf_extended_l4,
  female_outlf_table_iv_extended_l4,
  tex = TRUE,
  file = female_outlf_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2 + ivf1,
  headers = c("RF basic", "RF extended", "IV current", "RF lag4", "IV lag4"),
  dict = c(
    "lightning_quarterly_total" = "Lightning (current)",
    "lightning_quarterly_total_l4" = "Lightning (lag 4)",
    "fit_raw_quarterly_outage_hrs_per_1000" = "Outage hours / 1000 (IV, current)",
    "fit_raw_quarterly_outage_hrs_per_1000_l4" = "Outage hours / 1000 (IV, lag 4)",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female out of labor force for domestic care"
)

story_female_lfp_wood <- feols(female_lfp ~ share_cooking_wood_charcoal | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_female_lfp_electricity <- feols(female_lfp ~ share_cooking_electricity | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_female_lfp_grid <- feols(female_lfp ~ share_grid_full_time | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_female_lfp_combined <- story_female_lfp
story_female_lfp_combined_fe <- feols(female_lfp ~ share_cooking_wood_charcoal + share_cooking_electricity + share_grid_full_time + share_electricity_from_grid | estrato4 + year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)

story_domcare_wood <- feols(female_domestic_care_reason ~ share_cooking_wood_charcoal | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_domcare_electricity <- feols(female_domestic_care_reason ~ share_cooking_electricity | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_domcare_grid <- feols(female_domestic_care_reason ~ share_grid_full_time | year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)
story_domcare_combined <- story_domcare
story_domcare_combined_fe <- feols(female_domestic_care_reason ~ share_cooking_wood_charcoal + share_cooking_electricity + share_grid_full_time + share_electricity_from_grid | estrato4 + year + quarter, data = pooled, weights = ~weight_sum, cluster = ~estrato4)

etable(
  story_female_lfp_wood,
  story_female_lfp_electricity,
  story_female_lfp_grid,
  story_female_lfp_combined,
  story_female_lfp_combined_fe,
  tex = TRUE,
  file = female_lfp_story_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2,
  headers = c("Wood only", "Electric cooking only", "Grid only", "Combined", "Combined + stratum FE"),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_cooking_electricity" = "Electric cooking share",
    "share_grid_full_time" = "Full-time grid share",
    "share_electricity_from_grid" = "Grid electricity share"
  ),
  title = "Story table: female labor force participation"
)

etable(
  story_domcare_wood,
  story_domcare_electricity,
  story_domcare_grid,
  story_domcare_combined,
  story_domcare_combined_fe,
  tex = TRUE,
  file = female_domcare_story_table_path,
  replace = TRUE,
  fitstat = ~ n + wr2,
  headers = c("Wood only", "Electric cooking only", "Grid only", "Combined", "Combined + stratum FE"),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_cooking_electricity" = "Electric cooking share",
    "share_grid_full_time" = "Full-time grid share",
    "share_electricity_from_grid" = "Grid electricity share"
  ),
  title = "Story table: female domestic-care reason"
)

make_binned_plot(pooled, "share_grid_full_time", "female_lfp", "Female LFP and secure grid access", here::here(fig_dir, "figure_5_story_female_lfp_grid.png"))
make_binned_plot(pooled, "share_cooking_wood_charcoal", "female_domestic_care_reason", "Domestic care burden and wood/charcoal cooking", here::here(fig_dir, "figure_6_story_domestic_care_wood.png"))

cat("Wrote\n")
cat(results_path, "\n")
cat(first_stage_path, "\n")
cat(focus_path, "\n")
cat(story_path, "\n")
cat(robustness_path, "\n")
cat(female_employment_table_path, "\n")
cat(female_lfp_table_path, "\n")
cat(female_domcare_table_path, "\n")
cat(female_outlf_table_path, "\n")
cat(female_lfp_story_table_path, "\n")
cat(female_domcare_story_table_path, "\n")
cat(here::here(fig_dir, "figure_5_story_female_lfp_grid.png"), "\n")
cat(here::here(fig_dir, "figure_6_story_domestic_care_wood.png"), "\n")
