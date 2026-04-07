#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
  library(ggplot2)
})

power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
household_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
women_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_refresh", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
fig_dir <- here::here("Figures", "powerIV", "lightning_iv_refresh")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

panel_out <- here::here(out_dir, "panel_labor_results.csv")
household_out <- here::here(out_dir, "household_asset_results.csv")
women_out <- here::here(out_dir, "women_micro_results.csv")
latex_out <- here::here(out_dir, "lightning_iv_key_results.tex")
first_stage_csv <- here::here(out_dir, "lightning_iv_first_stage_key.csv")
first_stage_tex <- here::here(out_dir, "lightning_iv_first_stage_key.tex")
robustness_csv <- here::here(out_dir, "lightning_iv_hours_robustness.csv")
robustness_tex <- here::here(out_dir, "lightning_iv_hours_robustness.tex")
first_stage_fig <- here::here(fig_dir, "lightning_first_stage_binned.png")
reduced_form_fig <- here::here(fig_dir, "lightning_reduced_form_hours_binned.png")

if (!file.exists(power_path)) stop("Missing file: ", power_path, call. = FALSE)
if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(household_path)) stop("Missing file: ", household_path, call. = FALSE)
if (!file.exists(women_path)) stop("Missing file: ", women_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

extract_model_row <- function(model, pattern) {
  td <- tidy(model)
  row <- td[grepl(pattern, td$term), , drop = FALSE]
  if (nrow(row) == 0) {
    tibble(term = NA_character_, estimate = NA_real_, std.error = NA_real_, statistic = NA_real_, p.value = NA_real_)
  } else {
    tibble(
      term = row$term[[1]],
      estimate = row$estimate[[1]],
      std.error = row$std.error[[1]],
      statistic = row$statistic[[1]],
      p.value = row$p.value[[1]]
    )
  }
}

format_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(x, digits = digits, format = "f"))
}

format_p <- function(x) {
  ifelse(is.na(x), "", ifelse(x < 0.001, "<0.001", formatC(x, digits = 3, format = "f")))
}

weighted_mean_safe <- function(x, w) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(NA_real_)
  weighted.mean(x[keep], w[keep])
}

make_latex_table <- function(panel_results, women_results, out_path) {
  key_rows <- bind_rows(
    panel_results %>%
      filter(model_family == "iv", sample == "pre_2016_2019", lag == "l0", outcome == "female_hours_effective_employed") %>%
      mutate(label = "Female hours employed", unit = "Minutes", scale = 60000),
    panel_results %>%
      filter(model_family == "iv", sample == "recovery_2022_2024", lag == "l0", outcome == "female_hours_effective_population") %>%
      mutate(label = "Female hours population", unit = "Minutes", scale = 60000),
    panel_results %>%
      filter(model_family == "iv", sample == "pre_2016_2019", lag == "l0", outcome == "female_outlf_domestic_care") %>%
      mutate(label = "Out of labor force for domestic care", unit = "Percentage points", scale = 100000),
    women_results %>%
      filter(model_family == "iv", sample == "pre_2016_2019", lag == "l0", outcome == "wanted_to_work") %>%
      mutate(label = "Wanted to work", unit = "Percentage points", scale = 100000),
    women_results %>%
      filter(model_family == "iv", sample == "pre_2016_2019", lag == "l0", outcome == "wants_more_hours") %>%
      mutate(label = "Wanted more hours", unit = "Percentage points", scale = 100000),
    women_results %>%
      filter(model_family == "iv", sample == "recovery_2022_2024", lag == "l0", outcome == "effective_hours_population") %>%
      mutate(label = "Individual hours population", unit = "Minutes", scale = 60000)
  ) %>%
    mutate(
      sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
      coef_scaled = estimate * scale,
      se_scaled = std.error * scale,
      coef_se = paste0(format_num(coef_scaled), " (", format_num(se_scaled), ")"),
      p_fmt = format_p(p.value)
    ) %>%
    select(label, sample_label, unit, coef_se, p_fmt, nobs)

  lines <- c(
    "\\begin{table}[!htbp]",
    "\\centering",
    "\\caption{Lightning IV key results in readable units}",
    "\\label{tab:lightning_iv_key_results}",
    "\\begin{tabular}{lllccl}",
    "\\hline",
    "Outcome & Sample & Unit & IV effect (SE) & p-value & N \\\\",
    "\\hline"
  )

  for (ii in seq_len(nrow(key_rows))) {
    row <- key_rows[ii, , drop = FALSE]
    lines <- c(
      lines,
      paste0(
        row$label[[1]], " & ",
        row$sample_label[[1]], " & ",
        row$unit[[1]], " & ",
        row$coef_se[[1]], " & ",
        row$p_fmt[[1]], " & ",
        format(row$nobs[[1]], scientific = FALSE), " \\\\"
      )
    )
  }

  lines <- c(
    lines,
    "\\hline",
    "\\multicolumn{6}{p{0.9\\linewidth}}{\\footnotesize Notes: IV coefficients are rescaled for readability. For hour outcomes, entries report minutes per additional outage hour per household in the quarter. For binary outcomes, entries report percentage-point effects per additional outage hour per household. Panel models include estrato4 and year-quarter fixed effects. Women's micro models include woman-spell and year-quarter fixed effects. Standard errors clustered at estrato4.}\\\\",
    "\\end{tabular}",
    "\\end{table}"
  )

  writeLines(lines, out_path)
}

make_first_stage_table <- function(panel_results, women_results, out_csv, out_tex) {
  first_stage_rows <- bind_rows(
    panel_results %>%
      filter(model_family == "first_stage", sample == "pre_2016_2019", lag == "l0", outcome == "female_hours_effective_employed") %>%
      mutate(label = "Female hours employed", fe_label = "estrato4 + year-quarter"),
    panel_results %>%
      filter(model_family == "first_stage", sample == "recovery_2022_2024", lag == "l0", outcome == "female_hours_effective_population") %>%
      mutate(label = "Female hours population", fe_label = "estrato4 + year-quarter"),
    panel_results %>%
      filter(model_family == "first_stage", sample == "pre_2016_2019", lag == "l0", outcome == "female_outlf_domestic_care") %>%
      mutate(label = "Out of LF for domestic care", fe_label = "estrato4 + year-quarter"),
    women_results %>%
      filter(model_family == "first_stage", sample == "pre_2016_2019", lag == "l0", outcome == "wanted_to_work") %>%
      mutate(label = "Wanted to work", fe_label = "woman-spell + year-quarter"),
    women_results %>%
      filter(model_family == "first_stage", sample == "pre_2016_2019", lag == "l0", outcome == "wants_more_hours") %>%
      mutate(label = "Wanted more hours", fe_label = "woman-spell + year-quarter"),
    women_results %>%
      filter(model_family == "first_stage", sample == "recovery_2022_2024", lag == "l0", outcome == "effective_hours_population") %>%
      mutate(label = "Individual hours population", fe_label = "woman-spell + year-quarter")
  ) %>%
    mutate(
      sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
      coef_se = paste0(format_num(estimate, 1), " (", format_num(std.error, 1), ")"),
      f_fmt = format_num(first_stage_f, 2),
      p_fmt = format_p(p.value)
    ) %>%
    select(label, sample_label, fe_label, coef_se, f_fmt, p_fmt, nobs)

  write_csv(first_stage_rows, out_csv)

  lines <- c(
    "\\begin{table}[!htbp]",
    "\\centering",
    "\\caption{Lightning IV key first-stage results}",
    "\\label{tab:lightning_iv_first_stage}",
    "\\begin{tabular}{lllccc}",
    "\\hline",
    "Outcome & Sample & Fixed effects & First stage (SE) & F-stat & N \\\\",
    "\\hline"
  )

  for (ii in seq_len(nrow(first_stage_rows))) {
    row <- first_stage_rows[ii, , drop = FALSE]
    lines <- c(
      lines,
      paste0(
        row$label[[1]], " & ",
        row$sample_label[[1]], " & ",
        row$fe_label[[1]], " & ",
        row$coef_se[[1]], " & ",
        row$f_fmt[[1]], " & ",
        format(row$nobs[[1]], scientific = FALSE), " \\\\"
      )
    )
  }

  lines <- c(
    lines,
    "\\hline",
    "\\multicolumn{6}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report the coefficient on lightning in the first-stage regression where outage hours per 1,000 households are the dependent variable. Panel specifications include estrato4 and year-quarter fixed effects. Women's micro specifications include woman-spell and year-quarter fixed effects. Standard errors clustered at estrato4.}\\\\",
    "\\end{tabular}",
    "\\end{table}"
  )

  writeLines(lines, out_tex)
}

make_robustness_table <- function(panel_results, women_results, out_csv, out_tex) {
  panel_fs <- panel_results %>%
    filter(model_family == "first_stage") %>%
    select(sample, lag, outcome, fs_stat = first_stage_f)

  women_fs <- women_results %>%
    filter(model_family == "first_stage") %>%
    select(sample, lag, outcome, fs_stat = first_stage_f)

  panel_keep <- panel_results %>%
    filter(
      model_family == "iv",
      outcome %in% c("female_hours_effective_employed", "female_hours_effective_population"),
      sample %in% c("pre_2016_2019", "recovery_2022_2024")
    ) %>%
    left_join(panel_fs, by = c("sample", "lag", "outcome")) %>%
    mutate(
      label = case_when(
        outcome == "female_hours_effective_employed" ~ "Female hours employed",
        outcome == "female_hours_effective_population" ~ "Female hours population",
        TRUE ~ outcome
      ),
      unit = "Minutes",
      scale = 60000
    )

  women_keep <- women_results %>%
    filter(
      model_family == "iv",
      outcome == "effective_hours_population",
      sample %in% c("pre_2016_2019", "recovery_2022_2024")
    ) %>%
    left_join(women_fs, by = c("sample", "lag", "outcome")) %>%
    mutate(
      label = "Individual hours population",
      unit = "Minutes",
      scale = 60000
    )

  robustness_rows <- bind_rows(panel_keep, women_keep) %>%
    filter(
      (label == "Female hours employed" & sample == "pre_2016_2019") |
        (label == "Female hours population" & sample == "recovery_2022_2024") |
        (label == "Individual hours population" & sample == "recovery_2022_2024")
    ) %>%
    mutate(
      sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
      lag_label = recode(lag, l0 = "Current", l1 = "Lag 1", l2 = "Lag 2", l4 = "Lag 4"),
      coef_scaled = estimate * scale,
      se_scaled = std.error * scale,
      coef_se = paste0(format_num(coef_scaled), " (", format_num(se_scaled), ")"),
      f_fmt = format_num(fs_stat, 2),
      p_fmt = format_p(p.value)
    ) %>%
    select(label, sample_label, lag_label, coef_scaled, se_scaled, coef_se, p_fmt, f_fmt, nobs)

  write_csv(robustness_rows, out_csv)

  lines <- c(
    "\\begin{table}[!htbp]",
    "\\centering",
    "\\caption{Lightning IV hours robustness across lags}",
    "\\label{tab:lightning_iv_robustness}",
    "\\begin{tabular}{lllccc}",
    "\\hline",
    "Outcome & Sample & Lag & IV effect (SE) & First-stage F & N \\\\",
    "\\hline"
  )

  for (ii in seq_len(nrow(robustness_rows))) {
    row <- robustness_rows[ii, , drop = FALSE]
    lines <- c(
      lines,
      paste0(
        row$label[[1]], " & ",
        row$sample_label[[1]], " & ",
        row$lag_label[[1]], " & ",
        row$coef_se[[1]], " & ",
        row$f_fmt[[1]], " & ",
        format(row$nobs[[1]], scientific = FALSE), " \\\\"
      )
    )
  }

  lines <- c(
    lines,
    "\\hline",
    "\\multicolumn{6}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report IV coefficients in minutes per additional outage hour per household in the quarter. The table follows the same cleaned specification as the main lightning IV table and shows how the key hour outcomes move when the lightning instrument is paired with current, one-quarter, two-quarter, and four-quarter outage exposure. Standard errors clustered at estrato4.}\\\\",
    "\\end{tabular}",
    "\\end{table}"
  )

  writeLines(lines, out_tex)
}

build_binned_plot_data <- function(dat, x_var, y_var, weight_var, fe_txt, sample_label, outcome_label, n_bins = 20L) {
  needed <- c(x_var, y_var, weight_var)
  dat <- dat %>% filter(if_all(all_of(needed), ~ !is.na(.x)), .data[[weight_var]] > 0)
  if (nrow(dat) == 0) return(tibble())

  x_fit <- feols(as.formula(paste0(x_var, " ~ 1 | ", fe_txt)), data = dat, weights = as.formula(paste0("~", weight_var)))
  y_fit <- feols(as.formula(paste0(y_var, " ~ 1 | ", fe_txt)), data = dat, weights = as.formula(paste0("~", weight_var)))

  dat <- dat %>%
    mutate(
      resid_x = resid(x_fit),
      resid_y = resid(y_fit)
    ) %>%
    filter(!is.na(resid_x), !is.na(resid_y)) %>%
    mutate(bin = ntile(resid_x, n_bins)) %>%
    group_by(bin) %>%
    summarise(
      x = weighted_mean_safe(resid_x, .data[[weight_var]]),
      y = weighted_mean_safe(resid_y, .data[[weight_var]]),
      .groups = "drop"
    ) %>%
    mutate(sample = sample_label, outcome = outcome_label)

  dat
}

make_section_figures <- function(power_base, panel_base, first_stage_path, reduced_form_path) {
  plot_specs <- tibble::tribble(
    ~sample_name, ~year_min, ~year_max, ~rf_outcome, ~rf_label,
    "2016--2019", 2016L, 2019L, "female_hours_effective_employed", "Female hours employed",
    "2022--2024", 2022L, 2024L, "female_hours_effective_population", "Female hours population"
  )

  fs_plot_rows <- list()
  rf_plot_rows <- list()

  for (ii in seq_len(nrow(plot_specs))) {
    sample_name <- plot_specs$sample_name[[ii]]
    year_min <- plot_specs$year_min[[ii]]
    year_max <- plot_specs$year_max[[ii]]
    rf_outcome <- plot_specs$rf_outcome[[ii]]
    rf_label <- plot_specs$rf_label[[ii]]

    power_sample <- power_base %>%
      filter(year >= year_min, year <= year_max) %>%
      add_sample_lags()

    panel_sample <- panel_base %>%
      filter(year >= year_min, year <= year_max) %>%
      left_join(power_sample, by = c("estrato4", "year", "quarter", "year_quarter"))

    fs_dat <- panel_sample %>%
      filter(!is.na(weight_sum), weight_sum > 0, !is.na(lightning_l0), !is.na(outage_l0))

    rf_dat <- panel_sample %>%
      filter(!is.na(weight_sum), weight_sum > 0, !is.na(lightning_l0), !is.na(.data[[rf_outcome]]))

    fs_plot_rows[[ii]] <- build_binned_plot_data(
      dat = fs_dat,
      x_var = "lightning_l0",
      y_var = "outage_l0",
      weight_var = "weight_sum",
      fe_txt = "estrato4 + year_quarter",
      sample_label = sample_name,
      outcome_label = "Outage hours per 1,000 households"
    )

    rf_plot_rows[[ii]] <- build_binned_plot_data(
      dat = rf_dat,
      x_var = "lightning_l0",
      y_var = rf_outcome,
      weight_var = "weight_sum",
      fe_txt = "estrato4 + year_quarter",
      sample_label = sample_name,
      outcome_label = rf_label
    )
  }

  fs_plot_df <- bind_rows(fs_plot_rows)
  rf_plot_df <- bind_rows(rf_plot_rows)

  p_fs <- ggplot(fs_plot_df, aes(x = x, y = y)) +
    geom_point(size = 2.1) +
    geom_smooth(method = "lm", se = FALSE, color = "steelblue4", linewidth = 0.8) +
    facet_wrap(~sample, scales = "free") +
    theme_minimal(base_size = 11) +
    labs(
      title = "Residualized first stage: lightning and outage hours",
      x = "Residualized lightning",
      y = "Residualized outage hours per 1,000 households"
    )

  p_rf <- ggplot(rf_plot_df, aes(x = x, y = y)) +
    geom_point(size = 2.1) +
    geom_smooth(method = "lm", se = FALSE, color = "firebrick4", linewidth = 0.8) +
    facet_wrap(~sample, scales = "free_y") +
    theme_minimal(base_size = 11) +
    labs(
      title = "Residualized reduced form: lightning and female hours",
      x = "Residualized lightning",
      y = "Residualized female hours"
    )

  ggsave(first_stage_path, p_fs, width = 9, height = 4.8, dpi = 300)
  ggsave(reduced_form_path, p_rf, width = 9, height = 4.8, dpi = 300)
}

add_sample_lags <- function(df) {
  df <- df %>%
    arrange(estrato4, year, quarter) %>%
    group_by(estrato4) %>%
    mutate(
      lightning_l0 = lightning_quarterly_total,
      lightning_l1 = lag(lightning_quarterly_total, 1),
      lightning_l2 = lag(lightning_quarterly_total, 2),
      lightning_l4 = lag(lightning_quarterly_total, 4),
      outage_l0 = raw_quarterly_outage_hrs_per_1000,
      outage_l1 = lag(raw_quarterly_outage_hrs_per_1000, 1),
      outage_l2 = lag(raw_quarterly_outage_hrs_per_1000, 2),
      outage_l4 = lag(raw_quarterly_outage_hrs_per_1000, 4)
    ) %>%
    ungroup()
  df
}

run_panel_models <- function(dat, outcome, weight_var, fe_txt, cluster_fml, sample_name, lag_name, section_name, n_base_obs, n_units) {
  lightning_var <- paste0("lightning_", lag_name)
  outage_var <- paste0("outage_", lag_name)
  needed <- c(outcome, weight_var, "estrato4", "year_quarter", lightning_var, outage_var)
  dat <- dat %>% filter(if_all(all_of(needed), ~ !is.na(.x)), .data[[weight_var]] > 0)

  if (nrow(dat) == 0) {
    return(tibble(
      section = section_name,
      sample = sample_name,
      lag = lag_name,
      outcome = outcome,
      model_family = c("first_stage", "reduced_form", "iv"),
      term = NA_character_,
      estimate = NA_real_,
      std.error = NA_real_,
      statistic = NA_real_,
      p.value = NA_real_,
      nobs = 0L,
      n_base_obs = n_base_obs,
      n_units = n_units,
      controls = "none",
      fixed_effects = fe_txt,
      ivf1 = NA_real_,
      first_stage_f = NA_real_
    ))
  }

  if (is.na(sd(dat[[outcome]], na.rm = TRUE)) || sd(dat[[outcome]], na.rm = TRUE) == 0) {
    return(tibble(
      section = section_name,
      sample = sample_name,
      lag = lag_name,
      outcome = outcome,
      model_family = c("first_stage", "reduced_form", "iv"),
      term = NA_character_,
      estimate = NA_real_,
      std.error = NA_real_,
      statistic = NA_real_,
      p.value = NA_real_,
      nobs = nrow(dat),
      n_base_obs = n_base_obs,
      n_units = n_units,
      controls = "none",
      fixed_effects = fe_txt,
      ivf1 = NA_real_,
      first_stage_f = NA_real_
    ))
  }

  fs_formula <- as.formula(paste0(outage_var, " ~ ", lightning_var, " | ", fe_txt))
  rf_formula <- as.formula(paste0(outcome, " ~ ", lightning_var, " | ", fe_txt))
  iv_formula <- as.formula(paste0(outcome, " ~ 1 | ", fe_txt, " | ", outage_var, " ~ ", lightning_var))

  fs <- feols(fs_formula, data = dat, weights = as.formula(paste0("~", weight_var)), cluster = cluster_fml)
  rf <- feols(rf_formula, data = dat, weights = as.formula(paste0("~", weight_var)), cluster = cluster_fml)
  iv <- feols(iv_formula, data = dat, weights = as.formula(paste0("~", weight_var)), cluster = cluster_fml)

  fs_row <- extract_model_row(fs, paste0("^", lightning_var, "$"))
  rf_row <- extract_model_row(rf, paste0("^", lightning_var, "$"))
  iv_row <- extract_model_row(iv, "^fit_")
  fs_f <- safe_fitstat(fs, "f")
  ivf1 <- safe_fitstat(iv, "ivf1")

  bind_rows(
    fs_row %>% mutate(model_family = "first_stage"),
    rf_row %>% mutate(model_family = "reduced_form"),
    iv_row %>% mutate(model_family = "iv")
  ) %>%
    mutate(
      section = section_name,
      sample = sample_name,
      lag = lag_name,
      outcome = outcome,
      nobs = nobs(iv),
      n_base_obs = n_base_obs,
      n_units = n_units,
      controls = "none",
      fixed_effects = fe_txt,
      ivf1 = if_else(model_family == "iv", ivf1, NA_real_),
      first_stage_f = if_else(model_family == "first_stage", fs_f, NA_real_)
    ) %>%
    select(
      section, sample, lag, outcome, model_family, term, estimate, std.error, statistic, p.value,
      nobs, n_base_obs, n_units, controls, fixed_effects, ivf1, first_stage_f
    )
}

power_base <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000)
  )

panel_base <- read_parquet(panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    weight_sum = as.numeric(weight_sum),
    female_lfp = as.numeric(female_lfp),
    female_employment = as.numeric(female_employment),
    female_domestic_care_reason = as.numeric(female_domestic_care_reason),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care),
    female_hours_effective_employed = as.numeric(female_hours_effective_employed),
    female_hours_effective_population = as.numeric(female_hours_effective_population)
  )

household_base <- read_parquet(household_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    weight = as.numeric(weight_calibrated),
    refrigerator = as.numeric(refrigerator),
    washing_machine = as.numeric(washing_machine),
    satellite_dish_tv = as.numeric(satellite_dish_tv),
    computer = as.numeric(computer),
    internet_home = as.numeric(internet_home),
    internet_by_mobile = as.numeric(internet_by_mobile),
    vehicle = as.numeric(vehicle),
    n_mobile_phones = as.numeric(n_mobile_phones),
    appliance_count_basic = as.numeric(appliance_count_basic),
    appliance_count_connected = as.numeric(appliance_count_connected)
  )

women_base <- read_parquet(
  women_path,
  col_select = c(
    "woman_spell_id", "woman_id", "estrato4", "year", "quarter", "person_weight",
    "employed", "effective_hours", "domestic_care_reason", "outlf_domestic_care",
    "wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m"
  )
) %>%
  transmute(
    woman_spell_id = if_else(!is.na(woman_spell_id), woman_spell_id, woman_id),
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    person_weight = as.numeric(person_weight),
    employed = as.numeric(employed),
    effective_hours = as.numeric(effective_hours),
    effective_hours_population = if_else(employed == 1 & !is.na(effective_hours), effective_hours, 0),
    domestic_care_reason = as.numeric(domestic_care_reason),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    wanted_to_work = as.numeric(wanted_to_work),
    wants_more_hours = as.numeric(wants_more_hours),
    available_more_hours = as.numeric(available_more_hours),
    search_long = as.numeric(search_long),
    worked_last_12m = as.numeric(worked_last_12m)
  )

samples <- tibble::tribble(
  ~sample, ~year_min, ~year_max,
  "pre_2016_2019", 2016L, 2019L,
  "recovery_2022_2024", 2022L, 2024L
)

lag_names <- c("l0", "l1", "l2", "l4")

panel_outcomes <- c(
  "female_lfp",
  "female_employment",
  "female_domestic_care_reason",
  "female_outlf_domestic_care",
  "female_hours_effective_employed",
  "female_hours_effective_population"
)

household_outcomes <- c(
  "refrigerator",
  "washing_machine",
  "satellite_dish_tv",
  "computer",
  "internet_home",
  "internet_by_mobile",
  "vehicle",
  "n_mobile_phones",
  "appliance_count_basic",
  "appliance_count_connected"
)

women_outcomes <- c(
  "effective_hours",
  "effective_hours_population",
  "domestic_care_reason",
  "outlf_domestic_care",
  "wanted_to_work",
  "wants_more_hours",
  "available_more_hours",
  "search_long",
  "worked_last_12m"
)

panel_rows <- list()
household_rows <- list()
women_rows <- list()
idx_panel <- 1L
idx_household <- 1L
idx_women <- 1L

for (ii in seq_len(nrow(samples))) {
  sample_name <- samples$sample[[ii]]
  year_min <- samples$year_min[[ii]]
  year_max <- samples$year_max[[ii]]

  power_sample <- power_base %>%
    filter(year >= year_min, year <= year_max) %>%
    add_sample_lags()

  panel_sample <- panel_base %>%
    filter(year >= year_min, year <= year_max) %>%
    left_join(power_sample, by = c("estrato4", "year", "quarter", "year_quarter"))

  household_sample <- household_base %>%
    filter(year >= year_min, year <= year_max) %>%
    left_join(power_sample, by = c("estrato4", "year", "quarter", "year_quarter"))

  women_sample <- women_base %>%
    filter(year >= year_min, year <= year_max) %>%
    left_join(power_sample, by = c("estrato4", "year", "quarter", "year_quarter"))

  repeat_women <- women_sample %>%
    filter(!is.na(woman_spell_id)) %>%
    count(woman_spell_id, name = "n_quarters") %>%
    filter(n_quarters >= 2)

  women_sample <- women_sample %>% inner_join(repeat_women, by = "woman_spell_id")

  for (lag_name in lag_names) {
    for (outcome in panel_outcomes) {
      panel_rows[[idx_panel]] <- run_panel_models(
        dat = panel_sample,
        outcome = outcome,
        weight_var = "weight_sum",
        fe_txt = "estrato4 + year_quarter",
        cluster_fml = ~estrato4,
        sample_name = sample_name,
        lag_name = lag_name,
        section_name = "panel_labor",
        n_base_obs = nrow(panel_sample),
        n_units = n_distinct(panel_sample$estrato4)
      )
      idx_panel <- idx_panel + 1L
    }

    for (outcome in household_outcomes) {
      household_rows[[idx_household]] <- run_panel_models(
        dat = household_sample,
        outcome = outcome,
        weight_var = "weight",
        fe_txt = "estrato4 + year_quarter",
        cluster_fml = ~estrato4,
        sample_name = sample_name,
        lag_name = lag_name,
        section_name = "household_assets",
        n_base_obs = nrow(household_sample),
        n_units = n_distinct(household_sample$estrato4)
      )
      idx_household <- idx_household + 1L
    }

    for (outcome in women_outcomes) {
      women_rows[[idx_women]] <- run_panel_models(
        dat = women_sample,
        outcome = outcome,
        weight_var = "person_weight",
        fe_txt = "woman_spell_id + year_quarter",
        cluster_fml = ~estrato4,
        sample_name = sample_name,
        lag_name = lag_name,
        section_name = "women_micro",
        n_base_obs = nrow(women_sample),
        n_units = n_distinct(women_sample$woman_spell_id)
      )
      idx_women <- idx_women + 1L
    }
  }
}

panel_results <- bind_rows(panel_rows) %>% arrange(sample, lag, outcome, model_family)
household_results <- bind_rows(household_rows) %>% arrange(sample, lag, outcome, model_family)
women_results <- bind_rows(women_rows) %>% arrange(sample, lag, outcome, model_family)

write_csv(panel_results, panel_out)
write_csv(household_results, household_out)
write_csv(women_results, women_out)
make_latex_table(panel_results, women_results, latex_out)
make_first_stage_table(panel_results, women_results, first_stage_csv, first_stage_tex)
make_robustness_table(panel_results, women_results, robustness_csv, robustness_tex)
make_section_figures(power_base, panel_base, first_stage_fig, reduced_form_fig)

message("Wrote: ", panel_out)
message("Wrote: ", household_out)
message("Wrote: ", women_out)
message("Wrote: ", latex_out)
message("Wrote: ", first_stage_csv)
message("Wrote: ", first_stage_tex)
message("Wrote: ", robustness_csv)
message("Wrote: ", robustness_tex)
message("Wrote: ", first_stage_fig)
message("Wrote: ", reduced_form_fig)

print(panel_results %>% filter(model_family %in% c("first_stage", "iv")))
print(household_results %>% filter(model_family %in% c("first_stage", "iv")))
print(women_results %>% filter(model_family %in% c("first_stage", "iv")))
