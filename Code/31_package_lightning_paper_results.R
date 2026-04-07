#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(sf)
  library(scales)
  library(fixest)
  library(broom)
  library(PNADcIBGE)
})

panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
shape_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")
raw_dir <- here::here("data", "PNAD-C", "raw")
input_txt <- here::here("data", "PNAD-C", "raw", "input_PNADC_trimestral.txt")

root_out <- here::here("data", "powerIV", "regressions", "lightning_iv_paper_package")
main_dir <- here::here(root_out, "main_results")
appendix_dir <- here::here(root_out, "appendix")
dir.create(main_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(appendix_dir, recursive = TRUE, showWarnings = FALSE)

if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(shape_path)) stop("Missing file: ", shape_path, call. = FALSE)
if (!file.exists(input_txt)) stop("Missing file: ", input_txt, call. = FALSE)

format_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(x, digits = digits, format = "f"))
}

format_p <- function(x) {
  ifelse(is.na(x), "", ifelse(x < 0.001, "<0.001", formatC(x, digits = 3, format = "f")))
}

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

weighted_mean_safe <- function(x, w) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(NA_real_)
  weighted.mean(x[keep], w[keep])
}

weighted_quantile_safe <- function(x, w, probs) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(rep(NA_real_, length(probs)))
  x <- x[keep]
  w <- w[keep]
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]
  cw <- cumsum(w) / sum(w)
  sapply(probs, function(p) x[which(cw >= p)[1]])
}

extract_fit_row <- function(model, pattern = "^fit_") {
  td <- tidy(model)
  row <- td[grepl(pattern, td$term), , drop = FALSE]
  if (nrow(row) == 0) {
    tibble(term = NA_character_, estimate = NA_real_, std.error = NA_real_, p.value = NA_real_)
  } else {
    tibble(
      term = row$term[[1]],
      estimate = row$estimate[[1]],
      std.error = row$std.error[[1]],
      p.value = row$p.value[[1]]
    )
  }
}

add_lags <- function(df) {
  df %>%
    arrange(estrato4, year, quarter) %>%
    group_by(estrato4) %>%
    mutate(
      lightning_l0 = lightning_quarterly_total,
      outage_l0 = raw_quarterly_outage_hrs_per_1000,
      lightning_l1 = lag(lightning_quarterly_total, 1),
      outage_l1 = lag(raw_quarterly_outage_hrs_per_1000, 1),
      lightning_l2 = lag(lightning_quarterly_total, 2),
      outage_l2 = lag(raw_quarterly_outage_hrs_per_1000, 2),
      lightning_l4 = lag(lightning_quarterly_total, 4),
      outage_l4 = lag(raw_quarterly_outage_hrs_per_1000, 4)
    ) %>%
    ungroup()
}

panel <- read_parquet(panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    uf = as.character(uf),
    weight_sum = as.numeric(weight_sum),
    female_pop_weight = as.numeric(female_pop_weight),
    female_lfp = as.numeric(female_lfp),
    female_employment = as.numeric(female_employment),
    female_domestic_care_reason = as.numeric(female_domestic_care_reason),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care),
    female_hours_effective_employed = as.numeric(female_hours_effective_employed),
    female_hours_effective_population = as.numeric(female_hours_effective_population),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000)
  ) %>%
  mutate(
    female_outlf_weight = female_pop_weight * pmax(1 - female_lfp, 0),
    sample_window = case_when(
      year >= 2016 & year <= 2019 ~ "2016-2019",
      year >= 2022 & year <= 2024 ~ "2022-2024",
      TRUE ~ "2020-2021"
    )
  ) %>%
  add_lags()

panel_pre <- panel %>% filter(year >= 2016, year <= 2019)
panel_recovery <- panel %>% filter(year >= 2022, year <= 2024)

figure_desc_time <- here::here(main_dir, "figure_1_domestic_care_time_series.png")
figure_desc_map <- here::here(main_dir, "figure_2_domestic_care_map.png")
table_definition_csv <- here::here(main_dir, "table_1_domestic_care_definition.csv")
table_definition_tex <- here::here(main_dir, "table_1_domestic_care_definition.tex")
table_first_stage_csv <- here::here(main_dir, "table_2_lightning_first_stage.csv")
table_first_stage_tex <- here::here(main_dir, "table_2_lightning_first_stage.tex")
table_main_iv_csv <- here::here(main_dir, "table_3_lightning_domestic_care_iv.csv")
table_main_iv_tex <- here::here(main_dir, "table_3_lightning_domestic_care_iv.tex")
figure_rf <- here::here(main_dir, "figure_3_lightning_domestic_care_reduced_form.png")
paragraph_txt <- here::here(main_dir, "domestic_care_variable_justification.txt")
paragraph_tex <- here::here(main_dir, "domestic_care_variable_justification.tex")

table_reason_csv <- here::here(appendix_dir, "table_a1_reason_distribution_snapshots.csv")
table_reason_tex <- here::here(appendix_dir, "table_a1_reason_distribution_snapshots.tex")
table_support_csv <- here::here(appendix_dir, "table_a2_supporting_correlations.csv")
table_support_tex <- here::here(appendix_dir, "table_a2_supporting_correlations.tex")
table_domcare_lag_csv <- here::here(appendix_dir, "table_a3_domestic_care_iv_lags.csv")
table_domcare_lag_tex <- here::here(appendix_dir, "table_a3_domestic_care_iv_lags.tex")
figure_hist <- here::here(appendix_dir, "figure_a1_domestic_care_distribution.png")

time_series <- bind_rows(
  panel %>%
    group_by(year, quarter, year_quarter) %>%
    summarise(
      series = "Domestic care among women out of labor force",
      share_percent = weighted_mean_safe(female_outlf_domestic_care, female_outlf_weight) * 100,
      .groups = "drop"
    ),
  panel %>%
    group_by(year, quarter, year_quarter) %>%
    summarise(
      series = "Domestic care among all women",
      share_percent = weighted_mean_safe(female_domestic_care_reason, female_pop_weight) * 100,
      .groups = "drop"
    )
) %>%
  mutate(date = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1) * 3 + 1)))

ggplot(time_series, aes(x = date, y = share_percent, color = series)) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date(c("2020-01-01", "2022-01-01")), linetype = "dashed", color = "gray50") +
  scale_color_manual(values = c("#d95f0e", "#2b8cbe")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%YQ1") +
  labs(
    x = "Quarter",
    y = "Share (%)",
    color = "",
    title = "Domestic-care nonparticipation in PNAD-C"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(figure_desc_time, width = 10, height = 5.5, dpi = 300)

map_values <- panel %>%
  group_by(estrato4) %>%
  summarise(
    pre_percent = weighted_mean_safe(female_outlf_domestic_care[year >= 2016 & year <= 2019], female_outlf_weight[year >= 2016 & year <= 2019]) * 100,
    recovery_percent = weighted_mean_safe(female_outlf_domestic_care[year >= 2022 & year <= 2024], female_outlf_weight[year >= 2022 & year <= 2024]) * 100,
    .groups = "drop"
  )

estrato_map <- st_read(shape_path, quiet = TRUE) %>%
  st_make_valid() %>%
  left_join(map_values, by = "estrato4")

map_plot <- bind_rows(
  estrato_map %>% mutate(period = "Pre-pandemic 2016-2019", share_percent = pre_percent),
  estrato_map %>% mutate(period = "Post-resumption 2022-2024", share_percent = recovery_percent)
)

ggplot(map_plot) +
  geom_sf(aes(fill = share_percent), color = NA) +
  facet_wrap(~period, ncol = 1) +
  scale_fill_viridis_c(option = "C", direction = -1, labels = label_number(suffix = "%"), na.value = "gray90") +
  labs(
    fill = "",
    title = "Share of nonparticipating women citing domestic care"
  ) +
  theme_void(base_size = 12) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    legend.background = element_rect(fill = "white", color = NA),
    strip.text = element_text(size = 11)
  )

ggsave(figure_desc_map, width = 8, height = 10, dpi = 300)

definition_table <- bind_rows(
  tibble(
    outcome = "Domestic care among women out of labor force",
    denominator = "Women outside the labor force",
    weighted_mean_2016_2019 = weighted_mean_safe(panel_pre$female_outlf_domestic_care, panel_pre$female_outlf_weight) * 100,
    weighted_mean_2022_2024 = weighted_mean_safe(panel_recovery$female_outlf_domestic_care, panel_recovery$female_outlf_weight) * 100,
    cell_median_2016_2024 = median(panel$female_outlf_domestic_care, na.rm = TRUE) * 100,
    cell_sd_2016_2024 = sd(panel$female_outlf_domestic_care, na.rm = TRUE) * 100,
    cell_min_2016_2024 = min(panel$female_outlf_domestic_care, na.rm = TRUE) * 100,
    cell_max_2016_2024 = max(panel$female_outlf_domestic_care, na.rm = TRUE) * 100
  ),
  tibble(
    outcome = "Domestic care among all women",
    denominator = "All women",
    weighted_mean_2016_2019 = weighted_mean_safe(panel_pre$female_domestic_care_reason, panel_pre$female_pop_weight) * 100,
    weighted_mean_2022_2024 = weighted_mean_safe(panel_recovery$female_domestic_care_reason, panel_recovery$female_pop_weight) * 100,
    cell_median_2016_2024 = median(panel$female_domestic_care_reason, na.rm = TRUE) * 100,
    cell_sd_2016_2024 = sd(panel$female_domestic_care_reason, na.rm = TRUE) * 100,
    cell_min_2016_2024 = min(panel$female_domestic_care_reason, na.rm = TRUE) * 100,
    cell_max_2016_2024 = max(panel$female_domestic_care_reason, na.rm = TRUE) * 100
  )
)

write_csv(definition_table, table_definition_csv)

definition_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Definition and distribution of domestic-care outcomes}",
  "\\label{tab:domestic_care_definition}",
  "\\begin{tabular}{lcccccc}",
  "\\hline",
  "Outcome & 2016--2019 mean (\\%) & 2022--2024 mean (\\%) & Median (\\%) & SD & Min & Max \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(definition_table))) {
  row <- definition_table[ii, , drop = FALSE]
  definition_lines <- c(
    definition_lines,
    paste0(
      row$outcome[[1]], " & ",
      format_num(row$weighted_mean_2016_2019[[1]], 1), " & ",
      format_num(row$weighted_mean_2022_2024[[1]], 1), " & ",
      format_num(row$cell_median_2016_2024[[1]], 1), " & ",
      format_num(row$cell_sd_2016_2024[[1]], 1), " & ",
      format_num(row$cell_min_2016_2024[[1]], 1), " & ",
      format_num(row$cell_max_2016_2024[[1]], 1), " \\\\"
    )
  )
}

definition_lines <- c(
  definition_lines,
  "\\hline",
  "\\multicolumn{7}{p{0.92\\linewidth}}{\\footnotesize Notes: The main outcome is the share of working-age women outside the labor force who report domestic chores, children, or another dependent relative as the main reason for nonparticipation. The comparison outcome uses the same reason category but among all women.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(definition_lines, table_definition_tex)

run_fs <- function(df, outcome_var) {
  dat <- df %>%
    filter(!is.na(weight_sum), weight_sum > 0, !is.na(lightning_l0), !is.na(outage_l0), !is.na(.data[[outcome_var]]))
  fs <- feols(outage_l0 ~ lightning_l0 | estrato4 + year_quarter, data = dat, weights = ~weight_sum, cluster = ~estrato4)
  row <- tidy(fs)
  row <- row[row$term == "lightning_l0", , drop = FALSE]
  tibble(
    coef = row$estimate[[1]],
    se = row$std.error[[1]],
    f_stat = safe_num(fitstat(fs, "f")),
    nobs = nobs(fs)
  )
}

first_stage_table <- bind_rows(
  run_fs(panel_pre, "female_outlf_domestic_care") %>% mutate(sample = "2016--2019"),
  run_fs(panel_recovery, "female_outlf_domestic_care") %>% mutate(sample = "2022--2024")
) %>%
  select(sample, coef, se, f_stat, nobs)

write_csv(first_stage_table, table_first_stage_csv)

first_stage_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning first stage for the domestic-care sample}",
  "\\label{tab:lightning_first_stage_domcare}",
  "\\begin{tabular}{lccc}",
  "\\hline",
  "Sample & First stage (SE) & F-stat & N \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(first_stage_table))) {
  row <- first_stage_table[ii, , drop = FALSE]
  first_stage_lines <- c(
    first_stage_lines,
    paste0(
      row$sample[[1]], " & ",
      format_num(row$coef[[1]], 1), " (", format_num(row$se[[1]], 1), ") & ",
      format_num(row$f_stat[[1]], 2), " & ",
      format(row$nobs[[1]], scientific = FALSE), " \\\\"
    )
  )
}

first_stage_lines <- c(
  first_stage_lines,
  "\\hline",
  "\\multicolumn{4}{p{0.88\\linewidth}}{\\footnotesize Notes: Entries report the coefficient on lightning in first-stage regressions where quarterly outage hours per household are the dependent variable. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(first_stage_lines, table_first_stage_tex)

run_iv <- function(df, outcome_var, outcome_label) {
  dat <- df %>%
    filter(!is.na(weight_sum), weight_sum > 0, !is.na(lightning_l0), !is.na(outage_l0), !is.na(.data[[outcome_var]]))
  iv <- feols(
    as.formula(paste0(outcome_var, " ~ 1 | estrato4 + year_quarter | outage_l0 ~ lightning_l0")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  row <- extract_fit_row(iv)
  tibble(
    outcome = outcome_label,
    coef_pp = row$estimate[[1]] * 100000,
    se_pp = row$std.error[[1]] * 100000,
    p = row$p.value[[1]],
    nobs = nobs(iv)
  )
}

main_iv_table <- bind_rows(
  run_iv(panel_pre, "female_outlf_domestic_care", "Domestic care among women out of labor force") %>% mutate(sample = "2016--2019"),
  run_iv(panel_recovery, "female_outlf_domestic_care", "Domestic care among women out of labor force") %>% mutate(sample = "2022--2024"),
  run_iv(panel_pre, "female_domestic_care_reason", "Domestic care among all women") %>% mutate(sample = "2016--2019"),
  run_iv(panel_recovery, "female_domestic_care_reason", "Domestic care among all women") %>% mutate(sample = "2022--2024")
) %>%
  select(outcome, sample, coef_pp, se_pp, p, nobs)

write_csv(main_iv_table, table_main_iv_csv)

main_iv_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning IV effects on domestic-care nonparticipation}",
  "\\label{tab:lightning_domcare_iv}",
  "\\begin{tabular}{llccc}",
  "\\hline",
  "Outcome & Sample & IV effect (SE) & p-value & N \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(main_iv_table))) {
  row <- main_iv_table[ii, , drop = FALSE]
  main_iv_lines <- c(
    main_iv_lines,
    paste0(
      row$outcome[[1]], " & ",
      row$sample[[1]], " & ",
      format_num(row$coef_pp[[1]], 3), " (", format_num(row$se_pp[[1]], 3), ") & ",
      format_p(row$p[[1]]), " & ",
      format(row$nobs[[1]], scientific = FALSE), " \\\\"
    )
  )
}

main_iv_lines <- c(
  main_iv_lines,
  "\\hline",
  "\\multicolumn{5}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report percentage-point effects per additional outage hour per household in the quarter. The first two rows use the main mechanism outcome: the share of out-of-labor-force women citing domestic care as the main reason for nonparticipation. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(main_iv_lines, table_main_iv_tex)

rf_dat <- panel_pre %>%
  filter(!is.na(weight_sum), weight_sum > 0, !is.na(lightning_l0), !is.na(female_outlf_domestic_care))

rf_y <- feols(female_outlf_domestic_care ~ 1 | estrato4 + year_quarter, data = rf_dat, weights = ~weight_sum)
rf_x <- feols(lightning_l0 ~ 1 | estrato4 + year_quarter, data = rf_dat, weights = ~weight_sum)

rf_plot <- rf_dat %>%
  mutate(
    resid_y = resid(rf_y) * 100,
    resid_x = resid(rf_x)
  ) %>%
  mutate(bin = ntile(resid_x, 20)) %>%
  group_by(bin) %>%
  summarise(
    resid_x = mean(resid_x, na.rm = TRUE),
    resid_y = weighted_mean_safe(resid_y, weight_sum),
    .groups = "drop"
  )

ggplot(rf_plot, aes(x = resid_x, y = resid_y)) +
  geom_point(size = 2.2, color = "#d95f0e") +
  geom_smooth(method = "lm", se = FALSE, color = "gray40", linewidth = 0.7) +
  labs(
    x = "Residual lightning density",
    y = "Residual domestic-care share (pp)",
    title = "Reduced-form relationship: lightning and domestic-care nonparticipation"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(figure_rf, width = 7.5, height = 5.5, dpi = 300)

hist_dat <- panel %>%
  filter(sample_window %in% c("2016-2019", "2022-2024"), !is.na(female_outlf_domestic_care)) %>%
  mutate(
    share_percent = female_outlf_domestic_care * 100,
    sample_window = factor(sample_window, levels = c("2016-2019", "2022-2024"))
  )

ggplot(hist_dat, aes(x = share_percent)) +
  geom_histogram(binwidth = 2, fill = "#2c7fb8", color = "white", linewidth = 0.2) +
  facet_wrap(~sample_window, ncol = 1) +
  labs(
    x = "Share (%)",
    y = "Estrato-quarter cells",
    title = "Distribution of domestic-care nonparticipation"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(figure_hist, width = 8.5, height = 6.5, dpi = 300)

run_support <- function(df, outcome_var, outcome_label, scale, sample_label) {
  dat <- df %>%
    filter(!is.na(weight_sum), weight_sum > 0, !is.na(outage_l0), !is.na(.data[[outcome_var]]))
  ols <- feols(
    as.formula(paste0(outcome_var, " ~ outage_l0 | estrato4 + year_quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  row <- tidy(ols)
  row <- row[row$term == "outage_l0", , drop = FALSE]
  tibble(
    outcome = outcome_label,
    sample = sample_label,
    effect = row$estimate[[1]] * scale,
    se = row$std.error[[1]] * scale,
    p = row$p.value[[1]],
    nobs = nobs(ols)
  )
}

support_table <- bind_rows(
  run_support(panel_pre, "female_hours_effective_employed", "Female hours among employed (minutes)", 60000, "2016--2019"),
  run_support(panel_recovery, "female_hours_effective_population", "Female hours in population (minutes)", 60000, "2022--2024"),
  run_support(panel_pre, "female_employment", "Female employment (pp)", 100000, "2016--2019"),
  run_support(panel_recovery, "female_employment", "Female employment (pp)", 100000, "2022--2024")
) %>%
  select(outcome, sample, effect, se, p, nobs)

write_csv(support_table, table_support_csv)

support_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Supporting associations with broader labor outcomes}",
  "\\label{tab:lightning_supporting_corr}",
  "\\begin{tabular}{llccc}",
  "\\hline",
  "Outcome & Sample & FE association (SE) & p-value & N \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(support_table))) {
  row <- support_table[ii, , drop = FALSE]
  support_lines <- c(
    support_lines,
    paste0(
      row$outcome[[1]], " & ",
      row$sample[[1]], " & ",
      format_num(row$effect[[1]], 3), " (", format_num(row$se[[1]], 3), ") & ",
      format_p(row$p[[1]]), " & ",
      format(row$nobs[[1]], scientific = FALSE), " \\\\"
    )
  )
}

support_lines <- c(
  support_lines,
  "\\hline",
  "\\multicolumn{5}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report fixed-effect associations between outage hours and broader labor outcomes. These rows are included as supporting evidence only and are not interpreted as clean mechanism outcomes under the exclusion restriction. Hours are reported in minutes per additional outage hour per household; employment is in percentage points.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(support_lines, table_support_tex)

run_domcare_lag <- function(df, outcome_var, outcome_label, lag_name, sample_label) {
  lightning_var <- paste0("lightning_", lag_name)
  outage_var <- paste0("outage_", lag_name)
  dat <- df %>%
    filter(!is.na(weight_sum), weight_sum > 0, !is.na(.data[[lightning_var]]), !is.na(.data[[outage_var]]), !is.na(.data[[outcome_var]]))
  iv <- feols(
    as.formula(paste0(outcome_var, " ~ 1 | estrato4 + year_quarter | ", outage_var, " ~ ", lightning_var)),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  fs <- feols(
    as.formula(paste0(outage_var, " ~ ", lightning_var, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  row <- extract_fit_row(iv)
  tibble(
    outcome = outcome_label,
    sample = sample_label,
    lag = lag_name,
    effect_pp = row$estimate[[1]] * 100000,
    se_pp = row$std.error[[1]] * 100000,
    p = row$p.value[[1]],
    f_stat = safe_num(fitstat(fs, "f")),
    nobs = nobs(iv)
  )
}

domcare_lag_table <- bind_rows(
  run_domcare_lag(panel_pre, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l0", "2016--2019"),
  run_domcare_lag(panel_pre, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l1", "2016--2019"),
  run_domcare_lag(panel_pre, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l2", "2016--2019"),
  run_domcare_lag(panel_pre, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l4", "2016--2019"),
  run_domcare_lag(panel_recovery, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l0", "2022--2024"),
  run_domcare_lag(panel_recovery, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l1", "2022--2024"),
  run_domcare_lag(panel_recovery, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l2", "2022--2024"),
  run_domcare_lag(panel_recovery, "female_outlf_domestic_care", "Domestic care among women out of labor force", "l4", "2022--2024")
) %>%
  mutate(
    lag = recode(lag, l0 = "Current", l1 = "Lag 1", l2 = "Lag 2", l4 = "Lag 4")
  )

write_csv(domcare_lag_table, table_domcare_lag_csv)

lag_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Domestic-care IV estimates across lags}",
  "\\label{tab:lightning_domcare_lags}",
  "\\begin{tabular}{lllcc}",
  "\\hline",
  "Sample & Lag & IV effect (SE) & First-stage F & N \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(domcare_lag_table))) {
  row <- domcare_lag_table[ii, , drop = FALSE]
  lag_lines <- c(
    lag_lines,
    paste0(
      row$sample[[1]], " & ",
      row$lag[[1]], " & ",
      format_num(row$effect_pp[[1]], 3), " (", format_num(row$se_pp[[1]], 3), ") & ",
      format_num(row$f_stat[[1]], 2), " & ",
      format(row$nobs[[1]], scientific = FALSE), " \\\\"
    )
  )
}

lag_lines <- c(
  lag_lines,
  "\\hline",
  "\\multicolumn{5}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report percentage-point IV effects of outages on the main domestic-care outcome across alternative lags. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(lag_lines, table_domcare_lag_tex)

reason_labels <- c(
  "1" = "Had to take care of household chores, children, or another relative",
  "2" = "Was studying",
  "3" = "Health problem or pregnancy",
  "4" = "Was too young or too old to work",
  "5" = "Did not want to work",
  "6" = "Other reason"
)

run_reason_snapshot <- function(file_name, year_label) {
  x <- read_pnadc(
    microdata = file.path(raw_dir, file_name),
    input_txt = input_txt,
    vars = c("Ano", "Trimestre", "V2007", "VD4001", "VD4030", "V4078A", "V4078", "V2009", "V1033")
  )
  dat <- x %>%
    transmute(
      female = V2007 == "2",
      age = as.numeric(V2009),
      weight = as.numeric(V1033),
      out_labor_force = VD4001 == "2",
      reason = if_else(!is.na(VD4030), VD4030, if_else(!is.na(V4078A), V4078A, V4078))
    ) %>%
    filter(female, !is.na(age), age >= 18, age <= 64, out_labor_force, !is.na(weight), weight > 0, !is.na(reason), reason != "") %>%
    group_by(reason) %>%
    summarise(weight = sum(weight, na.rm = TRUE), .groups = "drop") %>%
    mutate(
      snapshot = year_label,
      share_percent = weight / sum(weight) * 100,
      reason_label = unname(reason_labels[reason])
    ) %>%
    arrange(desc(share_percent)) %>%
    select(snapshot, reason, reason_label, share_percent)
  dat
}

reason_snapshot_table <- bind_rows(
  run_reason_snapshot("PNADC_012016.txt", "2016 Q1"),
  run_reason_snapshot("PNADC_012020.txt", "2020 Q1"),
  run_reason_snapshot("PNADC_012024.txt", "2024 Q1")
)

write_csv(reason_snapshot_table, table_reason_csv)

reason_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Benchmark distribution of nonparticipation reasons among working-age women outside the labor force}",
  "\\label{tab:reason_distribution_snapshots}",
  "\\begin{tabular}{llc}",
  "\\hline",
  "Snapshot & Reason & Share (\\%) \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(reason_snapshot_table))) {
  row <- reason_snapshot_table[ii, , drop = FALSE]
  reason_lines <- c(
    reason_lines,
    paste0(
      row$snapshot[[1]], " & ",
      row$reason_label[[1]], " & ",
      format_num(row$share_percent[[1]], 1), " \\\\"
    )
  )
}

reason_lines <- c(
  reason_lines,
  "\\hline",
  "\\multicolumn{3}{p{0.88\\linewidth}}{\\footnotesize Notes: The domestic-care category is the response used to construct the main mechanism outcome. Shares are benchmark snapshots from the first quarter of 2016, 2020, and 2024, among women ages 18--64 outside the labor force.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(reason_lines, table_reason_tex)

paragraph <- paste(
  "The preferred mechanism outcome is the share of working-age women outside the labor force who report domestic chores, children, or another dependent relative as the main reason for nonparticipation.",
  "This variable comes directly from the PNAD Continua nonparticipation-reason question and is the narrowest outcome in the lightning design.",
  "Unlike hours, employment, or labor-force participation, it maps closely to the household time-allocation channel implied by both wood cooking and electricity outages: when home production becomes more time-intensive, more women explicitly report domestic responsibilities as the reason they are not working.",
  "The descriptive evidence also shows that this is a substantively important margin rather than a rare response: among working-age women outside the labor force, domestic care is the single most common reported reason for nonparticipation, accounting for roughly one-half of responses in benchmark PNAD-C snapshots.",
  "For that reason, the domestic-care outcome is the main causal IV endpoint, while broader labor outcomes are presented only as supporting correlations that help flesh out the broader economic story."
)

writeLines(paragraph, paragraph_txt)
writeLines(paste0(paragraph, "\n"), paragraph_tex)

message("Wrote main results to: ", main_dir)
message("Wrote appendix results to: ", appendix_dir)
