suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(broom)
  library(scales)
})

out_dir <- here("data", "powerIV", "regressions", "lpg_2023_reform_spec_sweep")
fig_dir <- here("Figures", "powerIV", "lpg")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

lfp <- read_parquet(here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    weight_sum,
    female_hours_effective_employed
  )

power <- read_parquet(here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_cooking_wood_charcoal,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

baseline_shift <- read_parquet(here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency
  )

baseline_wood <- power %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    quarter_index = year * 4 + quarter,
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01"))
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  filter(!is.na(share_cooking_wood_charcoal), !is.na(female_hours_effective_employed), !is.na(baseline_wood_2016)) %>%
  filter(!is.na(mean_rooms), !is.na(mean_bedrooms), !is.na(b_dep_t), !is.na(share_grid_full_time))

windows <- tibble::tribble(
  ~window_name, ~start_q,
  "2021Q1_2024Q4", 2021 * 4 + 1,
  "2022Q1_2024Q4", 2022 * 4 + 1,
  "2022Q3_2024Q4", 2022 * 4 + 3
)

reforms <- tibble::tribble(
  ~reform_name, ~reform_q,
  "2023Q2", 2023 * 4 + 2,
  "2023Q3", 2023 * 4 + 3
)

run_event_study <- function(data, outcome, reform_q) {
  data %>%
    mutate(event_time_q = quarter_index - reform_q) %>%
    feols(
      as.formula(paste0(outcome, " ~ i(event_time_q, baseline_wood_2016, ref = -1) + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | estrato4 + year_quarter")),
      data = ., weights = ~weight_sum, cluster = ~estrato4
    )
}

extract_terms <- function(model) {
  tidy(model) %>%
    filter(grepl("event_time_q::", term)) %>%
    mutate(event_time_q = as.integer(sub(".*event_time_q::(-?[0-9]+):.*", "\\1", term))) %>%
    arrange(event_time_q)
}

pretrend_stats <- function(model) {
  terms <- extract_terms(model)
  pre_terms <- terms %>% filter(event_time_q < -1) %>% pull(term)
  if (length(pre_terms) == 0) return(tibble(stat = NA_real_, p = NA_real_))
  wt <- wald(model, pre_terms)
  tibble(stat = as.numeric(wt$stat), p = as.numeric(wt$p))
}

results <- list()

for (i in seq_len(nrow(windows))) {
  wname <- windows$window_name[[i]]
  start_q <- windows$start_q[[i]]
  window_data <- panel %>% filter(quarter_index >= start_q, quarter_index <= 2024 * 4 + 4)

  for (j in seq_len(nrow(reforms))) {
    rname <- reforms$reform_name[[j]]
    reform_q <- reforms$reform_q[[j]]

    fs_model <- run_event_study(window_data, "share_cooking_wood_charcoal", reform_q)
    rf_model <- run_event_study(window_data, "female_hours_effective_employed", reform_q)

    fs_pre <- pretrend_stats(fs_model)
    rf_pre <- pretrend_stats(rf_model)

    results[[length(results) + 1]] <- tibble(
      window = wname,
      reform = rname,
      n = nobs(fs_model),
      fs_pretrend_stat = fs_pre$stat,
      fs_pretrend_p = fs_pre$p,
      rf_pretrend_stat = rf_pre$stat,
      rf_pretrend_p = rf_pre$p
    )
  }
}

results_df <- bind_rows(results) %>% arrange(fs_pretrend_p, rf_pretrend_p)
write.table(results_df, here("data", "powerIV", "regressions", "lpg_2023_reform_spec_sweep", "spec_sweep_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

best_window <- results_df$window[[1]]
best_reform <- results_df$reform[[1]]
best_reform_q <- reforms$reform_q[match(best_reform, reforms$reform_name)]
best_data <- panel %>% filter(quarter_index >= windows$start_q[match(best_window, windows$window_name)], quarter_index <= 2024 * 4 + 4) %>% mutate(event_time_q = quarter_index - best_reform_q)

best_bins <- best_data %>%
  mutate(baseline_wood_quartile = paste0("Q", ntile(baseline_wood_2016, 4)))

quartile_fs <- best_bins %>%
  group_by(quarter_date, baseline_wood_quartile) %>%
  summarise(wood_share = weighted.mean(share_cooking_wood_charcoal, weight_sum, na.rm = TRUE), .groups = "drop")

quartile_rf <- best_bins %>%
  group_by(quarter_date, baseline_wood_quartile) %>%
  summarise(female_hours_effective_employed = weighted.mean(female_hours_effective_employed, weight_sum, na.rm = TRUE), .groups = "drop")

reform_date <- as.Date(if (best_reform == "2023Q2") "2023-04-01" else "2023-07-01")

fs_plot <- ggplot(quartile_fs, aes(x = quarter_date, y = wood_share, color = baseline_wood_quartile)) +
  geom_vline(xintercept = reform_date, linetype = "dashed", color = "black") +
  geom_line(linewidth = 1) +
  scale_x_date(date_breaks = "6 months", date_labels = "%YQ%q") +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  labs(
    title = paste0("Wood Share by Baseline-Wood Quartile Around ", best_reform),
    subtitle = paste0("Best pretrend window from spec sweep: ", best_window),
    x = NULL,
    y = "Wood/charcoal cooking share",
    color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(face = "bold", size = 18), plot.subtitle = element_text(size = 12, color = "gray30"), panel.grid.minor = element_blank(), legend.position = "top")

rf_plot <- ggplot(quartile_rf, aes(x = quarter_date, y = female_hours_effective_employed, color = baseline_wood_quartile)) +
  geom_vline(xintercept = reform_date, linetype = "dashed", color = "black") +
  geom_line(linewidth = 1) +
  scale_x_date(date_breaks = "6 months", date_labels = "%YQ%q") +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  labs(
    title = paste0("Female Employed Hours by Baseline-Wood Quartile Around ", best_reform),
    subtitle = paste0("Best pretrend window from spec sweep: ", best_window),
    x = NULL,
    y = "Female effective weekly hours (employed)",
    color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(face = "bold", size = 18), plot.subtitle = element_text(size = 12, color = "gray30"), panel.grid.minor = element_blank(), legend.position = "top")

fs_plot_path <- here("Figures", "powerIV", "lpg", "quartile_first_stage_best_reform_spec.png")
rf_plot_path <- here("Figures", "powerIV", "lpg", "quartile_reduced_form_best_reform_spec.png")
ggsave(fs_plot_path, fs_plot, width = 12, height = 7, dpi = 300)
ggsave(rf_plot_path, rf_plot, width = 12, height = 7, dpi = 300)

cat(fs_plot_path, "\n")
cat(rf_plot_path, "\n")
print(results_df)
cat("primary_wood_available_in_aggregate_panel:", any(grepl("main|primary", names(read_parquet(here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")), ignore.case = TRUE))), "\n")
