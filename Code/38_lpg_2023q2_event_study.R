suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(broom)
  library(scales)
})

out_dir <- here("data", "powerIV", "regressions", "lpg_2023q2_event_study")
fig_dir <- here("Figures", "powerIV", "lpg")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_path <- here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
baseline_path <- here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

lfp <- read_parquet(lfp_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    weight_sum,
    female_hours_effective_employed,
    female_hours_effective_population
  )

power <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_cooking_wood_charcoal,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

baseline_shift <- read_parquet(baseline_path) %>%
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
    reform_q = 2023 * 4 + 2,
    event_time_q = quarter_index - reform_q,
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01"))
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  filter(!is.na(share_cooking_wood_charcoal), !is.na(female_hours_effective_employed), !is.na(baseline_wood_2016))

panel_m2 <- panel %>%
  filter(!is.na(mean_rooms), !is.na(mean_bedrooms), !is.na(b_dep_t), !is.na(share_grid_full_time))

panel_bins <- panel_m2 %>%
  mutate(baseline_wood_bin = if_else(baseline_wood_2016 >= median(baseline_wood_2016, na.rm = TRUE), "High baseline wood", "Low baseline wood"))

binned_fs <- panel_bins %>%
  group_by(quarter_date, baseline_wood_bin) %>%
  summarise(
    wood_share = weighted.mean(share_cooking_wood_charcoal, weight_sum, na.rm = TRUE),
    .groups = "drop"
  )

binned_rf <- panel_bins %>%
  group_by(quarter_date, baseline_wood_bin) %>%
  summarise(
    female_hours_effective_employed = weighted.mean(female_hours_effective_employed, weight_sum, na.rm = TRUE),
    .groups = "drop"
  )

fs_plot <- ggplot(binned_fs, aes(x = quarter_date, y = wood_share, color = baseline_wood_bin)) +
  geom_vline(xintercept = as.Date("2023-04-01"), linetype = "dashed", color = "black") +
  geom_line(linewidth = 1) +
  scale_x_date(date_breaks = "6 months", date_labels = "%YQ%q") +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  labs(
    title = "Wood Share by Baseline Exposure Around the 2023Q2 Reform",
    subtitle = "Weighted estrato-quarter means, split at the median 2016 wood share",
    x = NULL,
    y = "Wood/charcoal cooking share",
    color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank(),
    legend.position = "top"
  )

rf_plot <- ggplot(binned_rf, aes(x = quarter_date, y = female_hours_effective_employed, color = baseline_wood_bin)) +
  geom_vline(xintercept = as.Date("2023-04-01"), linetype = "dashed", color = "black") +
  geom_line(linewidth = 1) +
  scale_x_date(date_breaks = "6 months", date_labels = "%YQ%q") +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  labs(
    title = "Female Employed Hours by Baseline Exposure Around the 2023Q2 Reform",
    subtitle = "Weighted estrato-quarter means, split at the median 2016 wood share",
    x = NULL,
    y = "Female effective weekly hours (employed)",
    color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank(),
    legend.position = "top"
  )

fs_model <- feols(
  share_cooking_wood_charcoal ~ i(event_time_q, baseline_wood_2016, ref = -1) + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | estrato4 + year_quarter,
  data = panel_m2,
  weights = ~weight_sum,
  cluster = ~estrato4
)

rf_model <- feols(
  female_hours_effective_employed ~ i(event_time_q, baseline_wood_2016, ref = -1) + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | estrato4 + year_quarter,
  data = panel_m2,
  weights = ~weight_sum,
  cluster = ~estrato4
)

fs_terms <- tidy(fs_model) %>%
  filter(grepl("event_time_q::", term)) %>%
  mutate(event_time_q = as.integer(sub(".*event_time_q::(-?[0-9]+):.*", "\\1", term))) %>%
  arrange(event_time_q)

rf_terms <- tidy(rf_model) %>%
  filter(grepl("event_time_q::", term)) %>%
  mutate(event_time_q = as.integer(sub(".*event_time_q::(-?[0-9]+):.*", "\\1", term))) %>%
  arrange(event_time_q)

fs_pre_terms <- fs_terms %>% filter(event_time_q < -1) %>% pull(term)
rf_pre_terms <- rf_terms %>% filter(event_time_q < -1) %>% pull(term)

fs_pretrend <- if (length(fs_pre_terms) > 0) wald(fs_model, fs_pre_terms) else NULL
rf_pretrend <- if (length(rf_pre_terms) > 0) wald(rf_model, rf_pre_terms) else NULL

fs_es_plot <- ggplot(fs_terms, aes(x = event_time_q, y = estimate)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray40") +
  geom_vline(xintercept = -0.5, linetype = "dotted", color = "firebrick") +
  geom_point(size = 2.2, color = "#1f78b4") +
  geom_errorbar(aes(ymin = estimate - 1.96 * std.error, ymax = estimate + 1.96 * std.error), width = 0.15, color = "#1f78b4") +
  scale_x_continuous(breaks = sort(unique(fs_terms$event_time_q))) +
  labs(
    title = "Event Study: First Stage Around 2023Q2",
    subtitle = "Interaction of quarter-relative-to-reform with baseline 2016 wood share; omitted quarter is 2023Q1",
    x = "Quarters relative to 2023Q2",
    y = "Coefficient on baseline wood share"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank()
  )

rf_es_plot <- ggplot(rf_terms, aes(x = event_time_q, y = estimate)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray40") +
  geom_vline(xintercept = -0.5, linetype = "dotted", color = "firebrick") +
  geom_point(size = 2.2, color = "#33a02c") +
  geom_errorbar(aes(ymin = estimate - 1.96 * std.error, ymax = estimate + 1.96 * std.error), width = 0.15, color = "#33a02c") +
  scale_x_continuous(breaks = sort(unique(rf_terms$event_time_q))) +
  labs(
    title = "Event Study: Reduced Form Around 2023Q2",
    subtitle = "Outcome is female effective weekly hours among employed women; omitted quarter is 2023Q1",
    x = "Quarters relative to 2023Q2",
    y = "Coefficient on baseline wood share"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank()
  )

fs_binned_path <- here("Figures", "powerIV", "lpg", "binned_first_stage_2023q2_reform.png")
rf_binned_path <- here("Figures", "powerIV", "lpg", "binned_reduced_form_2023q2_reform.png")
fs_es_path <- here("Figures", "powerIV", "lpg", "event_study_first_stage_2023q2_reform.png")
rf_es_path <- here("Figures", "powerIV", "lpg", "event_study_reduced_form_2023q2_reform.png")
summary_path <- here("data", "powerIV", "regressions", "lpg_2023q2_event_study", "event_study_summary.tsv")
terms_path <- here("data", "powerIV", "regressions", "lpg_2023q2_event_study", "event_study_terms.tsv")

ggsave(fs_binned_path, fs_plot, width = 12, height = 7, dpi = 300)
ggsave(rf_binned_path, rf_plot, width = 12, height = 7, dpi = 300)
ggsave(fs_es_path, fs_es_plot, width = 12, height = 7, dpi = 300)
ggsave(rf_es_path, rf_es_plot, width = 12, height = 7, dpi = 300)

summary_df <- tibble(
  model = c("first_stage", "reduced_form"),
  n = c(nobs(fs_model), nobs(rf_model)),
  pretrend_stat = c(as.numeric(fs_pretrend$stat), as.numeric(rf_pretrend$stat)),
  pretrend_p = c(as.numeric(fs_pretrend$p), as.numeric(rf_pretrend$p))
)

terms_df <- bind_rows(
  fs_terms %>% mutate(model = "first_stage"),
  rf_terms %>% mutate(model = "reduced_form")
)

write.table(summary_df, summary_path, sep = "\t", row.names = FALSE, quote = FALSE)
write.table(terms_df, terms_path, sep = "\t", row.names = FALSE, quote = FALSE)

cat(fs_binned_path, "\n")
cat(rf_binned_path, "\n")
cat(fs_es_path, "\n")
cat(rf_es_path, "\n")
print(summary_df)
