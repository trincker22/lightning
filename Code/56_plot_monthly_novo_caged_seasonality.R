suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(tidyr)
})

fig_dir <- here("Figures", "powerIV", "auxilio_gas")
out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_monthly_novo_caged_event_study")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

novo <- read_parquet(here("data", "powerIV", "novo_caged", "novo_caged_estrato_month.parquet")) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter = ((month - 1) %/% 3) + 1,
    event_time_m = (year - 2022) * 12 + (month - 8)
  )

auxgas <- read_parquet(here("data", "powerIV", "monthly_source_audit", "auxgas_estrato_month_2021_2025.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    month,
    quarter = ((month - 1) %/% 3) + 1,
    auxgas_people,
    auxgas_families,
    auxgas_total_value
  )

visit1 <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    weight_sum
  )

panel <- novo %>%
  left_join(visit1, by = c("estrato4", "year", "quarter")) %>%
  left_join(auxgas, by = c("estrato4", "year", "month", "quarter")) %>%
  mutate(
    date = as.Date(sprintf("%04d-%02d-01", year, month)),
    female_net_per_weighted_household = female_net / weight_sum,
    female_admissions_per_weighted_household = female_admissions / weight_sum,
    female_separations_per_weighted_household = female_separations / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people / weight_sum
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  filter(date >= as.Date("2021-12-01"), date <= as.Date("2023-10-01"))

pre_exposure <- panel %>%
  filter((year == 2021 & month == 12) | (year == 2022 & month %in% c(2, 4, 6))) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_exposure = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(pre_exposure_quartile = dplyr::ntile(pre_people_exposure, 4))

panel <- panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  filter(!is.na(pre_exposure_quartile))

quartile_labels <- c(
  `1` = "Q1 Lowest Exposure",
  `2` = "Q2",
  `3` = "Q3",
  `4` = "Q4 Highest Exposure"
)

monthly_series <- panel %>%
  group_by(date, pre_exposure_quartile) %>%
  summarise(
    female_net_per_weighted_household = weighted.mean(female_net_per_weighted_household, weight_sum, na.rm = TRUE),
    female_admissions_per_weighted_household = weighted.mean(female_admissions_per_weighted_household, weight_sum, na.rm = TRUE),
    female_separations_per_weighted_household = weighted.mean(female_separations_per_weighted_household, weight_sum, na.rm = TRUE),
    n_estratos = n(),
    .groups = "drop"
  ) %>%
  mutate(pre_exposure_quartile = factor(pre_exposure_quartile, levels = 1:4, labels = quartile_labels))

seasonality_series <- panel %>%
  group_by(month, pre_exposure_quartile) %>%
  summarise(
    female_net_per_weighted_household = weighted.mean(female_net_per_weighted_household, weight_sum, na.rm = TRUE),
    female_admissions_per_weighted_household = weighted.mean(female_admissions_per_weighted_household, weight_sum, na.rm = TRUE),
    female_separations_per_weighted_household = weighted.mean(female_separations_per_weighted_household, weight_sum, na.rm = TRUE),
    n_estrato_months = n(),
    .groups = "drop"
  ) %>%
  mutate(pre_exposure_quartile = factor(pre_exposure_quartile, levels = 1:4, labels = quartile_labels))

write.table(monthly_series, file.path(out_dir, "monthly_novo_caged_quartile_series.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(seasonality_series, file.path(out_dir, "monthly_novo_caged_seasonality_series.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_time_net <- ggplot(monthly_series, aes(x = date, y = female_net_per_weighted_household, color = pre_exposure_quartile)) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date("2022-08-01"), linetype = "dashed", color = "gray40") +
  labs(
    x = NULL,
    y = "Female Net Formal Employment per Weighted Household",
    color = NULL,
    title = "Monthly Female Net Formal Employment by Auxilio Gas Exposure Quartile",
    subtitle = "Quartiles are based on pre-top-up Auxilio Gas people per weighted household"
  ) +
  theme_minimal(base_size = 12)

plot_time_adm <- ggplot(monthly_series, aes(x = date, y = female_admissions_per_weighted_household, color = pre_exposure_quartile)) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date("2022-08-01"), linetype = "dashed", color = "gray40") +
  labs(
    x = NULL,
    y = "Female Formal Admissions per Weighted Household",
    color = NULL,
    title = "Monthly Female Formal Admissions by Auxilio Gas Exposure Quartile",
    subtitle = "Quartiles are based on pre-top-up Auxilio Gas people per weighted household"
  ) +
  theme_minimal(base_size = 12)

plot_season_net <- ggplot(seasonality_series, aes(x = month, y = female_net_per_weighted_household, color = pre_exposure_quartile, group = pre_exposure_quartile)) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 2) +
  scale_x_continuous(breaks = 1:12) +
  labs(
    x = "Calendar Month",
    y = "Average Female Net Formal Employment per Weighted Household",
    color = NULL,
    title = "Seasonality in Female Net Formal Employment by Exposure Quartile",
    subtitle = "Averages pool all available years within each calendar month"
  ) +
  theme_minimal(base_size = 12)

plot_season_adm <- ggplot(seasonality_series, aes(x = month, y = female_admissions_per_weighted_household, color = pre_exposure_quartile, group = pre_exposure_quartile)) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 2) +
  scale_x_continuous(breaks = 1:12) +
  labs(
    x = "Calendar Month",
    y = "Average Female Formal Admissions per Weighted Household",
    color = NULL,
    title = "Seasonality in Female Formal Admissions by Exposure Quartile",
    subtitle = "Averages pool all available years within each calendar month"
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(fig_dir, "monthly_novo_caged_female_net_by_exposure_quartile.png"), plot_time_net, width = 10, height = 5.75, dpi = 300)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_net_by_exposure_quartile.pdf"), plot_time_net, width = 10, height = 5.75)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_admissions_by_exposure_quartile.png"), plot_time_adm, width = 10, height = 5.75, dpi = 300)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_admissions_by_exposure_quartile.pdf"), plot_time_adm, width = 10, height = 5.75)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_net_seasonality_by_exposure_quartile.png"), plot_season_net, width = 10, height = 5.75, dpi = 300)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_net_seasonality_by_exposure_quartile.pdf"), plot_season_net, width = 10, height = 5.75)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_admissions_seasonality_by_exposure_quartile.png"), plot_season_adm, width = 10, height = 5.75, dpi = 300)
ggsave(file.path(fig_dir, "monthly_novo_caged_female_admissions_seasonality_by_exposure_quartile.pdf"), plot_season_adm, width = 10, height = 5.75)

print(monthly_series)
print(seasonality_series)
