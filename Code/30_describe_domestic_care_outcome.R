#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(sf)
  library(tidyr)
  library(scales)
})

panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
shape_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_domestic_care_descriptives", "tables")
fig_dir <- here::here("Figures", "powerIV", "lightning_iv_domestic_care_descriptives")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

summary_out <- here::here(out_dir, "domestic_care_summary.csv")
yearly_out <- here::here(out_dir, "domestic_care_yearly.csv")
quarterly_out <- here::here(out_dir, "domestic_care_quarterly.csv")
map_out <- here::here(out_dir, "female_outlf_domestic_care_map_values.csv")
hist_fig <- here::here(fig_dir, "female_outlf_domestic_care_distribution.png")
time_fig <- here::here(fig_dir, "domestic_care_over_time.png")
map_fig <- here::here(fig_dir, "female_outlf_domestic_care_map.png")

if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(shape_path)) stop("Missing file: ", shape_path, call. = FALSE)

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

panel <- read_parquet(panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    uf = as.character(uf),
    female_pop_weight = as.numeric(female_pop_weight),
    female_lfp = as.numeric(female_lfp),
    female_domestic_care_reason = as.numeric(female_domestic_care_reason),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care)
  ) %>%
  mutate(
    female_outlf_weight = female_pop_weight * pmax(1 - female_lfp, 0),
    sample_window = case_when(
      year >= 2016 & year <= 2019 ~ "2016-2019",
      year >= 2022 & year <= 2024 ~ "2022-2024",
      TRUE ~ "2020-2021"
    )
  )

variables <- tibble::tribble(
  ~variable, ~label, ~weight_var,
  "female_outlf_domestic_care", "Out of labor force for domestic care", "female_outlf_weight",
  "female_domestic_care_reason", "Domestic care reason", "female_pop_weight"
)

summary_rows <- list()
idx_summary <- 1L

for (ii in seq_len(nrow(variables))) {
  variable <- variables$variable[[ii]]
  label <- variables$label[[ii]]
  weight_var <- variables$weight_var[[ii]]

  for (window_name in c("All 2016-2024", "2016-2019", "2020-2021", "2022-2024")) {
    dat <- if (window_name == "All 2016-2024") {
      panel
    } else {
      panel %>% filter(sample_window == window_name)
    }

    x <- dat[[variable]]
    w <- dat[[weight_var]]
    q <- weighted_quantile_safe(x, w, c(0.1, 0.25, 0.5, 0.75, 0.9))

    summary_rows[[idx_summary]] <- tibble(
      variable = variable,
      label = label,
      sample_window = window_name,
      weighted_mean_pp = weighted_mean_safe(x, w) * 100,
      weighted_p10_pp = q[[1]] * 100,
      weighted_p25_pp = q[[2]] * 100,
      weighted_median_pp = q[[3]] * 100,
      weighted_p75_pp = q[[4]] * 100,
      weighted_p90_pp = q[[5]] * 100,
      cell_mean_pp = mean(x, na.rm = TRUE) * 100,
      cell_median_pp = median(x, na.rm = TRUE) * 100,
      cell_sd_pp = sd(x, na.rm = TRUE) * 100,
      cell_min_pp = min(x, na.rm = TRUE) * 100,
      cell_max_pp = max(x, na.rm = TRUE) * 100,
      share_zero_cells = mean(x == 0, na.rm = TRUE),
      n_cells = sum(!is.na(x))
    )
    idx_summary <- idx_summary + 1L
  }
}

summary_results <- bind_rows(summary_rows)
write_csv(summary_results, summary_out)

yearly_rows <- list()
idx_yearly <- 1L

for (ii in seq_len(nrow(variables))) {
  variable <- variables$variable[[ii]]
  label <- variables$label[[ii]]
  weight_var <- variables$weight_var[[ii]]

  yearly_rows[[idx_yearly]] <- panel %>%
    group_by(year) %>%
    summarise(
      variable = variable,
      label = label,
      weighted_mean_pp = weighted_mean_safe(.data[[variable]], .data[[weight_var]]) * 100,
      cell_median_pp = median(.data[[variable]], na.rm = TRUE) * 100,
      cell_p25_pp = quantile(.data[[variable]], 0.25, na.rm = TRUE),
      cell_p75_pp = quantile(.data[[variable]], 0.75, na.rm = TRUE),
      share_zero_cells = mean(.data[[variable]] == 0, na.rm = TRUE),
      n_cells = sum(!is.na(.data[[variable]])),
      .groups = "drop"
    ) %>%
    mutate(
      cell_p25_pp = cell_p25_pp * 100,
      cell_p75_pp = cell_p75_pp * 100
    )
  idx_yearly <- idx_yearly + 1L
}

yearly_results <- bind_rows(yearly_rows) %>%
  arrange(variable, year)
write_csv(yearly_results, yearly_out)

quarterly_rows <- list()
idx_quarterly <- 1L

for (ii in seq_len(nrow(variables))) {
  variable <- variables$variable[[ii]]
  label <- variables$label[[ii]]
  weight_var <- variables$weight_var[[ii]]

  quarterly_rows[[idx_quarterly]] <- panel %>%
    group_by(year, quarter, year_quarter) %>%
    summarise(
      variable = variable,
      label = label,
      weighted_mean_pp = weighted_mean_safe(.data[[variable]], .data[[weight_var]]) * 100,
      .groups = "drop"
    )
  idx_quarterly <- idx_quarterly + 1L
}

quarterly_results <- bind_rows(quarterly_rows) %>%
  arrange(variable, year, quarter) %>%
  mutate(date = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1) * 3 + 1)))
write_csv(quarterly_results, quarterly_out)

plot_hist <- panel %>%
  filter(!is.na(female_outlf_domestic_care), sample_window %in% c("2016-2019", "2022-2024")) %>%
  mutate(
    female_outlf_domestic_care_pp = female_outlf_domestic_care * 100,
    sample_window = factor(sample_window, levels = c("2016-2019", "2022-2024"))
  )

ggplot(plot_hist, aes(x = female_outlf_domestic_care_pp)) +
  geom_histogram(binwidth = 2, fill = "#2c7fb8", color = "white", linewidth = 0.2) +
  facet_wrap(~sample_window, ncol = 1) +
  labs(
    x = "Share of women out of labor force citing domestic care (pp)",
    y = "Estrato-quarter cells",
    title = "Distribution of female domestic-burden nonparticipation"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(hist_fig, width = 9, height = 7, dpi = 300)

plot_time <- quarterly_results %>%
  mutate(
    series = factor(label, levels = c("Out of labor force for domestic care", "Domestic care reason"))
  )

ggplot(plot_time, aes(x = date, y = weighted_mean_pp, color = series, group = series)) +
  geom_line(linewidth = 0.8) +
  geom_vline(
    xintercept = c(
      as.Date("2020-01-01"),
      as.Date("2022-01-01")
    ),
    linetype = "dashed",
    color = "gray50"
  ) +
  scale_color_manual(values = c("#d95f0e", "#2b8cbe")) +
  labs(
    x = "Quarter",
    y = "National weighted mean (pp)",
    color = "",
    title = "Domestic-burden outcomes over time"
  ) +
  scale_x_date(date_breaks = "1 year", date_labels = "%YQ1") +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(time_fig, width = 10, height = 5.5, dpi = 300)

estrato_map <- st_read(shape_path, quiet = TRUE) %>%
  st_make_valid()

map_values <- panel %>%
  group_by(estrato4) %>%
  summarise(
    overall_pp = weighted_mean_safe(female_outlf_domestic_care, female_outlf_weight) * 100,
    pre_pp = weighted_mean_safe(female_outlf_domestic_care[year >= 2016 & year <= 2019], female_outlf_weight[year >= 2016 & year <= 2019]) * 100,
    recovery_pp = weighted_mean_safe(female_outlf_domestic_care[year >= 2022 & year <= 2024], female_outlf_weight[year >= 2022 & year <= 2024]) * 100,
    .groups = "drop"
  )

write_csv(map_values, map_out)

map_plot_data <- estrato_map %>%
  left_join(map_values, by = "estrato4") %>%
  pivot_longer(
    cols = c("overall_pp", "pre_pp", "recovery_pp"),
    names_to = "period",
    values_to = "value_pp"
  ) %>%
  mutate(
    period = recode(
      period,
      overall_pp = "Overall 2016-2024",
      pre_pp = "Pre-pandemic 2016-2019",
      recovery_pp = "Post-resumption 2022-2024"
    )
  )

ggplot(map_plot_data) +
  geom_sf(aes(fill = value_pp), color = NA) +
  facet_wrap(~period, ncol = 1) +
  scale_fill_viridis_c(option = "C", direction = -1, na.value = "gray90", labels = label_number(suffix = " pp")) +
  labs(
    fill = "",
    title = "Female domestic-burden nonparticipation across Brazil"
  ) +
  theme_void(base_size = 12) +
  theme(
    strip.text = element_text(size = 11),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    legend.background = element_rect(fill = "white", color = NA)
  )

ggsave(map_fig, width = 8, height = 12, dpi = 300)

message("Wrote: ", summary_out)
message("Wrote: ", yearly_out)
message("Wrote: ", quarterly_out)
message("Wrote: ", map_out)
message("Wrote: ", hist_fig)
message("Wrote: ", time_fig)
message("Wrote: ", map_fig)
print(summary_results)
