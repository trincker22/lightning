#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(scales)
})

uf_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet")
nat_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_national_2016_2024.parquet")
out_dir <- here::here("Figures", "powerIV", "lpg")
dispersion_png <- here::here("Figures", "powerIV", "lpg", "uf_relative_price_dispersion_over_time.png")
dispersion_pdf <- here::here("Figures", "powerIV", "lpg", "uf_relative_price_dispersion_over_time.pdf")
prepost_png <- here::here("Figures", "powerIV", "lpg", "uf_relative_price_pre_vs_post_2023.png")
prepost_pdf <- here::here("Figures", "powerIV", "lpg", "uf_relative_price_pre_vs_post_2023.pdf")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

uf <- read_parquet(uf_path) %>%
  transmute(
    uf_sigla,
    year,
    quarter,
    log_lpg_price_quarterly_uf
  )

nat <- read_parquet(nat_path) %>%
  transmute(
    year,
    quarter,
    log_lpg_price_quarterly
  )

plot_dat <- uf %>%
  left_join(nat, by = c("year", "quarter")) %>%
  mutate(
    quarter_date = as.Date(sprintf("%d-%02d-01", year, (quarter - 1L) * 3L + 1L)),
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly,
    post_2023 = year >= 2023
  ) %>%
  filter(!is.na(rel_log_lpg_price_uf))

dispersion_dat <- plot_dat %>%
  group_by(quarter_date) %>%
  summarise(
    q10 = quantile(rel_log_lpg_price_uf, 0.10, na.rm = TRUE),
    q25 = quantile(rel_log_lpg_price_uf, 0.25, na.rm = TRUE),
    q50 = quantile(rel_log_lpg_price_uf, 0.50, na.rm = TRUE),
    q75 = quantile(rel_log_lpg_price_uf, 0.75, na.rm = TRUE),
    q90 = quantile(rel_log_lpg_price_uf, 0.90, na.rm = TRUE),
    .groups = "drop"
  )

p_dispersion <- ggplot(dispersion_dat, aes(x = quarter_date)) +
  geom_hline(yintercept = 0, linewidth = 0.5, color = "#9a3412", linetype = "dashed") +
  geom_ribbon(aes(ymin = q10, ymax = q90), fill = "#cbd5e1", alpha = 0.55) +
  geom_ribbon(aes(ymin = q25, ymax = q75), fill = "#3b82f6", alpha = 0.35) +
  geom_line(aes(y = q50), color = "#1d4ed8", linewidth = 0.9) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  labs(
    title = "Cross-UF Dispersion in LPG Prices Relative to the National Level",
    subtitle = "Median, interquartile band, and 10th-90th percentile band of log UF price minus log national price",
    x = NULL,
    y = "Log UF price - log national price",
    caption = "Source: ANP retail LPG price surveys. Each quarter summarizes the cross-UF distribution."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#475569"),
    axis.title.y = element_text(face = "bold"),
    plot.caption = element_text(color = "#64748b")
  )

prepost_dat <- plot_dat %>%
  mutate(period = if_else(post_2023, "Post-2023", "Pre-2023")) %>%
  group_by(uf_sigla, period) %>%
  summarise(avg_rel_log = mean(rel_log_lpg_price_uf, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = period, values_from = avg_rel_log) %>%
  filter(!is.na(`Pre-2023`), !is.na(`Post-2023`))

line_bounds <- range(c(prepost_dat$`Pre-2023`, prepost_dat$`Post-2023`), na.rm = TRUE)

p_prepost <- ggplot(prepost_dat, aes(x = `Pre-2023`, y = `Post-2023`)) +
  geom_abline(intercept = 0, slope = 1, color = "#9a3412", linetype = "dashed", linewidth = 0.6) +
  geom_point(color = "#1d4ed8", size = 2.2, alpha = 0.9) +
  geom_text(aes(label = uf_sigla), nudge_y = 0.002, size = 3.1, check_overlap = TRUE, color = "#0f172a") +
  coord_equal(xlim = line_bounds, ylim = line_bounds) +
  scale_x_continuous(labels = label_number(accuracy = 0.01)) +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  labs(
    title = "UF Relative LPG Prices Before vs After 2023",
    subtitle = "Each point is a UF. Values are average log UF price minus log national price.",
    x = "Average relative price before 2023",
    y = "Average relative price from 2023 onward",
    caption = "Source: ANP retail LPG price surveys. Points near the 45-degree line indicate persistent rank ordering."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#475569"),
    axis.title = element_text(face = "bold"),
    plot.caption = element_text(color = "#64748b")
  )

ggsave(dispersion_png, p_dispersion, width = 10.8, height = 6.2, dpi = 320)
ggsave(dispersion_pdf, p_dispersion, width = 10.8, height = 6.2)
ggsave(prepost_png, p_prepost, width = 8.2, height = 7.0, dpi = 320)
ggsave(prepost_pdf, p_prepost, width = 8.2, height = 7.0)

cat(dispersion_png, "\n")
cat(dispersion_pdf, "\n")
cat(prepost_png, "\n")
cat(prepost_pdf, "\n")
