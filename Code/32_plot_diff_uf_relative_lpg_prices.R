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
out_png <- here::here("Figures", "powerIV", "lpg", "diff_uf_relative_to_national_lpg_price_full_period.png")
out_pdf <- here::here("Figures", "powerIV", "lpg", "diff_uf_relative_to_national_lpg_price_full_period.pdf")
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
    quarter_index = year * 4L + quarter,
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly
  ) %>%
  arrange(uf_sigla, quarter_index) %>%
  group_by(uf_sigla) %>%
  mutate(
    d_rel_log_lpg_price_uf = if_else(
      dplyr::lag(quarter_index) == quarter_index - 1L,
      rel_log_lpg_price_uf - dplyr::lag(rel_log_lpg_price_uf),
      NA_real_
    )
  ) %>%
  ungroup() %>%
  filter(!is.na(d_rel_log_lpg_price_uf))

uf_levels <- sort(unique(plot_dat$uf_sigla))
uf_palette <- setNames(grDevices::hcl.colors(length(uf_levels), palette = "Dynamic"), uf_levels)

p <- ggplot(plot_dat, aes(x = quarter_date, y = d_rel_log_lpg_price_uf, color = uf_sigla, group = uf_sigla)) +
  geom_hline(yintercept = 0, linewidth = 0.5, color = "#9a3412", linetype = "dashed") +
  geom_line(alpha = 0.9, linewidth = 0.55) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  scale_color_manual(values = uf_palette, name = "UF") +
  labs(
    title = "Quarter-to-Quarter Changes in UF LPG Prices Relative to the National Level",
    subtitle = "First difference of log UF LPG price minus log national LPG price",
    x = NULL,
    y = expression(Delta*" [log UF price - log national price]"),
    caption = "Source: ANP retail LPG price surveys. Each line is a UF-quarter series."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#475569"),
    axis.title.y = element_text(face = "bold"),
    plot.caption = element_text(color = "#64748b"),
    legend.position = "right",
    legend.title = element_text(face = "bold"),
    legend.key.height = unit(0.32, "cm")
  )

ggsave(out_png, p, width = 11.8, height = 7.0, dpi = 320)
ggsave(out_pdf, p, width = 11.8, height = 7.0)

cat(out_png, "\n")
cat(out_pdf, "\n")
