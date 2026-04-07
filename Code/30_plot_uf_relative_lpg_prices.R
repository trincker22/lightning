#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(scales)
})

uf_path <- here::here('data', 'powerIV', 'prices', 'anp_lpg_quarter_uf_2016_2024.parquet')
nat_path <- here::here('data', 'powerIV', 'prices', 'anp_lpg_quarter_national_2016_2024.parquet')
out_dir <- here::here('Figures', 'powerIV', 'lpg')
out_png <- here::here('Figures', 'powerIV', 'lpg', 'uf_relative_to_national_lpg_price_full_period.png')
out_pdf <- here::here('Figures', 'powerIV', 'lpg', 'uf_relative_to_national_lpg_price_full_period.pdf')
out_levels_png <- here::here('Figures', 'powerIV', 'lpg', 'uf_minus_national_lpg_price_levels_full_period.png')
out_levels_pdf <- here::here('Figures', 'powerIV', 'lpg', 'uf_minus_national_lpg_price_levels_full_period.pdf')
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

uf <- read_parquet(uf_path) %>%
  transmute(
    uf_sigla,
    year,
    quarter,
    lpg_price_quarterly_uf,
    log_lpg_price_quarterly_uf
  )

nat <- read_parquet(nat_path) %>%
  transmute(
    year,
    quarter,
    lpg_price_quarterly,
    log_lpg_price_quarterly
  )

plot_dat <- uf %>%
  left_join(nat, by = c('year', 'quarter')) %>%
  mutate(
    quarter_date = as.Date(sprintf('%d-%02d-01', year, (quarter - 1L) * 3L + 1L)),
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly,
    rel_pct_lpg_price_uf = exp(rel_log_lpg_price_uf) - 1,
    rel_level_lpg_price_uf = lpg_price_quarterly_uf - lpg_price_quarterly
  ) %>%
  filter(!is.na(rel_log_lpg_price_uf))

uf_levels <- sort(unique(plot_dat$uf_sigla))
uf_palette <- setNames(grDevices::hcl.colors(length(uf_levels), palette = "Dynamic"), uf_levels)

p <- ggplot(plot_dat, aes(x = quarter_date, y = rel_log_lpg_price_uf, color = uf_sigla, group = uf_sigla)) +
  geom_hline(yintercept = 0, linewidth = 0.5, color = '#9a3412', linetype = 'dashed') +
  geom_line(alpha = 0.9, linewidth = 0.55) +
  scale_x_date(date_breaks = '2 years', date_labels = '%Y') +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  scale_color_manual(values = uf_palette, name = 'UF') +
  labs(
    title = 'UF LPG Prices Relative to the National Level',
    subtitle = 'Quarterly log LPG price in each UF minus the national quarterly log LPG price',
    x = NULL,
    y = 'Log UF price - log national price',
    caption = 'Source: ANP retail LPG price surveys. Each line is a UF-quarter series.'
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    plot.title = element_text(face = 'bold'),
    plot.subtitle = element_text(color = '#475569'),
    axis.title.y = element_text(face = 'bold'),
    plot.caption = element_text(color = '#64748b'),
    legend.position = 'right',
    legend.title = element_text(face = 'bold'),
    legend.key.height = unit(0.32, 'cm')
  )

plevels <- ggplot(plot_dat, aes(x = quarter_date, y = rel_level_lpg_price_uf, color = uf_sigla, group = uf_sigla)) +
  geom_hline(yintercept = 0, linewidth = 0.5, color = '#9a3412', linetype = 'dashed') +
  geom_line(alpha = 0.9, linewidth = 0.55) +
  scale_x_date(date_breaks = '2 years', date_labels = '%Y') +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  scale_color_manual(values = uf_palette, name = 'UF') +
  labs(
    title = 'UF LPG Prices Relative to the National Level',
    subtitle = 'Quarterly UF LPG price minus the national quarterly LPG price',
    x = NULL,
    y = 'UF price - national price (R$)',
    caption = 'Source: ANP retail LPG price surveys. Each line is a UF-quarter series.'
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    plot.title = element_text(face = 'bold'),
    plot.subtitle = element_text(color = '#475569'),
    axis.title.y = element_text(face = 'bold'),
    plot.caption = element_text(color = '#64748b'),
    legend.position = 'right',
    legend.title = element_text(face = 'bold'),
    legend.key.height = unit(0.32, 'cm')
  )

ggsave(out_png, p, width = 11.8, height = 7.0, dpi = 320)
ggsave(out_pdf, p, width = 11.8, height = 7.0)
ggsave(out_levels_png, plevels, width = 11.8, height = 7.0, dpi = 320)
ggsave(out_levels_pdf, plevels, width = 11.8, height = 7.0)

cat(out_png, '\n')
cat(out_pdf, '\n')
cat(out_levels_png, '\n')
cat(out_levels_pdf, '\n')
