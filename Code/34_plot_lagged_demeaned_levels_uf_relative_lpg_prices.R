library(arrow)
library(dplyr)
library(ggplot2)
library(scales)
library(here)

uf_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet"))
national_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_national_2016_2024.parquet"))

plot_df <- uf_prices %>%
  select(uf_sigla, year, quarter, lpg_price_quarterly_uf) %>%
  left_join(
    national_prices %>%
      select(year, quarter, lpg_price_quarterly),
    by = c("year", "quarter")
  ) %>%
  mutate(
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01")),
    quarter_index = year * 4 + quarter,
    rel_level_lpg_price_uf = lpg_price_quarterly_uf - lpg_price_quarterly
  ) %>%
  arrange(uf_sigla, quarter_date) %>%
  group_by(uf_sigla) %>%
  mutate(
    lag_quarter_index = lag(quarter_index),
    lag_rel_level_lpg_price_uf = if_else(quarter_index - lag_quarter_index == 1, lag(rel_level_lpg_price_uf), NA_real_),
    lag_rel_level_lpg_price_uf_dm = lag_rel_level_lpg_price_uf - mean(lag_rel_level_lpg_price_uf, na.rm = TRUE)
  ) %>%
  ungroup()

p <- ggplot(
  filter(plot_df, !is.na(lag_rel_level_lpg_price_uf_dm)),
  aes(x = quarter_date, y = lag_rel_level_lpg_price_uf_dm, color = uf_sigla, group = uf_sigla)
) +
  geom_hline(yintercept = 0, linetype = "dashed", linewidth = 0.5, color = "firebrick") +
  geom_line(linewidth = 0.75, alpha = 0.8) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  labs(
    title = "Lagged UF LPG Prices Relative to the National Level, Demeaned Within UF",
    subtitle = "One-quarter lag of [UF LPG price - national LPG price], demeaned within UF",
    x = NULL,
    y = "Demeaned lagged [UF price - national price]",
    color = "UF",
    caption = "Source: ANP retail LPG price surveys. Each line is a UF-quarter series in levels, lagged one quarter and demeaned within UF."
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank(),
    legend.position = "right",
    legend.title = element_text(face = "bold"),
    axis.title.y = element_text(margin = margin(r = 10)),
    plot.caption = element_text(hjust = 1)
  )

png_path <- here("Figures", "powerIV", "lpg", "lagged_demeaned_levels_uf_relative_to_national_lpg_price_full_period.png")
pdf_path <- here("Figures", "powerIV", "lpg", "lagged_demeaned_levels_uf_relative_to_national_lpg_price_full_period.pdf")

ggsave(png_path, p, width = 14, height = 8.5, dpi = 300)
ggsave(pdf_path, p, width = 14, height = 8.5)

cat(png_path, "\n")
cat(pdf_path, "\n")
