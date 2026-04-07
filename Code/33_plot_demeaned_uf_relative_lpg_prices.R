library(arrow)
library(dplyr)
library(ggplot2)
library(scales)
library(here)

uf_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet"))
national_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_national_2016_2024.parquet"))

plot_df <- uf_prices %>%
  select(uf_sigla, year, quarter, lpg_price_quarterly_uf, log_lpg_price_quarterly_uf) %>%
  left_join(
    national_prices %>%
      select(year, quarter, lpg_price_quarterly, log_lpg_price_quarterly),
    by = c("year", "quarter")
  ) %>%
  mutate(
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01")),
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly
  ) %>%
  group_by(uf_sigla) %>%
  mutate(
    rel_log_lpg_price_uf_dm = rel_log_lpg_price_uf - mean(rel_log_lpg_price_uf, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  arrange(uf_sigla, quarter_date)

p <- ggplot(plot_df, aes(x = quarter_date, y = rel_log_lpg_price_uf_dm, color = uf_sigla, group = uf_sigla)) +
  geom_hline(yintercept = 0, linetype = "dashed", linewidth = 0.5, color = "firebrick") +
  geom_line(linewidth = 0.75, alpha = 0.8) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  labs(
    title = "UF LPG Prices Relative to the National Level, Demeaned Within UF",
    subtitle = "Log UF LPG price minus log national LPG price, minus each UF's sample mean relative price",
    x = NULL,
    y = "Demeaned [log UF price - log national price]",
    color = "UF",
    caption = "Source: ANP retail LPG price surveys. Each line is a UF-quarter series demeaned within UF."
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

png_path <- here("Figures", "powerIV", "lpg", "demeaned_uf_relative_to_national_lpg_price_full_period.png")
pdf_path <- here("Figures", "powerIV", "lpg", "demeaned_uf_relative_to_national_lpg_price_full_period.pdf")

ggsave(png_path, p, width = 14, height = 8.5, dpi = 300)
ggsave(pdf_path, p, width = 14, height = 8.5)

cat(png_path, "\n")
cat(pdf_path, "\n")
