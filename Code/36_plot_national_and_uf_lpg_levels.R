library(arrow)
library(dplyr)
library(ggplot2)
library(scales)
library(here)

uf_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet"))
national_prices <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_quarter_national_2016_2024.parquet"))

national_plot_df <- national_prices %>%
  mutate(
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01"))
  ) %>%
  arrange(quarter_date)

uf_plot_df <- uf_prices %>%
  mutate(
    quarter_date = as.Date(paste0(year, "-", c("01", "04", "07", "10")[quarter], "-01"))
  ) %>%
  arrange(uf_sigla, quarter_date)

uf_with_national <- uf_plot_df %>%
  left_join(
    national_plot_df %>% select(year, quarter, quarter_date, lpg_price_quarterly),
    by = c("year", "quarter", "quarter_date")
  )

p_national <- ggplot(national_plot_df, aes(x = quarter_date, y = lpg_price_quarterly)) +
  geom_line(linewidth = 1.1, color = "black") +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  labs(
    title = "National LPG Price Level Over Time",
    subtitle = "Quarterly national ANP retail LPG price",
    x = NULL,
    y = "National LPG price",
    caption = "Source: ANP retail LPG price surveys."
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12, color = "gray30"),
    panel.grid.minor = element_blank(),
    axis.title.y = element_text(margin = margin(r = 10)),
    plot.caption = element_text(hjust = 1)
  )

p_uf_levels <- ggplot(uf_with_national, aes(x = quarter_date)) +
  geom_line(aes(y = lpg_price_quarterly_uf, color = uf_sigla, group = uf_sigla), linewidth = 0.7, alpha = 0.8) +
  geom_line(aes(y = lpg_price_quarterly), linewidth = 1.4, color = "black") +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  scale_y_continuous(labels = label_number(accuracy = 0.1)) +
  labs(
    title = "UF LPG Price Levels and the National Price",
    subtitle = "Colored lines are UF-quarter prices; black line is the national quarterly price",
    x = NULL,
    y = "LPG price level",
    color = "UF",
    caption = "Source: ANP retail LPG price surveys."
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

sd_summary <- uf_with_national %>%
  group_by(year, quarter) %>%
  summarise(sd_uf_price = sd(lpg_price_quarterly_uf, na.rm = TRUE), .groups = "drop") %>%
  mutate(post_2023 = year >= 2023) %>%
  group_by(post_2023) %>%
  summarise(mean_quarterly_sd_uf_price = mean(sd_uf_price, na.rm = TRUE), .groups = "drop")

corr_summary <- uf_with_national %>%
  group_by(uf_sigla) %>%
  summarise(corr_with_national = cor(lpg_price_quarterly_uf, lpg_price_quarterly, use = "complete.obs"), .groups = "drop")

national_png <- here("Figures", "powerIV", "lpg", "national_lpg_price_level_full_period.png")
national_pdf <- here("Figures", "powerIV", "lpg", "national_lpg_price_level_full_period.pdf")
uf_png <- here("Figures", "powerIV", "lpg", "uf_lpg_price_levels_with_national_full_period.png")
uf_pdf <- here("Figures", "powerIV", "lpg", "uf_lpg_price_levels_with_national_full_period.pdf")
summary_tsv <- here("data", "powerIV", "regressions", "lpg_state_price_alternatives", "uf_level_dispersion_and_correlation_summary.tsv")

write.table(
  bind_rows(
    sd_summary %>% mutate(metric = "mean_quarterly_sd_uf_price_levels", group = if_else(post_2023, "post_2023", "pre_2023")) %>% select(metric, group, value = mean_quarterly_sd_uf_price),
    corr_summary %>% mutate(metric = "corr_with_national_price_level", group = uf_sigla) %>% select(metric, group, value = corr_with_national)
  ),
  file = summary_tsv,
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

ggsave(national_png, p_national, width = 12, height = 7, dpi = 300)
ggsave(national_pdf, p_national, width = 12, height = 7)
ggsave(uf_png, p_uf_levels, width = 14, height = 8.5, dpi = 300)
ggsave(uf_pdf, p_uf_levels, width = 14, height = 8.5)

cat(national_png, "\n")
cat(uf_png, "\n")
print(sd_summary)
print(summary(corr_summary$corr_with_national))
