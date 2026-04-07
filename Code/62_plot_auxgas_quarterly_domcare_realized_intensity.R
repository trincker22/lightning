#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(readr)
  library(dplyr)
  library(ggplot2)
})

in_path <- here("data", "powerIV", "regressions", "auxilio_gas_quarterly_realized_intensity", "quarterly_realized_intensity_terms.tsv")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
out_png <- here("Figures", "powerIV", "auxilio_gas", "quarterly_realized_intensity_domcare_m2_people.png")
out_pdf <- here("Figures", "powerIV", "auxilio_gas", "quarterly_realized_intensity_domcare_m2_people.pdf")

dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

plot_df <- read_tsv(in_path, show_col_types = FALSE) %>%
  filter(
    outcome %in% c("female_domestic_care_reason", "female_outlf_domestic_care"),
    spec == "M2_people",
    treatment == "people"
  ) %>%
  mutate(
    lag_label = factor(lag_label, levels = c("t", "t-1", "t-2")),
    outcome_label = case_when(
      outcome == "female_domestic_care_reason" ~ "Domestic Care Reason",
      outcome == "female_outlf_domestic_care" ~ "Out of Labor Force for Domestic Care",
      TRUE ~ outcome
    )
  )

p <- ggplot(plot_df, aes(x = lag_label, y = estimate)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.8) +
  geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#1d4ed8", linewidth = 0.8) +
  geom_point(size = 2.8, color = "#1d4ed8") +
  facet_wrap(~ outcome_label, ncol = 1, scales = "free_y") +
  labs(
    x = "Quarter of Realized Auxilio Gas Intensity",
    y = "Coefficient",
    title = "Quarterly Realized Auxilio Gas Intensity and Domestic Care Outcomes",
    subtitle = "M2 people-intensity specification with estrato and year-quarter fixed effects"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    axis.title = element_text(face = "bold"),
    strip.text = element_text(face = "bold")
  )

ggsave(out_png, p, width = 9.5, height = 7.0, dpi = 300)
ggsave(out_pdf, p, width = 9.5, height = 7.0)

cat(out_png, "\n")
cat(out_pdf, "\n")
