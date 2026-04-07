#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(readr)
  library(ggplot2)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_income_eligibility_sweep")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

household_results <- read_tsv(
  file.path(out_dir, "income_eligibility_household_results.tsv"),
  show_col_types = FALSE
)

confirmed_hits <- read_tsv(
  file.path(out_dir, "income_eligibility_confirmed_hits.tsv"),
  show_col_types = FALSE
)

fuel_df <- household_results %>%
  filter(
    sample == "eligible_05mw_labor_households",
    spec == "M2"
  ) %>%
  filter(
    (outcome == "cooking_gas_any" & design == "pre_families_post" & treatment == "z_pre_families_post") |
      (outcome == "cooking_gas_bottled" & design == "pre_families_post" & treatment == "z_pre_families_post") |
      (outcome == "cooking_wood_charcoal" & design == "pre_people_post" & treatment == "z_pre_people_post")
  ) %>%
  transmute(
    panel = "Eligible households: cooking fuel",
    label = case_when(
      outcome == "cooking_gas_any" ~ "Any gas use | Cross FE",
      outcome == "cooking_gas_bottled" ~ "Bottled gas use | Cross FE",
      outcome == "cooking_wood_charcoal" ~ "Wood/charcoal use | Cross FE",
      TRUE ~ outcome
    ),
    estimate = coef,
    std_error = se,
    model = "Cross FE"
  )

women_keep <- confirmed_hits %>%
  filter(
    (sample == "eligible_05mw_labor" & outcome == "domestic_care_reason" & design == "delta_families_post" & treatment == "z_delta_families_post") |
      (sample == "eligible_05mw_labor_kids" & outcome == "domestic_care_reason" & design == "delta_families_post" & treatment == "z_delta_families_post") |
      (sample == "eligible_05mw_labor" & outcome == "effective_hours_population" & design == "pre_families_post" & treatment == "z_pre_families_post") |
      (sample == "eligible_05mw_labor_kids" & outcome == "effective_hours_population" & design == "pre_families_post" & treatment == "z_pre_families_post") |
      (sample == "eligible_05mw_labor_wood" & outcome == "wants_more_hours" & design == "pre_families_post" & treatment == "z_pre_families_post")
  )

women_cross_df <- women_keep %>%
  transmute(
    panel = "Eligible women: downstream outcomes",
    label = case_when(
      sample == "eligible_05mw_labor" & outcome == "domestic_care_reason" ~ "Domestic care reason, eligible women | Cross FE",
      sample == "eligible_05mw_labor_kids" & outcome == "domestic_care_reason" ~ "Domestic care reason, eligible mothers | Cross FE",
      sample == "eligible_05mw_labor" & outcome == "effective_hours_population" ~ "Effective hours (population), eligible women | Cross FE",
      sample == "eligible_05mw_labor_kids" & outcome == "effective_hours_population" ~ "Effective hours (population), eligible mothers | Cross FE",
      sample == "eligible_05mw_labor_wood" & outcome == "wants_more_hours" ~ "Wants more hours, eligible wood users | Cross FE",
      TRUE ~ paste(sample, outcome, "Cross FE")
    ),
    estimate = cross_coef,
    std_error = cross_se,
    model = "Cross FE"
  )

women_fe_df <- women_keep %>%
  transmute(
    panel = "Eligible women: downstream outcomes",
    label = case_when(
      sample == "eligible_05mw_labor" & outcome == "domestic_care_reason" ~ "Domestic care reason, eligible women | Woman FE",
      sample == "eligible_05mw_labor_kids" & outcome == "domestic_care_reason" ~ "Domestic care reason, eligible mothers | Woman FE",
      sample == "eligible_05mw_labor" & outcome == "effective_hours_population" ~ "Effective hours (population), eligible women | Woman FE",
      sample == "eligible_05mw_labor_kids" & outcome == "effective_hours_population" ~ "Effective hours (population), eligible mothers | Woman FE",
      sample == "eligible_05mw_labor_wood" & outcome == "wants_more_hours" ~ "Wants more hours, eligible wood users | Woman FE",
      TRUE ~ paste(sample, outcome, "Woman FE")
    ),
    estimate = fe_coef,
    std_error = fe_se,
    model = "Woman FE"
  )

plot_df <- bind_rows(fuel_df, women_cross_df, women_fe_df) %>%
  mutate(
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )

label_order <- c(
  "Any gas use | Cross FE",
  "Bottled gas use | Cross FE",
  "Wood/charcoal use | Cross FE",
  "Domestic care reason, eligible mothers | Woman FE",
  "Domestic care reason, eligible mothers | Cross FE",
  "Domestic care reason, eligible women | Woman FE",
  "Domestic care reason, eligible women | Cross FE",
  "Effective hours (population), eligible mothers | Woman FE",
  "Effective hours (population), eligible mothers | Cross FE",
  "Effective hours (population), eligible women | Woman FE",
  "Effective hours (population), eligible women | Cross FE",
  "Wants more hours, eligible wood users | Woman FE",
  "Wants more hours, eligible wood users | Cross FE"
)

plot_df <- plot_df %>%
  mutate(label = factor(label, levels = rev(label_order)))

p <- ggplot(plot_df, aes(x = estimate, y = label, color = model)) +
  geom_vline(xintercept = 0, linewidth = 0.5, linetype = "dashed", color = "gray50") +
  geom_errorbar(aes(xmin = conf_low, xmax = conf_high), width = 0.18, linewidth = 0.7, orientation = "y") +
  geom_point(size = 2.6) +
  facet_wrap(~panel, ncol = 1, scales = "free") +
  scale_color_manual(
    values = c("Cross FE" = "#1f78b4", "Woman FE" = "#d95f02")
  ) +
  labs(
    x = "Coefficient",
    y = NULL,
    title = "Auxilio Gas Results in the Income-Eligible Sample",
    subtitle = "Points are estimates; bars are 95% confidence intervals",
    caption = "Eligible is proxied by household labor income per capita <= one-half minimum wage. Fuel rows use eligible households; downstream rows show eligible-women results."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold"),
    axis.text.y = element_text(size = 10),
    plot.caption.position = "plot"
  )

ggsave(
  file.path(fig_dir, "auxilio_gas_income_eligibility_coefficients.png"),
  p,
  width = 10,
  height = 9.2,
  dpi = 300
)

ggsave(
  file.path(fig_dir, "auxilio_gas_income_eligibility_coefficients.pdf"),
  p,
  width = 10,
  height = 9.2
)

print(plot_df)
