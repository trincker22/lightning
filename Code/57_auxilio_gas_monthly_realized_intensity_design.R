suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(tidyr)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_monthly_realized_intensity")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

novo <- read_parquet(here("data", "powerIV", "novo_caged", "novo_caged_estrato_month.parquet")) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter = ((month - 1) %/% 3) + 1,
    year_month = sprintf("%04d-%02d", year, month),
    date = as.Date(sprintf("%04d-%02d-01", year, month))
  )

auxgas_raw <- read_parquet(here("data", "powerIV", "monthly_source_audit", "auxgas_estrato_month_2021_2025.parquet")) %>%
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

base_panel <- novo %>%
  left_join(visit1, by = c("estrato4", "year", "quarter")) %>%
  left_join(auxgas_raw, by = c("estrato4", "year", "month", "quarter")) %>%
  mutate(
    auxgas_people = coalesce(auxgas_people, 0),
    auxgas_families = coalesce(auxgas_families, 0),
    auxgas_total_value = coalesce(auxgas_total_value, 0),
    female_net_per_weighted_household = female_net / weight_sum,
    female_admissions_per_weighted_household = female_admissions / weight_sum,
    female_separations_per_weighted_household = female_separations / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people / weight_sum,
    auxgas_families_per_weighted_household = auxgas_families / weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value / weight_sum
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  filter(date >= as.Date("2022-01-01"), date <= as.Date("2023-10-01"))

pre_exposure <- base_panel %>%
  filter((year == 2021 & month == 12) | (year == 2022 & month %in% c(2, 4, 6))) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_exposure = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    pre_families_exposure = mean(auxgas_families_per_weighted_household, na.rm = TRUE),
    pre_value_exposure = mean(auxgas_value_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

panel <- base_panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  arrange(estrato4, date) %>%
  group_by(estrato4) %>%
  mutate(
    lag1_auxgas_value_per_weighted_household = lag(auxgas_value_per_weighted_household, 1),
    lag2_auxgas_value_per_weighted_household = lag(auxgas_value_per_weighted_household, 2),
    lag1_auxgas_people_per_weighted_household = lag(auxgas_people_per_weighted_household, 1),
    lag2_auxgas_people_per_weighted_household = lag(auxgas_people_per_weighted_household, 2)
  ) %>%
  ungroup()

outcomes <- tribble(
  ~outcome_name, ~outcome_var,
  "female_net", "female_net_per_weighted_household",
  "female_admissions", "female_admissions_per_weighted_household",
  "female_separations", "female_separations_per_weighted_household"
)

specs <- tribble(
  ~spec, ~rhs,
  "M1_value", "auxgas_value_per_weighted_household + lag1_auxgas_value_per_weighted_household + lag2_auxgas_value_per_weighted_household",
  "M2_value", "auxgas_value_per_weighted_household + lag1_auxgas_value_per_weighted_household + lag2_auxgas_value_per_weighted_household + i(month, pre_people_exposure, ref = 7)",
  "M1_people", "auxgas_people_per_weighted_household + lag1_auxgas_people_per_weighted_household + lag2_auxgas_people_per_weighted_household",
  "M2_people", "auxgas_people_per_weighted_household + lag1_auxgas_people_per_weighted_household + lag2_auxgas_people_per_weighted_household + i(month, pre_people_exposure, ref = 7)"
)

coef_map <- tribble(
  ~term, ~lag_label,
  "auxgas_value_per_weighted_household", "t",
  "lag1_auxgas_value_per_weighted_household", "t-1",
  "lag2_auxgas_value_per_weighted_household", "t-2",
  "auxgas_people_per_weighted_household", "t",
  "lag1_auxgas_people_per_weighted_household", "t-1",
  "lag2_auxgas_people_per_weighted_household", "t-2"
)

terms_out <- list()
summary_out <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]

  for (j in seq_len(nrow(specs))) {
    spec_name <- specs$spec[[j]]
    rhs <- specs$rhs[[j]]

    vars_needed <- unique(c(
      "estrato4", "year_month", outcome_var,
      all.vars(as.formula(paste("~", rhs)))
    ))

    reg_data <- panel %>%
      filter(if_all(all_of(vars_needed), ~ !is.na(.)))

    model <- feols(
      as.formula(paste0(outcome_var, " ~ ", rhs, " | estrato4 + year_month")),
      data = reg_data,
      cluster = ~estrato4
    )

    tidy_tbl <- tidy(model) %>%
      filter(term %in% coef_map$term) %>%
      left_join(coef_map, by = "term") %>%
      mutate(
        outcome = outcome_name,
        spec = spec_name,
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(outcome, spec, term, lag_label, estimate, std.error, conf_low, conf_high, p.value, n)

    summary_out[[length(summary_out) + 1]] <- tidy_tbl
    terms_out[[length(terms_out) + 1]] <- tidy_tbl
  }
}

terms_df <- bind_rows(terms_out) %>%
  mutate(
    lag_label = factor(lag_label, levels = c("t", "t-1", "t-2"))
  ) %>%
  arrange(outcome, spec, lag_label)

summary_df <- terms_df %>%
  select(outcome, spec, lag_label, estimate, std.error, p.value, n)

write.table(terms_df, file.path(out_dir, "monthly_realized_intensity_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, file.path(out_dir, "monthly_realized_intensity_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(pre_exposure, file.path(out_dir, "monthly_realized_intensity_pre_exposure.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~spec, ~file_stub, ~title,
  "female_net", "M2_value", "monthly_realized_value_female_net_novo_caged", "Female Net Formal Employment and Realized Auxilio Gas Value",
  "female_admissions", "M2_value", "monthly_realized_value_female_admissions_novo_caged", "Female Formal Admissions and Realized Auxilio Gas Value",
  "female_separations", "M2_value", "monthly_realized_value_female_separations_novo_caged", "Female Formal Separations and Realized Auxilio Gas Value"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(outcome == plot_specs$outcome[[i]], spec == plot_specs$spec[[i]])

  p <- ggplot(plot_df, aes(x = lag_label, y = estimate)) +
    geom_hline(yintercept = 0, color = "black", linewidth = 0.9) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#2C7FB8", linewidth = 0.8) +
    geom_point(size = 3, color = "#2C7FB8") +
    labs(
      x = "Realized Auxilio Gas Timing",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = "Monthly estrato panel; includes estrato and year-month FE plus month-of-year x pre-exposure controls"
    ) +
    theme_minimal(base_size = 16) +
    theme(
      plot.title = element_text(size = 21, face = "plain"),
      plot.subtitle = element_text(size = 12.5, color = "black"),
      axis.title = element_text(size = 16),
      axis.text = element_text(size = 12),
      panel.grid.major = element_line(color = "#DDDDDD", linewidth = 0.8),
      panel.grid.minor = element_line(color = "#E9E9E9", linewidth = 0.5)
    )

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 10.5, height = 6.2, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 10.5, height = 6.2)
}

print(summary_df)
