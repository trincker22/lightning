#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_quarterly_realized_intensity")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

auxgas_q <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = as.character(year_quarter),
    auxgas_families,
    auxgas_total_value,
    auxgas_people
  )

lfp <- read_parquet(here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    lfp_weight_sum = weight_sum,
    female_domestic_care_reason,
    female_outlf_domestic_care,
    female_hours_effective_employed,
    female_hours_effective_population,
    female_employment,
    female_lfp
  )

power <- read_parquet(here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    power_weight_sum = weight_sum,
    share_cooking_gas_any,
    share_cooking_gas_bottled,
    share_cooking_wood_charcoal,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

baseline_shift <- read_parquet(here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  mutate(
    auxgas_families = coalesce(auxgas_families, 0),
    auxgas_total_value = coalesce(auxgas_total_value, 0),
    auxgas_people = coalesce(auxgas_people, 0),
    auxgas_families_per_weighted_household = auxgas_families / power_weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value / power_weight_sum,
    auxgas_people_per_weighted_household = auxgas_people / power_weight_sum,
    b_dep_t = baseline_child_dependency * (year - 2016)
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(lfp_weight_sum), lfp_weight_sum > 0) %>%
  filter(!is.na(power_weight_sum), power_weight_sum > 0) %>%
  arrange(estrato4, year, quarter) %>%
  group_by(estrato4) %>%
  mutate(
    quarter_index = year * 4L + quarter,
    lag1_auxgas_people_per_weighted_household = if_else(
      quarter_index - lag(quarter_index, 1L) == 1L,
      lag(auxgas_people_per_weighted_household, 1L),
      NA_real_
    ),
    lag2_auxgas_people_per_weighted_household = if_else(
      quarter_index - lag(quarter_index, 2L) == 2L,
      lag(auxgas_people_per_weighted_household, 2L),
      NA_real_
    ),
    lag1_auxgas_value_per_weighted_household = if_else(
      quarter_index - lag(quarter_index, 1L) == 1L,
      lag(auxgas_value_per_weighted_household, 1L),
      NA_real_
    ),
    lag2_auxgas_value_per_weighted_household = if_else(
      quarter_index - lag(quarter_index, 2L) == 2L,
      lag(auxgas_value_per_weighted_household, 2L),
      NA_real_
    )
  ) %>%
  ungroup()

outcomes <- tribble(
  ~outcome_group, ~outcome_name, ~outcome_var, ~weight_var,
  "fuel", "share_cooking_gas_bottled", "share_cooking_gas_bottled", "power_weight_sum",
  "fuel", "share_cooking_gas_any", "share_cooking_gas_any", "power_weight_sum",
  "fuel", "share_cooking_wood_charcoal", "share_cooking_wood_charcoal", "power_weight_sum",
  "domcare", "female_domestic_care_reason", "female_domestic_care_reason", "lfp_weight_sum",
  "domcare", "female_outlf_domestic_care", "female_outlf_domestic_care", "lfp_weight_sum",
  "lmi", "female_hours_effective_employed", "female_hours_effective_employed", "lfp_weight_sum",
  "lmi", "female_hours_effective_population", "female_hours_effective_population", "lfp_weight_sum",
  "lmi", "female_employment", "female_employment", "lfp_weight_sum",
  "lmi", "female_lfp", "female_lfp", "lfp_weight_sum"
)

specs <- tribble(
  ~spec, ~rhs,
  "M1_people", "auxgas_people_per_weighted_household + lag1_auxgas_people_per_weighted_household + lag2_auxgas_people_per_weighted_household",
  "M2_people", "auxgas_people_per_weighted_household + lag1_auxgas_people_per_weighted_household + lag2_auxgas_people_per_weighted_household + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time",
  "M1_value", "auxgas_value_per_weighted_household + lag1_auxgas_value_per_weighted_household + lag2_auxgas_value_per_weighted_household",
  "M2_value", "auxgas_value_per_weighted_household + lag1_auxgas_value_per_weighted_household + lag2_auxgas_value_per_weighted_household + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
)

coef_map <- tribble(
  ~term, ~lag_label, ~treatment,
  "auxgas_people_per_weighted_household", "t", "people",
  "lag1_auxgas_people_per_weighted_household", "t-1", "people",
  "lag2_auxgas_people_per_weighted_household", "t-2", "people",
  "auxgas_value_per_weighted_household", "t", "value",
  "lag1_auxgas_value_per_weighted_household", "t-1", "value",
  "lag2_auxgas_value_per_weighted_household", "t-2", "value"
)

terms_out <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_group <- outcomes$outcome_group[[i]]
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]
  weight_var <- outcomes$weight_var[[i]]

  for (j in seq_len(nrow(specs))) {
    spec_name <- specs$spec[[j]]
    rhs <- specs$rhs[[j]]

    vars_needed <- unique(c(
      "estrato4", "year_quarter", weight_var, outcome_var,
      all.vars(as.formula(paste("~", rhs)))
    ))

    reg_data <- panel %>%
      mutate(analysis_weight = .data[[weight_var]]) %>%
      filter(if_all(all_of(vars_needed), ~ !is.na(.))) %>%
      filter(!is.na(analysis_weight), analysis_weight > 0)

    model <- feols(
      as.formula(paste0(outcome_var, " ~ ", rhs, " | estrato4 + year_quarter")),
      data = reg_data,
      weights = ~analysis_weight,
      cluster = ~estrato4
    )

    tidy_tbl <- tidy(model) %>%
      filter(term %in% coef_map$term) %>%
      left_join(coef_map, by = "term") %>%
      mutate(
        outcome_group = outcome_group,
        outcome = outcome_name,
        weight_var = weight_var,
        spec = spec_name,
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(
        outcome_group,
        outcome,
        weight_var,
        spec,
        treatment,
        term,
        lag_label,
        estimate,
        std.error,
        conf_low,
        conf_high,
        p.value,
        n
      )

    terms_out[[length(terms_out) + 1L]] <- tidy_tbl
  }
}

terms_df <- bind_rows(terms_out) %>%
  mutate(
    outcome_group = factor(outcome_group, levels = c("fuel", "domcare", "lmi")),
    lag_label = factor(lag_label, levels = c("t", "t-1", "t-2")),
    spec = factor(spec, levels = c("M1_people", "M2_people", "M1_value", "M2_value"))
  ) %>%
  arrange(outcome_group, outcome, spec, lag_label)

summary_df <- terms_df %>%
  select(outcome_group, outcome, weight_var, spec, treatment, lag_label, estimate, std.error, p.value, n)

coverage_df <- panel %>%
  summarise(
    n_rows = n(),
    n_estratos = n_distinct(estrato4),
    first_year_quarter = min(year_quarter),
    last_year_quarter = max(year_quarter),
    mean_people_per_weighted_household = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    mean_value_per_weighted_household = mean(auxgas_value_per_weighted_household, na.rm = TRUE)
  )

write.table(
  terms_df,
  file.path(out_dir, "quarterly_realized_intensity_terms.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

write.table(
  summary_df,
  file.path(out_dir, "quarterly_realized_intensity_summary.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

write.table(
  coverage_df,
  file.path(out_dir, "quarterly_realized_intensity_coverage.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

print(coverage_df)
print(summary_df)
