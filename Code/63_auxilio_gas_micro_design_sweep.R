#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(tidyr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_micro_design_sweep")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

extract_term <- function(model, term_name) {
  tbl <- tidy(model)
  row <- tbl[tbl$term == term_name, , drop = FALSE]
  if (nrow(row) == 0) {
    return(tibble(estimate = NA_real_, std.error = NA_real_, p.value = NA_real_))
  }
  tibble(
    estimate = row$estimate[[1]],
    std.error = row$std.error[[1]],
    p.value = row$p.value[[1]]
  )
}

safe_weighted_mean <- function(x, w) {
  ok <- !is.na(x) & !is.na(w)
  if (!any(ok)) {
    return(NA_real_)
  }
  weighted.mean(x[ok], w[ok])
}

auxgas_q <- read_parquet(
  here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet"),
  col_select = c("estrato4", "year", "quarter", "year_quarter", "auxgas_people")
) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter,
    auxgas_people
  )

visit1_q <- read_parquet(
  here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet"),
  col_select = c("estrato4", "year", "quarter", "year_quarter", "weight_sum")
) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = gsub("-", "", year_quarter),
    weight_sum
  )

treatment_panel <- visit1_q %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  mutate(
    auxgas_people = coalesce(auxgas_people, 0),
    people_per_weighted_household = auxgas_people / weight_sum,
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3)),
    event_time_q = (year - 2022) * 4 + (quarter - 3)
  ) %>%
  filter(year >= 2021, year <= 2024, !is.na(weight_sum), weight_sum > 0)

pre_exposure <- treatment_panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_per_weighted_household = mean(people_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

post_exposure <- treatment_panel %>%
  filter(post_topup_2022q3 == 1) %>%
  group_by(estrato4) %>%
  summarise(
    post_people_per_weighted_household = mean(people_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(pre_exposure, by = "estrato4") %>%
  mutate(
    delta_people_per_weighted_household = post_people_per_weighted_household - pre_people_per_weighted_household,
    ratio_people_per_weighted_household = if_else(
      pre_people_per_weighted_household > 0,
      post_people_per_weighted_household / pre_people_per_weighted_household,
      NA_real_
    )
  )

delta_cutoff <- quantile(post_exposure$delta_people_per_weighted_household, 0.75, na.rm = TRUE)

treatment_metrics <- post_exposure %>%
  mutate(
    high_delta_p75 = as.integer(delta_people_per_weighted_household >= delta_cutoff)
  ) %>%
  arrange(desc(delta_people_per_weighted_household))

treatment_panel <- treatment_panel %>%
  left_join(treatment_metrics, by = "estrato4") %>%
  mutate(
    z_pre_people_post = pre_people_per_weighted_household * post_topup_2022q3,
    z_delta_people_post = delta_people_per_weighted_household * post_topup_2022q3,
    high_delta_p75_post = high_delta_p75 * post_topup_2022q3
  )

write.table(
  treatment_metrics,
  file.path(out_dir, "treatment_metrics.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

households <- read_parquet(
  here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet"),
  col_select = c(
    "year", "quarter", "year_quarter", "estrato4", "weight_calibrated",
    "n_rooms", "n_bedrooms", "electricity_from_grid", "electricity_grid_full_time",
    "cooking_gas_any", "cooking_gas_bottled", "cooking_wood_charcoal"
  )
) %>%
  transmute(
    year,
    quarter,
    year_quarter = gsub("-", "", year_quarter),
    estrato4 = as.character(estrato4),
    weight_calibrated,
    n_rooms,
    n_bedrooms,
    electricity_from_grid,
    electricity_grid_full_time,
    cooking_gas_any,
    cooking_gas_bottled,
    cooking_wood_charcoal
  ) %>%
  filter(year >= 2022, year <= 2024, !is.na(weight_calibrated), weight_calibrated > 0) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter, post_topup_2022q3,
        pre_people_per_weighted_household, delta_people_per_weighted_household,
        z_pre_people_post, z_delta_people_post, high_delta_p75, high_delta_p75_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

household_outcomes <- tibble::tribble(
  ~outcome, ~preferred_sign,
  "cooking_gas_any", 1,
  "cooking_gas_bottled", 1,
  "cooking_wood_charcoal", -1
)

household_treatments <- tibble::tribble(
  ~treatment, ~design,
  "z_pre_people_post", "pre_exposure_post",
  "z_delta_people_post", "realized_delta_post",
  "high_delta_p75_post", "high_delta_p75_post"
)

household_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "n_rooms + n_bedrooms + electricity_grid_full_time"
)

household_rows <- list()
hh_i <- 1L

for (i in seq_len(nrow(household_outcomes))) {
  outcome <- household_outcomes$outcome[[i]]
  preferred_sign <- household_outcomes$preferred_sign[[i]]

  for (j in seq_len(nrow(household_treatments))) {
    treatment <- household_treatments$treatment[[j]]
    design <- household_treatments$design[[j]]

    for (k in seq_len(nrow(household_specs))) {
      spec <- household_specs$spec[[k]]
      controls <- household_specs$controls[[k]]
      needed_controls <- all.vars(as.formula(paste("~", controls)))
      needed_vars <- unique(c("estrato4", "year_quarter", "weight_calibrated", outcome, treatment, needed_controls))

      reg_data <- households %>%
        filter(if_all(all_of(needed_vars), ~ !is.na(.x)))

      model <- feols(
        as.formula(paste0(outcome, " ~ ", treatment, " + ", controls, " | estrato4 + year_quarter")),
        data = reg_data,
        weights = ~weight_calibrated,
        cluster = ~estrato4
      )

      term <- extract_term(model, treatment)

      household_rows[[hh_i]] <- tibble(
        sample = "household_micro_2022_2024",
        outcome = outcome,
        design = design,
        treatment = treatment,
        spec = spec,
        model_family = "household_fe",
        nobs = nobs(model),
        coef = term$estimate,
        se = term$std.error,
        p = term$p.value,
        preferred_sign = preferred_sign,
        sensible_sign = case_when(
          preferred_sign > 0 ~ coef > 0,
          preferred_sign < 0 ~ coef < 0,
          TRUE ~ NA
        ),
        weighted_mean_outcome = safe_weighted_mean(reg_data[[outcome]], reg_data$weight_calibrated)
      )
      hh_i <- hh_i + 1L
    }
  }
}

household_results <- bind_rows(household_rows) %>%
  arrange(p)

write.table(
  household_results,
  file.path(out_dir, "household_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

women <- read_parquet(
  here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet"),
  col_select = c(
    "woman_id", "year", "quarter", "year_quarter", "estrato4", "person_weight", "age",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14", "employed",
    "domestic_care_reason", "worked_last_12m", "effective_hours",
    "visit1_n_rooms", "visit1_n_bedrooms"
  )
) %>%
  transmute(
    woman_id,
    year,
    quarter,
    year_quarter,
    estrato4 = as.character(estrato4),
    person_weight,
    age,
    female_secondaryplus,
    female_tertiary,
    hh_size,
    n_children_0_14,
    employed,
    domestic_care_reason,
    worked_last_12m,
    effective_hours,
    visit1_n_rooms,
    visit1_n_bedrooms
  ) %>%
  filter(((year == 2021 & quarter == 4) | year >= 2022), year <= 2024, !is.na(person_weight), person_weight > 0) %>%
  mutate(
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter, post_topup_2022q3,
        pre_people_per_weighted_household, delta_people_per_weighted_household,
        z_pre_people_post, z_delta_people_post, high_delta_p75, high_delta_p75_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

women_outcomes <- tibble::tribble(
  ~outcome, ~preferred_sign,
  "domestic_care_reason", -1,
  "employed", 1,
  "worked_last_12m", 1,
  "effective_hours_population", 1
)

women_treatments <- tibble::tribble(
  ~treatment, ~design,
  "z_pre_people_post", "pre_exposure_post",
  "z_delta_people_post", "realized_delta_post",
  "high_delta_p75_post", "high_delta_p75_post"
)

women_cs_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + age + age_sq + female_secondaryplus + female_tertiary"
)

women_fe_specs <- tibble::tribble(
  ~spec, ~controls,
  "WFE", "hh_size + n_children_0_14 + age + age_sq"
)

women_rows <- list()
w_i <- 1L

for (i in seq_len(nrow(women_outcomes))) {
  outcome <- women_outcomes$outcome[[i]]
  preferred_sign <- women_outcomes$preferred_sign[[i]]

  for (j in seq_len(nrow(women_treatments))) {
    treatment <- women_treatments$treatment[[j]]
    design <- women_treatments$design[[j]]

    for (k in seq_len(nrow(women_cs_specs))) {
      spec <- women_cs_specs$spec[[k]]
      controls <- women_cs_specs$controls[[k]]
      needed_controls <- all.vars(as.formula(paste("~", controls)))
      needed_vars <- unique(c("estrato4", "year_quarter", "person_weight", outcome, treatment, needed_controls))

      reg_data <- women %>%
        filter(if_all(all_of(needed_vars), ~ !is.na(.x)))

      model <- feols(
        as.formula(paste0(outcome, " ~ ", treatment, " + ", controls, " | estrato4 + year_quarter")),
        data = reg_data,
        weights = ~person_weight,
        cluster = ~estrato4
      )

      term <- extract_term(model, treatment)

      women_rows[[w_i]] <- tibble(
        sample = "women_linked_2021q4_2024q4",
        outcome = outcome,
        design = design,
        treatment = treatment,
        spec = spec,
        model_family = "women_cross_section_fe",
        nobs = nobs(model),
        coef = term$estimate,
        se = term$std.error,
        p = term$p.value,
        preferred_sign = preferred_sign,
        sensible_sign = case_when(
          preferred_sign > 0 ~ coef > 0,
          preferred_sign < 0 ~ coef < 0,
          TRUE ~ NA
        ),
        weighted_mean_outcome = safe_weighted_mean(reg_data[[outcome]], reg_data$person_weight)
      )
      w_i <- w_i + 1L
    }

    for (k in seq_len(nrow(women_fe_specs))) {
      spec <- women_fe_specs$spec[[k]]
      controls <- women_fe_specs$controls[[k]]
      needed_controls <- all.vars(as.formula(paste("~", controls)))
      needed_vars <- unique(c("woman_id", "estrato4", "year_quarter", "person_weight", outcome, treatment, needed_controls))

      reg_data <- women %>%
        filter(if_all(all_of(needed_vars), ~ !is.na(.x)))

      model <- feols(
        as.formula(paste0(outcome, " ~ ", treatment, " + ", controls, " | woman_id + year_quarter")),
        data = reg_data,
        weights = ~person_weight,
        cluster = ~estrato4
      )

      term <- extract_term(model, treatment)

      women_rows[[w_i]] <- tibble(
        sample = "women_linked_2021q4_2024q4",
        outcome = outcome,
        design = design,
        treatment = treatment,
        spec = spec,
        model_family = "women_fixed_effects",
        nobs = nobs(model),
        coef = term$estimate,
        se = term$std.error,
        p = term$p.value,
        preferred_sign = preferred_sign,
        sensible_sign = case_when(
          preferred_sign > 0 ~ coef > 0,
          preferred_sign < 0 ~ coef < 0,
          TRUE ~ NA
        ),
        weighted_mean_outcome = safe_weighted_mean(reg_data[[outcome]], reg_data$person_weight)
      )
      w_i <- w_i + 1L
    }
  }
}

women_results <- bind_rows(women_rows) %>%
  arrange(p)

write.table(
  women_results,
  file.path(out_dir, "women_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

best_results <- bind_rows(
  household_results %>%
    filter(sensible_sign) %>%
    group_by(model_family, outcome) %>%
    slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
    ungroup(),
  women_results %>%
    filter(sensible_sign) %>%
    group_by(model_family, outcome) %>%
    slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
    ungroup()
) %>%
  arrange(p)

write.table(
  best_results,
  file.path(out_dir, "best_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

hh_trends <- households %>%
  filter(!is.na(high_delta_p75)) %>%
  group_by(year, quarter, year_quarter, high_delta_p75) %>%
  summarise(
    cooking_gas_bottled = safe_weighted_mean(cooking_gas_bottled, weight_calibrated),
    cooking_wood_charcoal = safe_weighted_mean(cooking_wood_charcoal, weight_calibrated),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = c(cooking_gas_bottled, cooking_wood_charcoal),
    names_to = "outcome",
    values_to = "value"
  )

women_trends <- women %>%
  filter(!is.na(high_delta_p75)) %>%
  group_by(year, quarter, year_quarter, high_delta_p75) %>%
  summarise(
    domestic_care_reason = safe_weighted_mean(domestic_care_reason, person_weight),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = domestic_care_reason,
    names_to = "outcome",
    values_to = "value"
  )

trend_plot_df <- bind_rows(hh_trends, women_trends) %>%
  mutate(
    quarter_date = as.Date(sprintf("%d-%02d-01", year, c(1, 4, 7, 10)[quarter])),
    high_delta_group = if_else(high_delta_p75 == 1, "Top quartile top-up growth", "Other estratos"),
    outcome = recode(
      outcome,
      cooking_gas_bottled = "Bottled gas use",
      cooking_wood_charcoal = "Wood or charcoal use",
      domestic_care_reason = "Domestic care reason"
    )
  )

trend_plot <- ggplot(trend_plot_df, aes(x = quarter_date, y = value, color = high_delta_group)) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date("2022-07-01"), linetype = "dashed", color = "gray30") +
  facet_wrap(~outcome, scales = "free_y", ncol = 1) +
  scale_color_manual(values = c("Other estratos" = "#7a7a7a", "Top quartile top-up growth" = "#1f78b4")) +
  labs(
    x = NULL,
    y = "Weighted mean",
    color = NULL,
    title = "Outcomes in High Top-Up-Growth Estratos",
    subtitle = "Top quartile is defined by the post-2022Q3 increase in Auxilio Gas people per weighted household"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom")

ggsave(
  file.path(fig_dir, "auxilio_gas_high_delta_trends.png"),
  trend_plot,
  width = 9,
  height = 9,
  dpi = 300
)

ggsave(
  file.path(fig_dir, "auxilio_gas_high_delta_trends.pdf"),
  trend_plot,
  width = 9,
  height = 9
)

coef_plot_df <- best_results %>%
  mutate(
    outcome_label = recode(
      outcome,
      cooking_gas_any = "Gas any",
      cooking_gas_bottled = "Bottled gas",
      cooking_wood_charcoal = "Wood or charcoal",
      domestic_care_reason = "Domestic care reason",
      employed = "Employed",
      worked_last_12m = "Worked last 12 months",
      effective_hours_population = "Effective hours (population)"
    ),
    design_label = recode(
      design,
      pre_exposure_post = "Pre-exposure x post",
      realized_delta_post = "Realized top-up delta x post",
      high_delta_p75_post = "Top quartile top-up delta x post"
    ),
    model_label = paste(model_family, spec, sep = " | "),
    conf_low = coef - 1.96 * se,
    conf_high = coef + 1.96 * se
  )

coef_plot <- ggplot(coef_plot_df, aes(x = design_label, y = coef, color = model_label)) +
  geom_hline(yintercept = 0, color = "gray30") +
  geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.1, linewidth = 0.6) +
  geom_point(size = 2.2) +
  coord_flip() +
  facet_wrap(~outcome_label, scales = "free_y", ncol = 1) +
  scale_color_brewer(palette = "Dark2") +
  labs(
    x = NULL,
    y = "Coefficient",
    color = NULL,
    title = "Best Gas-Design Coefficients by Outcome"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom")

ggsave(
  file.path(fig_dir, "auxilio_gas_best_coefficients.png"),
  coef_plot,
  width = 9,
  height = 10,
  dpi = 300
)

ggsave(
  file.path(fig_dir, "auxilio_gas_best_coefficients.pdf"),
  coef_plot,
  width = 9,
  height = 10
)

print(treatment_metrics %>% summarise(
  n_estratos = n(),
  share_high_delta_p75 = mean(high_delta_p75, na.rm = TRUE),
  median_pre_people = median(pre_people_per_weighted_household, na.rm = TRUE),
  median_delta_people = median(delta_people_per_weighted_household, na.rm = TRUE),
  p75_delta_people = quantile(delta_people_per_weighted_household, 0.75, na.rm = TRUE),
  median_ratio_people = median(ratio_people_per_weighted_household, na.rm = TRUE)
))

print(household_results)
print(women_results)
print(best_results)
