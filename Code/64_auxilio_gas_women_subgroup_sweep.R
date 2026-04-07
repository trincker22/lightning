#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_women_subgroup_sweep")
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
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3))
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
    delta_people_per_weighted_household = post_people_per_weighted_household - pre_people_per_weighted_household
  )

delta_cutoff <- quantile(post_exposure$delta_people_per_weighted_household, 0.75, na.rm = TRUE)

treatment_panel <- treatment_panel %>%
  left_join(post_exposure, by = "estrato4") %>%
  mutate(
    high_delta_p75 = as.integer(delta_people_per_weighted_household >= delta_cutoff),
    z_pre_people_post = pre_people_per_weighted_household * post_topup_2022q3,
    z_delta_people_post = delta_people_per_weighted_household * post_topup_2022q3,
    high_delta_p75_post = high_delta_p75 * post_topup_2022q3
  )

women <- read_parquet(
  here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet"),
  col_select = c(
    "woman_id", "year", "quarter", "year_quarter", "estrato4", "person_weight", "age",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14", "employed",
    "domestic_care_reason", "wanted_to_work", "wants_more_hours", "worked_last_12m",
    "effective_hours", "visit1_n_rooms", "visit1_n_bedrooms", "visit1_cooking_wood_charcoal",
    "visit1_cooking_gas_any", "visit1_vehicle", "visit1_computer", "visit1_internet_home",
    "visit1_same_quarter", "carry_abs_distance_q"
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
    wanted_to_work,
    wants_more_hours,
    worked_last_12m,
    effective_hours,
    visit1_n_rooms,
    visit1_n_bedrooms,
    visit1_cooking_wood_charcoal,
    visit1_cooking_gas_any,
    visit1_vehicle,
    visit1_computer,
    visit1_internet_home,
    visit1_same_quarter,
    carry_abs_distance_q
  ) %>%
  filter(((year == 2021 & quarter == 4) | year >= 2022), year <= 2024, !is.na(person_weight), person_weight > 0) %>%
  mutate(
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0),
    low_assets = visit1_vehicle == 0 & visit1_computer == 0 & visit1_internet_home == 0
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter,
        z_pre_people_post, z_delta_people_post, high_delta_p75_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

sample_defs <- tibble::tribble(
  ~sample, ~filter_expr,
  "all", "TRUE",
  "wood_visit1", "visit1_cooking_wood_charcoal == 1",
  "kids", "n_children_0_14 > 0",
  "wood_kids", "visit1_cooking_wood_charcoal == 1 & n_children_0_14 > 0",
  "low_assets", "low_assets == TRUE",
  "carry_le1", "carry_abs_distance_q <= 1",
  "same_quarter", "visit1_same_quarter == TRUE"
)

outcomes <- tibble::tribble(
  ~outcome, ~preferred_sign,
  "domestic_care_reason", -1,
  "wanted_to_work", -1,
  "wants_more_hours", -1,
  "employed", 1,
  "worked_last_12m", 1,
  "effective_hours_population", 1
)

treatments <- tibble::tribble(
  ~treatment, ~design,
  "z_pre_people_post", "pre_exposure_post",
  "z_delta_people_post", "realized_delta_post",
  "high_delta_p75_post", "high_delta_p75_post"
)

cross_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + age + age_sq + female_secondaryplus + female_tertiary"
)

cross_rows <- list()
c_i <- 1L

for (s in seq_len(nrow(sample_defs))) {
  sample_name <- sample_defs$sample[[s]]
  sample_filter <- rlang::parse_expr(sample_defs$filter_expr[[s]])
  dat_sample <- women %>% filter(!!sample_filter)

  for (i in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[i]]
    preferred_sign <- outcomes$preferred_sign[[i]]

    for (j in seq_len(nrow(treatments))) {
      treatment <- treatments$treatment[[j]]
      design <- treatments$design[[j]]

      for (k in seq_len(nrow(cross_specs))) {
        spec <- cross_specs$spec[[k]]
        controls <- cross_specs$controls[[k]]
        needed_controls <- all.vars(as.formula(paste("~", controls)))
        needed_vars <- unique(c("estrato4", "year_quarter", "person_weight", outcome, treatment, needed_controls))

        reg_data <- dat_sample %>%
          filter(if_all(all_of(needed_vars), ~ !is.na(.x)))

        if (nrow(reg_data) == 0) {
          next
        }

        model <- feols(
          as.formula(paste0(outcome, " ~ ", treatment, " + ", controls, " | estrato4 + year_quarter")),
          data = reg_data,
          weights = ~person_weight,
          cluster = ~estrato4
        )

        term <- extract_term(model, treatment)

        cross_rows[[c_i]] <- tibble(
          sample = sample_name,
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
        c_i <- c_i + 1L
      }
    }
  }
}

cross_results <- bind_rows(cross_rows) %>%
  arrange(p)

write.table(
  cross_results,
  file.path(out_dir, "women_subgroup_cross_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

cross_hits <- cross_results %>%
  filter(sensible_sign, p < 0.05) %>%
  group_by(sample, outcome, design, treatment) %>%
  slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
  ungroup()

fe_rows <- list()
f_i <- 1L

for (i in seq_len(nrow(cross_hits))) {
  sample_name <- cross_hits$sample[[i]]
  outcome <- cross_hits$outcome[[i]]
  preferred_sign <- cross_hits$preferred_sign[[i]]
  treatment <- cross_hits$treatment[[i]]
  design <- cross_hits$design[[i]]
  sample_filter <- rlang::parse_expr(sample_defs$filter_expr[[match(sample_name, sample_defs$sample)]])

  reg_data <- women %>%
    filter(!!sample_filter) %>%
    filter(if_all(all_of(c("woman_id", "estrato4", "year_quarter", "person_weight", outcome, treatment, "hh_size", "n_children_0_14", "age", "age_sq")), ~ !is.na(.x)))

  if (nrow(reg_data) == 0) {
    next
  }

  model <- feols(
    as.formula(paste0(outcome, " ~ ", treatment, " + hh_size + n_children_0_14 + age + age_sq | woman_id + year_quarter")),
    data = reg_data,
    weights = ~person_weight,
    cluster = ~estrato4
  )

  term <- extract_term(model, treatment)

  fe_rows[[f_i]] <- tibble(
    sample = sample_name,
    outcome = outcome,
    design = design,
    treatment = treatment,
    spec = "WFE",
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
  f_i <- f_i + 1L
}

fe_results <- bind_rows(fe_rows) %>%
  arrange(p)

write.table(
  fe_results,
  file.path(out_dir, "women_subgroup_fe_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

confirmed_hits <- cross_hits %>%
  select(sample, outcome, design, treatment, spec, cross_coef = coef, cross_se = se, cross_p = p) %>%
  inner_join(
    fe_results %>%
      select(sample, outcome, design, treatment, fe_coef = coef, fe_se = se, fe_p = p, fe_sensible = sensible_sign),
    by = c("sample", "outcome", "design", "treatment")
  ) %>%
  filter(fe_sensible, fe_p < 0.10) %>%
  arrange(fe_p, cross_p)

write.table(
  confirmed_hits,
  file.path(out_dir, "women_subgroup_confirmed_hits.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

best_results <- bind_rows(cross_results, fe_results) %>%
  filter(sensible_sign) %>%
  group_by(sample, outcome) %>%
  slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  arrange(p)

write.table(
  best_results,
  file.path(out_dir, "women_subgroup_best_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

sample_summary <- women %>%
  summarise(
    all = n(),
    wood_visit1 = sum(visit1_cooking_wood_charcoal == 1, na.rm = TRUE),
    kids = sum(n_children_0_14 > 0, na.rm = TRUE),
    wood_kids = sum(visit1_cooking_wood_charcoal == 1 & n_children_0_14 > 0, na.rm = TRUE),
    low_assets = sum(low_assets == TRUE, na.rm = TRUE),
    carry_le1 = sum(carry_abs_distance_q <= 1, na.rm = TRUE),
    same_quarter = sum(visit1_same_quarter == TRUE, na.rm = TRUE)
  ) %>%
  tidyr::pivot_longer(cols = everything(), names_to = "sample", values_to = "n_rows")

write.table(
  sample_summary,
  file.path(out_dir, "women_subgroup_sample_sizes.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

plot_df <- best_results %>%
  filter(p < 0.10) %>%
  mutate(
    outcome_label = recode(
      outcome,
      domestic_care_reason = "Domestic care reason",
      wanted_to_work = "Wanted to work",
      wants_more_hours = "Wanted more hours",
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
    conf_low = coef - 1.96 * se,
    conf_high = coef + 1.96 * se
  )

if (nrow(plot_df) > 0) {
  p <- ggplot(plot_df, aes(x = reorder(sample, coef), y = coef, color = model_family)) +
    geom_hline(yintercept = 0, color = "gray30") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, linewidth = 0.6) +
    geom_point(size = 2.1) +
    coord_flip() +
    facet_grid(outcome_label ~ design_label, scales = "free_y") +
    scale_color_brewer(palette = "Dark2") +
    labs(
      x = NULL,
      y = "Coefficient",
      color = NULL,
      title = "Best Women-Level Gas Results by Subgroup"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")

  ggsave(
    file.path(fig_dir, "auxilio_gas_women_subgroup_best.png"),
    p,
    width = 12,
    height = 10,
    dpi = 300
  )

  ggsave(
    file.path(fig_dir, "auxilio_gas_women_subgroup_best.pdf"),
    p,
    width = 12,
    height = 10
  )
}

kids_domcare_trends <- women %>%
  filter(n_children_0_14 > 0, !is.na(high_delta_p75_post)) %>%
  mutate(high_delta_p75 = as.integer(high_delta_p75_post > 0)) %>%
  group_by(year, quarter, year_quarter, high_delta_p75) %>%
  summarise(
    domestic_care_reason = safe_weighted_mean(domestic_care_reason, person_weight),
    .groups = "drop"
  ) %>%
  mutate(
    quarter_date = as.Date(sprintf("%d-%02d-01", year, c(1, 4, 7, 10)[quarter])),
    topup_group = if_else(high_delta_p75 == 1, "Top quartile top-up growth", "Other estratos")
  )

kids_domcare_plot <- ggplot(kids_domcare_trends, aes(x = quarter_date, y = domestic_care_reason, color = topup_group)) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date("2022-07-01"), linetype = "dashed", color = "gray30") +
  scale_color_manual(values = c("Other estratos" = "#7a7a7a", "Top quartile top-up growth" = "#1f78b4")) +
  labs(
    x = NULL,
    y = "Weighted mean",
    color = NULL,
    title = "Domestic Care Reason Among Mothers",
    subtitle = "Top quartile is defined by the post-2022Q3 increase in Auxilio Gas people per weighted household"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom")

ggsave(
  file.path(fig_dir, "auxilio_gas_kids_domcare_trends.png"),
  kids_domcare_plot,
  width = 9,
  height = 5.5,
  dpi = 300
)

ggsave(
  file.path(fig_dir, "auxilio_gas_kids_domcare_trends.pdf"),
  kids_domcare_plot,
  width = 9,
  height = 5.5
)

print(sample_summary)
print(cross_results %>% filter(sensible_sign) %>% arrange(p))
print(fe_results %>% filter(sensible_sign) %>% arrange(p))
print(confirmed_hits)
print(best_results)
