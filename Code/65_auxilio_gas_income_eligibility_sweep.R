#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(readr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_income_eligibility_sweep")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

income_proxy_path <- here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_household_labor_income_proxy_2021q4_2024q4.parquet")
input_txt <- here("data", "PNAD-C", "raw", "input_PNADC_trimestral.txt")

extract_layout_line <- function(input_lines, var_name) {
  hits <- grep(paste0("^@[0-9]+\\s+", var_name, "\\s"), input_lines, value = TRUE)
  if (length(hits) == 0) {
    stop(sprintf("Could not find layout line for %s", var_name))
  }
  hits[[1]]
}

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

half_minimum_wage <- function(year, quarter) {
  case_when(
    year == 2021 ~ 550,
    year == 2022 ~ 606,
    year == 2023 & quarter == 1 ~ 651,
    year == 2023 & quarter >= 2 ~ 660,
    year == 2024 ~ 706,
    TRUE ~ NA_real_
  )
}

if (file.exists(income_proxy_path)) {
  income_proxy <- read_parquet(income_proxy_path)
} else {
  txt_files <- sort(Sys.glob(here("data", "PNAD-C", "raw", "PNADC_*20*.txt")))
  txt_files <- txt_files[grepl("PNADC_(042021|0[1-4]2022|0[1-4]2023|0[1-4]2024)\\.txt$", basename(txt_files))]

  vars <- c("Ano", "Trimestre", "UPA", "V1014", "V1008", "V2003", "VD4019")
  input_lines <- readLines(input_txt, warn = FALSE, encoding = "latin1")

  layout <- bind_rows(lapply(vars, function(v) {
    line <- extract_layout_line(input_lines, v)
    m <- regmatches(line, regexec("@([0-9]+)\\s+([A-Za-z0-9]+)\\s+\\$?([0-9]+)\\.", line))[[1]]
    tibble(
      var = m[[3]],
      start = as.integer(m[[2]]),
      end = as.integer(m[[2]]) + as.integer(m[[4]]) - 1L
    )
  }))

  fwf <- fwf_positions(layout$start, layout$end, col_names = layout$var)

  hh_income_list <- lapply(txt_files, function(tf) {
    dat <- read_fwf(
      file = tf,
      col_positions = fwf,
      col_types = paste(rep("c", nrow(layout)), collapse = ""),
      locale = locale(encoding = "latin1"),
      progress = FALSE,
      trim_ws = FALSE
    ) %>%
      transmute(
        year = as.integer(Ano),
        quarter = as.integer(Trimestre),
        hh_match_key = paste(as.integer(Ano), as.integer(Trimestre), UPA, V1014, V1008, sep = "_"),
        person_number = V2003,
        labor_income_habitual = suppressWarnings(as.numeric(VD4019))
      )

    dat %>%
      group_by(year, quarter, hh_match_key) %>%
      summarise(
        hh_labor_income_habitual = sum(coalesce(labor_income_habitual, 0), na.rm = TRUE),
        hh_member_count_income = n(),
        hh_positive_income_earners = sum(!is.na(labor_income_habitual) & labor_income_habitual > 0, na.rm = TRUE),
        .groups = "drop"
      )
  })

  income_proxy <- bind_rows(hh_income_list)
  write_parquet(income_proxy, income_proxy_path)
}

auxgas_q <- read_parquet(
  here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet"),
  col_select = c("estrato4", "year", "quarter", "year_quarter", "auxgas_families", "auxgas_people")
) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter,
    auxgas_families,
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
    auxgas_families = coalesce(auxgas_families, 0),
    auxgas_people = coalesce(auxgas_people, 0),
    families_per_weighted_household = auxgas_families / weight_sum,
    people_per_weighted_household = auxgas_people / weight_sum,
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3))
  ) %>%
  filter(year >= 2021, year <= 2024, !is.na(weight_sum), weight_sum > 0)

pre_exposure <- treatment_panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_families_per_weighted_household = mean(families_per_weighted_household, na.rm = TRUE),
    pre_people_per_weighted_household = mean(people_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

post_exposure <- treatment_panel %>%
  filter(post_topup_2022q3 == 1) %>%
  group_by(estrato4) %>%
  summarise(
    post_families_per_weighted_household = mean(families_per_weighted_household, na.rm = TRUE),
    post_people_per_weighted_household = mean(people_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(pre_exposure, by = "estrato4") %>%
  mutate(
    delta_families_per_weighted_household = post_families_per_weighted_household - pre_families_per_weighted_household,
    delta_people_per_weighted_household = post_people_per_weighted_household - pre_people_per_weighted_household
  )

treatment_panel <- treatment_panel %>%
  left_join(post_exposure, by = "estrato4") %>%
  mutate(
    z_pre_families_post = pre_families_per_weighted_household * post_topup_2022q3,
    z_delta_families_post = delta_families_per_weighted_household * post_topup_2022q3,
    z_pre_people_post = pre_people_per_weighted_household * post_topup_2022q3,
    z_delta_people_post = delta_people_per_weighted_household * post_topup_2022q3
  )

treatments <- tibble::tribble(
  ~treatment, ~design,
  "z_pre_families_post", "pre_families_post",
  "z_delta_families_post", "delta_families_post",
  "z_pre_people_post", "pre_people_post",
  "z_delta_people_post", "delta_people_post"
)

households <- read_parquet(
  here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet"),
  col_select = c(
    "year", "quarter", "year_quarter", "estrato4", "upa", "panel", "household_number", "weight_calibrated",
    "n_rooms", "n_bedrooms", "electricity_grid_full_time",
    "cooking_gas_any", "cooking_gas_bottled", "cooking_wood_charcoal"
  )
) %>%
  transmute(
    year,
    quarter,
    year_quarter = gsub("-", "", year_quarter),
    estrato4 = as.character(estrato4),
    hh_match_key = paste(year, quarter, upa, panel, household_number, sep = "_"),
    weight_calibrated,
    n_rooms,
    n_bedrooms,
    electricity_grid_full_time,
    cooking_gas_any,
    cooking_gas_bottled,
    cooking_wood_charcoal
  ) %>%
  filter(year >= 2022, year <= 2024, !is.na(weight_calibrated), weight_calibrated > 0) %>%
  left_join(income_proxy, by = c("year", "quarter", "hh_match_key")) %>%
  mutate(
    hh_size_final = as.numeric(hh_member_count_income),
    hh_labor_income_pc = if_else(hh_size_final > 0, hh_labor_income_habitual / hh_size_final, NA_real_),
    half_min_wage = half_minimum_wage(year, quarter),
    eligible_05mw_labor = !is.na(hh_labor_income_pc) & !is.na(half_min_wage) & hh_labor_income_pc <= half_min_wage
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter,
        z_pre_families_post, z_delta_families_post,
        z_pre_people_post, z_delta_people_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

household_outcomes <- tibble::tribble(
  ~outcome, ~preferred_sign,
  "cooking_gas_any", 1,
  "cooking_gas_bottled", 1,
  "cooking_wood_charcoal", -1
)

household_samples <- tibble::tribble(
  ~sample, ~filter_expr,
  "all_households", "TRUE",
  "eligible_05mw_labor_households", "eligible_05mw_labor == TRUE"
)

household_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "n_rooms + n_bedrooms + electricity_grid_full_time"
)

household_rows <- list()
h_i <- 1L

for (s in seq_len(nrow(household_samples))) {
  sample_name <- household_samples$sample[[s]]
  sample_filter <- rlang::parse_expr(household_samples$filter_expr[[s]])
  dat_sample <- households %>% filter(!!sample_filter)

  for (i in seq_len(nrow(household_outcomes))) {
    outcome <- household_outcomes$outcome[[i]]
    preferred_sign <- household_outcomes$preferred_sign[[i]]

    for (j in seq_len(nrow(treatments))) {
      treatment <- treatments$treatment[[j]]
      design <- treatments$design[[j]]

      for (k in seq_len(nrow(household_specs))) {
        spec <- household_specs$spec[[k]]
        controls <- household_specs$controls[[k]]
        needed_controls <- all.vars(as.formula(paste("~", controls)))
        needed_vars <- unique(c("estrato4", "year_quarter", "weight_calibrated", outcome, treatment, needed_controls))

        reg_data <- dat_sample %>%
          filter(if_all(all_of(needed_vars), ~ !is.na(.x)))

        if (nrow(reg_data) == 0) {
          next
        }

        model <- feols(
          as.formula(paste0(outcome, " ~ ", treatment, " + ", controls, " | estrato4 + year_quarter")),
          data = reg_data,
          weights = ~weight_calibrated,
          cluster = ~estrato4
        )

        term <- extract_term(model, treatment)

        household_rows[[h_i]] <- tibble(
          sample = sample_name,
          outcome = outcome,
          design = design,
          treatment = treatment,
          spec = spec,
          model_family = "household_cross_section_fe",
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
        h_i <- h_i + 1L
      }
    }
  }
}

household_results <- bind_rows(household_rows) %>%
  arrange(p)

write.table(
  household_results,
  file.path(out_dir, "income_eligibility_household_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

women <- read_parquet(
  here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet"),
  col_select = c(
    "woman_id", "hh_match_key", "year", "quarter", "year_quarter", "estrato4", "person_weight", "age",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14", "employed",
    "domestic_care_reason", "wanted_to_work", "wants_more_hours", "worked_last_12m",
    "effective_hours", "visit1_n_rooms", "visit1_n_bedrooms", "visit1_cooking_wood_charcoal"
  )
) %>%
  transmute(
    woman_id,
    hh_match_key,
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
    visit1_cooking_wood_charcoal
  ) %>%
  filter(((year == 2021 & quarter == 4) | year >= 2022), year <= 2024, !is.na(person_weight), person_weight > 0) %>%
  left_join(income_proxy, by = c("year", "quarter", "hh_match_key")) %>%
  mutate(
    hh_size_final = coalesce(as.numeric(hh_size), as.numeric(hh_member_count_income)),
    hh_labor_income_pc = if_else(hh_size_final > 0, hh_labor_income_habitual / hh_size_final, NA_real_),
    half_min_wage = half_minimum_wage(year, quarter),
    eligible_05mw_labor = !is.na(hh_labor_income_pc) & !is.na(half_min_wage) & hh_labor_income_pc <= half_min_wage,
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter,
        z_pre_families_post, z_delta_families_post,
        z_pre_people_post, z_delta_people_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

sample_defs <- tibble::tribble(
  ~sample, ~filter_expr,
  "all", "TRUE",
  "eligible_05mw_labor", "eligible_05mw_labor == TRUE",
  "eligible_05mw_labor_kids", "eligible_05mw_labor == TRUE & n_children_0_14 > 0",
  "eligible_05mw_labor_wood", "eligible_05mw_labor == TRUE & visit1_cooking_wood_charcoal == 1"
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

cross_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "hh_size_final + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + age + age_sq + female_secondaryplus + female_tertiary"
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
  file.path(out_dir, "income_eligibility_cross_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

cross_hits <- cross_results %>%
  filter(sensible_sign, p < 0.10) %>%
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
    filter(if_all(all_of(c("woman_id", "estrato4", "year_quarter", "person_weight", outcome, treatment, "hh_size_final", "n_children_0_14", "age", "age_sq")), ~ !is.na(.x)))

  if (nrow(reg_data) == 0) {
    next
  }

  model <- feols(
    as.formula(paste0(outcome, " ~ ", treatment, " + hh_size_final + n_children_0_14 + age + age_sq | woman_id + year_quarter")),
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
  file.path(out_dir, "income_eligibility_fe_results.tsv"),
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
  file.path(out_dir, "income_eligibility_confirmed_hits.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

sample_summary <- women %>%
  mutate(
    eligible_flag = as.integer(coalesce(eligible_05mw_labor, FALSE)),
    eligible_kids_flag = as.integer(coalesce(eligible_05mw_labor, FALSE) & coalesce(n_children_0_14, 0) > 0),
    eligible_wood_flag = as.integer(coalesce(eligible_05mw_labor, FALSE) & coalesce(visit1_cooking_wood_charcoal, 0) == 1)
  ) %>%
  summarise(
    all = n(),
    eligible_05mw_labor = sum(eligible_flag, na.rm = TRUE),
    eligible_05mw_labor_kids = sum(eligible_kids_flag, na.rm = TRUE),
    eligible_05mw_labor_wood = sum(eligible_wood_flag, na.rm = TRUE),
    mean_hh_labor_income_pc = mean(hh_labor_income_pc, na.rm = TRUE),
    median_hh_labor_income_pc = median(hh_labor_income_pc, na.rm = TRUE),
    share_eligible_05mw_labor = mean(eligible_flag, na.rm = TRUE)
  )

write.table(
  sample_summary,
  file.path(out_dir, "income_eligibility_sample_summary.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

if (nrow(confirmed_hits) > 0) {
  top_hit <- confirmed_hits %>% slice(1)

  plot_df <- women %>%
    filter(eligible_05mw_labor == TRUE, n_children_0_14 > 0) %>%
    group_by(year, quarter) %>%
    summarise(
      domestic_care_reason = safe_weighted_mean(domestic_care_reason, person_weight),
      .groups = "drop"
    ) %>%
    mutate(
      quarter_date = as.Date(sprintf("%d-%02d-01", year, c(1, 4, 7, 10)[quarter]))
    )

  p <- ggplot(plot_df, aes(x = quarter_date, y = domestic_care_reason)) +
    geom_line(linewidth = 0.9, color = "#1f78b4") +
    geom_vline(xintercept = as.Date("2022-07-01"), linetype = "dashed", color = "gray30") +
    labs(
      x = NULL,
      y = "Weighted mean",
      title = "Domestic Care Reason in Eligible Mothers",
      subtitle = "Eligible is proxied by household labor income per capita <= one-half minimum wage"
    ) +
    theme_minimal(base_size = 12)

  ggsave(
    file.path(fig_dir, "auxilio_gas_income_eligible_mothers_domcare.png"),
    p,
    width = 9,
    height = 5.5,
    dpi = 300
  )

  ggsave(
    file.path(fig_dir, "auxilio_gas_income_eligible_mothers_domcare.pdf"),
    p,
    width = 9,
    height = 5.5
  )
}

print(sample_summary)
print(cross_results %>% filter(sensible_sign) %>% arrange(p))
print(fe_results %>% filter(sensible_sign) %>% arrange(p))
print(confirmed_hits)
