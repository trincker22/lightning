#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_domcare_triple_diff")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

visit1_proxy_path <- here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_visit1_household_eligibility_proxy_2022_2024.parquet")
if (!file.exists(visit1_proxy_path)) {
  stop("Missing visit-1 eligibility proxy cache: ", visit1_proxy_path, call. = FALSE)
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
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
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

visit1_proxy <- read_parquet(visit1_proxy_path)

auxgas_q <- read_parquet(
  here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet"),
  col_select = c("estrato4", "year", "quarter", "year_quarter", "auxgas_families")
) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter,
    auxgas_families
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
    families_per_weighted_household = auxgas_families / weight_sum,
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3))
  ) %>%
  filter(year >= 2021, year <= 2024, !is.na(weight_sum), weight_sum > 0)

pre_exposure <- treatment_panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_families_per_weighted_household = mean(families_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

treatment_panel <- treatment_panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  select(
    estrato4, year, quarter, year_quarter,
    post_topup_2022q3,
    pre_families_per_weighted_household
  )

women <- read_parquet(
  here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet"),
  col_select = c(
    "woman_id", "year", "quarter", "year_quarter", "estrato4", "person_weight", "age",
    "visit1_year", "visit1_quarter", "visit1_upa", "visit1_panel", "visit1_household_number",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14",
    "domestic_care_reason", "outlf_domestic_care",
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
    visit1_year,
    visit1_quarter,
    visit1_upa,
    visit1_panel,
    visit1_household_number,
    female_secondaryplus,
    female_tertiary,
    hh_size,
    n_children_0_14,
    domestic_care_reason,
    outlf_domestic_care,
    visit1_n_rooms,
    visit1_n_bedrooms
  ) %>%
  filter(((year == 2021 & quarter == 4) | year >= 2022), year <= 2024, !is.na(person_weight), person_weight > 0) %>%
  mutate(
    visit1_hh_match_key = if_else(
      !is.na(visit1_year) & !is.na(visit1_quarter) & !is.na(visit1_upa) & !is.na(visit1_panel) & !is.na(visit1_household_number),
      paste(as.integer(visit1_year), as.integer(visit1_quarter), visit1_upa, visit1_panel, visit1_household_number, sep = "_"),
      NA_character_
    )
  ) %>%
  left_join(
    visit1_proxy %>%
      rename(
        visit1_year = year,
        visit1_quarter = quarter,
        visit1_hh_match_key = hh_match_key,
        visit1_has_bpc = has_bpc_visit1,
        visit1_has_bolsa = has_bolsa_visit1,
        visit1_vd5008_pc = vd5008_pc
      ),
    by = c("visit1_year", "visit1_quarter", "visit1_hh_match_key")
  ) %>%
  mutate(
    visit1_has_bpc = coalesce(visit1_has_bpc, 0L),
    visit1_has_bolsa = coalesce(visit1_has_bolsa, 0L),
    visit1_half_min_wage = half_minimum_wage(visit1_year, visit1_quarter),
    eligible_vd5008_only = !is.na(visit1_vd5008_pc) & !is.na(visit1_half_min_wage) & visit1_vd5008_pc <= visit1_half_min_wage,
    eligible_vd5008_only_no_bpc = !is.na(visit1_vd5008_pc) & !is.na(visit1_half_min_wage) & visit1_vd5008_pc <= visit1_half_min_wage & visit1_has_bpc == 0,
    eligible_vd5008_bolsa_no_bpc = !is.na(visit1_vd5008_pc) & !is.na(visit1_half_min_wage) & visit1_vd5008_pc <= visit1_half_min_wage & visit1_has_bolsa == 1 & visit1_has_bpc == 0,
    hh_size_final = as.numeric(hh_size),
    age_sq = age^2
  ) %>%
  left_join(
    treatment_panel,
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

eligibility_defs <- tibble::tribble(
  ~eligible_var, ~eligible_label,
  "eligible_vd5008_only", "VD5008 women",
  "eligible_vd5008_only_no_bpc", "VD5008 women, no BPC",
  "eligible_vd5008_bolsa_no_bpc", "VD5008 + Bolsa women, no BPC"
)

outcomes <- tibble::tribble(
  ~outcome_var, ~outcome, ~outcome_label,
  "domestic_care_reason", "female_domestic_care_reason", "Female domestic care reason",
  "outlf_domestic_care", "outlf_domestic_care", "Out of labor force for domestic care"
)

specs <- tibble::tribble(
  ~spec, ~spec_label, ~controls,
  "M1", "No controls", "1",
  "M2", "Women controls", "hh_size_final + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + age + age_sq + female_secondaryplus + female_tertiary"
)

rows <- list()
i <- 1L

for (e in seq_len(nrow(eligibility_defs))) {
  eligible_var <- eligibility_defs$eligible_var[[e]]
  eligible_label <- eligibility_defs$eligible_label[[e]]

  for (o in seq_len(nrow(outcomes))) {
    outcome_var <- outcomes$outcome_var[[o]]
    outcome_name <- outcomes$outcome[[o]]
    outcome_label <- outcomes$outcome_label[[o]]

    for (s in seq_len(nrow(specs))) {
      spec <- specs$spec[[s]]
      spec_label <- specs$spec_label[[s]]
      controls <- specs$controls[[s]]
      needed_controls <- all.vars(as.formula(paste("~", controls)))
      needed_vars <- unique(c(
        "estrato4", "year_quarter", "person_weight", outcome_var,
        "post_topup_2022q3", "pre_families_per_weighted_household",
        eligible_var, needed_controls
      ))

      reg_data <- women %>%
        filter(if_all(all_of(needed_vars), ~ !is.na(.x))) %>%
        mutate(
          eligible_flag = as.integer(.data[[eligible_var]]),
          eligible_pre = eligible_flag * pre_families_per_weighted_household,
          eligible_post = eligible_flag * post_topup_2022q3,
          pre_post = pre_families_per_weighted_household * post_topup_2022q3,
          triple_term = eligible_flag * pre_families_per_weighted_household * post_topup_2022q3
        )

      if (nrow(reg_data) == 0) {
        next
      }

      model <- feols(
        as.formula(paste0(
          outcome_var,
          " ~ eligible_flag + eligible_pre + eligible_post + pre_post + triple_term + ",
          controls,
          " | estrato4 + year_quarter"
        )),
        data = reg_data,
        weights = ~person_weight,
        cluster = ~estrato4
      )

      term <- extract_term(model, "triple_term")

      rows[[i]] <- tibble(
        sample = "all_women",
        sample_label = "All women",
        eligible_var = eligible_var,
        eligible_label = eligible_label,
        outcome = outcome_name,
        outcome_var = outcome_var,
        outcome_label = outcome_label,
        design = "eligible_pre_post_triple_diff",
        design_label = "Eligible x pre exposure x post",
        spec = spec,
        spec_label = spec_label,
        model_family = "women_triple_diff_fe",
        nobs = nobs(model),
        coef = term$estimate,
        se = term$std.error,
        p = term$p.value,
        weighted_mean_outcome = safe_weighted_mean(reg_data[[outcome_var]], reg_data$person_weight)
      )
      i <- i + 1L
    }
  }
}

results <- bind_rows(rows) %>%
  arrange(outcome, p)

write.table(
  results,
  file.path(out_dir, "domcare_triple_diff_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

paper_table <- results %>%
  filter(spec == "M2") %>%
  transmute(
    panel = "Domestic care",
    sample = eligible_var,
    sample_label = eligible_label,
    outcome,
    outcome_label,
    design,
    design_label,
    spec_label,
    nobs,
    coef,
    se,
    p,
    coef_per_10_families_per_100_hh = coef * 0.1,
    se_per_10_families_per_100_hh = se * 0.1,
    weighted_mean_outcome
  ) %>%
  mutate(
    sample_order = case_when(
      sample == "eligible_vd5008_only" ~ 1L,
      sample == "eligible_vd5008_only_no_bpc" ~ 2L,
      sample == "eligible_vd5008_bolsa_no_bpc" ~ 3L,
      TRUE ~ 4L
    ),
    outcome_order = case_when(
      outcome == "female_domestic_care_reason" ~ 1L,
      outcome == "outlf_domestic_care" ~ 2L,
      TRUE ~ 3L
    )
  ) %>%
  arrange(outcome_order, sample_order) %>%
  select(-sample_order, -outcome_order)

write.table(
  paper_table,
  file.path(out_dir, "domcare_triple_diff_paper_table.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

sample_summary <- women %>%
  mutate(
    eligible_vd5008_only_flag = as.integer(coalesce(eligible_vd5008_only, FALSE)),
    eligible_vd5008_only_no_bpc_flag = as.integer(coalesce(eligible_vd5008_only_no_bpc, FALSE)),
    eligible_vd5008_bolsa_no_bpc_flag = as.integer(coalesce(eligible_vd5008_bolsa_no_bpc, FALSE))
  ) %>%
  summarise(
    all = n(),
    eligible_vd5008_only = sum(eligible_vd5008_only_flag, na.rm = TRUE),
    eligible_vd5008_only_no_bpc = sum(eligible_vd5008_only_no_bpc_flag, na.rm = TRUE),
    eligible_vd5008_bolsa_no_bpc = sum(eligible_vd5008_bolsa_no_bpc_flag, na.rm = TRUE)
  )

write.table(
  sample_summary,
  file.path(out_dir, "domcare_triple_diff_sample_summary.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

print(sample_summary)
print(results %>% filter(spec == "M2") %>% arrange(outcome, p))
