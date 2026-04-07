#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_primary_fuel_sweep")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

visit1_proxy_path <- here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_visit1_household_eligibility_proxy_2022_2024.parquet")

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

first_nonmissing <- function(x) {
  y <- x[!is.na(x) & x != ""]
  if (length(y) == 0) {
    return(NA)
  }
  y[[1]]
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

is_gas_main <- function(x) {
  case_when(
    is.na(x) ~ NA_integer_,
    grepl("g[aá]s", x, ignore.case = TRUE, perl = TRUE) ~ 1L,
    TRUE ~ 0L
  )
}

is_wood_main <- function(x) {
  case_when(
    is.na(x) ~ NA_integer_,
    grepl("lenha|carv", x, ignore.case = TRUE, perl = TRUE) ~ 1L,
    TRUE ~ 0L
  )
}

is_electric_main <- function(x) {
  case_when(
    is.na(x) ~ NA_integer_,
    grepl("energia el[eé]trica", x, ignore.case = TRUE, perl = TRUE) ~ 1L,
    TRUE ~ 0L
  )
}

if (file.exists(visit1_proxy_path)) {
  visit1_proxy <- read_parquet(visit1_proxy_path)
} else {
  visit1_txt_files <- sort(Sys.glob(here("data", "PNAD-C", "raw", "PNADC_202[2-4]_visita1.txt")))

  visit1_proxy <- bind_rows(lapply(visit1_txt_files, function(tf) {
    year_tag <- sub("^PNADC_([0-9]{4})_visita1\\.txt$", "\\1", basename(tf))
    input_file <- Sys.glob(here("data", "PNAD-C", "raw", paste0("input_PNADC_", year_tag, "_visita1_*.txt")))[1]
    input_lines <- readLines(input_file, warn = FALSE, encoding = "latin1")
    vars <- c("Ano", "Trimestre", "UPA", "V1014", "V1008", "V5001A", "V5002A", "V5003A", "VD5002", "VD5008")

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

    read_fwf(
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
        bpc_flag = V5001A,
        bolsa_flag = V5002A,
        other_prog_flag = V5003A,
        vd5002_pc = suppressWarnings(as.numeric(VD5002)),
        vd5008_pc = suppressWarnings(as.numeric(VD5008))
      ) %>%
      group_by(year, quarter, hh_match_key) %>%
      summarise(
        has_bpc_visit1 = as.integer(any(bpc_flag == "1", na.rm = TRUE)),
        has_bolsa_visit1 = as.integer(any(bolsa_flag == "1", na.rm = TRUE)),
        has_other_prog_visit1 = as.integer(any(other_prog_flag == "1", na.rm = TRUE)),
        vd5002_pc = first_nonmissing(vd5002_pc),
        vd5008_pc = first_nonmissing(vd5008_pc),
        .groups = "drop"
      )
  }))

  write_parquet(visit1_proxy, visit1_proxy_path)
}

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

post_exposure <- treatment_panel %>%
  filter(post_topup_2022q3 == 1) %>%
  group_by(estrato4) %>%
  summarise(
    post_families_per_weighted_household = mean(families_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(pre_exposure, by = "estrato4") %>%
  mutate(
    delta_families_per_weighted_household = post_families_per_weighted_household - pre_families_per_weighted_household
  )

treatment_panel <- treatment_panel %>%
  left_join(post_exposure, by = "estrato4") %>%
  mutate(
    z_pre_families_post = pre_families_per_weighted_household * post_topup_2022q3,
    z_delta_families_post = delta_families_per_weighted_household * post_topup_2022q3
  )

treatments <- tibble::tribble(
  ~treatment, ~design,
  "z_pre_families_post", "pre_families_post",
  "z_delta_families_post", "delta_families_post"
)

households <- read_parquet(
  here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet"),
  col_select = c(
    "year", "quarter", "year_quarter", "estrato4", "upa", "panel", "household_number", "weight_calibrated",
    "n_rooms", "n_bedrooms", "electricity_grid_full_time", "cooking_main_fuel"
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
    cooking_main_fuel
  ) %>%
  filter(year >= 2022, year <= 2024, !is.na(weight_calibrated), weight_calibrated > 0) %>%
  left_join(visit1_proxy, by = c("year", "quarter", "hh_match_key")) %>%
  mutate(
    half_min_wage = half_minimum_wage(year, quarter),
    eligible_vd5008_or_bpc = ((!is.na(vd5008_pc) & !is.na(half_min_wage) & vd5008_pc <= half_min_wage) | has_bpc_visit1 == 1),
    eligible_vd5008_bolsa_or_bpc = (((!is.na(vd5008_pc) & !is.na(half_min_wage) & vd5008_pc <= half_min_wage) & has_bolsa_visit1 == 1) | has_bpc_visit1 == 1),
    gas_primary = is_gas_main(cooking_main_fuel),
    wood_primary = is_wood_main(cooking_main_fuel),
    electricity_primary = is_electric_main(cooking_main_fuel),
    modern_primary = case_when(
      is.na(cooking_main_fuel) ~ NA_integer_,
      gas_primary == 1L | electricity_primary == 1L ~ 1L,
      TRUE ~ 0L
    )
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter,
        z_pre_families_post, z_delta_families_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

household_outcomes <- tibble::tribble(
  ~outcome, ~preferred_sign,
  "gas_primary", 1,
  "wood_primary", -1,
  "modern_primary", 1
)

household_samples <- tibble::tribble(
  ~sample, ~filter_expr,
  "all_households", "TRUE",
  "eligible_vd5008_or_bpc_households", "eligible_vd5008_or_bpc == TRUE",
  "eligible_vd5008_bolsa_or_bpc_households", "eligible_vd5008_bolsa_or_bpc == TRUE"
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
          spec_label = if_else(spec == "M2", "HH controls", "No controls"),
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
  file.path(out_dir, "primary_fuel_household_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

women <- read_parquet(
  here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet"),
  col_select = c(
    "woman_id", "year", "quarter", "year_quarter", "estrato4", "person_weight", "age",
    "visit1_year", "visit1_quarter", "visit1_upa", "visit1_panel", "visit1_household_number",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14", "employed",
    "domestic_care_reason", "wanted_to_work", "wants_more_hours", "worked_last_12m",
    "effective_hours", "visit1_n_rooms", "visit1_n_bedrooms", "visit1_cooking_main_fuel"
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
    employed,
    domestic_care_reason,
    wanted_to_work,
    wants_more_hours,
    worked_last_12m,
    effective_hours,
    visit1_n_rooms,
    visit1_n_bedrooms,
    visit1_cooking_main_fuel
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
    visit1_half_min_wage = half_minimum_wage(visit1_year, visit1_quarter),
    eligible_vd5008_or_bpc = ((!is.na(visit1_vd5008_pc) & !is.na(visit1_half_min_wage) & visit1_vd5008_pc <= visit1_half_min_wage) | visit1_has_bpc == 1),
    eligible_vd5008_bolsa_or_bpc = (((!is.na(visit1_vd5008_pc) & !is.na(visit1_half_min_wage) & visit1_vd5008_pc <= visit1_half_min_wage) & visit1_has_bolsa == 1) | visit1_has_bpc == 1),
    visit1_primary_wood = is_wood_main(visit1_cooking_main_fuel) == 1L,
    hh_size_final = as.numeric(hh_size),
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  ) %>%
  left_join(
    treatment_panel %>%
      select(
        estrato4, year, quarter, year_quarter,
        z_pre_families_post, z_delta_families_post
      ),
    by = c("estrato4", "year", "quarter", "year_quarter")
  )

sample_defs <- tibble::tribble(
  ~sample, ~filter_expr,
  "all_women", "TRUE",
  "eligible_vd5008_or_bpc", "eligible_vd5008_or_bpc == TRUE",
  "eligible_vd5008_bolsa_or_bpc", "eligible_vd5008_bolsa_or_bpc == TRUE",
  "eligible_vd5008_or_bpc_primary_wood", "eligible_vd5008_or_bpc == TRUE & visit1_primary_wood == TRUE",
  "eligible_vd5008_bolsa_or_bpc_primary_wood", "eligible_vd5008_bolsa_or_bpc == TRUE & visit1_primary_wood == TRUE"
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
          spec_label = if_else(spec == "M2", "Women controls", "No controls"),
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
  file.path(out_dir, "primary_fuel_women_results.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

household_hits <- household_results %>%
  filter(sensible_sign, p < 0.10) %>%
  group_by(sample, outcome, design, treatment) %>%
  slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
  ungroup()

women_hits <- cross_results %>%
  filter(sensible_sign, p < 0.10) %>%
  group_by(sample, outcome, design, treatment) %>%
  slice_min(order_by = p, n = 1, with_ties = FALSE) %>%
  ungroup()

confirmed_hits <- bind_rows(
  household_hits %>% mutate(result_family = "household_primary_fuel"),
  women_hits %>% mutate(result_family = "women_primary_wood_heterogeneity")
) %>%
  arrange(p)

write.table(
  confirmed_hits,
  file.path(out_dir, "primary_fuel_confirmed_hits.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

paper_table_households <- household_results %>%
  filter(
    sample %in% c("all_households", "eligible_vd5008_or_bpc_households", "eligible_vd5008_bolsa_or_bpc_households"),
    outcome %in% c("wood_primary", "gas_primary"),
    design == "pre_families_post",
    spec == "M2"
  ) %>%
  transmute(
    panel = "Primary fuel",
    sample,
    sample_label = case_when(
      sample == "all_households" ~ "All households",
      sample == "eligible_vd5008_or_bpc_households" ~ "VD5008 or BPC households",
      sample == "eligible_vd5008_bolsa_or_bpc_households" ~ "VD5008 + Bolsa or BPC households",
      TRUE ~ sample
    ),
    outcome,
    outcome_label = case_when(
      outcome == "wood_primary" ~ "Wood primary",
      outcome == "gas_primary" ~ "Gas primary",
      TRUE ~ outcome
    ),
    design,
    design_label = "Pre exposure x post",
    spec_label,
    nobs,
    coef,
    se,
    p,
    coef_per_10_families_per_100_hh = coef * 0.1,
    se_per_10_families_per_100_hh = se * 0.1,
    weighted_mean_outcome
  )

paper_table_women <- cross_results %>%
  filter(
    sample %in% c("all_women", "eligible_vd5008_or_bpc", "eligible_vd5008_bolsa_or_bpc"),
    outcome == "domestic_care_reason",
    design == "delta_families_post",
    spec == "M2"
  ) %>%
  transmute(
    panel = "Domestic care",
    sample,
    sample_label = case_when(
      sample == "all_women" ~ "All women",
      sample == "eligible_vd5008_or_bpc" ~ "VD5008 or BPC women",
      sample == "eligible_vd5008_bolsa_or_bpc" ~ "VD5008 + Bolsa or BPC women",
      TRUE ~ sample
    ),
    outcome,
    outcome_label = "Domestic care reason",
    design,
    design_label = "Realized growth x post",
    spec_label,
    nobs,
    coef,
    se,
    p,
    coef_per_10_families_per_100_hh = coef * 0.1,
    se_per_10_families_per_100_hh = se * 0.1,
    weighted_mean_outcome
  )

paper_table <- bind_rows(paper_table_households, paper_table_women) %>%
  mutate(
    panel_order = case_when(
      panel == "Primary fuel" ~ 1L,
      panel == "Domestic care" ~ 2L,
      TRUE ~ 3L
    ),
    sample_order = case_when(
      sample %in% c("all_households", "all_women") ~ 1L,
      sample %in% c("eligible_vd5008_or_bpc_households", "eligible_vd5008_or_bpc") ~ 2L,
      sample %in% c("eligible_vd5008_bolsa_or_bpc_households", "eligible_vd5008_bolsa_or_bpc") ~ 3L,
      TRUE ~ 4L
    ),
    outcome_order = case_when(
      outcome == "wood_primary" ~ 1L,
      outcome == "gas_primary" ~ 2L,
      outcome == "domestic_care_reason" ~ 3L,
      TRUE ~ 4L
    )
  ) %>%
  arrange(panel_order, sample_order, outcome_order) %>%
  select(-panel_order, -sample_order, -outcome_order)

write.table(
  paper_table,
  file.path(out_dir, "primary_fuel_domcare_paper_table.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

sample_summary <- women %>%
  mutate(
    eligible_vd5008_or_bpc_flag = as.integer(coalesce(eligible_vd5008_or_bpc, FALSE)),
    eligible_vd5008_bolsa_or_bpc_flag = as.integer(coalesce(eligible_vd5008_bolsa_or_bpc, FALSE)),
    primary_wood_flag = as.integer(coalesce(visit1_primary_wood, FALSE)),
    eligible_vd5008_or_bpc_primary_wood_flag = as.integer(coalesce(eligible_vd5008_or_bpc, FALSE) & coalesce(visit1_primary_wood, FALSE)),
    eligible_vd5008_bolsa_or_bpc_primary_wood_flag = as.integer(coalesce(eligible_vd5008_bolsa_or_bpc, FALSE) & coalesce(visit1_primary_wood, FALSE))
  ) %>%
  summarise(
    all = n(),
    eligible_vd5008_or_bpc = sum(eligible_vd5008_or_bpc_flag, na.rm = TRUE),
    eligible_vd5008_bolsa_or_bpc = sum(eligible_vd5008_bolsa_or_bpc_flag, na.rm = TRUE),
    primary_wood_households = sum(primary_wood_flag, na.rm = TRUE),
    eligible_vd5008_or_bpc_primary_wood = sum(eligible_vd5008_or_bpc_primary_wood_flag, na.rm = TRUE),
    eligible_vd5008_bolsa_or_bpc_primary_wood = sum(eligible_vd5008_bolsa_or_bpc_primary_wood_flag, na.rm = TRUE),
    share_primary_wood = mean(primary_wood_flag, na.rm = TRUE)
  )

write.table(
  sample_summary,
  file.path(out_dir, "primary_fuel_sample_summary.tsv"),
  sep = "\t",
  row.names = FALSE,
  quote = FALSE
)

print(sample_summary)
print(household_results %>% filter(sensible_sign) %>% arrange(p))
print(cross_results %>% filter(sensible_sign) %>% arrange(p))
print(confirmed_hits)
