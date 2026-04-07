#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

linked_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")
power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_preperiod_dependence_interactions", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_out <- here::here(out_dir, "dependence_interaction_results.csv")
hits_out <- here::here(out_dir, "dependence_interaction_hits.csv")

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

extract_term <- function(model, term_name) {
  td <- tidy(model)
  row <- td[td$term == term_name, , drop = FALSE]
  if (nrow(row) == 0) {
    tibble(term = term_name, estimate = NA_real_, std.error = NA_real_, p.value = NA_real_)
  } else {
    tibble(
      term = row$term[[1]],
      estimate = row$estimate[[1]],
      std.error = row$std.error[[1]],
      p.value = row$p.value[[1]]
    )
  }
}

linear_combo <- function(model, term_a, term_b) {
  cf <- coef(model)
  vc <- vcov(model)
  if (!(term_a %in% names(cf)) || !(term_b %in% names(cf))) {
    return(tibble(estimate = NA_real_, std.error = NA_real_, p.value = NA_real_))
  }
  est <- cf[[term_a]] + cf[[term_b]]
  vv <- vc[term_a, term_a] + vc[term_b, term_b] + 2 * vc[term_a, term_b]
  se <- sqrt(vv)
  z <- est / se
  p <- 2 * pnorm(abs(z), lower.tail = FALSE)
  tibble(estimate = est, std.error = se, p.value = p)
}

baseline <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency = as.numeric(baseline_child_dependency),
    baseline_female_share_secondaryplus = as.numeric(baseline_female_share_secondaryplus),
    baseline_female_share_tertiary = as.numeric(baseline_female_share_tertiary),
    baseline_rural_share = as.numeric(baseline_rural_share),
    baseline_log_income_p50 = as.numeric(baseline_log_income_p50)
  )

power <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    lightning_l0 = as.numeric(lightning_quarterly_total),
    outage_l0 = as.numeric(raw_quarterly_outage_hrs_per_1000)
  ) %>%
  filter(year >= 2016, year <= 2019)

linked <- read_parquet(
  linked_path,
  col_select = c(
    "year", "quarter", "year_quarter", "estrato4", "person_weight",
    "employed", "effective_hours", "wanted_to_work", "wants_more_hours",
    "available_more_hours", "search_long", "worked_last_12m",
    "outlf_domestic_care", "visit1_match_any", "visit1_same_quarter",
    "visit1_cooking_electricity", "visit1_appliance_count_basic",
    "visit1_appliance_count_connected"
  )
) %>%
  transmute(
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = as.character(year_quarter),
    estrato4 = as.character(estrato4),
    person_weight = as.numeric(person_weight),
    employed = as.numeric(employed),
    effective_hours = as.numeric(effective_hours),
    effective_hours_population = if_else(!is.na(employed) & employed == 1 & !is.na(effective_hours), effective_hours, 0),
    wanted_to_work = as.numeric(wanted_to_work),
    wants_more_hours = as.numeric(wants_more_hours),
    available_more_hours = as.numeric(available_more_hours),
    search_long = as.numeric(search_long),
    worked_last_12m = as.numeric(worked_last_12m),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    visit1_match_any = as.logical(visit1_match_any),
    visit1_same_quarter = as.logical(visit1_same_quarter),
    visit1_cooking_electricity = as.numeric(visit1_cooking_electricity),
    visit1_appliance_count_basic = as.numeric(visit1_appliance_count_basic),
    visit1_appliance_count_connected = as.numeric(visit1_appliance_count_connected)
  ) %>%
  filter(year >= 2016, year <= 2019, visit1_match_any == TRUE, visit1_same_quarter == TRUE) %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  mutate(
    trend = year - 2016,
    b_dep_t = baseline_child_dependency * trend,
    b_secplus_t = baseline_female_share_secondaryplus * trend,
    b_tertiary_t = baseline_female_share_tertiary * trend,
    b_rural_t = baseline_rural_share * trend,
    b_inc_p50_t = baseline_log_income_p50 * trend
  )

linked <- linked %>%
  mutate(
    dep_cooking_electricity = visit1_cooking_electricity,
    dep_appliance_basic_z = (visit1_appliance_count_basic - mean(visit1_appliance_count_basic, na.rm = TRUE)) / sd(visit1_appliance_count_basic, na.rm = TRUE),
    dep_appliance_connected_z = (visit1_appliance_count_connected - mean(visit1_appliance_count_connected, na.rm = TRUE)) / sd(visit1_appliance_count_connected, na.rm = TRUE)
  )

dependence_defs <- tibble::tribble(
  ~dep_var, ~dep_label, ~dep_type, ~value_label,
  "dep_cooking_electricity", "Electric cooking", "binary", "electric-cooking households",
  "dep_appliance_basic_z", "Basic appliance count (z)", "continuous", "1 SD higher basic appliance count",
  "dep_appliance_connected_z", "Connected appliance count (z)", "continuous", "1 SD higher connected appliance count"
)

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label, ~scale, ~effect_direction,
  "outlf_domestic_care", "Domestic care", 100000, "positive",
  "effective_hours", "Effective hours", 60000, "negative",
  "effective_hours_population", "Effective hours population", 60000, "negative",
  "wanted_to_work", "Wanted to work", 100000, "negative",
  "wants_more_hours", "Wanted more hours", 100000, "negative",
  "available_more_hours", "Available more hours", 100000, "negative",
  "search_long", "Long job search", 100000, "positive",
  "worked_last_12m", "Worked last 12 months", 100000, "negative"
)

controls_txt <- "b_dep_t + b_secplus_t + b_tertiary_t + b_rural_t + b_inc_p50_t"

rows <- list()
i <- 1L

for (dd in seq_len(nrow(dependence_defs))) {
  dep_var <- dependence_defs$dep_var[[dd]]
  dep_label <- dependence_defs$dep_label[[dd]]
  dep_type <- dependence_defs$dep_type[[dd]]
  value_label <- dependence_defs$value_label[[dd]]

  for (oo in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[oo]]
    outcome_label <- outcomes$outcome_label[[oo]]
    scale <- outcomes$scale[[oo]]
    effect_direction <- outcomes$effect_direction[[oo]]

    dat <- linked %>%
      filter(
        !is.na(person_weight),
        person_weight > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]]),
        !is.na(.data[[dep_var]]),
        !is.na(b_dep_t),
        !is.na(b_secplus_t),
        !is.na(b_tertiary_t),
        !is.na(b_rural_t),
        !is.na(b_inc_p50_t)
      ) %>%
      mutate(
        dep = .data[[dep_var]],
        outage_dep = outage_l0 * dep,
        lightning_dep = lightning_l0 * dep
      )

    if (nrow(dat) == 0) next

    iv <- feols(
      as.formula(paste0(outcome, " ~ dep + ", controls_txt, " | estrato4 + year_quarter | outage_l0 + outage_dep ~ lightning_l0 + lightning_dep")),
      data = dat,
      weights = ~person_weight,
      cluster = ~estrato4
    )

    base_row <- extract_term(iv, "fit_outage_l0")
    int_row <- extract_term(iv, "fit_outage_dep")

    effect_high <- if (dep_type == "binary") {
      linear_combo(iv, "fit_outage_l0", "fit_outage_dep")
    } else {
      linear_combo(iv, "fit_outage_l0", "fit_outage_dep")
    }

    rows[[i]] <- tibble(
      dependence = dep_label,
      dependence_var = dep_var,
      dependence_type = dep_type,
      dependence_value = value_label,
      outcome = outcome,
      outcome_label = outcome_label,
      effect_direction = effect_direction,
      base_effect = base_row$estimate[[1]] * scale,
      base_se = base_row$std.error[[1]] * scale,
      base_p = base_row$p.value[[1]],
      interaction_effect = int_row$estimate[[1]] * scale,
      interaction_se = int_row$std.error[[1]] * scale,
      interaction_p = int_row$p.value[[1]],
      high_effect = effect_high$estimate[[1]] * scale,
      high_se = effect_high$std.error[[1]] * scale,
      high_p = effect_high$p.value[[1]],
      ivf1 = safe_num(fitstat(iv, "ivf1")),
      nobs = nobs(iv)
    )
    i <- i + 1L
  }
}

results <- bind_rows(rows)

hits <- results %>%
  filter(!is.na(interaction_p)) %>%
  mutate(
    sign_matches = case_when(
      effect_direction == "positive" & interaction_effect > 0 ~ TRUE,
      effect_direction == "negative" & interaction_effect < 0 ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>%
  arrange(interaction_p)

write_csv(results, results_out)
write_csv(hits, hits_out)

message("Wrote: ", results_out)
message("Wrote: ", hits_out)
print(hits)
