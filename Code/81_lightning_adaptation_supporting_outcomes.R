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

out_dir <- here::here("data", "powerIV", "regressions", "lightning_adaptation_supporting_outcomes", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_out <- here::here(out_dir, "supporting_outcomes_split_results.csv")

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
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024))

linked <- read_parquet(
  linked_path,
  col_select = c(
    "year", "quarter", "year_quarter", "estrato4", "person_weight",
    "outlf_domestic_care", "wanted_to_work", "wants_more_hours",
    "employed", "effective_hours",
    "visit1_match_any", "visit1_same_quarter",
    "visit1_computer", "visit1_cooking_electricity", "visit1_appliance_count_basic"
  )
) %>%
  transmute(
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = as.character(year_quarter),
    estrato4 = as.character(estrato4),
    person_weight = as.numeric(person_weight),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    wanted_to_work = as.numeric(wanted_to_work),
    wants_more_hours = as.numeric(wants_more_hours),
    employed = as.numeric(employed),
    effective_hours = as.numeric(effective_hours),
    effective_hours_population = if_else(!is.na(employed) & employed == 1 & !is.na(effective_hours), effective_hours, 0),
    visit1_match_any = as.logical(visit1_match_any),
    visit1_same_quarter = as.logical(visit1_same_quarter),
    visit1_computer = as.numeric(visit1_computer),
    visit1_cooking_electricity = as.numeric(visit1_cooking_electricity),
    visit1_appliance_count_basic = as.numeric(visit1_appliance_count_basic)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024), visit1_match_any == TRUE) %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  mutate(
    post = if_else(year >= 2022, 1, 0),
    trend = year - 2016,
    b_dep_t = baseline_child_dependency * trend,
    b_secplus_t = baseline_female_share_secondaryplus * trend,
    b_tertiary_t = baseline_female_share_tertiary * trend,
    b_rural_t = baseline_rural_share * trend,
    b_inc_p50_t = baseline_log_income_p50 * trend
  )

appliance_cut <- median(linked$visit1_appliance_count_basic, na.rm = TRUE)

sample_defs <- tibble::tribble(
  ~sample, ~filter_expr,
  "same_quarter_linked", "visit1_same_quarter == TRUE",
  "same_quarter_no_computer", "visit1_same_quarter == TRUE & visit1_computer == 0",
  "same_quarter_with_computer", "visit1_same_quarter == TRUE & visit1_computer == 1",
  "same_quarter_electric_cooking", "visit1_same_quarter == TRUE & visit1_cooking_electricity == 1",
  "same_quarter_no_electric_cooking", "visit1_same_quarter == TRUE & visit1_cooking_electricity == 0",
  "same_quarter_high_appliance", paste0("visit1_same_quarter == TRUE & visit1_appliance_count_basic >= ", format(appliance_cut, scientific = FALSE)),
  "same_quarter_low_appliance", paste0("visit1_same_quarter == TRUE & visit1_appliance_count_basic < ", format(appliance_cut, scientific = FALSE))
)

outcomes <- tibble::tribble(
  ~outcome, ~scale, ~label,
  "outlf_domestic_care", 100000, "Domestic care (pp)",
  "effective_hours_population", 60000, "Effective hours population (minutes)",
  "wanted_to_work", 100000, "Wanted to work (pp)",
  "wants_more_hours", 100000, "Wanted more hours (pp)"
)

controls_txt <- "b_dep_t + b_secplus_t + b_tertiary_t + b_rural_t + b_inc_p50_t"

rows <- list()
i <- 1L

for (ss in seq_len(nrow(sample_defs))) {
  dat_sample <- linked %>% filter(!!rlang::parse_expr(sample_defs$filter_expr[[ss]]))

  for (oo in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[oo]]
    scale <- outcomes$scale[[oo]]
    label <- outcomes$label[[oo]]

    dat <- dat_sample %>%
      filter(
        !is.na(person_weight),
        person_weight > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]]),
        !is.na(b_dep_t),
        !is.na(b_secplus_t),
        !is.na(b_tertiary_t),
        !is.na(b_rural_t),
        !is.na(b_inc_p50_t)
      ) %>%
      mutate(
        outage_post = outage_l0 * post,
        lightning_post = lightning_l0 * post
      )

    if (nrow(dat) == 0) next

    iv <- feols(
      as.formula(paste0(outcome, " ~ ", controls_txt, " | estrato4 + year_quarter | outage_l0 + outage_post ~ lightning_l0 + lightning_post")),
      data = dat,
      weights = ~person_weight,
      cluster = ~estrato4
    )

    pre_row <- extract_term(iv, "fit_outage_l0")
    diff_row <- extract_term(iv, "fit_outage_post")
    post_row <- linear_combo(iv, "fit_outage_l0", "fit_outage_post")

    rows[[i]] <- tibble(
      sample = sample_defs$sample[[ss]],
      outcome = outcome,
      outcome_label = label,
      pre_effect = pre_row$estimate[[1]] * scale,
      pre_se = pre_row$std.error[[1]] * scale,
      pre_p = pre_row$p.value[[1]],
      post_diff = diff_row$estimate[[1]] * scale,
      post_diff_se = diff_row$std.error[[1]] * scale,
      post_diff_p = diff_row$p.value[[1]],
      post_effect = post_row$estimate[[1]] * scale,
      post_effect_se = post_row$std.error[[1]] * scale,
      post_effect_p = post_row$p.value[[1]],
      ivf1 = safe_num(fitstat(iv, "ivf1")),
      nobs = nobs(iv)
    )
    i <- i + 1L
  }
}

results <- bind_rows(rows)
write_csv(results, results_out)

message("Wrote: ", results_out)
print(results)
