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

out_dir <- here::here("data", "powerIV", "regressions", "lightning_adaptation_checks", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

composition_out <- here::here(out_dir, "linked_sample_composition_pre_post.csv")
results_out <- here::here(out_dir, "adaptation_split_results.csv")

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
    "outlf_domestic_care", "visit1_match_any", "visit1_same_quarter",
    "visit1_internet_home", "visit1_internet_by_mobile", "visit1_computer",
    "visit1_cooking_electricity", "visit1_appliance_count_basic"
  )
) %>%
  transmute(
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = as.character(year_quarter),
    estrato4 = as.character(estrato4),
    person_weight = as.numeric(person_weight),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    visit1_match_any = as.logical(visit1_match_any),
    visit1_same_quarter = as.logical(visit1_same_quarter),
    visit1_internet_home = as.numeric(visit1_internet_home),
    visit1_internet_by_mobile = as.numeric(visit1_internet_by_mobile),
    visit1_computer = as.numeric(visit1_computer),
    visit1_cooking_electricity = as.numeric(visit1_cooking_electricity),
    visit1_appliance_count_basic = as.numeric(visit1_appliance_count_basic)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024), visit1_match_any == TRUE) %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  mutate(
    post = if_else(year >= 2022, 1, 0),
    period = if_else(year >= 2022, "post", "pre"),
    trend = year - 2016,
    b_dep_t = baseline_child_dependency * trend,
    b_secplus_t = baseline_female_share_secondaryplus * trend,
    b_tertiary_t = baseline_female_share_tertiary * trend,
    b_rural_t = baseline_rural_share * trend,
    b_inc_p50_t = baseline_log_income_p50 * trend
  )

composition <- linked %>%
  group_by(period) %>%
  summarise(
    nobs = n(),
    internet_home = weighted.mean(visit1_internet_home, person_weight, na.rm = TRUE),
    internet_mobile = weighted.mean(visit1_internet_by_mobile, person_weight, na.rm = TRUE),
    computer = weighted.mean(visit1_computer, person_weight, na.rm = TRUE),
    electric_cooking = weighted.mean(visit1_cooking_electricity, person_weight, na.rm = TRUE),
    appliance_count_basic = weighted.mean(visit1_appliance_count_basic, person_weight, na.rm = TRUE),
    same_quarter = mean(visit1_same_quarter, na.rm = TRUE),
    .groups = "drop"
  )

write_csv(composition, composition_out)

sample_defs <- tibble::tribble(
  ~sample, ~filter_expr,
  "all_linked", "TRUE",
  "all_no_internet", "visit1_internet_home == 0",
  "all_with_internet", "visit1_internet_home == 1",
  "all_no_computer", "visit1_computer == 0",
  "all_with_computer", "visit1_computer == 1",
  "same_quarter_linked", "visit1_same_quarter == TRUE",
  "same_quarter_no_internet", "visit1_same_quarter == TRUE & visit1_internet_home == 0",
  "same_quarter_with_internet", "visit1_same_quarter == TRUE & visit1_internet_home == 1",
  "same_quarter_no_computer", "visit1_same_quarter == TRUE & visit1_computer == 0",
  "same_quarter_with_computer", "visit1_same_quarter == TRUE & visit1_computer == 1"
)

controls_txt <- "b_dep_t + b_secplus_t + b_tertiary_t + b_rural_t + b_inc_p50_t"

rows <- list()
i <- 1L

for (ss in seq_len(nrow(sample_defs))) {
  sample_name <- sample_defs$sample[[ss]]
  dat <- linked %>%
    filter(!!rlang::parse_expr(sample_defs$filter_expr[[ss]])) %>%
    filter(
      !is.na(person_weight),
      person_weight > 0,
      !is.na(lightning_l0),
      !is.na(outage_l0),
      !is.na(outlf_domestic_care),
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
    as.formula(paste0("outlf_domestic_care ~ ", controls_txt, " | estrato4 + year_quarter | outage_l0 + outage_post ~ lightning_l0 + lightning_post")),
    data = dat,
    weights = ~person_weight,
    cluster = ~estrato4
  )

  fs_pre <- feols(
    as.formula(paste0("outage_l0 ~ lightning_l0 + lightning_post + ", controls_txt, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~person_weight,
    cluster = ~estrato4
  )

  fs_post <- feols(
    as.formula(paste0("outage_post ~ lightning_l0 + lightning_post + ", controls_txt, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~person_weight,
    cluster = ~estrato4
  )

  pre_row <- extract_term(iv, "fit_outage_l0")
  diff_row <- extract_term(iv, "fit_outage_post")
  post_row <- linear_combo(iv, "fit_outage_l0", "fit_outage_post")

  rows[[i]] <- tibble(
    sample = sample_name,
    pre_pp = pre_row$estimate[[1]] * 100000,
    pre_pp_se = pre_row$std.error[[1]] * 100000,
    pre_p = pre_row$p.value[[1]],
    post_diff_pp = diff_row$estimate[[1]] * 100000,
    post_diff_pp_se = diff_row$std.error[[1]] * 100000,
    post_diff_p = diff_row$p.value[[1]],
    post_total_pp = post_row$estimate[[1]] * 100000,
    post_total_pp_se = post_row$std.error[[1]] * 100000,
    post_total_p = post_row$p.value[[1]],
    fs_pre_f = safe_num(fitstat(fs_pre, "f")),
    fs_post_diff_f = safe_num(fitstat(fs_post, "f")),
    ivf1 = safe_num(fitstat(iv, "ivf1")),
    nobs = nobs(iv)
  )
  i <- i + 1L
}

results <- bind_rows(rows)
write_csv(results, results_out)

message("Wrote: ", composition_out)
message("Wrote: ", results_out)
print(composition)
print(results)
