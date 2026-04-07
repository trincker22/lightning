#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
panel_split_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_urban_rural_with_power_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_domestic_care_trend_controls", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

period_interaction_out <- here::here(out_dir, "period_interaction_results.csv")
subsample_out <- here::here(out_dir, "subsample_results.csv")

if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(panel_split_path)) stop("Missing file: ", panel_split_path, call. = FALSE)
if (!file.exists(baseline_path)) stop("Missing file: ", baseline_path, call. = FALSE)

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

panel <- read_parquet(panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    weight_sum = as.numeric(weight_sum),
    lightning_l0 = as.numeric(lightning_quarterly_total),
    outage_l0 = as.numeric(raw_quarterly_outage_hrs_per_1000),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care),
    female_domestic_care_reason = as.numeric(female_domestic_care_reason)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024)) %>%
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

panel_split <- read_parquet(panel_split_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    urban_rural = as.character(urban_rural),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    weight_sum = as.numeric(weight_sum),
    lightning_l0 = as.numeric(lightning_quarterly_total),
    outage_l0 = as.numeric(raw_quarterly_outage_hrs_per_1000),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care),
    female_domestic_care_reason = as.numeric(female_domestic_care_reason)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024)) %>%
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

controls_txt <- "b_dep_t + b_secplus_t + b_tertiary_t + b_rural_t + b_inc_p50_t"

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "female_outlf_domestic_care", "Domestic care among women out of LF",
  "female_domestic_care_reason", "Domestic care among all women"
)

subsamples <- tibble::tribble(
  ~sample_name, ~data_source,
  "all", "panel",
  "high_child_dependency", "panel",
  "urban_only", "panel_split"
)

child_cut <- median(panel$baseline_child_dependency, na.rm = TRUE)

period_rows <- list()
subsample_rows <- list()
i <- 1L
j <- 1L

for (kk in seq_len(nrow(outcomes))) {
  outcome <- outcomes$outcome[[kk]]
  outcome_label <- outcomes$outcome_label[[kk]]

  dat_all <- panel %>%
    filter(
      !is.na(weight_sum),
      weight_sum > 0,
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

  iv_pool <- feols(
    as.formula(paste0(outcome, " ~ ", controls_txt, " | estrato4 + year_quarter | outage_l0 + outage_post ~ lightning_l0 + lightning_post")),
    data = dat_all,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_pre <- feols(
    as.formula(paste0("outage_l0 ~ lightning_l0 + lightning_post + ", controls_txt, " | estrato4 + year_quarter")),
    data = dat_all,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_post <- feols(
    as.formula(paste0("outage_post ~ lightning_l0 + lightning_post + ", controls_txt, " | estrato4 + year_quarter")),
    data = dat_all,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  pre_row <- extract_term(iv_pool, "fit_outage_l0")
  diff_row <- extract_term(iv_pool, "fit_outage_post")
  post_row <- linear_combo(iv_pool, "fit_outage_l0", "fit_outage_post")

  period_rows[[i]] <- tibble(
    outcome = outcome,
    outcome_label = outcome_label,
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
    ivf1 = safe_num(fitstat(iv_pool, "ivf1")),
    nobs = nobs(iv_pool)
  )
  i <- i + 1L

  for (ss in seq_len(nrow(subsamples))) {
    sample_name <- subsamples$sample_name[[ss]]
    data_source <- subsamples$data_source[[ss]]

    dat <- if (data_source == "panel") panel else panel_split

    if (sample_name == "high_child_dependency") {
      dat <- dat %>% filter(baseline_child_dependency >= child_cut)
    }
    if (sample_name == "urban_only") {
      dat <- dat %>% filter(urban_rural == "urban")
    }

    dat <- dat %>%
      filter(
        !is.na(weight_sum),
        weight_sum > 0,
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

    fe_txt <- "estrato4 + year_quarter"

    iv_sub <- feols(
      as.formula(paste0(outcome, " ~ ", controls_txt, " | ", fe_txt, " | outage_l0 + outage_post ~ lightning_l0 + lightning_post")),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )

    fs_sub_pre <- feols(
      as.formula(paste0("outage_l0 ~ lightning_l0 + lightning_post + ", controls_txt, " | ", fe_txt)),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )

    fs_sub_post <- feols(
      as.formula(paste0("outage_post ~ lightning_l0 + lightning_post + ", controls_txt, " | ", fe_txt)),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )

    pre_sub <- extract_term(iv_sub, "fit_outage_l0")
    diff_sub <- extract_term(iv_sub, "fit_outage_post")
    post_sub <- linear_combo(iv_sub, "fit_outage_l0", "fit_outage_post")

    subsample_rows[[j]] <- tibble(
      sample = sample_name,
      outcome = outcome,
      outcome_label = outcome_label,
      pre_pp = pre_sub$estimate[[1]] * 100000,
      pre_pp_se = pre_sub$std.error[[1]] * 100000,
      pre_p = pre_sub$p.value[[1]],
      post_diff_pp = diff_sub$estimate[[1]] * 100000,
      post_diff_pp_se = diff_sub$std.error[[1]] * 100000,
      post_diff_p = diff_sub$p.value[[1]],
      post_total_pp = post_sub$estimate[[1]] * 100000,
      post_total_pp_se = post_sub$std.error[[1]] * 100000,
      post_total_p = post_sub$p.value[[1]],
      fs_pre_f = safe_num(fitstat(fs_sub_pre, "f")),
      fs_post_diff_f = safe_num(fitstat(fs_sub_post, "f")),
      ivf1 = safe_num(fitstat(iv_sub, "ivf1")),
      nobs = nobs(iv_sub)
    )
    j <- j + 1L
  }
}

period_results <- bind_rows(period_rows)
subsample_results <- bind_rows(subsample_rows)

write_csv(period_results, period_interaction_out)
write_csv(subsample_results, subsample_out)

message("Wrote: ", period_interaction_out)
message("Wrote: ", subsample_out)
print(period_results)
print(subsample_results)
