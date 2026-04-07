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
asset_panel_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_domestic_care_targeted_heterogeneity", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

interaction_out <- here::here(out_dir, "interaction_results.csv")
split_out <- here::here(out_dir, "split_results.csv")
urban_rural_out <- here::here(out_dir, "urban_rural_results.csv")

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

weighted_mean_safe <- function(x, w) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(NA_real_)
  weighted.mean(x[keep], w[keep])
}

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "female_outlf_domestic_care", "Domestic care among women out of LF",
  "female_domestic_care_reason", "Domestic care among all women"
)

baseline <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency = as.numeric(baseline_child_dependency)
  )

baseline_appliance <- read_parquet(asset_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    weight_sum = as.numeric(weight_sum),
    mean_appliance_count_basic = as.numeric(mean_appliance_count_basic)
  ) %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(
    baseline_appliance_count_basic = weighted_mean_safe(mean_appliance_count_basic, weight_sum),
    .groups = "drop"
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
  left_join(baseline_appliance, by = "estrato4")

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
  mutate(
    rural = if_else(urban_rural == "rural", 1, 0),
    estrato4_ur = paste(estrato4, urban_rural, sep = "_")
  )

interaction_specs <- tibble::tribble(
  ~het_var, ~het_label,
  "baseline_child_dependency", "Baseline child dependency",
  "baseline_appliance_count_basic", "Baseline appliance count basic"
)

interaction_rows <- list()
split_rows <- list()
urban_rural_rows <- list()
i1 <- 1L
i2 <- 1L
i3 <- 1L

for (ii in seq_len(nrow(outcomes))) {
  outcome <- outcomes$outcome[[ii]]
  outcome_label <- outcomes$outcome_label[[ii]]

  for (jj in seq_len(nrow(interaction_specs))) {
    het_var <- interaction_specs$het_var[[jj]]
    het_label <- interaction_specs$het_label[[jj]]

    dat <- panel %>%
      filter(
        !is.na(weight_sum),
        weight_sum > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]]),
        !is.na(.data[[het_var]])
      )

    het_mean <- mean(dat[[het_var]], na.rm = TRUE)
    het_sd <- sd(dat[[het_var]], na.rm = TRUE)

    if (!is.na(het_sd) && het_sd > 0) {
      dat_interaction <- dat %>%
        mutate(
          het_std = (.data[[het_var]] - het_mean) / het_sd,
          outage_x_het = outage_l0 * het_std,
          lightning_x_het = lightning_l0 * het_std
        )

      iv <- feols(
        as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 + outage_x_het ~ lightning_l0 + lightning_x_het")),
        data = dat_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_base <- feols(
        outage_l0 ~ lightning_l0 + lightning_x_het | estrato4 + year_quarter,
        data = dat_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_het <- feols(
        outage_x_het ~ lightning_l0 + lightning_x_het | estrato4 + year_quarter,
        data = dat_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      base_row <- extract_term(iv, "fit_outage_l0")
      het_row <- extract_term(iv, "fit_outage_x_het")

      interaction_rows[[i1]] <- tibble(
        outcome = outcome,
        outcome_label = outcome_label,
        heterogeneity = het_label,
        base_pp = base_row$estimate[[1]] * 100000,
        base_pp_se = base_row$std.error[[1]] * 100000,
        base_p = base_row$p.value[[1]],
        interaction_pp_per_sd = het_row$estimate[[1]] * 100000,
        interaction_pp_per_sd_se = het_row$std.error[[1]] * 100000,
        interaction_p = het_row$p.value[[1]],
        fs_base_f = safe_num(fitstat(fs_base, "f")),
        fs_interaction_f = safe_num(fitstat(fs_het, "f")),
        ivf1 = safe_num(fitstat(iv, "ivf1")),
        nobs = nobs(iv)
      )
      i1 <- i1 + 1L
    }

    cut_val <- median(dat[[het_var]], na.rm = TRUE)

    for (split_name in c("Low", "High")) {
      ds <- dat %>%
        mutate(split = if_else(.data[[het_var]] >= cut_val, "High", "Low")) %>%
        filter(split == split_name)

      if (nrow(ds) == 0) next

      iv_split <- feols(
        as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 ~ lightning_l0")),
        data = ds,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_split <- feols(
        outage_l0 ~ lightning_l0 | estrato4 + year_quarter,
        data = ds,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      split_row <- extract_term(iv_split, "fit_outage_l0")

      split_rows[[i2]] <- tibble(
        outcome = outcome,
        outcome_label = outcome_label,
        heterogeneity = het_label,
        split = split_name,
        effect_pp = split_row$estimate[[1]] * 100000,
        se_pp = split_row$std.error[[1]] * 100000,
        p = split_row$p.value[[1]],
        first_stage_f = safe_num(fitstat(fs_split, "f")),
        ivf1 = safe_num(fitstat(iv_split, "ivf1")),
        nobs = nobs(iv_split)
      )
      i2 <- i2 + 1L
    }
  }

  dat_ur <- panel_split %>%
    filter(
      !is.na(weight_sum),
      weight_sum > 0,
      !is.na(lightning_l0),
      !is.na(outage_l0),
      !is.na(.data[[outcome]])
    ) %>%
    mutate(
      outage_rural = outage_l0 * rural,
      lightning_rural = lightning_l0 * rural
    )

  iv_ur <- feols(
    as.formula(paste0(outcome, " ~ 1 | estrato4_ur + year_quarter | outage_l0 + outage_rural ~ lightning_l0 + lightning_rural")),
    data = dat_ur,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_urban <- feols(
    outage_l0 ~ lightning_l0 + lightning_rural | estrato4_ur + year_quarter,
    data = dat_ur,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_rural <- feols(
    outage_rural ~ lightning_l0 + lightning_rural | estrato4_ur + year_quarter,
    data = dat_ur,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  urban_row <- extract_term(iv_ur, "fit_outage_l0")
  rural_diff_row <- extract_term(iv_ur, "fit_outage_rural")
  rural_total_row <- linear_combo(iv_ur, "fit_outage_l0", "fit_outage_rural")

  urban_rural_rows[[i3]] <- tibble(
    outcome = outcome,
    outcome_label = outcome_label,
    urban_pp = urban_row$estimate[[1]] * 100000,
    urban_pp_se = urban_row$std.error[[1]] * 100000,
    urban_p = urban_row$p.value[[1]],
    rural_diff_pp = rural_diff_row$estimate[[1]] * 100000,
    rural_diff_pp_se = rural_diff_row$std.error[[1]] * 100000,
    rural_diff_p = rural_diff_row$p.value[[1]],
    rural_total_pp = rural_total_row$estimate[[1]] * 100000,
    rural_total_pp_se = rural_total_row$std.error[[1]] * 100000,
    rural_total_p = rural_total_row$p.value[[1]],
    fs_urban_f = safe_num(fitstat(fs_urban, "f")),
    fs_rural_diff_f = safe_num(fitstat(fs_rural, "f")),
    ivf1 = safe_num(fitstat(iv_ur, "ivf1")),
    nobs = nobs(iv_ur)
  )
  i3 <- i3 + 1L
}

interaction_results <- bind_rows(interaction_rows)
split_results <- bind_rows(split_rows)
urban_rural_results <- bind_rows(urban_rural_rows)

write_csv(interaction_results, interaction_out)
write_csv(split_results, split_out)
write_csv(urban_rural_results, urban_rural_out)

message("Wrote: ", interaction_out)
message("Wrote: ", split_out)
message("Wrote: ", urban_rural_out)
print(interaction_results)
print(split_results)
print(urban_rural_results)
