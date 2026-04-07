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
asset_panel_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")
asset_split_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_urban_rural_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_domestic_care_heterogeneity", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

pooled_period_out <- here::here(out_dir, "pooled_period_interaction_results.csv")
appliance_interaction_out <- here::here(out_dir, "appliance_interaction_results.csv")
appliance_split_out <- here::here(out_dir, "appliance_split_results.csv")
urban_rural_interaction_out <- here::here(out_dir, "urban_rural_interaction_results.csv")
urban_rural_split_out <- here::here(out_dir, "urban_rural_split_results.csv")
summary_tex <- here::here(out_dir, "domestic_care_heterogeneity_summary.tex")

if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(panel_split_path)) stop("Missing file: ", panel_split_path, call. = FALSE)
if (!file.exists(asset_panel_path)) stop("Missing file: ", asset_panel_path, call. = FALSE)
if (!file.exists(asset_split_path)) stop("Missing file: ", asset_split_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

weighted_mean_safe <- function(x, w) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(NA_real_)
  weighted.mean(x[keep], w[keep])
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

format_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(x, digits = digits, format = "f"))
}

format_p <- function(x) {
  ifelse(is.na(x), "", ifelse(x < 0.001, "<0.001", formatC(x, digits = 3, format = "f")))
}

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "female_outlf_domestic_care", "Domestic care among women out of LF",
  "female_domestic_care_reason", "Domestic care among all women"
)

periods <- tibble::tribble(
  ~sample, ~year_min, ~year_max,
  "all_available", 2016L, 2024L,
  "pre_2016_2019", 2016L, 2019L,
  "post_2022_2024", 2022L, 2024L
)

baseline_main <- read_parquet(asset_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    weight_sum = as.numeric(weight_sum),
    mean_appliance_count_basic = as.numeric(mean_appliance_count_basic),
    mean_appliance_count_connected = as.numeric(mean_appliance_count_connected)
  ) %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(
    baseline_appliance_count_basic = weighted_mean_safe(mean_appliance_count_basic, weight_sum),
    baseline_appliance_count_connected = weighted_mean_safe(mean_appliance_count_connected, weight_sum),
    .groups = "drop"
  )

baseline_split <- read_parquet(asset_split_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    urban_rural = as.character(urban_rural),
    year = as.integer(year),
    weight_sum = as.numeric(weight_sum),
    mean_appliance_count_basic = as.numeric(mean_appliance_count_basic),
    mean_appliance_count_connected = as.numeric(mean_appliance_count_connected)
  ) %>%
  filter(year == 2016) %>%
  group_by(estrato4, urban_rural) %>%
  summarise(
    baseline_appliance_count_basic = weighted_mean_safe(mean_appliance_count_basic, weight_sum),
    baseline_appliance_count_connected = weighted_mean_safe(mean_appliance_count_connected, weight_sum),
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
  left_join(baseline_main, by = "estrato4") %>%
  mutate(post = if_else(year >= 2022, 1, 0))

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
  left_join(baseline_split, by = c("estrato4", "urban_rural")) %>%
  mutate(
    post = if_else(year >= 2022, 1, 0),
    rural = if_else(urban_rural == "rural", 1, 0),
    estrato4_ur = paste(estrato4, urban_rural, sep = "_")
  )

pooled_period_rows <- list()
appliance_interaction_rows <- list()
appliance_split_rows <- list()
urban_rural_interaction_rows <- list()
urban_rural_split_rows <- list()

i_pool <- 1L
i_app_int <- 1L
i_app_split <- 1L
i_ur_int <- 1L
i_ur_split <- 1L

for (ii in seq_len(nrow(outcomes))) {
  outcome <- outcomes$outcome[[ii]]
  outcome_label <- outcomes$outcome_label[[ii]]

  dat_pool <- panel %>%
    filter(
      !is.na(weight_sum),
      weight_sum > 0,
      !is.na(lightning_l0),
      !is.na(outage_l0),
      !is.na(.data[[outcome]])
    ) %>%
    mutate(
      outage_post = outage_l0 * post,
      lightning_post = lightning_l0 * post
    )

  iv_pool <- feols(
    as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 + outage_post ~ lightning_l0 + lightning_post")),
    data = dat_pool,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_pool_base <- feols(
    outage_l0 ~ lightning_l0 + lightning_post | estrato4 + year_quarter,
    data = dat_pool,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_pool_post <- feols(
    outage_post ~ lightning_l0 + lightning_post | estrato4 + year_quarter,
    data = dat_pool,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  pre_row <- extract_term(iv_pool, "fit_outage_l0")
  post_diff_row <- extract_term(iv_pool, "fit_outage_post")
  post_total_row <- linear_combo(iv_pool, "fit_outage_l0", "fit_outage_post")

  pooled_period_rows[[i_pool]] <- tibble(
    outcome = outcome,
    outcome_label = outcome_label,
    pre_coef = pre_row$estimate[[1]],
    pre_se = pre_row$std.error[[1]],
    pre_p = pre_row$p.value[[1]],
    pre_pp = pre_row$estimate[[1]] * 100000,
    pre_pp_se = pre_row$std.error[[1]] * 100000,
    post_diff_coef = post_diff_row$estimate[[1]],
    post_diff_se = post_diff_row$std.error[[1]],
    post_diff_p = post_diff_row$p.value[[1]],
    post_diff_pp = post_diff_row$estimate[[1]] * 100000,
    post_diff_pp_se = post_diff_row$std.error[[1]] * 100000,
    post_total_coef = post_total_row$estimate[[1]],
    post_total_se = post_total_row$std.error[[1]],
    post_total_p = post_total_row$p.value[[1]],
    post_total_pp = post_total_row$estimate[[1]] * 100000,
    post_total_pp_se = post_total_row$std.error[[1]] * 100000,
    fs_pre_f = safe_fitstat(fs_pool_base, "f"),
    fs_post_diff_f = safe_fitstat(fs_pool_post, "f"),
    ivf1 = safe_fitstat(iv_pool, "ivf1"),
    ivfall = safe_fitstat(iv_pool, "ivfall"),
    nobs = nobs(iv_pool)
  )
  i_pool <- i_pool + 1L

  for (appliance_var in c("baseline_appliance_count_basic", "baseline_appliance_count_connected")) {
    appliance_label <- ifelse(
      appliance_var == "baseline_appliance_count_basic",
      "Baseline appliance count basic",
      "Baseline appliance count connected"
    )

    dat_app <- panel %>%
      filter(
        !is.na(weight_sum),
        weight_sum > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]]),
        !is.na(.data[[appliance_var]])
      )

    app_mean <- mean(dat_app[[appliance_var]], na.rm = TRUE)
    app_sd <- sd(dat_app[[appliance_var]], na.rm = TRUE)

    if (!is.na(app_sd) && app_sd > 0) {
      dat_app_interaction <- dat_app %>%
        mutate(
          appliance_std = (.data[[appliance_var]] - app_mean) / app_sd,
          outage_x_appliance = outage_l0 * appliance_std,
          lightning_x_appliance = lightning_l0 * appliance_std
        )

      iv_app <- feols(
        as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 + outage_x_appliance ~ lightning_l0 + lightning_x_appliance")),
        data = dat_app_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_app_base <- feols(
        outage_l0 ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter,
        data = dat_app_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_app_interaction <- feols(
        outage_x_appliance ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter,
        data = dat_app_interaction,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      base_row <- extract_term(iv_app, "fit_outage_l0")
      interaction_row <- extract_term(iv_app, "fit_outage_x_appliance")

      appliance_interaction_rows[[i_app_int]] <- tibble(
        outcome = outcome,
        outcome_label = outcome_label,
        appliance = appliance_label,
        base_coef = base_row$estimate[[1]],
        base_se = base_row$std.error[[1]],
        base_p = base_row$p.value[[1]],
        base_pp = base_row$estimate[[1]] * 100000,
        base_pp_se = base_row$std.error[[1]] * 100000,
        interaction_coef = interaction_row$estimate[[1]],
        interaction_se = interaction_row$std.error[[1]],
        interaction_p = interaction_row$p.value[[1]],
        interaction_pp_per_sd = interaction_row$estimate[[1]] * 100000,
        interaction_pp_per_sd_se = interaction_row$std.error[[1]] * 100000,
        fs_base_f = safe_fitstat(fs_app_base, "f"),
        fs_interaction_f = safe_fitstat(fs_app_interaction, "f"),
        ivf1 = safe_fitstat(iv_app, "ivf1"),
        ivfall = safe_fitstat(iv_app, "ivfall"),
        nobs = nobs(iv_app)
      )
      i_app_int <- i_app_int + 1L
    }

    split_cut <- median(dat_app[[appliance_var]], na.rm = TRUE)

    for (jj in seq_len(nrow(periods))) {
      sample_name <- periods$sample[[jj]]
      year_min <- periods$year_min[[jj]]
      year_max <- periods$year_max[[jj]]

      ds_all <- dat_app %>%
        filter(year >= year_min, year <= year_max) %>%
        mutate(
          split = if_else(.data[[appliance_var]] >= split_cut, "High appliance", "Low appliance")
        )

      for (split_name in c("Low appliance", "High appliance")) {
        ds <- ds_all %>% filter(split == split_name)
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

        appliance_split_rows[[i_app_split]] <- tibble(
          outcome = outcome,
          outcome_label = outcome_label,
          appliance = appliance_label,
          sample = sample_name,
          split = split_name,
          split_cut = split_cut,
          coef = split_row$estimate[[1]],
          se = split_row$std.error[[1]],
          p = split_row$p.value[[1]],
          pp = split_row$estimate[[1]] * 100000,
          pp_se = split_row$std.error[[1]] * 100000,
          first_stage_f = safe_fitstat(fs_split, "f"),
          ivf1 = safe_fitstat(iv_split, "ivf1"),
          nobs = nobs(iv_split)
        )
        i_app_split <- i_app_split + 1L
      }
    }
  }

  dat_ur <- panel_split %>%
    filter(
      !is.na(weight_sum),
      weight_sum > 0,
      !is.na(lightning_l0),
      !is.na(outage_l0),
      !is.na(.data[[outcome]]),
      !is.na(rural)
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

  fs_ur_base <- feols(
    outage_l0 ~ lightning_l0 + lightning_rural | estrato4_ur + year_quarter,
    data = dat_ur,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_ur_rural <- feols(
    outage_rural ~ lightning_l0 + lightning_rural | estrato4_ur + year_quarter,
    data = dat_ur,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  urban_row <- extract_term(iv_ur, "fit_outage_l0")
  rural_diff_row <- extract_term(iv_ur, "fit_outage_rural")
  rural_total_row <- linear_combo(iv_ur, "fit_outage_l0", "fit_outage_rural")

  urban_rural_interaction_rows[[i_ur_int]] <- tibble(
    outcome = outcome,
    outcome_label = outcome_label,
    urban_coef = urban_row$estimate[[1]],
    urban_se = urban_row$std.error[[1]],
    urban_p = urban_row$p.value[[1]],
    urban_pp = urban_row$estimate[[1]] * 100000,
    urban_pp_se = urban_row$std.error[[1]] * 100000,
    rural_diff_coef = rural_diff_row$estimate[[1]],
    rural_diff_se = rural_diff_row$std.error[[1]],
    rural_diff_p = rural_diff_row$p.value[[1]],
    rural_diff_pp = rural_diff_row$estimate[[1]] * 100000,
    rural_diff_pp_se = rural_diff_row$std.error[[1]] * 100000,
    rural_total_coef = rural_total_row$estimate[[1]],
    rural_total_se = rural_total_row$std.error[[1]],
    rural_total_p = rural_total_row$p.value[[1]],
    rural_total_pp = rural_total_row$estimate[[1]] * 100000,
    rural_total_pp_se = rural_total_row$std.error[[1]] * 100000,
    fs_urban_f = safe_fitstat(fs_ur_base, "f"),
    fs_rural_diff_f = safe_fitstat(fs_ur_rural, "f"),
    ivf1 = safe_fitstat(iv_ur, "ivf1"),
    ivfall = safe_fitstat(iv_ur, "ivfall"),
    nobs = nobs(iv_ur)
  )
  i_ur_int <- i_ur_int + 1L

  for (jj in seq_len(nrow(periods))) {
    sample_name <- periods$sample[[jj]]
    year_min <- periods$year_min[[jj]]
    year_max <- periods$year_max[[jj]]

    ds_all <- panel_split %>%
      filter(
        year >= year_min,
        year <= year_max,
        !is.na(weight_sum),
        weight_sum > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]])
      )

    for (group_name in c("urban", "rural")) {
      ds <- ds_all %>% filter(urban_rural == group_name)
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

      urban_rural_split_rows[[i_ur_split]] <- tibble(
        outcome = outcome,
        outcome_label = outcome_label,
        sample = sample_name,
        urban_rural = group_name,
        coef = split_row$estimate[[1]],
        se = split_row$std.error[[1]],
        p = split_row$p.value[[1]],
        pp = split_row$estimate[[1]] * 100000,
        pp_se = split_row$std.error[[1]] * 100000,
        first_stage_f = safe_fitstat(fs_split, "f"),
        ivf1 = safe_fitstat(iv_split, "ivf1"),
        nobs = nobs(iv_split)
      )
      i_ur_split <- i_ur_split + 1L
    }
  }
}

pooled_period_results <- bind_rows(pooled_period_rows)
appliance_interaction_results <- bind_rows(appliance_interaction_rows)
appliance_split_results <- bind_rows(appliance_split_rows)
urban_rural_interaction_results <- bind_rows(urban_rural_interaction_rows)
urban_rural_split_results <- bind_rows(urban_rural_split_rows)

write_csv(pooled_period_results, pooled_period_out)
write_csv(appliance_interaction_results, appliance_interaction_out)
write_csv(appliance_split_results, appliance_split_out)
write_csv(urban_rural_interaction_results, urban_rural_interaction_out)
write_csv(urban_rural_split_results, urban_rural_split_out)

summary_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning IV domestic-care heterogeneity summary}",
  "\\label{tab:lightning_iv_domestic_care_heterogeneity}",
  "\\begin{tabular}{llccc}",
  "\\hline",
  "Panel & Specification & Effect (SE) & p-value & First-stage F \\\\",
  "\\hline"
)

pool_table <- pooled_period_results %>%
  transmute(
    panel = outcome_label,
    spec = "Pre-period effect",
    coef_se = paste0(format_num(pre_pp), " (", format_num(pre_pp_se), ")"),
    p_fmt = format_p(pre_p),
    f_fmt = format_num(fs_pre_f, 2)
  )

for (ii in seq_len(nrow(pool_table))) {
  row <- pool_table[ii, , drop = FALSE]
  summary_lines <- c(
    summary_lines,
    paste0(row$panel[[1]], " & ", row$spec[[1]], " & ", row$coef_se[[1]], " & ", row$p_fmt[[1]], " & ", row$f_fmt[[1]], " \\\\")
  )
}

best_app <- appliance_split_results %>%
  filter(sample == "all_available", appliance == "Baseline appliance count basic") %>%
  arrange(outcome, desc(split)) %>%
  mutate(
    panel = outcome_label,
    spec = paste0(split, ", pooled"),
    coef_se = paste0(format_num(pp), " (", format_num(pp_se), ")"),
    p_fmt = format_p(p),
    f_fmt = format_num(first_stage_f, 2)
  ) %>%
  select(panel, spec, coef_se, p_fmt, f_fmt)

for (ii in seq_len(nrow(best_app))) {
  row <- best_app[ii, , drop = FALSE]
  summary_lines <- c(
    summary_lines,
    paste0(row$panel[[1]], " & ", row$spec[[1]], " & ", row$coef_se[[1]], " & ", row$p_fmt[[1]], " & ", row$f_fmt[[1]], " \\\\")
  )
}

best_ur <- urban_rural_split_results %>%
  filter(sample == "all_available") %>%
  mutate(
    panel = outcome_label,
    spec = paste0(tools::toTitleCase(urban_rural), ", pooled"),
    coef_se = paste0(format_num(pp), " (", format_num(pp_se), ")"),
    p_fmt = format_p(p),
    f_fmt = format_num(first_stage_f, 2)
  ) %>%
  select(panel, spec, coef_se, p_fmt, f_fmt)

for (ii in seq_len(nrow(best_ur))) {
  row <- best_ur[ii, , drop = FALSE]
  summary_lines <- c(
    summary_lines,
    paste0(row$panel[[1]], " & ", row$spec[[1]], " & ", row$coef_se[[1]], " & ", row$p_fmt[[1]], " & ", row$f_fmt[[1]], " \\\\")
  )
}

summary_lines <- c(
  summary_lines,
  "\\hline",
  "\\multicolumn{5}{p{0.92\\linewidth}}{\\footnotesize Notes: Entries report percentage-point effects per additional outage hour per household in the quarter. All pooled period and appliance models use estrato4 and year-quarter fixed effects. Urban/rural interaction models use estrato4-by-urban/rural and year-quarter fixed effects; urban-only and rural-only split models use estrato4 and year-quarter fixed effects within subgroup. Standard errors are clustered at estrato4.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(summary_lines, summary_tex)

message("Wrote: ", pooled_period_out)
message("Wrote: ", appliance_interaction_out)
message("Wrote: ", appliance_split_out)
message("Wrote: ", urban_rural_interaction_out)
message("Wrote: ", urban_rural_split_out)
message("Wrote: ", summary_tex)

print(pooled_period_results)
print(appliance_interaction_results)
print(appliance_split_results)
print(urban_rural_interaction_results)
print(urban_rural_split_results)
