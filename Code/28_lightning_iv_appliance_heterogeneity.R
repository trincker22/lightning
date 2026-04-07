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
household_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_appliance_heterogeneity", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

interaction_out <- here::here(out_dir, "interaction_results.csv")
split_out <- here::here(out_dir, "split_results.csv")
split_tex <- here::here(out_dir, "split_results.tex")

if (!file.exists(panel_path)) stop("Missing file: ", panel_path, call. = FALSE)
if (!file.exists(household_path)) stop("Missing file: ", household_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

weighted_mean_safe <- function(x, w) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(NA_real_)
  weighted.mean(x[keep], w[keep])
}

extract_first_match <- function(model, pattern) {
  td <- tidy(model)
  row <- td[grepl(pattern, td$term), , drop = FALSE]
  if (nrow(row) == 0) {
    tibble(term = NA_character_, estimate = NA_real_, std.error = NA_real_, p.value = NA_real_)
  } else {
    tibble(
      term = row$term[[1]],
      estimate = row$estimate[[1]],
      std.error = row$std.error[[1]],
      p.value = row$p.value[[1]]
    )
  }
}

format_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(x, digits = digits, format = "f"))
}

format_p <- function(x) {
  ifelse(is.na(x), "", ifelse(x < 0.001, "<0.001", formatC(x, digits = 3, format = "f")))
}

baseline_appliance <- read_parquet(household_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    weight = as.numeric(weight_calibrated),
    washing_machine = as.numeric(washing_machine),
    cooking_electricity = as.numeric(cooking_electricity)
  ) %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(
    baseline_washing_machine = weighted_mean_safe(washing_machine, weight),
    baseline_cooking_electricity = weighted_mean_safe(cooking_electricity, weight),
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
    female_hours_effective_employed = as.numeric(female_hours_effective_employed),
    male_hours_effective_employed = as.numeric(male_hours_effective_employed),
    gender_hours_effective_employed_gap = as.numeric(gender_hours_effective_employed_gap),
    female_hours_effective_population = as.numeric(female_hours_effective_population),
    male_hours_effective_population = as.numeric(male_hours_effective_population),
    gender_hours_effective_population_gap = as.numeric(gender_hours_effective_population_gap)
  ) %>%
  left_join(baseline_appliance, by = "estrato4")

samples <- tibble::tribble(
  ~sample, ~year_min, ~year_max, ~female_outcome, ~male_outcome, ~gap_outcome,
  "pre_2016_2019", 2016L, 2019L, "female_hours_effective_employed", "male_hours_effective_employed", "gender_hours_effective_employed_gap",
  "recovery_2022_2024", 2022L, 2024L, "female_hours_effective_population", "male_hours_effective_population", "gender_hours_effective_population_gap"
)

appliance_specs <- tibble::tribble(
  ~appliance_var, ~appliance_label,
  "baseline_washing_machine", "Baseline washing machine share",
  "baseline_cooking_electricity", "Baseline electric cooking share"
)

interaction_rows <- list()
split_rows <- list()
idx_interaction <- 1L
idx_split <- 1L

run_interaction_iv <- function(df, outcome, appliance_var, appliance_label, sample_name, group_label) {
  needed <- c("weight_sum", "estrato4", "year_quarter", "lightning_l0", "outage_l0", outcome, appliance_var)
  dat <- df %>% filter(if_all(all_of(needed), ~ !is.na(.x)), weight_sum > 0)
  if (nrow(dat) == 0) return(NULL)

  appliance_mean <- mean(dat[[appliance_var]], na.rm = TRUE)
  appliance_sd <- sd(dat[[appliance_var]], na.rm = TRUE)
  if (is.na(appliance_sd) || appliance_sd == 0) return(NULL)

  appliance_centered <- dat[[appliance_var]] - appliance_mean
  appliance_std <- appliance_centered / appliance_sd

  dat <- dat %>%
    mutate(
      appliance_std = appliance_std,
      outage_x_appliance = outage_l0 * appliance_std,
      lightning_x_appliance = lightning_l0 * appliance_std
    )

  iv <- feols(
    as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 + outage_x_appliance ~ lightning_l0 + lightning_x_appliance")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  fs_main <- feols(outage_l0 ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter, data = dat, weights = ~weight_sum, cluster = ~estrato4)
  fs_inter <- feols(outage_x_appliance ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter, data = dat, weights = ~weight_sum, cluster = ~estrato4)

  base_row <- extract_first_match(iv, "^fit_outage_l0$")
  interact_row <- extract_first_match(iv, "^fit_outage_x_appliance$")

  tibble(
    sample = sample_name,
    group = group_label,
    appliance = appliance_label,
    appliance_mean = appliance_mean,
    appliance_sd = appliance_sd,
    outcome = outcome,
    base_coef = base_row$estimate[[1]],
    base_se = base_row$std.error[[1]],
    base_p = base_row$p.value[[1]],
    interaction_coef = interact_row$estimate[[1]],
    interaction_se = interact_row$std.error[[1]],
    interaction_p = interact_row$p.value[[1]],
    base_minutes = base_row$estimate[[1]] * 60000,
    base_minutes_se = base_row$std.error[[1]] * 60000,
    interaction_minutes_per_sd = interact_row$estimate[[1]] * 60000,
    interaction_minutes_per_sd_se = interact_row$std.error[[1]] * 60000,
    ivfall = safe_num(fitstat(iv, "ivfall")),
    fs_main_f = safe_num(fitstat(fs_main, "f")),
    fs_inter_f = safe_num(fitstat(fs_inter, "f")),
    nobs = nobs(iv)
  )
}

run_split_iv <- function(df, outcome, appliance_var, appliance_label, sample_name, group_label) {
  needed <- c("weight_sum", "estrato4", "year_quarter", "lightning_l0", "outage_l0", outcome, appliance_var)
  dat <- df %>% filter(if_all(all_of(needed), ~ !is.na(.x)), weight_sum > 0)
  if (nrow(dat) == 0) return(NULL)

  split_cut <- median(dat[[appliance_var]], na.rm = TRUE)
  dat <- dat %>%
    mutate(appliance_split = if_else(.data[[appliance_var]] >= split_cut, "High appliance", "Low appliance"))

  out <- list()
  kk <- 1L

  for (split_name in c("Low appliance", "High appliance")) {
    ds <- dat %>% filter(appliance_split == split_name)
    if (nrow(ds) == 0) next

    iv <- feols(
      as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 ~ lightning_l0")),
      data = ds,
      weights = ~weight_sum,
      cluster = ~estrato4
    )
    fs <- feols(outage_l0 ~ lightning_l0 | estrato4 + year_quarter, data = ds, weights = ~weight_sum, cluster = ~estrato4)
    iv_row <- extract_first_match(iv, "^fit_")

    out[[kk]] <- tibble(
      sample = sample_name,
      group = group_label,
      appliance = appliance_label,
      split = split_name,
      split_cut = split_cut,
      outcome = outcome,
      coef = iv_row$estimate[[1]],
      se = iv_row$std.error[[1]],
      p = iv_row$p.value[[1]],
      minutes = iv_row$estimate[[1]] * 60000,
      minutes_se = iv_row$std.error[[1]] * 60000,
      first_stage_f = safe_num(fitstat(fs, "f")),
      nobs = nobs(iv)
    )
    kk <- kk + 1L
  }

  bind_rows(out)
}

for (ii in seq_len(nrow(samples))) {
  sample_name <- samples$sample[[ii]]
  year_min <- samples$year_min[[ii]]
  year_max <- samples$year_max[[ii]]

  dat <- panel %>% filter(year >= year_min, year <= year_max)

  outcome_specs <- tibble::tribble(
    ~group_label, ~outcome,
    "Women", samples$female_outcome[[ii]],
    "Men", samples$male_outcome[[ii]],
    "Female minus male gap", samples$gap_outcome[[ii]]
  )

  for (jj in seq_len(nrow(appliance_specs))) {
    appliance_var <- appliance_specs$appliance_var[[jj]]
    appliance_label <- appliance_specs$appliance_label[[jj]]

    for (kk in seq_len(nrow(outcome_specs))) {
      group_label <- outcome_specs$group_label[[kk]]
      outcome <- outcome_specs$outcome[[kk]]

      interaction_rows[[idx_interaction]] <- run_interaction_iv(
        df = dat,
        outcome = outcome,
        appliance_var = appliance_var,
        appliance_label = appliance_label,
        sample_name = sample_name,
        group_label = group_label
      )
      idx_interaction <- idx_interaction + 1L

      split_rows[[idx_split]] <- run_split_iv(
        df = dat,
        outcome = outcome,
        appliance_var = appliance_var,
        appliance_label = appliance_label,
        sample_name = sample_name,
        group_label = group_label
      )
      idx_split <- idx_split + 1L
    }
  }
}

interaction_results <- bind_rows(interaction_rows) %>%
  arrange(appliance, sample, group)

split_results <- bind_rows(split_rows) %>%
  arrange(appliance, sample, group, split)

write_csv(interaction_results, interaction_out)
write_csv(split_results, split_out)

split_key <- split_results %>%
  filter(
    (appliance == "Baseline washing machine share" & sample == "pre_2016_2019") |
      (appliance == "Baseline electric cooking share" & sample == "recovery_2022_2024")
  ) %>%
  mutate(
    sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
    group_label = group,
    coef_se = paste0(format_num(minutes), " (", format_num(minutes_se), ")"),
    f_fmt = format_num(first_stage_f, 2),
    p_fmt = format_p(p)
  ) %>%
  select(appliance, sample_label, group_label, split, coef_se, p_fmt, f_fmt, nobs)

lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning IV hours effects by baseline appliance exposure}",
  "\\label{tab:lightning_iv_appliance_split}",
  "\\begin{tabular}{llllccc}",
  "\\hline",
  "Appliance baseline & Sample & Group & Split & IV effect (SE) & p-value & First-stage F \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(split_key))) {
  row <- split_key[ii, , drop = FALSE]
  lines <- c(
    lines,
    paste0(
      row$appliance[[1]], " & ",
      row$sample_label[[1]], " & ",
      row$group_label[[1]], " & ",
      row$split[[1]], " & ",
      row$coef_se[[1]], " & ",
      row$p_fmt[[1]], " & ",
      row$f_fmt[[1]], " \\\\"
    )
  )
}

lines <- c(
  lines,
  "\\hline",
  "\\multicolumn{7}{p{0.92\\linewidth}}{\\footnotesize Notes: Entries report IV coefficients in minutes per additional outage hour per household in the quarter. High and low appliance groups are defined using the median 2016 estrato-level baseline share for each appliance measure. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4. The corresponding interaction specifications are reported in the CSV output.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(lines, split_tex)

message("Wrote: ", interaction_out)
message("Wrote: ", split_out)
message("Wrote: ", split_tex)
print(interaction_results)
print(split_results)
