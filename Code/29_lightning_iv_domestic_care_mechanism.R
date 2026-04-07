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

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_domestic_care_mechanism", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

main_out <- here::here(out_dir, "main_results.csv")
interaction_out <- here::here(out_dir, "interaction_results.csv")
split_out <- here::here(out_dir, "split_results.csv")
main_tex <- here::here(out_dir, "domestic_care_main.tex")
split_tex <- here::here(out_dir, "domestic_care_split.tex")

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
    female_domestic_care_reason = as.numeric(female_domestic_care_reason),
    female_outlf_domestic_care = as.numeric(female_outlf_domestic_care)
  ) %>%
  left_join(baseline_appliance, by = "estrato4")

samples <- tibble::tribble(
  ~sample, ~year_min, ~year_max,
  "pre_2016_2019", 2016L, 2019L,
  "recovery_2022_2024", 2022L, 2024L
)

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "female_domestic_care_reason", "Domestic care reason",
  "female_outlf_domestic_care", "Out of labor force for domestic care"
)

appliance_specs <- tibble::tribble(
  ~appliance_var, ~appliance_label,
  "baseline_washing_machine", "Baseline washing machine share",
  "baseline_cooking_electricity", "Baseline electric cooking share"
)

main_rows <- list()
interaction_rows <- list()
split_rows <- list()
idx_main <- 1L
idx_interaction <- 1L
idx_split <- 1L

for (ii in seq_len(nrow(samples))) {
  sample_name <- samples$sample[[ii]]
  year_min <- samples$year_min[[ii]]
  year_max <- samples$year_max[[ii]]

  dat_sample <- panel %>%
    filter(year >= year_min, year <= year_max)

  for (jj in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[jj]]
    outcome_label <- outcomes$outcome_label[[jj]]

    dat_main <- dat_sample %>%
      filter(
        !is.na(weight_sum),
        weight_sum > 0,
        !is.na(lightning_l0),
        !is.na(outage_l0),
        !is.na(.data[[outcome]])
      )

    if (nrow(dat_main) > 0) {
      iv_main <- feols(
        as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 ~ lightning_l0")),
        data = dat_main,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      fs_main <- feols(
        outage_l0 ~ lightning_l0 | estrato4 + year_quarter,
        data = dat_main,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      iv_row <- extract_first_match(iv_main, "^fit_")

      main_rows[[idx_main]] <- tibble(
        sample = sample_name,
        outcome = outcome,
        outcome_label = outcome_label,
        coef = iv_row$estimate[[1]],
        se = iv_row$std.error[[1]],
        p = iv_row$p.value[[1]],
        pp = iv_row$estimate[[1]] * 100000,
        pp_se = iv_row$std.error[[1]] * 100000,
        first_stage_f = safe_num(fitstat(fs_main, "f")),
        nobs = nobs(iv_main)
      )
      idx_main <- idx_main + 1L
    }

    for (kk in seq_len(nrow(appliance_specs))) {
      appliance_var <- appliance_specs$appliance_var[[kk]]
      appliance_label <- appliance_specs$appliance_label[[kk]]

      dat_het <- dat_sample %>%
        filter(
          !is.na(weight_sum),
          weight_sum > 0,
          !is.na(lightning_l0),
          !is.na(outage_l0),
          !is.na(.data[[outcome]]),
          !is.na(.data[[appliance_var]])
        )

      if (nrow(dat_het) == 0) next

      appliance_mean <- mean(dat_het[[appliance_var]], na.rm = TRUE)
      appliance_sd <- sd(dat_het[[appliance_var]], na.rm = TRUE)

      if (!is.na(appliance_sd) && appliance_sd > 0) {
        dat_interaction <- dat_het %>%
          mutate(
            appliance_std = (.data[[appliance_var]] - appliance_mean) / appliance_sd,
            outage_x_appliance = outage_l0 * appliance_std,
            lightning_x_appliance = lightning_l0 * appliance_std
          )

        iv_interaction <- feols(
          as.formula(paste0(outcome, " ~ 1 | estrato4 + year_quarter | outage_l0 + outage_x_appliance ~ lightning_l0 + lightning_x_appliance")),
          data = dat_interaction,
          weights = ~weight_sum,
          cluster = ~estrato4
        )

        fs_main_interaction <- feols(
          outage_l0 ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter,
          data = dat_interaction,
          weights = ~weight_sum,
          cluster = ~estrato4
        )

        fs_interaction <- feols(
          outage_x_appliance ~ lightning_l0 + lightning_x_appliance | estrato4 + year_quarter,
          data = dat_interaction,
          weights = ~weight_sum,
          cluster = ~estrato4
        )

        base_row <- extract_first_match(iv_interaction, "^fit_outage_l0$")
        interact_row <- extract_first_match(iv_interaction, "^fit_outage_x_appliance$")

        interaction_rows[[idx_interaction]] <- tibble(
          sample = sample_name,
          outcome = outcome,
          outcome_label = outcome_label,
          appliance = appliance_label,
          appliance_mean = appliance_mean,
          appliance_sd = appliance_sd,
          base_coef = base_row$estimate[[1]],
          base_se = base_row$std.error[[1]],
          base_p = base_row$p.value[[1]],
          interaction_coef = interact_row$estimate[[1]],
          interaction_se = interact_row$std.error[[1]],
          interaction_p = interact_row$p.value[[1]],
          base_pp = base_row$estimate[[1]] * 100000,
          base_pp_se = base_row$std.error[[1]] * 100000,
          interaction_pp_per_sd = interact_row$estimate[[1]] * 100000,
          interaction_pp_per_sd_se = interact_row$std.error[[1]] * 100000,
          fs_main_f = safe_num(fitstat(fs_main_interaction, "f")),
          fs_interaction_f = safe_num(fitstat(fs_interaction, "f")),
          nobs = nobs(iv_interaction)
        )
        idx_interaction <- idx_interaction + 1L
      }

      split_cut <- median(dat_het[[appliance_var]], na.rm = TRUE)
      dat_split <- dat_het %>%
        mutate(appliance_split = if_else(.data[[appliance_var]] >= split_cut, "High appliance", "Low appliance"))

      for (split_name in c("Low appliance", "High appliance")) {
        ds <- dat_split %>% filter(appliance_split == split_name)
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

        iv_split_row <- extract_first_match(iv_split, "^fit_")

        split_rows[[idx_split]] <- tibble(
          sample = sample_name,
          outcome = outcome,
          outcome_label = outcome_label,
          appliance = appliance_label,
          split = split_name,
          split_cut = split_cut,
          coef = iv_split_row$estimate[[1]],
          se = iv_split_row$std.error[[1]],
          p = iv_split_row$p.value[[1]],
          pp = iv_split_row$estimate[[1]] * 100000,
          pp_se = iv_split_row$std.error[[1]] * 100000,
          first_stage_f = safe_num(fitstat(fs_split, "f")),
          nobs = nobs(iv_split)
        )
        idx_split <- idx_split + 1L
      }
    }
  }
}

main_results <- bind_rows(main_rows) %>%
  arrange(outcome, sample)

interaction_results <- bind_rows(interaction_rows) %>%
  arrange(outcome, appliance, sample)

split_results <- bind_rows(split_rows) %>%
  arrange(outcome, appliance, sample, split)

write_csv(main_results, main_out)
write_csv(interaction_results, interaction_out)
write_csv(split_results, split_out)

main_table <- main_results %>%
  mutate(
    sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
    coef_se = paste0(format_num(pp), " (", format_num(pp_se), ")"),
    p_fmt = format_p(p),
    f_fmt = format_num(first_stage_f, 2)
  ) %>%
  select(outcome_label, sample_label, coef_se, p_fmt, f_fmt, nobs)

main_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning IV domestic-care outcomes}",
  "\\label{tab:lightning_iv_domestic_care_main}",
  "\\begin{tabular}{llccc}",
  "\\hline",
  "Outcome & Sample & IV effect (SE) & p-value & First-stage F \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(main_table))) {
  row <- main_table[ii, , drop = FALSE]
  main_lines <- c(
    main_lines,
    paste0(
      row$outcome_label[[1]], " & ",
      row$sample_label[[1]], " & ",
      row$coef_se[[1]], " & ",
      row$p_fmt[[1]], " & ",
      row$f_fmt[[1]], " \\\\"
    )
  )
}

main_lines <- c(
  main_lines,
  "\\hline",
  "\\multicolumn{5}{p{0.9\\linewidth}}{\\footnotesize Notes: Entries report percentage-point effects per additional outage hour per household in the quarter. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4. The excluded 2020--2021 pandemic period is omitted.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(main_lines, main_tex)

split_table <- split_results %>%
  mutate(
    sample_label = if_else(sample == "pre_2016_2019", "2016--2019", "2022--2024"),
    coef_se = paste0(format_num(pp), " (", format_num(pp_se), ")"),
    p_fmt = format_p(p),
    f_fmt = format_num(first_stage_f, 2)
  ) %>%
  select(outcome_label, appliance, sample_label, split, coef_se, p_fmt, f_fmt, nobs)

split_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Lightning IV domestic-care outcomes by baseline appliance exposure}",
  "\\label{tab:lightning_iv_domestic_care_split}",
  "\\begin{tabular}{llllccc}",
  "\\hline",
  "Outcome & Appliance baseline & Sample & Split & IV effect (SE) & p-value & First-stage F \\\\",
  "\\hline"
)

for (ii in seq_len(nrow(split_table))) {
  row <- split_table[ii, , drop = FALSE]
  split_lines <- c(
    split_lines,
    paste0(
      row$outcome_label[[1]], " & ",
      row$appliance[[1]], " & ",
      row$sample_label[[1]], " & ",
      row$split[[1]], " & ",
      row$coef_se[[1]], " & ",
      row$p_fmt[[1]], " & ",
      row$f_fmt[[1]], " \\\\"
    )
  )
}

split_lines <- c(
  split_lines,
  "\\hline",
  "\\multicolumn{7}{p{0.92\\linewidth}}{\\footnotesize Notes: Entries report percentage-point effects per additional outage hour per household in the quarter. High and low appliance groups are defined using the median 2016 estrato-level baseline share for each appliance measure. All specifications include estrato4 and year-quarter fixed effects and cluster standard errors at estrato4. Interaction specifications using continuous baseline appliance shares are reported in the CSV output.}\\\\",
  "\\end{tabular}",
  "\\end{table}"
)

writeLines(split_lines, split_tex)

message("Wrote: ", main_out)
message("Wrote: ", interaction_out)
message("Wrote: ", split_out)
message("Wrote: ", main_tex)
message("Wrote: ", split_tex)
print(main_results)
print(interaction_results)
print(split_results)
