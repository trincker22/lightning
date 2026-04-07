#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

power_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
women_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_women_micro_fe_comparison", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

results_out <- here::here(out_dir, "women_micro_fe_comparison_results.csv")
key_out <- here::here(out_dir, "women_micro_fe_comparison_key.csv")

if (!file.exists(power_path)) stop("Missing file: ", power_path, call. = FALSE)
if (!file.exists(women_path)) stop("Missing file: ", women_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

extract_row <- function(model, pattern) {
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

add_l0 <- function(df) {
  df %>%
    arrange(estrato4, year, quarter) %>%
    group_by(estrato4) %>%
    mutate(
      lightning_l0 = as.numeric(lightning_quarterly_total),
      outage_l0 = as.numeric(raw_quarterly_outage_hrs_per_1000)
    ) %>%
    ungroup()
}

power_base <- read_parquet(power_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024)) %>%
  add_l0()

women_base <- read_parquet(
  women_path,
  col_select = c(
    "woman_spell_id", "woman_id", "estrato4", "year", "quarter", "person_weight",
    "employed", "effective_hours", "domestic_care_reason", "outlf_domestic_care",
    "wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m"
  )
) %>%
  transmute(
    woman_spell_id = coalesce(woman_spell_id, woman_id),
    estrato4 = as.character(estrato4),
    year = as.integer(year),
    quarter = as.integer(quarter),
    year_quarter = sprintf("%04dQ%d", year, quarter),
    person_weight = as.numeric(person_weight),
    employed = as.numeric(employed),
    effective_hours = as.numeric(effective_hours),
    effective_hours_population = if_else(employed == 1 & !is.na(effective_hours), effective_hours, 0),
    domestic_care_reason = as.numeric(domestic_care_reason),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    wanted_to_work = as.numeric(wanted_to_work),
    wants_more_hours = as.numeric(wants_more_hours),
    available_more_hours = as.numeric(available_more_hours),
    search_long = as.numeric(search_long),
    worked_last_12m = as.numeric(worked_last_12m)
  ) %>%
  filter((year >= 2016 & year <= 2019) | (year >= 2022 & year <= 2024))

outcomes <- tibble::tribble(
  ~outcome, ~label, ~scale, ~unit,
  "outlf_domestic_care", "Domestic care among women out of LF", 100000, "pp",
  "domestic_care_reason", "Domestic care among all women", 100000, "pp",
  "effective_hours_population", "Effective hours population", 60000, "minutes",
  "effective_hours", "Effective hours employed", 60000, "minutes",
  "wanted_to_work", "Wanted to work", 100000, "pp",
  "wants_more_hours", "Wanted more hours", 100000, "pp"
)

samples <- tibble::tribble(
  ~sample, ~year_min, ~year_max,
  "pre_2016_2019", 2016L, 2019L,
  "recovery_2022_2024", 2022L, 2024L
)

specs <- tibble::tribble(
  ~spec, ~repeat_only, ~fe_txt,
  "cross_fe_all", FALSE, "estrato4 + year_quarter",
  "cross_fe_repeat", TRUE, "estrato4 + year_quarter",
  "woman_fe_repeat", TRUE, "woman_spell_id + year_quarter"
)

rows <- list()
idx <- 1L

for (ii in seq_len(nrow(samples))) {
  sample_name <- samples$sample[[ii]]
  year_min <- samples$year_min[[ii]]
  year_max <- samples$year_max[[ii]]

  women_sample_full <- women_base %>%
    filter(year >= year_min, year <= year_max) %>%
    left_join(power_base, by = c("estrato4", "year", "quarter", "year_quarter"))

  repeat_ids <- women_sample_full %>%
    filter(!is.na(woman_spell_id)) %>%
    count(woman_spell_id, name = "n_quarters") %>%
    filter(n_quarters >= 2)

  women_sample_repeat <- women_sample_full %>%
    inner_join(repeat_ids, by = "woman_spell_id")

  for (jj in seq_len(nrow(specs))) {
    spec_name <- specs$spec[[jj]]
    repeat_only <- specs$repeat_only[[jj]]
    fe_txt <- specs$fe_txt[[jj]]

    dat_spec <- if (repeat_only) women_sample_repeat else women_sample_full

    for (kk in seq_len(nrow(outcomes))) {
      outcome <- outcomes$outcome[[kk]]
      label <- outcomes$label[[kk]]
      scale <- outcomes$scale[[kk]]
      unit <- outcomes$unit[[kk]]

      needed <- c(outcome, "person_weight", "estrato4", "year_quarter", "lightning_l0", "outage_l0")
      if (spec_name == "woman_fe_repeat") needed <- c(needed, "woman_spell_id")

      dat <- dat_spec %>%
        filter(if_all(all_of(needed), ~ !is.na(.x)), person_weight > 0)

      if (nrow(dat) == 0) next
      if (is.na(sd(dat[[outcome]], na.rm = TRUE)) || sd(dat[[outcome]], na.rm = TRUE) == 0) next

      fs_formula <- as.formula(paste0("outage_l0 ~ lightning_l0 | ", fe_txt))
      rf_formula <- as.formula(paste0(outcome, " ~ lightning_l0 | ", fe_txt))
      iv_formula <- as.formula(paste0(outcome, " ~ 1 | ", fe_txt, " | outage_l0 ~ lightning_l0"))

      fs <- feols(fs_formula, data = dat, weights = ~person_weight, cluster = ~estrato4)
      rf <- feols(rf_formula, data = dat, weights = ~person_weight, cluster = ~estrato4)
      iv <- feols(iv_formula, data = dat, weights = ~person_weight, cluster = ~estrato4)

      fs_row <- extract_row(fs, "^lightning_l0$")
      rf_row <- extract_row(rf, "^lightning_l0$")
      iv_row <- extract_row(iv, "^fit_")

      rows[[idx]] <- tibble(
        sample = sample_name,
        spec = spec_name,
        repeat_only = repeat_only,
        fixed_effects = fe_txt,
        outcome = outcome,
        outcome_label = label,
        unit = unit,
        model_family = "first_stage",
        estimate = fs_row$estimate[[1]],
        std.error = fs_row$std.error[[1]],
        p.value = fs_row$p.value[[1]],
        effect_scaled = fs_row$estimate[[1]],
        se_scaled = fs_row$std.error[[1]],
        first_stage_f = safe_num(fitstat(fs, "f")),
        ivf1 = NA_real_,
        nobs = nobs(fs),
        n_units = ifelse(spec_name == "woman_fe_repeat", n_distinct(dat$woman_spell_id), n_distinct(dat$estrato4))
      )
      idx <- idx + 1L

      rows[[idx]] <- tibble(
        sample = sample_name,
        spec = spec_name,
        repeat_only = repeat_only,
        fixed_effects = fe_txt,
        outcome = outcome,
        outcome_label = label,
        unit = unit,
        model_family = "reduced_form",
        estimate = rf_row$estimate[[1]],
        std.error = rf_row$std.error[[1]],
        p.value = rf_row$p.value[[1]],
        effect_scaled = rf_row$estimate[[1]] * scale,
        se_scaled = rf_row$std.error[[1]] * scale,
        first_stage_f = NA_real_,
        ivf1 = NA_real_,
        nobs = nobs(rf),
        n_units = ifelse(spec_name == "woman_fe_repeat", n_distinct(dat$woman_spell_id), n_distinct(dat$estrato4))
      )
      idx <- idx + 1L

      rows[[idx]] <- tibble(
        sample = sample_name,
        spec = spec_name,
        repeat_only = repeat_only,
        fixed_effects = fe_txt,
        outcome = outcome,
        outcome_label = label,
        unit = unit,
        model_family = "iv",
        estimate = iv_row$estimate[[1]],
        std.error = iv_row$std.error[[1]],
        p.value = iv_row$p.value[[1]],
        effect_scaled = iv_row$estimate[[1]] * scale,
        se_scaled = iv_row$std.error[[1]] * scale,
        first_stage_f = NA_real_,
        ivf1 = safe_num(fitstat(iv, "ivf1")),
        nobs = nobs(iv),
        n_units = ifelse(spec_name == "woman_fe_repeat", n_distinct(dat$woman_spell_id), n_distinct(dat$estrato4))
      )
      idx <- idx + 1L
    }
  }
}

results <- bind_rows(rows) %>%
  arrange(outcome, sample, spec, model_family)

key_results <- results %>%
  filter(model_family == "iv") %>%
  select(sample, spec, outcome, outcome_label, unit, effect_scaled, se_scaled, p.value, ivf1, nobs, n_units)

write_csv(results, results_out)
write_csv(key_results, key_out)

message("Wrote: ", results_out)
message("Wrote: ", key_out)
print(key_results)
