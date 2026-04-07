suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_topup_design")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

auxgas <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet")) %>%
  transmute(
    codigo_ibge = as.integer(codigo_ibge),
    anomes,
    year,
    month,
    quarter,
    auxgas_families,
    auxgas_total_value,
    auxgas_avg_benefit,
    auxgas_people,
    auxgas_female_rf,
    auxgas_female_rf_share
  )

crosswalk <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_estrato_municipio_crosswalk.parquet")) %>%
  transmute(
    codigo_ibge = as.integer(municipio_code %/% 10),
    estrato4 = as.character(estrato4)
  )

lfp <- read_parquet(here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    weight_sum,
    female_hours_effective_employed,
    female_hours_effective_population
  )

power <- read_parquet(here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    share_cooking_wood_charcoal,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

baseline_shift <- read_parquet(here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency
  )

auxgas_q <- auxgas %>%
  left_join(crosswalk, by = "codigo_ibge") %>%
  filter(!is.na(estrato4)) %>%
  group_by(estrato4, year, quarter) %>%
  summarise(
    auxgas_families = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people = sum(auxgas_people, na.rm = TRUE),
    auxgas_female_rf = sum(auxgas_female_rf, na.rm = TRUE),
    auxgas_avg_benefit = if_else(auxgas_families > 0, auxgas_total_value / auxgas_families, NA_real_),
    n_municipalities = n_distinct(codigo_ibge),
    .groups = "drop"
  ) %>%
  mutate(year_quarter = sprintf("%dQ%d", year, quarter))

pre_exposure <- auxgas_q %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_auxgas_families = mean(auxgas_families, na.rm = TRUE),
    pre_auxgas_total_value = mean(auxgas_total_value, na.rm = TRUE),
    pre_auxgas_people = mean(auxgas_people, na.rm = TRUE),
    pre_auxgas_avg_benefit = mean(auxgas_avg_benefit, na.rm = TRUE),
    .groups = "drop"
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(pre_exposure, by = "estrato4") %>%
  left_join(baseline_shift, by = "estrato4") %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3)),
    z_topup_families = pre_auxgas_families * post_topup_2022q3,
    z_topup_people = pre_auxgas_people * post_topup_2022q3,
    z_topup_value = pre_auxgas_total_value * post_topup_2022q3
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
)

designs <- tibble::tribble(
  ~design, ~instrument,
  "families_post", "z_topup_families",
  "people_post", "z_topup_people",
  "value_post", "z_topup_value"
)

term_row <- function(model, term) {
  tbl <- tidy(model)
  row <- tbl[tbl$term == term, , drop = FALSE]
  if (nrow(row) == 0) return(tibble(estimate = NA_real_, std.error = NA_real_, p.value = NA_real_))
  tibble(estimate = row$estimate[[1]], std.error = row$std.error[[1]], p.value = row$p.value[[1]])
}

wald_f <- function(model, term) {
  out <- tryCatch(wald(model, term), error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  stat <- suppressWarnings(as.numeric(unlist(out$stat)))
  stat <- stat[!is.na(stat)]
  if (length(stat) == 0) return(NA_real_)
  stat[[1]]
}

run_fs <- function(data, instrument_var, controls) {
  feols(
    as.formula(paste0("share_cooking_wood_charcoal ~ ", instrument_var, " + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_rf <- function(data, outcome, instrument_var, controls) {
  feols(
    as.formula(paste0(outcome, " ~ ", instrument_var, " + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_iv <- function(data, outcome, instrument_var, controls) {
  feols(
    as.formula(paste0(outcome, " ~ ", controls, " | estrato4 + year_quarter | share_cooking_wood_charcoal ~ ", instrument_var)),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

results <- list()

for (i in seq_len(nrow(designs))) {
  instrument_var <- designs$instrument[[i]]
  design_name <- designs$design[[i]]

  for (j in seq_len(nrow(specs))) {
    controls <- specs$controls[[j]]
    spec_name <- specs$spec[[j]]

    vars_needed <- unique(c(
      "estrato4", "year_quarter", "weight_sum", "share_cooking_wood_charcoal",
      "female_hours_effective_employed", "female_hours_effective_population",
      instrument_var, all.vars(as.formula(paste("~", controls)))
    ))

    reg_data <- panel %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))

    fs_model <- run_fs(reg_data, instrument_var, controls)
    rf_emp_model <- run_rf(reg_data, "female_hours_effective_employed", instrument_var, controls)
    iv_emp_model <- run_iv(reg_data, "female_hours_effective_employed", instrument_var, controls)

    fs_term <- term_row(fs_model, instrument_var)
    rf_emp_term <- term_row(rf_emp_model, instrument_var)
    iv_emp_term <- term_row(iv_emp_model, "fit_share_cooking_wood_charcoal")

    results[[length(results) + 1]] <- tibble(
      design = design_name,
      spec = spec_name,
      n = nobs(fs_model),
      fs_coef = fs_term$estimate,
      fs_se = fs_term$std.error,
      fs_p = fs_term$p.value,
      fs_f = wald_f(fs_model, instrument_var),
      rf_emp_coef = rf_emp_term$estimate,
      rf_emp_se = rf_emp_term$std.error,
      rf_emp_p = rf_emp_term$p.value,
      iv_emp_coef = iv_emp_term$estimate,
      iv_emp_se = iv_emp_term$std.error,
      iv_emp_p = iv_emp_term$p.value
    )
  }
}

results_df <- bind_rows(results)
coverage_df <- tibble(
  metric = c(
    "auxgas_panel_rows",
    "crosswalk_rows",
    "matched_municipalities_in_auxgas_raw",
    "matched_estrato_quarters",
    "first_year",
    "last_year"
  ),
  value = c(
    as.character(nrow(auxgas_q)),
    as.character(nrow(crosswalk)),
    as.character(n_distinct(auxgas %>% left_join(crosswalk, by = "codigo_ibge") %>% filter(!is.na(estrato4)) %>% pull(codigo_ibge))),
    as.character(n_distinct(paste(auxgas_q$estrato4, auxgas_q$year_quarter))),
    as.character(min(auxgas_q$year, na.rm = TRUE)),
    as.character(max(auxgas_q$year, na.rm = TRUE))
  )
)

write_parquet(auxgas_q, here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet"))
write.table(results_df, here("data", "powerIV", "regressions", "auxilio_gas_topup_design", "topup_design_results.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(coverage_df, here("data", "powerIV", "regressions", "auxilio_gas_topup_design", "topup_design_coverage.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

cat(here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet"), "\n")
print(coverage_df)
print(results_df)
