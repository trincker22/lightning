suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here("data", "powerIV", "regressions", "lpg_2023q2_reform_designs")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_path <- here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_uf_path <- here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet")
macro_path <- here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP", "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
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

lfp <- read_parquet(lfp_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    uf,
    uf_sigla = unname(uf_to_sigla[uf]),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    weight_sum,
    female_hours_effective_employed,
    female_hours_effective_population
  )

power <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_cooking_wood_charcoal,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

lpg_uf <- read_parquet(lpg_uf_path) %>%
  transmute(
    uf_sigla = as.character(uf_sigla),
    year,
    quarter,
    lpg_price_quarterly_uf,
    log_lpg_price_quarterly_uf
  )

macro <- read_parquet(macro_path) %>%
  transmute(
    uf_sigla = as.character(uf_sigla),
    year,
    quarter,
    uf_unemployment_rate,
    uf_labor_demand_proxy,
    uf_real_wage_proxy,
    uf_share_secondaryplus,
    uf_share_tertiary
  )

baseline_shift <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency
  )

baseline_wood <- power %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

pre_reform_uf <- lpg_uf %>%
  filter(year < 2023 | (year == 2023 & quarter <= 1)) %>%
  group_by(uf_sigla) %>%
  summarise(
    pre_reform_uf_price_level = mean(lpg_price_quarterly_uf, na.rm = TRUE),
    pre_reform_uf_log_price = mean(log_lpg_price_quarterly_uf, na.rm = TRUE),
    .groups = "drop"
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(pre_reform_uf, by = "uf_sigla") %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    post_reform_2023q2 = as.integer(year > 2023 | (year == 2023 & quarter >= 2)),
    z_reform_common = baseline_wood_2016 * post_reform_2023q2,
    z_reform_intensity = baseline_wood_2016 * pre_reform_uf_price_level * post_reform_2023q2,
    z_reform_intensity_log = baseline_wood_2016 * pre_reform_uf_log_price * post_reform_2023q2
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time",
  "M3", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

designs <- tibble::tribble(
  ~design, ~instrument,
  "common_post", "z_reform_common",
  "intensity_level_post", "z_reform_intensity",
  "intensity_log_post", "z_reform_intensity_log"
)

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
    spec_name <- specs$spec[[j]]
    controls <- specs$controls[[j]]

    vars_needed <- unique(c(
      "estrato4", "year_quarter", "weight_sum", "share_cooking_wood_charcoal",
      "female_hours_effective_employed", "female_hours_effective_population",
      instrument_var, all.vars(as.formula(paste("~", controls)))
    ))

    reg_data <- panel %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))

    fs_model <- run_fs(reg_data, instrument_var, controls)
    rf_emp_model <- run_rf(reg_data, "female_hours_effective_employed", instrument_var, controls)
    iv_emp_model <- run_iv(reg_data, "female_hours_effective_employed", instrument_var, controls)
    rf_pop_model <- run_rf(reg_data, "female_hours_effective_population", instrument_var, controls)
    iv_pop_model <- run_iv(reg_data, "female_hours_effective_population", instrument_var, controls)

    fs_term <- term_row(fs_model, instrument_var)
    rf_emp_term <- term_row(rf_emp_model, instrument_var)
    rf_pop_term <- term_row(rf_pop_model, instrument_var)
    iv_emp_term <- term_row(iv_emp_model, "fit_share_cooking_wood_charcoal")
    iv_pop_term <- term_row(iv_pop_model, "fit_share_cooking_wood_charcoal")

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
      iv_emp_p = iv_emp_term$p.value,
      rf_pop_coef = rf_pop_term$estimate,
      rf_pop_se = rf_pop_term$std.error,
      rf_pop_p = rf_pop_term$p.value,
      iv_pop_coef = iv_pop_term$estimate,
      iv_pop_se = iv_pop_term$std.error,
      iv_pop_p = iv_pop_term$p.value
    )
  }
}

results_df <- bind_rows(results)
write.table(results_df, file = here("data", "powerIV", "regressions", "lpg_2023q2_reform_designs", "reform_results.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
print(results_df)
