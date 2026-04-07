suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here("data", "powerIV", "regressions", "lpg_timing_levels_checks")
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
    female_hours_effective_employed
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
    year_quarter = sprintf("%dQ%d", year, quarter),
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

national_quarter <- lpg_uf %>%
  group_by(year, quarter, year_quarter) %>%
  summarise(
    lpg_price_quarterly_national = mean(lpg_price_quarterly_uf, na.rm = TRUE),
    log_lpg_price_quarterly_national = mean(log_lpg_price_quarterly_uf, na.rm = TRUE),
    .groups = "drop"
  )

price_series <- lpg_uf %>%
  left_join(national_quarter, by = c("year", "quarter", "year_quarter")) %>%
  mutate(
    quarter_index = year * 4 + quarter,
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly_national,
    rel_level_lpg_price_uf = lpg_price_quarterly_uf - lpg_price_quarterly_national
  ) %>%
  arrange(uf_sigla, year, quarter) %>%
  group_by(uf_sigla) %>%
  mutate(
    log_lpg_price_quarterly_uf_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(log_lpg_price_quarterly_uf), NA_real_),
    lpg_price_quarterly_uf_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(lpg_price_quarterly_uf), NA_real_),
    rel_log_lpg_price_uf_dm = rel_log_lpg_price_uf - mean(rel_log_lpg_price_uf, na.rm = TRUE),
    rel_log_lpg_price_uf_dm_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(rel_log_lpg_price_uf_dm), NA_real_)
  ) %>%
  ungroup() %>%
  select(
    uf_sigla, year, quarter, year_quarter,
    lpg_price_quarterly_uf, log_lpg_price_quarterly_uf,
    lpg_price_quarterly_uf_l1, log_lpg_price_quarterly_uf_l1,
    rel_log_lpg_price_uf_dm_l1
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(price_series, by = c("uf_sigla", "year", "quarter", "year_quarter")) %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    z_log_0 = baseline_wood_2016 * log_lpg_price_quarterly_uf,
    z_log_1 = baseline_wood_2016 * log_lpg_price_quarterly_uf_l1,
    z_level_0 = baseline_wood_2016 * lpg_price_quarterly_uf,
    z_level_1 = baseline_wood_2016 * lpg_price_quarterly_uf_l1,
    z_rel_log_dm_1 = baseline_wood_2016 * rel_log_lpg_price_uf_dm_l1
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
)

designs <- tibble::tribble(
  ~design, ~instrument,
  "log_t", "z_log_0",
  "log_t1", "z_log_1",
  "level_t", "z_level_0",
  "level_t1", "z_level_1",
  "demeaned_rel_log_t1", "z_rel_log_dm_1"
)

run_fs <- function(data, instrument_var, controls) {
  feols(
    as.formula(paste0("share_cooking_wood_charcoal ~ ", instrument_var, " + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_rf <- function(data, instrument_var, controls) {
  feols(
    as.formula(paste0("female_hours_effective_employed ~ ", instrument_var, " + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_iv <- function(data, instrument_var, controls) {
  feols(
    as.formula(paste0("female_hours_effective_employed ~ ", controls, " | estrato4 + year_quarter | share_cooking_wood_charcoal ~ ", instrument_var)),
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
      "female_hours_effective_employed", instrument_var, all.vars(as.formula(paste("~", controls)))
    ))

    reg_data <- panel %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))

    fs_model <- run_fs(reg_data, instrument_var, controls)
    rf_model <- run_rf(reg_data, instrument_var, controls)
    iv_model <- run_iv(reg_data, instrument_var, controls)

    fs_term <- term_row(fs_model, instrument_var)
    rf_term <- term_row(rf_model, instrument_var)
    iv_term <- term_row(iv_model, "fit_share_cooking_wood_charcoal")

    results[[length(results) + 1]] <- tibble(
      design = design_name,
      spec = spec_name,
      n = nobs(fs_model),
      fs_coef = fs_term$estimate,
      fs_se = fs_term$std.error,
      fs_p = fs_term$p.value,
      fs_f = wald_f(fs_model, instrument_var),
      rf_coef = rf_term$estimate,
      rf_se = rf_term$std.error,
      rf_p = rf_term$p.value,
      iv_coef = iv_term$estimate,
      iv_se = iv_term$std.error,
      iv_p = iv_term$p.value
    )
  }
}

results_df <- bind_rows(results)
write.table(results_df, file = here("data", "powerIV", "regressions", "lpg_timing_levels_checks", "timing_levels_results.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
print(results_df)
