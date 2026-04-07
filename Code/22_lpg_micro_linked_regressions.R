#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(fixest)
  library(broom)
})

linked_in <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")
power_in <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_in <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")
macro_in <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
out_dir <- here::here("data", "powerIV", "regressions", "lpg_micro_linked")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

audit_out <- here::here("data", "powerIV", "regressions", "lpg_micro_linked", "linked_sample_audit.csv")
audit_yq_out <- here::here("data", "powerIV", "regressions", "lpg_micro_linked", "linked_sample_audit_by_year_quarter.csv")
fs_out <- here::here("data", "powerIV", "regressions", "lpg_micro_linked", "first_stage.csv")
rf_out <- here::here("data", "powerIV", "regressions", "lpg_micro_linked", "reduced_form.csv")
iv_out <- here::here("data", "powerIV", "regressions", "lpg_micro_linked", "iv.csv")

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP", "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

cols_needed <- c(
  "year", "quarter", "year_quarter", "uf", "estrato4", "person_weight", "age", "female_secondaryplus", "female_tertiary",
  "employed", "domestic_care_reason", "outlf_domestic_care", "wanted_to_work", "wants_more_hours", "available_more_hours",
  "search_long", "worked_last_12m", "effective_hours", "hh_size", "n_children_0_14", "visit1_match_any", "visit1_same_quarter",
  "carry_abs_distance_q", "visit1_n_rooms", "visit1_n_bedrooms", "visit1_cooking_wood_charcoal", "visit1_cooking_gas_any",
  "visit1_cooking_electricity", "visit1_electricity_from_grid", "visit1_electricity_grid_full_time", "visit1_internet_home",
  "visit1_vehicle", "visit1_refrigerator", "visit1_washing_machine", "visit1_satellite_dish_tv", "visit1_computer",
  "visit1_appliance_count_basic", "visit1_appliance_count_connected"
)

women <- read_parquet(linked_in, col_select = all_of(cols_needed)) %>%
  mutate(
    estrato4 = as.character(estrato4),
    uf_sigla = unname(uf_to_sigla[uf]),
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0),
    visit1_cooking_modern = case_when(
      is.na(visit1_cooking_gas_any) & is.na(visit1_cooking_electricity) ~ NA_real_,
      TRUE ~ pmax(coalesce(visit1_cooking_gas_any, 0), coalesce(visit1_cooking_electricity, 0))
    )
  )

baseline_power <- read_parquet(power_in, col_select = c("estrato4", "year", "share_cooking_wood_charcoal")) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

lpg_quarter_estrato <- read_parquet(lpg_in, col_select = c("estrato4", "year", "quarter", "log_lpg_price_quarterly_estrato")) %>%
  mutate(estrato4 = as.character(estrato4))

macro_panel <- read_parquet(macro_in, col_select = c("uf_sigla", "year", "quarter", "uf_unemployment_rate", "uf_labor_demand_proxy", "uf_real_wage_proxy", "uf_share_secondaryplus", "uf_share_tertiary")) %>%
  mutate(uf_sigla = as.character(uf_sigla))

women_reg <- women %>%
  left_join(baseline_power, by = "estrato4") %>%
  left_join(lpg_quarter_estrato, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro_panel, by = c("uf_sigla", "year", "quarter")) %>%
  mutate(z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato)

sample_defs <- tibble::tribble(
  ~sample, ~sample_filter,
  "all_linked", "visit1_match_any == TRUE",
  "carry_le1", "visit1_match_any == TRUE & carry_abs_distance_q <= 1",
  "same_quarter", "visit1_same_quarter == TRUE"
)

control_sets <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "visit1_n_rooms + visit1_n_bedrooms + hh_size + n_children_0_14",
  "M3", "visit1_n_rooms + visit1_n_bedrooms + hh_size + n_children_0_14 + age + age_sq + female_secondaryplus + female_tertiary + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

outcomes <- c(
  "domestic_care_reason",
  "outlf_domestic_care",
  "wanted_to_work",
  "wants_more_hours",
  "available_more_hours",
  "search_long",
  "worked_last_12m",
  "effective_hours",
  "effective_hours_population"
)

audit_rows <- list()
audit_yq_rows <- list()
fs_rows <- list()
rf_rows <- list()
iv_rows <- list()
a_i <- 1L
ay_i <- 1L
fs_i <- 1L
rf_i <- 1L
iv_i <- 1L

for (s in seq_len(nrow(sample_defs))) {
  sample_name <- sample_defs$sample[[s]]
  dat_sample <- women_reg %>% filter(!!rlang::parse_expr(sample_defs$sample_filter[[s]]))

  audit_rows[[a_i]] <- tibble(
    sample = sample_name,
    n = nrow(dat_sample),
    mean_carry_abs_distance_q = mean(dat_sample$carry_abs_distance_q, na.rm = TRUE),
    share_carry_0 = mean(dat_sample$carry_abs_distance_q == 0, na.rm = TRUE),
    share_carry_1 = mean(dat_sample$carry_abs_distance_q == 1, na.rm = TRUE),
    share_carry_2plus = mean(dat_sample$carry_abs_distance_q >= 2, na.rm = TRUE),
    wood_share = weighted.mean(dat_sample$visit1_cooking_wood_charcoal, dat_sample$person_weight, na.rm = TRUE),
    gas_share = weighted.mean(dat_sample$visit1_cooking_gas_any, dat_sample$person_weight, na.rm = TRUE),
    electric_share = weighted.mean(dat_sample$visit1_cooking_electricity, dat_sample$person_weight, na.rm = TRUE),
    rooms_mean = weighted.mean(dat_sample$visit1_n_rooms, dat_sample$person_weight, na.rm = TRUE),
    bedrooms_mean = weighted.mean(dat_sample$visit1_n_bedrooms, dat_sample$person_weight, na.rm = TRUE),
    fridge_share = weighted.mean(dat_sample$visit1_refrigerator, dat_sample$person_weight, na.rm = TRUE),
    washing_share = weighted.mean(dat_sample$visit1_washing_machine, dat_sample$person_weight, na.rm = TRUE),
    computer_share = weighted.mean(dat_sample$visit1_computer, dat_sample$person_weight, na.rm = TRUE),
    internet_share = weighted.mean(dat_sample$visit1_internet_home, dat_sample$person_weight, na.rm = TRUE),
    vehicle_share = weighted.mean(dat_sample$visit1_vehicle, dat_sample$person_weight, na.rm = TRUE),
    connected_count = weighted.mean(dat_sample$visit1_appliance_count_connected, dat_sample$person_weight, na.rm = TRUE)
  )
  a_i <- a_i + 1L

  audit_yq_rows[[ay_i]] <- dat_sample %>%
    group_by(year, quarter) %>%
    summarise(
      sample = sample_name,
      n = n(),
      mean_carry_abs_distance_q = mean(carry_abs_distance_q, na.rm = TRUE),
      wood_share = weighted.mean(visit1_cooking_wood_charcoal, person_weight, na.rm = TRUE),
      gas_share = weighted.mean(visit1_cooking_gas_any, person_weight, na.rm = TRUE),
      electric_share = weighted.mean(visit1_cooking_electricity, person_weight, na.rm = TRUE),
      .groups = "drop"
    )
  ay_i <- ay_i + 1L

  for (i in seq_len(nrow(control_sets))) {
    spec <- control_sets$spec[[i]]
    controls <- control_sets$controls[[i]]
    needed_controls <- all.vars(as.formula(paste("~", controls)))

    fs_needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", "visit1_cooking_wood_charcoal", needed_controls))
    dat_fs <- dat_sample %>% filter(if_all(all_of(fs_needed), ~ !is.na(.x)))
    fs_model <- feols(as.formula(paste0("visit1_cooking_wood_charcoal ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat_fs, weights = ~person_weight, cluster = ~estrato4)
    fs_term <- tidy(fs_model) %>% filter(term == "z_wood_lpg_estrato")
    suppressWarnings(capture.output(fs_wald <- wald(fs_model, "z_wood_lpg_estrato")))
    fs_rows[[fs_i]] <- tibble(
      sample = sample_name,
      spec = spec,
      nobs = nobs(fs_model),
      coef = fs_term$estimate[[1]],
      se = fs_term$std.error[[1]],
      p = fs_term$p.value[[1]],
      fstat = as.numeric(unlist(fs_wald$stat)[1])
    )
    fs_i <- fs_i + 1L

    for (y in outcomes) {
      needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", "visit1_cooking_wood_charcoal", y, needed_controls))
      dat <- dat_sample %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
      rf_model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
      iv_model <- feols(as.formula(paste0(y, " ~ ", controls, " | estrato4 + year_quarter | visit1_cooking_wood_charcoal ~ z_wood_lpg_estrato")), data = dat, weights = ~person_weight, cluster = ~estrato4)
      rf_term <- tidy(rf_model) %>% filter(term == "z_wood_lpg_estrato")
      iv_term <- tidy(iv_model) %>% filter(grepl("^fit_", term))

      rf_rows[[rf_i]] <- tibble(
        sample = sample_name,
        spec = spec,
        outcome = y,
        nobs = nobs(rf_model),
        coef = rf_term$estimate[[1]],
        se = rf_term$std.error[[1]],
        p = rf_term$p.value[[1]]
      )
      rf_i <- rf_i + 1L

      suppressWarnings(ivwald_raw <- fitstat(iv_model, "ivwald1"))
      iv_rows[[iv_i]] <- tibble(
        sample = sample_name,
        spec = spec,
        outcome = y,
        nobs = nobs(iv_model),
        coef = if (nrow(iv_term) == 0) NA_real_ else iv_term$estimate[[1]],
        se = if (nrow(iv_term) == 0) NA_real_ else iv_term$std.error[[1]],
        p = if (nrow(iv_term) == 0) NA_real_ else iv_term$p.value[[1]],
        ivwald1 = suppressWarnings(as.numeric(unlist(ivwald_raw)[1]))
      )
      iv_i <- iv_i + 1L
    }
  }
}

audit_tbl <- bind_rows(audit_rows)
audit_yq_tbl <- bind_rows(audit_yq_rows)
fs_tbl <- bind_rows(fs_rows)
rf_tbl <- bind_rows(rf_rows)
iv_tbl <- bind_rows(iv_rows)

write_csv(audit_tbl, audit_out)
write_csv(audit_yq_tbl, audit_yq_out)
write_csv(fs_tbl, fs_out)
write_csv(rf_tbl, rf_out)
write_csv(iv_tbl, iv_out)

print(audit_tbl)
print(fs_tbl)
print(rf_tbl %>% filter(outcome %in% c("domestic_care_reason", "wanted_to_work", "wants_more_hours", "effective_hours", "effective_hours_population")))
print(iv_tbl %>% filter(outcome %in% c("domestic_care_reason", "wanted_to_work", "wants_more_hours", "effective_hours", "effective_hours_population")))
