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
out_dir <- here::here("data", "powerIV", "regressions", "lpg_micro_panel")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

rf_out <- here::here("data", "powerIV", "regressions", "lpg_micro_panel", "reduced_form_core.csv")
het_out <- here::here("data", "powerIV", "regressions", "lpg_micro_panel", "reduced_form_heterogeneity.csv")
fs_out <- here::here("data", "powerIV", "regressions", "lpg_micro_panel", "first_stage_cooking.csv")
within_out <- here::here("data", "powerIV", "regressions", "lpg_micro_panel", "within_person_reduced_form.csv")

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP", "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

cols_needed <- c(
  "woman_id", "year", "quarter", "year_quarter", "uf", "estrato4", "person_weight", "age", "female_secondaryplus", "female_tertiary",
  "employed", "wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m", "effective_hours",
  "hh_size", "n_children_0_14", "visit1_match_any", "carry_abs_distance_q", "visit1_same_quarter", "visit1_n_rooms", "visit1_n_bedrooms",
  "visit1_electricity_from_grid", "visit1_electricity_grid_full_time", "visit1_washing_machine", "visit1_cooking_main_fuel", "visit1_cooking_wood_charcoal"
)

available_cols <- names(read_parquet(linked_in, as_data_frame = FALSE))
read_cols <- intersect(cols_needed, available_cols)

women <- read_parquet(linked_in, col_select = all_of(read_cols)) %>%
  mutate(
    estrato4 = as.character(estrato4),
    uf_sigla = unname(uf_to_sigla[uf]),
    age_sq = age^2,
    has_children = as.integer(n_children_0_14 > 0),
    no_washing_machine = as.integer(!is.na(visit1_washing_machine) & visit1_washing_machine == 0),
    weak_grid = as.integer(!is.na(visit1_electricity_grid_full_time) & visit1_electricity_grid_full_time == 0),
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0),
    mainfuel_wood = as.integer(visit1_cooking_main_fuel == "Lenha ou carvão"),
    mainfuel_gas = as.integer(visit1_cooking_main_fuel %in% c("Gás de botijão", "Gás encanado")),
    mainfuel_modern = as.integer(visit1_cooking_main_fuel %in% c("Gás de botijão", "Gás encanado", "Energia elétrica"))
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

control_sets <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "age + age_sq + female_secondaryplus + female_tertiary + hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + visit1_electricity_from_grid + visit1_electricity_grid_full_time",
  "M3", "age + age_sq + female_secondaryplus + female_tertiary + hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + visit1_electricity_from_grid + visit1_electricity_grid_full_time + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

core_outcomes <- c(
  "effective_hours",
  "effective_hours_population",
  "wanted_to_work",
  "wants_more_hours",
  "available_more_hours",
  "search_long",
  "worked_last_12m"
)

het_vars <- c("hh_size", "has_children", "no_washing_machine", "weak_grid")

sample_fs <- tibble::tribble(
  ~sample, ~sample_filter,
  "all_linked", "visit1_match_any == TRUE",
  "carry_le2", "visit1_match_any == TRUE & carry_abs_distance_q <= 2",
  "carry_le1", "visit1_match_any == TRUE & carry_abs_distance_q <= 1"
)

rf_rows <- list()
het_rows <- list()
fs_rows <- list()
within_rows <- list()
rf_i <- 1L
het_i <- 1L
fs_i <- 1L
within_i <- 1L

for (i in seq_len(nrow(control_sets))) {
  spec <- control_sets$spec[[i]]
  controls <- control_sets$controls[[i]]
  needed_controls <- all.vars(as.formula(paste("~", controls)))

  for (y in core_outcomes) {
    needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", y, needed_controls))
    dat <- women_reg %>% filter(visit1_match_any, if_all(all_of(needed), ~ !is.na(.x)))
    model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
    term <- tidy(model) %>% filter(term == "z_wood_lpg_estrato")
    rf_rows[[rf_i]] <- tibble(spec = spec, outcome = y, nobs = nobs(model), coef = term$estimate[[1]], se = term$std.error[[1]], p = term$p.value[[1]])
    rf_i <- rf_i + 1L
  }

  for (h in het_vars) {
    for (y in c("effective_hours", "effective_hours_population", "wanted_to_work", "wants_more_hours")) {
      needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", h, y, needed_controls))
      dat <- women_reg %>% filter(visit1_match_any, if_all(all_of(needed), ~ !is.na(.x)))
      model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + z_wood_lpg_estrato:", h, " + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
      tbl <- tidy(model)
      b1 <- tbl %>% filter(term == "z_wood_lpg_estrato")
      b2 <- tbl %>% filter(term == paste0("z_wood_lpg_estrato:", h))
      het_rows[[het_i]] <- tibble(spec = spec, outcome = y, heterogeneity = h, nobs = nobs(model), coef_main = b1$estimate[[1]], p_main = b1$p.value[[1]], coef_interaction = b2$estimate[[1]], p_interaction = b2$p.value[[1]])
      het_i <- het_i + 1L
    }
  }
}

for (s in seq_len(nrow(sample_fs))) {
  sample_name <- sample_fs$sample[[s]]
  dat_sample <- women_reg %>% filter(!!rlang::parse_expr(sample_fs$sample_filter[[s]]))

  for (i in seq_len(nrow(control_sets))) {
    spec <- control_sets$spec[[i]]
    controls <- control_sets$controls[[i]]
    needed_controls <- all.vars(as.formula(paste("~", controls)))

    for (y in c("mainfuel_wood", "mainfuel_gas", "mainfuel_modern", "visit1_cooking_wood_charcoal")) {
      needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", y, needed_controls))
      dat <- dat_sample %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
      model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
      term <- tidy(model) %>% filter(term == "z_wood_lpg_estrato")
      suppressWarnings(capture.output(w <- wald(model, "z_wood_lpg_estrato")))
      fs_rows[[fs_i]] <- tibble(sample = sample_name, spec = spec, outcome = y, nobs = nobs(model), coef = term$estimate[[1]], se = term$std.error[[1]], p = term$p.value[[1]], fstat = as.numeric(unlist(w$stat)[1]))
      fs_i <- fs_i + 1L
    }
  }
}

if ("woman_id" %in% names(women_reg)) {
  repeat_women <- women_reg %>%
    filter(visit1_match_any) %>%
    count(woman_id, name = "n_woman_q") %>%
    filter(n_woman_q >= 2)

  women_within <- women_reg %>% inner_join(repeat_women, by = "woman_id")

  for (i in seq_len(nrow(control_sets))) {
    spec <- control_sets$spec[[i]]
    controls <- control_sets$controls[[i]]
    needed_controls <- all.vars(as.formula(paste("~", controls)))

    for (y in core_outcomes) {
      needed <- unique(c("woman_id", "year_quarter", "person_weight", "z_wood_lpg_estrato", y, needed_controls))
      dat <- women_within %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
      model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | woman_id + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
      term <- tidy(model) %>% filter(term == "z_wood_lpg_estrato")
      within_rows[[within_i]] <- tibble(spec = spec, outcome = y, nobs = nobs(model), n_women = n_distinct(dat$woman_id), coef = term$estimate[[1]], se = term$std.error[[1]], p = term$p.value[[1]])
      within_i <- within_i + 1L
    }
  }
}

rf_tbl <- bind_rows(rf_rows)
het_tbl <- bind_rows(het_rows)
fs_tbl <- bind_rows(fs_rows)
within_tbl <- bind_rows(within_rows)

write_csv(rf_tbl, rf_out)
write_csv(het_tbl, het_out)
write_csv(fs_tbl, fs_out)
write_csv(within_tbl, within_out)

print(rf_tbl)
print(het_tbl)
print(fs_tbl)
print(within_tbl)
