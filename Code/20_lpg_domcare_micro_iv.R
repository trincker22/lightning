#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(PNADcIBGE)
  library(arrow)
  library(dplyr)
  library(stringr)
  library(fixest)
  library(broom)
  library(readr)
})

raw_dir <- here::here("data", "PNAD-C", "raw")
input_txt <- here::here("data", "PNAD-C", "raw", "input_PNADC_trimestral.txt")
dict_file <- here::here("data", "PNAD-C", "raw", "dicionario_PNADC_microdados_trimestral.xls")

micro_out <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_2016_2024.parquet")
results_dir <- here::here("data", "powerIV", "regressions", "lpg_domcare_micro")
dir.create(dirname(micro_out), recursive = TRUE, showWarnings = FALSE)
dir.create(results_dir, recursive = TRUE, showWarnings = FALSE)
first_stage_out <- here::here(results_dir, "first_stage.csv")
reduced_form_out <- here::here(results_dir, "reduced_form.csv")
iv_out <- here::here(results_dir, "iv.csv")
match_out <- here::here(results_dir, "match_summary.csv")
mechanism_rf_out <- here::here(results_dir, "mechanism_reduced_form.csv")
hours_link_out <- here::here(results_dir, "hours_link.csv")

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

wald_f <- function(model, term) {
  out <- tryCatch(wald(model, term), error = function(e) NA_real_)
  safe_num(out)
}

pick_term <- function(tbl, pattern) {
  z <- tbl[grepl(pattern, tbl$term), , drop = FALSE]
  if (nrow(z) == 0) return(NULL)
  z[1, , drop = FALSE]
}

required_micro_cols <- c(
  "year", "quarter", "year_quarter", "uf", "estrato4", "upa", "panel", "household_number", "person_number", "woman_id",
  "hh_match_key", "person_weight", "sex", "age", "education", "female", "working_age",
  "out_labor_force", "domestic_reason_text", "domestic_care_reason", "female_secondaryplus",
  "female_tertiary", "child_0_14", "wants_more_hours", "available_more_hours",
  "wanted_to_work", "search_duration_text", "search_long", "worked_last_12m",
  "employed", "effective_hours"
)

if (file.exists(micro_out) && all(required_micro_cols %in% names(read_parquet(micro_out, as_data_frame = FALSE)))) {
  message("Reading cached person-level micro: ", micro_out)
  women <- read_parquet(micro_out)
} else {
  vars <- c(
    "Ano", "Trimestre", "UF", "Estrato", "UPA", "V1008", "V1014", "V1033",
    "V2003", "V2007", "V2009", "VD3004", "VD4001", "VD4002", "V4078", "V4078A", "VD4030",
    "V4063A", "V4064A", "V4073", "V4076", "V4082", "V4039C"
  )
  zip_files <- sort(Sys.glob(here::here("data", "PNAD-C", "raw", "PNADC_*20*.zip")))
  zip_files <- zip_files[grepl("PNADC_[0-9]{2}20[0-9]{2}_", basename(zip_files))]

  education_secondaryplus <- c("Médio completo ou equivalente", "Superior incompleto ou equivalente", "Superior completo")
  education_tertiary <- c("Superior incompleto ou equivalente", "Superior completo")

  q_list <- vector("list", length(zip_files))
  for (i in seq_along(zip_files)) {
    zf <- zip_files[[i]]
    message("Reading ", basename(zf))
    dat <- read_pnadc(zf, input_txt, vars = vars)
    dat <- pnadc_labeller(dat, dict_file)
    q_list[[i]] <- dat %>%
      mutate(across(everything(), as.character)) %>%
      transmute(
        year = as.integer(Ano),
        quarter = as.integer(Trimestre),
        year_quarter = sprintf("%sQ%s", Ano, Trimestre),
        uf = UF,
        estrato4 = substr(Estrato, 1, 4),
        upa = UPA,
        panel = V1014,
        household_number = V1008,
        person_number = V2003,
        hh_match_key = paste(Ano, Trimestre, UPA, V1014, V1008, sep = "_"),
        person_weight = suppressWarnings(as.numeric(V1033)),
        sex = V2007,
        age = suppressWarnings(as.integer(V2009)),
        education = VD3004,
        female = V2007 == "Mulher",
        working_age = !is.na(age) & age >= 18 & age <= 64,
        out_labor_force = VD4001 == "Pessoas fora da força de trabalho",
        employed = VD4002 == "Pessoas ocupadas",
        domestic_reason_text = coalesce(VD4030, V4078A, V4078),
        domestic_care_reason = str_detect(coalesce(VD4030, V4078A, V4078, ""), regex("afazeres domésticos|afazeres domesticos|filho|parente|dependente", ignore_case = TRUE)),
        female_secondaryplus = !is.na(education) & education %in% education_secondaryplus,
        female_tertiary = !is.na(education) & education %in% education_tertiary,
        child_0_14 = !is.na(age) & age <= 14,
        wants_more_hours = V4063A == "Sim",
        available_more_hours = V4064A == "Sim",
        wanted_to_work = V4073 == "Sim",
        search_duration_text = V4076,
        search_long = V4076 %in% c("De 1 ano a menos de 2 anos", "2 anos ou mais"),
        worked_last_12m = V4082 == "Sim",
        effective_hours = suppressWarnings(as.numeric(V4039C))
      )
  }

  person_df <- bind_rows(q_list)

  hh_composition <- person_df %>%
    group_by(hh_match_key) %>%
    summarise(
      hh_size = n(),
      n_children_0_14 = sum(child_0_14, na.rm = TRUE),
      .groups = "drop"
    )

  hh <- read_parquet(here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")) %>%
    mutate(
      hh_match_key = paste(year, quarter, upa, panel, household_number, sep = "_"),
      estrato4 = as.character(estrato4)
    ) %>%
    transmute(
      hh_match_key,
      year,
      quarter,
      estrato4,
      n_rooms,
      n_bedrooms,
      cooking_wood_charcoal
    )

  women <- person_df %>%
    filter(female, working_age, !is.na(person_weight), person_weight > 0) %>%
    mutate(woman_id = paste(upa, panel, household_number, person_number, sep = "_")) %>%
    left_join(hh_composition, by = "hh_match_key") %>%
    left_join(hh, by = c("hh_match_key", "year", "quarter", "estrato4")) %>%
    mutate(outlf_domestic_care = as.integer(domestic_care_reason & out_labor_force))

  write_parquet(women, micro_out)
  message("Wrote cached person-level micro: ", micro_out)
}

power_panel <- read_parquet(here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")) %>%
  mutate(estrato4 = as.character(estrato4))

baseline_power <- power_panel %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

lpg_quarter_estrato <- read_parquet(here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  select(estrato4, year, quarter, log_lpg_price_quarterly_estrato)

macro_panel <- read_parquet(here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")) %>%
  mutate(uf_sigla = as.character(uf_sigla)) %>%
  select(uf_sigla, year, quarter, uf_unemployment_rate, uf_labor_demand_proxy, uf_real_wage_proxy, uf_share_secondaryplus, uf_share_tertiary)

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP", "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

women_reg <- women %>%
  mutate(uf_sigla = unname(uf_to_sigla[uf])) %>%
  left_join(baseline_power, by = "estrato4") %>%
  left_join(lpg_quarter_estrato, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro_panel, by = c("uf_sigla", "year", "quarter")) %>%
  mutate(
    age_sq = age^2,
    z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  )

match_tbl <- tibble(
  metric = c("female_working_age_n", "matched_hh_controls_rate", "matched_rooms_rate", "matched_bedrooms_rate", "matched_cooking_wood_rate"),
  value = c(
    nrow(women_reg),
    mean(!is.na(women_reg$hh_size)),
    mean(!is.na(women_reg$n_rooms)),
    mean(!is.na(women_reg$n_bedrooms)),
    mean(!is.na(women_reg$cooking_wood_charcoal))
  )
)
write_csv(match_tbl, match_out)

control_sets <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "hh_size + n_children_0_14 + n_rooms + n_bedrooms",
  "M3", "hh_size + n_children_0_14 + n_rooms + n_bedrooms + age + age_sq + female_secondaryplus + female_tertiary + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

rows_fs <- list()
rows_rf <- list()
rows_iv <- list()
rows_mech <- list()
rows_hours <- list()
ix_fs <- 1L
ix_rf <- 1L
ix_iv <- 1L
ix_mech <- 1L
ix_hours <- 1L

for (i in seq_len(nrow(control_sets))) {
  spec <- control_sets$spec[[i]]
  controls <- control_sets$controls[[i]]
  needed_controls <- all.vars(as.formula(paste("~", controls)))

  needed_fs <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", "cooking_wood_charcoal", needed_controls))
  dat_fs <- women_reg %>% filter(if_all(all_of(needed_fs), ~ !is.na(.x)))
  fs_model <- feols(as.formula(paste0("cooking_wood_charcoal ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat_fs, weights = ~person_weight, cluster = ~estrato4)
  fs_t <- tidy(fs_model) %>% filter(term == "z_wood_lpg_estrato")
  rows_fs[[ix_fs]] <- tibble(spec = spec, nobs = nobs(fs_model), coef = fs_t$estimate[[1]], se = fs_t$std.error[[1]], p = fs_t$p.value[[1]], fstat = wald_f(fs_model, "z_wood_lpg_estrato"))
  ix_fs <- ix_fs + 1L

  for (y in c("domestic_care_reason", "outlf_domestic_care")) {
    needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", "cooking_wood_charcoal", y, needed_controls))
    dat <- women_reg %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
    rf_model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
    iv_model <- feols(as.formula(paste0(y, " ~ ", controls, " | estrato4 + year_quarter | cooking_wood_charcoal ~ z_wood_lpg_estrato")), data = dat, weights = ~person_weight, cluster = ~estrato4)
    rf_t <- tidy(rf_model) %>% filter(term == "z_wood_lpg_estrato")
    iv_t <- pick_term(tidy(iv_model), "^fit_")
    rows_rf[[ix_rf]] <- tibble(spec = spec, outcome = y, nobs = nobs(rf_model), coef = rf_t$estimate[[1]], se = rf_t$std.error[[1]], p = rf_t$p.value[[1]])
    ix_rf <- ix_rf + 1L
    rows_iv[[ix_iv]] <- tibble(spec = spec, outcome = y, nobs = nobs(iv_model), coef = iv_t$estimate[[1]], se = iv_t$std.error[[1]], p = iv_t$p.value[[1]], ivwald1 = safe_num(fitstat(iv_model, "ivwald1")))
    ix_iv <- ix_iv + 1L
  }

  for (y in c("wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m")) {
    needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", y, needed_controls))
    dat <- women_reg %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
    mech_model <- feols(as.formula(paste0(y, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4)
    mech_t <- tidy(mech_model) %>% filter(term == "z_wood_lpg_estrato")
    rows_mech[[ix_mech]] <- tibble(spec = spec, outcome = y, nobs = nobs(mech_model), coef = mech_t$estimate[[1]], se = mech_t$std.error[[1]], p = mech_t$p.value[[1]])
    ix_mech <- ix_mech + 1L
  }

  for (x in c("domestic_care_reason", "wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m")) {
    needed <- unique(c("estrato4", "year_quarter", "person_weight", "effective_hours_population", x, needed_controls))
    dat <- women_reg %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
    hours_model <- tryCatch(
      feols(as.formula(paste0("effective_hours_population ~ ", x, " + ", controls, " | estrato4 + year_quarter")), data = dat, weights = ~person_weight, cluster = ~estrato4),
      error = function(e) NULL
    )
    hours_t <- if (is.null(hours_model)) tibble() else tidy(hours_model) %>% filter(term == x)
    rows_hours[[ix_hours]] <- tibble(
      spec = spec,
      regressor = x,
      nobs = if (is.null(hours_model)) nrow(dat) else nobs(hours_model),
      coef = if (nrow(hours_t) == 0) NA_real_ else hours_t$estimate[[1]],
      se = if (nrow(hours_t) == 0) NA_real_ else hours_t$std.error[[1]],
      p = if (nrow(hours_t) == 0) NA_real_ else hours_t$p.value[[1]]
    )
    ix_hours <- ix_hours + 1L
  }
}

first_stage_tbl <- bind_rows(rows_fs)
reduced_form_tbl <- bind_rows(rows_rf)
iv_tbl <- bind_rows(rows_iv)
mechanism_rf_tbl <- bind_rows(rows_mech)
hours_link_tbl <- bind_rows(rows_hours)

write_csv(first_stage_tbl, first_stage_out)
write_csv(reduced_form_tbl, reduced_form_out)
write_csv(iv_tbl, iv_out)
write_csv(mechanism_rf_tbl, mechanism_rf_out)
write_csv(hours_link_tbl, hours_link_out)

print(match_tbl)
print(first_stage_tbl)
print(reduced_form_tbl)
print(iv_tbl)
print(mechanism_rf_tbl)
print(hours_link_tbl)
