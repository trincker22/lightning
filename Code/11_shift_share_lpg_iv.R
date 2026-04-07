#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(fixest)
  library(broom)
  library(readr)
  library(stringi)
})

root_dir <- here::here()
coverage_script <- here::here("Code", "07a_power_coverage_audit.R")
if (file.exists(coverage_script)) {
  cov_status <- tryCatch(system2("Rscript", coverage_script), error = function(e) 1L)
  if (!identical(cov_status, 0L)) {
    warning("Coverage audit refresh failed in Script 11.")
  }
}

price_dir <- here::here("data", "powerIV", "prices")
out_dir <- here::here("data", "powerIV", "regressions", "shift_share_lpg")
table_dir <- here::here(out_dir, "tables")
dir.create(price_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

lpg_station_path <- here::here(price_dir, "anp_lpg_station_2016_2024.parquet")
lpg_monthly_path <- here::here(price_dir, "anp_lpg_monthly_national_2016_2024.parquet")
lpg_quarter_path <- here::here(price_dir, "anp_lpg_quarter_national_2016_2024.parquet")
lpg_monthly_uf_path <- here::here(price_dir, "anp_lpg_monthly_uf_2016_2024.parquet")
lpg_quarter_uf_path <- here::here(price_dir, "anp_lpg_quarter_uf_2016_2024.parquet")
lpg_muni_quarter_path <- here::here(price_dir, "anp_lpg_quarter_municipio_2016_2024.parquet")
lpg_quarter_estrato_path <- here::here(price_dir, "anp_lpg_quarter_estrato_2016_2024.parquet")
crosswalk_path <- here::here("data", "PNAD-C", "Municipios_por_Estratos.csv")

results_path <- here::here(table_dir, "shift_share_lpg_iv_results.csv")
results_fe_path <- here::here(table_dir, "shift_share_lpg_iv_hours_fe_results.csv")
qa_path <- here::here(table_dir, "shift_share_lpg_iv_qa.csv")

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")

parse_lpg_price <- function(x) {
  x_chr <- trimws(as.character(x))
  x_chr <- gsub("[^0-9,.-]", "", x_chr)
  both_sep <- grepl(",", x_chr) & grepl("\\.", x_chr)
  comma_only <- grepl(",", x_chr) & !grepl("\\.", x_chr)
  x_chr[both_sep] <- gsub(",", ".", gsub("\\.", "", x_chr[both_sep]))
  x_chr[comma_only] <- gsub(",", ".", x_chr[comma_only])
  suppressWarnings(as.numeric(x_chr))
}

normalize_municipio <- function(x) {
  y <- stri_trans_general(as.character(x), "Latin-ASCII")
  y <- toupper(trimws(y))
  y <- gsub("[^A-Z0-9 ]", " ", y)
  y <- gsub("\\s+", " ", y)
  trimws(y)
}

if (!file.exists(lpg_station_path)) {
  years <- 2016:2024
  semesters <- 1:2
  dt_list <- list()
  idx <- 1L
  for (yy in years) {
    for (ss in semesters) {
      url <- sprintf("https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/glp/glp-%d-%02d.csv", yy, ss)
      message("Downloading ", yy, " semester ", ss)
      dt <- tryCatch(
        fread(
          url,
          sep = ";",
          encoding = "Latin-1",
          select = c("Estado - Sigla", "Municipio", "Data da Coleta", "Valor de Venda"),
          showProgress = FALSE
        ),
        error = function(e) NULL
      )
      if (is.null(dt)) {
        message("Skipping missing ANP file: ", url)
        next
      }
      setnames(dt, old = c("Estado - Sigla", "Municipio", "Data da Coleta", "Valor de Venda"), new = c("uf_sigla", "municipio", "data_coleta", "valor_venda"))
      dt[, valor_venda := parse_lpg_price(valor_venda)]
      dt[, year_file := yy]
      dt[, semester_file := ss]
      dt[, date := as.Date(data_coleta, format = "%d/%m/%Y")]
      dt <- dt[!is.na(date) & !is.na(valor_venda)]
      dt_list[[idx]] <- dt
      idx <- idx + 1L
    }
  }
  if (length(dt_list) == 0) {
    stop("No ANP LPG files were downloaded.")
  }
  lpg_station <- rbindlist(dt_list, fill = TRUE)
  write_parquet(as.data.frame(lpg_station), lpg_station_path)
} else {
  lpg_station <- as.data.table(read_parquet(lpg_station_path))
  if (!"date" %in% names(lpg_station)) {
    lpg_station[, date := as.Date(data_coleta, format = "%d/%m/%Y")]
  }
}

if (!is.numeric(lpg_station$valor_venda)) {
  lpg_station[, valor_venda := parse_lpg_price(valor_venda)]
}
lpg_station <- lpg_station[!is.na(valor_venda) & !is.na(date)]

lpg_monthly <- lpg_station[, .(
  lpg_price_monthly = base::mean(valor_venda, na.rm = TRUE),
  n_station_obs = .N
), by = .(year = year(date), month = month(date))]

lpg_quarter <- lpg_monthly[, .(
  lpg_price_quarterly = mean(lpg_price_monthly, na.rm = TRUE),
  n_months = .N,
  n_station_obs_quarter = sum(n_station_obs, na.rm = TRUE)
), by = .(year, quarter = quarter(make_date(year, month, 1L)))]

lpg_quarter[, log_lpg_price_quarterly := log(lpg_price_quarterly)]
lpg_quarter <- lpg_quarter[!is.na(log_lpg_price_quarterly)]

lpg_monthly_uf <- lpg_station[, .(
  lpg_price_monthly_uf = base::mean(valor_venda, na.rm = TRUE),
  n_station_obs = .N
), by = .(uf_sigla, year = year(date), month = month(date))]

lpg_quarter_uf <- lpg_monthly_uf[, .(
  lpg_price_quarterly_uf = mean(lpg_price_monthly_uf, na.rm = TRUE),
  n_months = .N,
  n_station_obs_quarter = sum(n_station_obs, na.rm = TRUE)
), by = .(uf_sigla, year, quarter = quarter(make_date(year, month, 1L)))]

lpg_quarter_uf[, log_lpg_price_quarterly_uf := log(lpg_price_quarterly_uf)]
lpg_quarter_uf <- lpg_quarter_uf[!is.na(log_lpg_price_quarterly_uf)]

lpg_muni_quarter <- lpg_station[, .(
  lpg_price_quarterly_muni = mean(valor_venda, na.rm = TRUE),
  n_station_obs_quarter = .N
), by = .(
  uf_sigla,
  municipio_norm = normalize_municipio(municipio),
  year = year(date),
  quarter = quarter(date)
)]

if (!file.exists(crosswalk_path)) {
  stop("Missing municipality-to-estrato crosswalk: ", crosswalk_path)
}
estrato_crosswalk <- fread(crosswalk_path, sep = ";", encoding = "UTF-8")
if (ncol(estrato_crosswalk) < 4) {
  stop("Crosswalk format is invalid: expected at least 4 columns.")
}
setnames(estrato_crosswalk, old = names(estrato_crosswalk)[1:4], new = c("estrato_name", "estrato_code", "municipio_name", "municipio_code"))
uf_code_to_sigla <- c(
  "11" = "RO", "12" = "AC", "13" = "AM", "14" = "RR", "15" = "PA", "16" = "AP", "17" = "TO",
  "21" = "MA", "22" = "PI", "23" = "CE", "24" = "RN", "25" = "PB", "26" = "PE", "27" = "AL", "28" = "SE", "29" = "BA",
  "31" = "MG", "32" = "ES", "33" = "RJ", "35" = "SP",
  "41" = "PR", "42" = "SC", "43" = "RS",
  "50" = "MS", "51" = "MT", "52" = "GO", "53" = "DF"
)
estrato_map <- estrato_crosswalk[, .(
  estrato4 = sprintf("%04d", as.integer(estrato_code)),
  municipio_code = as.integer(municipio_code),
  municipio_norm = normalize_municipio(municipio_name)
)]
estrato_map[, uf_sigla := unname(uf_code_to_sigla[substr(sprintf("%07d", municipio_code), 1, 2)])]
estrato_map <- unique(estrato_map[!is.na(estrato4) & !is.na(uf_sigla) & municipio_norm != ""])

lpg_muni_joined <- merge(
  lpg_muni_quarter,
  estrato_map[, .(uf_sigla, municipio_norm, estrato4)],
  by = c("uf_sigla", "municipio_norm"),
  all.x = TRUE
)

lpg_quarter_estrato <- lpg_muni_joined[!is.na(estrato4), .(
  lpg_price_quarterly_estrato = weighted.mean(lpg_price_quarterly_muni, n_station_obs_quarter, na.rm = TRUE),
  n_muni_cells = .N,
  n_station_obs_quarter = sum(n_station_obs_quarter, na.rm = TRUE)
), by = .(estrato4, year, quarter)]
lpg_quarter_estrato[, log_lpg_price_quarterly_estrato := log(lpg_price_quarterly_estrato)]
lpg_quarter_estrato <- lpg_quarter_estrato[!is.na(log_lpg_price_quarterly_estrato)]

write_parquet(as.data.frame(lpg_monthly), lpg_monthly_path)
write_parquet(as.data.frame(lpg_quarter), lpg_quarter_path)
write_parquet(as.data.frame(lpg_monthly_uf), lpg_monthly_uf_path)
write_parquet(as.data.frame(lpg_quarter_uf), lpg_quarter_uf_path)
write_parquet(as.data.frame(lpg_muni_quarter), lpg_muni_quarter_path)
write_parquet(as.data.frame(lpg_quarter_estrato), lpg_quarter_estrato_path)

lfp_panel <- read_parquet(lfp_panel_path) %>%
  mutate(estrato4 = as.character(estrato4))

power_panel <- read_parquet(power_panel_path) %>%
  mutate(estrato4 = as.character(estrato4))

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP",
  "Tocantins" = "TO", "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN",
  "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS", "Mato Grosso do Sul" = "MS",
  "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

baseline <- power_panel %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(
    baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE),
    baseline_share_vehicle_2016 = mean(share_vehicle, na.rm = TRUE),
    baseline_share_internet_2016 = mean(share_internet_home, na.rm = TRUE),
    .groups = "drop"
  )

panel <- lfp_panel %>%
  select(
    estrato4, uf, year, quarter, weight_sum,
    female_lfp, female_hours_effective_employed, female_hours_effective_population,
    female_discouraged, gender_hours_effective_employed_gap, male_hours_effective_employed
  ) %>%
  mutate(uf_sigla = unname(uf_to_sigla[uf])) %>%
  left_join(
    power_panel %>%
      select(
        estrato4, year, quarter, capital, rm_ride,
        share_cooking_wood_charcoal, share_cooking_gas_any, share_cooking_electricity,
        share_vehicle, share_internet_home, share_electricity_from_grid, share_grid_full_time
    ),
    by = c("estrato4", "year", "quarter")
  ) %>%
  left_join(baseline, by = "estrato4") %>%
  left_join(
    as_tibble(lpg_quarter) %>% select(year, quarter, log_lpg_price_quarterly),
    by = c("year", "quarter")
  ) %>%
  left_join(
    as_tibble(lpg_quarter_uf) %>% select(uf_sigla, year, quarter, log_lpg_price_quarterly_uf),
    by = c("uf_sigla", "year", "quarter")
  ) %>%
  left_join(
    as_tibble(lpg_quarter_estrato) %>% select(estrato4, year, quarter, log_lpg_price_quarterly_estrato),
    by = c("estrato4", "year", "quarter")
  ) %>%
  mutate(
    z_wood_lpg_national = baseline_wood_2016 * log_lpg_price_quarterly,
    z_wood_lpg_uf = baseline_wood_2016 * log_lpg_price_quarterly_uf,
    z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato,
    z_vehicle_lpg_national = baseline_share_vehicle_2016 * log_lpg_price_quarterly,
    z_vehicle_lpg_uf = baseline_share_vehicle_2016 * log_lpg_price_quarterly_uf,
    z_vehicle_lpg_estrato = baseline_share_vehicle_2016 * log_lpg_price_quarterly_estrato,
    z_internet_lpg_national = baseline_share_internet_2016 * log_lpg_price_quarterly,
    z_internet_lpg_uf = baseline_share_internet_2016 * log_lpg_price_quarterly_uf,
    z_internet_lpg_estrato = baseline_share_internet_2016 * log_lpg_price_quarterly_estrato
  ) %>%
  filter(
    !is.na(weight_sum), weight_sum > 0,
    !is.na(share_vehicle),
    !is.na(share_internet_home),
    !is.na(share_electricity_from_grid),
    !is.na(share_grid_full_time)
  )

q75_wood <- as.numeric(stats::quantile(panel$baseline_wood_2016, probs = 0.75, na.rm = TRUE))

panel <- panel %>%
  mutate(
    high_wood_baseline = if_else(!is.na(baseline_wood_2016) & baseline_wood_2016 >= q75_wood, 1L, 0L),
    outside_capital_rm = if_else((is.na(capital) | capital == "") & (is.na(rm_ride) | rm_ride == ""), 1L, 0L),
    quarter_id = year * 4L + quarter
  ) %>%
  arrange(estrato4, year, quarter) %>%
  group_by(estrato4) %>%
  mutate(
    next_qid = dplyr::lead(quarter_id, 1L),
    next2_qid = dplyr::lead(quarter_id, 2L),
    female_hours_effective_employed_lead1 = if_else(next_qid == quarter_id + 1L, dplyr::lead(female_hours_effective_employed, 1L), NA_real_),
    female_hours_effective_employed_lead2 = if_else(next_qid == quarter_id + 1L & next2_qid == quarter_id + 2L, dplyr::lead(female_hours_effective_employed, 2L), NA_real_)
  ) %>%
  ungroup() %>%
  select(-next_qid, -next2_qid)

controls <- "share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time"
fe_specs <- tibble::tribble(
  ~fe_name, ~fe_formula,
  "year_quarter", "year + quarter",
  "uf_year_quarter", "uf + year + quarter",
  "estrato_year_quarter", "estrato4 + year + quarter",
  "estrato_year", "estrato4 + year",
  "estrato_quarter", "estrato4 + quarter",
  "estrato_quarter_uf_year", "estrato4 + quarter + uf^year"
)

hours_outcomes <- c(
  "female_hours_effective_employed",
  "female_lfp",
  "female_discouraged",
  "gender_hours_effective_employed_gap"
)
iv_specs <- tibble::tribble(
  ~iv_variant, ~instrument_var, ~vehicle_interaction_var, ~internet_interaction_var,
  "national", "z_wood_lpg_national", "z_vehicle_lpg_national", "z_internet_lpg_national",
  "uf_quarter", "z_wood_lpg_uf", "z_vehicle_lpg_uf", "z_internet_lpg_uf",
  "estrato_quarter", "z_wood_lpg_estrato", "z_vehicle_lpg_estrato", "z_internet_lpg_estrato"
)

safe_stat <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_stat(out)
}

safe_feols <- function(formula_obj, data_obj, weight_formula, cluster_formula) {
  tryCatch(
    feols(formula_obj, data = data_obj, weights = weight_formula, cluster = cluster_formula),
    error = function(e) NULL
  )
}

rows <- list()
ix <- 1L

for (jj in seq_len(nrow(iv_specs))) {
  iv_variant <- iv_specs$iv_variant[[jj]]
  instrument_var <- iv_specs$instrument_var[[jj]]
  vehicle_interaction_var <- iv_specs$vehicle_interaction_var[[jj]]
  internet_interaction_var <- iv_specs$internet_interaction_var[[jj]]
  controls_with_shift_share <- paste(
    controls,
    vehicle_interaction_var,
    internet_interaction_var,
    sep = " + "
  )

  for (ii in seq_len(nrow(fe_specs))) {
    fe_name <- fe_specs$fe_name[[ii]]
    fe_formula <- fe_specs$fe_formula[[ii]]

    dat_fs <- panel %>% filter(!is.na(share_cooking_wood_charcoal), !is.na(.data[[instrument_var]]))
    fs <- safe_feols(
      as.formula(paste0("share_cooking_wood_charcoal ~ ", instrument_var, " + ", controls_with_shift_share, " | ", fe_formula)),
      dat_fs,
      ~weight_sum,
      ~estrato4
    )
    if (!is.null(fs)) {
      fs_row <- tidy(fs, conf.int = TRUE) %>%
        filter(term == instrument_var) %>%
        mutate(
          model = "first_stage_wood",
          iv_variant = iv_variant,
          fe = fe_name,
          outcome = "share_cooking_wood_charcoal",
          endogenous = "share_cooking_wood_charcoal",
          nobs = nobs(fs),
          fsf = safe_stat(fitstat(fs, "f")),
          ivf1 = NA_real_
        )
      rows[[ix]] <- fs_row
      ix <- ix + 1L
    }

    for (yy in hours_outcomes) {
      dat <- panel %>% filter(!is.na(.data[[yy]]), !is.na(share_cooking_wood_charcoal), !is.na(.data[[instrument_var]]))

      rf <- safe_feols(
        as.formula(paste0(yy, " ~ ", instrument_var, " + ", controls_with_shift_share, " | ", fe_formula)),
        dat,
        ~weight_sum,
        ~estrato4
      )
      if (!is.null(rf)) {
        rf_row <- tidy(rf, conf.int = TRUE) %>%
          filter(term == instrument_var) %>%
          mutate(
            model = "reduced_form",
            iv_variant = iv_variant,
            fe = fe_name,
            outcome = yy,
            endogenous = "none",
            nobs = nobs(rf),
            fsf = NA_real_,
            ivf1 = NA_real_
          )
        rows[[ix]] <- rf_row
        ix <- ix + 1L
      }

      iv <- safe_feols(
        as.formula(paste0(yy, " ~ ", controls_with_shift_share, " | ", fe_formula, " | share_cooking_wood_charcoal ~ ", instrument_var)),
        dat,
        ~weight_sum,
        ~estrato4
      )
      if (!is.null(iv)) {
        iv_row <- tidy(iv, conf.int = TRUE) %>%
          filter(grepl("^fit_", term)) %>%
          mutate(
            model = "iv_wood",
            iv_variant = iv_variant,
            fe = fe_name,
            outcome = yy,
            endogenous = "share_cooking_wood_charcoal",
            nobs = nobs(iv),
            fsf = NA_real_,
            ivf1 = safe_stat(fitstat(iv, "ivf1"))
          )
        rows[[ix]] <- iv_row
        ix <- ix + 1L
      }
    }
  }
}

results <- bind_rows(rows) %>%
  select(model, iv_variant, fe, outcome, endogenous, term, estimate, std.error, statistic, p.value, conf.low, conf.high, nobs, fsf, ivf1)

write_csv(results, results_path)
write_csv(results, results_fe_path)

finalspec_path <- here::here(table_dir, "shift_share_lpg_iv_finalspec_results.csv")
final_controls <- paste(controls, "z_vehicle_lpg_estrato", "z_internet_lpg_estrato", sep = " + ")
final_rows <- list()
fx <- 1L

pick_val <- function(tbl, term_name, col_name) {
  if (is.null(tbl) || nrow(tbl) == 0) return(NA_real_)
  z <- tbl[tbl$term == term_name, , drop = FALSE]
  if (nrow(z) == 0) return(NA_real_)
  v <- z[[col_name]][1]
  if (is.null(v) || length(v) == 0) return(NA_real_)
  as.numeric(v)
}

pick_first_fit <- function(tbl, col_name) {
  if (is.null(tbl) || nrow(tbl) == 0) return(NA_real_)
  z <- tbl[grepl("^fit_", tbl$term), , drop = FALSE]
  if (nrow(z) == 0) return(NA_real_)
  v <- z[[col_name]][1]
  if (is.null(v) || length(v) == 0) return(NA_real_)
  as.numeric(v)
}

final_specs <- tibble::tribble(
  ~spec_name, ~sample_name, ~sample_filter, ~outcome, ~fe_formula,
  "A1_main_full", "full", "TRUE", "female_hours_effective_employed", "estrato4 + quarter + uf^year",
  "A2_main_full_pop", "full", "TRUE", "female_hours_effective_population", "estrato4 + quarter + uf^year",
  "A3_main_full_lfp", "full", "TRUE", "female_lfp", "estrato4 + quarter + uf^year",
  "A4_falsification_male_hours", "full", "TRUE", "male_hours_effective_employed", "estrato4 + quarter + uf^year",
  "A5_dynamic_lead1", "full", "TRUE", "female_hours_effective_employed_lead1", "estrato4 + quarter + uf^year",
  "A6_dynamic_lead2", "full", "TRUE", "female_hours_effective_employed_lead2", "estrato4 + quarter + uf^year",
  "B1_hetero_highwood", "high_wood", "high_wood_baseline == 1", "female_hours_effective_employed", "estrato4 + quarter + uf^year",
  "B2_hetero_outside_caprm", "outside_caprm", "outside_capital_rm == 1", "female_hours_effective_employed", "estrato4 + quarter + uf^year",
  "B3_hetero_highwood_outside", "high_wood_outside_caprm", "high_wood_baseline == 1 & outside_capital_rm == 1", "female_hours_effective_employed", "estrato4 + quarter + uf^year",
  "C1_robust_fe_main", "full", "TRUE", "female_hours_effective_employed", "estrato4 + year + quarter",
  "C2_robust_fe_lfp", "full", "TRUE", "female_lfp", "estrato4 + year + quarter",
  "C3_robust_fe_male_hours", "full", "TRUE", "male_hours_effective_employed", "estrato4 + year + quarter"
)

for (kk in seq_len(nrow(final_specs))) {
  s <- final_specs[kk, ]
  dat <- panel %>%
    filter(
      !!rlang::parse_expr(s$sample_filter),
      !is.na(.data[[s$outcome]]),
      !is.na(share_cooking_wood_charcoal),
      !is.na(z_wood_lpg_estrato),
      !is.na(z_vehicle_lpg_estrato),
      !is.na(z_internet_lpg_estrato),
      !is.na(weight_sum),
      weight_sum > 0
    )

  fs_model <- safe_feols(
    as.formula(paste0("share_cooking_wood_charcoal ~ z_wood_lpg_estrato + ", final_controls, " | ", s$fe_formula)),
    dat,
    ~weight_sum,
    ~estrato4
  )

  rf_model <- safe_feols(
    as.formula(paste0(s$outcome, " ~ z_wood_lpg_estrato + ", final_controls, " | ", s$fe_formula)),
    dat,
    ~weight_sum,
    ~estrato4
  )

  iv_model <- safe_feols(
    as.formula(paste0(s$outcome, " ~ ", final_controls, " | ", s$fe_formula, " | share_cooking_wood_charcoal ~ z_wood_lpg_estrato")),
    dat,
    ~weight_sum,
    ~estrato4
  )

  fs_coef <- if (is.null(fs_model)) NULL else tidy(fs_model)
  rf_coef <- if (is.null(rf_model)) NULL else tidy(rf_model)
  iv_coef <- if (is.null(iv_model)) NULL else tidy(iv_model)

  final_rows[[fx]] <- data.frame(
    spec_name = s$spec_name,
    sample = s$sample_name,
    outcome = s$outcome,
    fe = s$fe_formula,
    nobs = ifelse(is.null(iv_model), nrow(dat), nobs(iv_model)),
    fs_coef = pick_val(fs_coef, "z_wood_lpg_estrato", "estimate"),
    fs_p = pick_val(fs_coef, "z_wood_lpg_estrato", "p.value"),
    fs_f = ifelse(is.null(fs_model), NA_real_, safe_fitstat(fs_model, "f")),
    rf_coef = pick_val(rf_coef, "z_wood_lpg_estrato", "estimate"),
    rf_p = pick_val(rf_coef, "z_wood_lpg_estrato", "p.value"),
    iv_coef = pick_first_fit(iv_coef, "estimate"),
    iv_se = pick_first_fit(iv_coef, "std.error"),
    iv_p = pick_first_fit(iv_coef, "p.value"),
    ivf1 = ifelse(is.null(iv_model), NA_real_, safe_fitstat(iv_model, "ivf1")),
    kpr = ifelse(is.null(iv_model), NA_real_, safe_fitstat(iv_model, "kpr")),
    ar_p_beta0 = pick_val(rf_coef, "z_wood_lpg_estrato", "p.value"),
    stringsAsFactors = FALSE
  )
  fx <- fx + 1L
}

finalspec_results <- bind_rows(final_rows)
write_csv(finalspec_results, finalspec_path)

qa <- tibble(
  metric = c(
    "panel_rows",
    "panel_rows_with_uf_price",
    "panel_rows_with_estrato_price",
    "panel_rows_with_national_price",
    "panel_rows_with_unmapped_uf",
    "panel_years",
    "panel_quarters",
    "lpg_station_rows",
    "lpg_monthly_rows",
    "lpg_quarter_rows",
    "lpg_monthly_uf_rows",
    "lpg_quarter_uf_rows",
    "lpg_muni_quarter_rows",
    "lpg_quarter_estrato_rows",
    "lpg_muni_match_rate",
    "baseline_estrato_count"
  ),
  value = c(
    as.character(nrow(panel)),
    as.character(sum(!is.na(panel$z_wood_lpg_uf))),
    as.character(sum(!is.na(panel$z_wood_lpg_estrato))),
    as.character(sum(!is.na(panel$z_wood_lpg_national))),
    as.character(sum(is.na(panel$uf_sigla))),
    paste(sort(unique(panel$year)), collapse = ","),
    paste(sort(unique(panel$quarter)), collapse = ","),
    as.character(nrow(lpg_station)),
    as.character(nrow(lpg_monthly)),
    as.character(nrow(lpg_quarter)),
    as.character(nrow(lpg_monthly_uf)),
    as.character(nrow(lpg_quarter_uf)),
    as.character(nrow(lpg_muni_quarter)),
    as.character(nrow(lpg_quarter_estrato)),
    as.character(mean(!is.na(lpg_muni_joined$estrato4))),
    as.character(nrow(baseline))
  )
)
write_csv(qa, qa_path)

if (file.exists(coverage_script)) {
  cov_status <- tryCatch(system2("Rscript", coverage_script), error = function(e) 1L)
  if (!identical(cov_status, 0L)) {
    warning("Coverage audit refresh failed after Script 11 output write.")
  }
}

message("Wrote: ", results_path)
message("Wrote: ", results_fe_path)
message("Wrote: ", qa_path)
message("Wrote: ", finalspec_path)
print(results %>% filter(model %in% c("first_stage_wood", "iv_wood")))
