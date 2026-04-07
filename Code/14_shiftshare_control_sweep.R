#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(PNADcIBGE)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

root_dir <- here::here()

raw_dir <- here::here("data", "PNAD-C", "raw")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_quarter_estrato_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "shift_share_lpg", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
main_out <- here::here(out_dir, "shiftshare_control_sweep_main.csv")
placebo_out <- here::here(out_dir, "shiftshare_control_sweep_placebo.csv")

weighted_share <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) return(NA_real_)
  weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
}

weighted_quantile <- function(x, w, probs) {
  keep <- !is.na(x) & !is.na(w) & w > 0
  if (!any(keep)) return(rep(NA_real_, length(probs)))
  x <- x[keep]
  w <- w[keep]
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]
  cw <- cumsum(w) / sum(w)
  sapply(probs, function(p) {
    idx <- which(cw >= p)[1]
    if (is.na(idx)) return(NA_real_)
    x[idx]
  })
}

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

get_pnadc_quiet <- function(...) {
  suppressMessages(suppressWarnings(suppressPackageStartupMessages(get_pnadc(...))))
}

extract_pnadc_data <- function(x) {
  if (is.data.frame(x)) return(as_tibble(x))
  if (is.list(x)) {
    if (!is.null(x$variables) && is.data.frame(x$variables)) return(as_tibble(x$variables))
    if (!is.null(x$data) && is.data.frame(x$data)) return(as_tibble(x$data))
    data_frames <- x[vapply(x, is.data.frame, logical(1))]
    if (length(data_frames) > 0) return(as_tibble(data_frames[[1]]))
  }
  stop("PNAD-C fetch did not return a tabular object.", call. = FALSE)
}

if (!file.exists(baseline_path)) {
  vars_base <- c(
    "Ano", "Trimestre", "UF", "Estrato", "V1022", "V1033", "V2007", "V2009",
    "VD3004", "VD4019"
  )

  q_list <- list()
  for (q in 1:4) {
    message("Building 2016 baseline from PNAD-C quarter ", q)
    dat <- tryCatch(
      get_pnadc_quiet(
        year = 2016,
        quarter = q,
        vars = vars_base,
        labels = TRUE,
        deflator = FALSE,
        design = FALSE,
        savedir = raw_dir,
        reload = FALSE
      ),
      error = function(e) {
        get_pnadc_quiet(
          year = 2016,
          quarter = q,
          vars = vars_base,
          labels = TRUE,
          deflator = FALSE,
          design = FALSE,
          savedir = raw_dir,
          reload = TRUE
        )
      }
    )
    qdf <- extract_pnadc_data(dat) %>%
      mutate(across(everything(), as.character)) %>%
      transmute(
        estrato4 = substr(Estrato, 1, 4),
        weight = suppressWarnings(as.numeric(V1033)),
        area_situation = V1022,
        sex = V2007,
        age = suppressWarnings(as.integer(V2009)),
        education = VD3004,
        income = suppressWarnings(as.numeric(VD4019))
      )
    q_list[[paste0("q", q)]] <- qdf
  }

  base_df <- bind_rows(q_list) %>%
    filter(!is.na(weight), weight > 0)

  baseline <- base_df %>%
    group_by(estrato4) %>%
    summarise(
      baseline_female_age_25_34 = weighted_share(sex == "Mulher" & age >= 25 & age <= 34, weight),
      baseline_female_age_35_54 = weighted_share(sex == "Mulher" & age >= 35 & age <= 54, weight),
      baseline_female_age_55plus = weighted_share(sex == "Mulher" & age >= 55, weight),
      baseline_child_dependency = {
        num <- sum(weight[!is.na(age) & age >= 0 & age <= 14], na.rm = TRUE)
        den <- sum(weight[!is.na(age) & age >= 15 & age <= 64], na.rm = TRUE)
        ifelse(den > 0, num / den, NA_real_)
      },
      baseline_female_share_secondaryplus = weighted_share(
        sex == "Mulher" & !is.na(education) &
          education %in% c("Médio completo ou equivalente", "Superior incompleto ou equivalente", "Superior completo"),
        weight
      ),
      baseline_female_share_tertiary = weighted_share(
        sex == "Mulher" & !is.na(education) &
          education %in% c("Superior incompleto ou equivalente", "Superior completo"),
        weight
      ),
      baseline_rural_share = weighted_share(area_situation == "Rural", weight),
      baseline_hh_size_proxy = sum(weight, na.rm = TRUE) / n(),
      .groups = "drop"
    )

  income_moments <- base_df %>%
    filter(!is.na(income), income > 0, !is.na(age), age >= 18) %>%
    group_by(estrato4) %>%
    summarise(
      baseline_log_income_p25 = weighted_quantile(log1p(income), weight, 0.25)[[1]],
      baseline_log_income_p50 = weighted_quantile(log1p(income), weight, 0.50)[[1]],
      baseline_log_income_p75 = weighted_quantile(log1p(income), weight, 0.75)[[1]],
      .groups = "drop"
    )

  baseline <- baseline %>%
    left_join(income_moments, by = "estrato4")

  write_parquet(as.data.frame(baseline), baseline_path)
} else {
  baseline <- read_parquet(baseline_path) %>%
    mutate(estrato4 = as.character(estrato4))
}

if (!file.exists(macro_path)) {
  stop("Missing macro panel: ", macro_path, call. = FALSE)
}

macro_panel <- read_parquet(macro_path) %>%
  mutate(uf_sigla = as.character(uf_sigla))

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

lfp <- read_parquet(lfp_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    uf,
    uf_sigla = unname(uf_to_sigla[uf]),
    year, quarter, weight_sum,
    female_hours_effective_employed, male_hours_effective_employed, female_lfp
  )

power <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year, quarter,
    share_cooking_wood_charcoal,
    share_vehicle, mean_rooms, mean_bedrooms
  )

lpg <- read_parquet(lpg_quarter_estrato_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year, quarter,
    log_lpg_price_quarterly_estrato
  )

baseline_wood <- power %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(lpg, by = c("estrato4", "year", "quarter")) %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(baseline, by = "estrato4") %>%
  left_join(
    macro_panel %>% select(uf_sigla, year, quarter, uf_unemployment_rate, uf_labor_demand_proxy, uf_real_wage_proxy),
    by = c("uf_sigla", "year", "quarter")
  ) %>%
  mutate(
    z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato,
    trend = year - 2016,
    b_age2534_t = baseline_female_age_25_34 * trend,
    b_age3554_t = baseline_female_age_35_54 * trend,
    b_age55plus_t = baseline_female_age_55plus * trend,
    b_dep_t = baseline_child_dependency * trend,
    b_secplus_t = baseline_female_share_secondaryplus * trend,
    b_tertiary_t = baseline_female_share_tertiary * trend,
    b_inc_p25_t = baseline_log_income_p25 * trend,
    b_inc_p50_t = baseline_log_income_p50 * trend,
    b_inc_p75_t = baseline_log_income_p75 * trend,
    b_rural_t = baseline_rural_share * trend
  ) %>%
  filter(
    !is.na(weight_sum), weight_sum > 0,
    !is.na(share_cooking_wood_charcoal),
    !is.na(z_wood_lpg_estrato),
    !is.na(share_vehicle), !is.na(mean_rooms), !is.na(mean_bedrooms),
    !is.na(uf_unemployment_rate), !is.na(uf_labor_demand_proxy), !is.na(uf_real_wage_proxy)
  )

specs <- tibble::tribble(
  ~spec_name, ~controls,
  "S1_macro_hh_base", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy",
  "S2_add_age_dependency", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + b_age2534_t + b_age3554_t + b_age55plus_t + b_dep_t",
  "S3_add_female_education", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + b_age2534_t + b_age3554_t + b_age55plus_t + b_dep_t + b_secplus_t + b_tertiary_t",
  "S4_add_income_moments", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + b_age2534_t + b_age3554_t + b_age55plus_t + b_dep_t + b_secplus_t + b_tertiary_t + b_inc_p25_t + b_inc_p50_t + b_inc_p75_t",
  "S5_add_rural_mix", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + b_age2534_t + b_age3554_t + b_age55plus_t + b_dep_t + b_secplus_t + b_tertiary_t + b_inc_p25_t + b_inc_p50_t + b_inc_p75_t + b_rural_t"
)

run_iv <- function(df, outcome, controls) {
  f_fs <- as.formula(paste0("share_cooking_wood_charcoal ~ z_wood_lpg_estrato + ", controls, " | estrato4 + quarter"))
  f_rf <- as.formula(paste0(outcome, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + quarter"))
  f_iv <- as.formula(paste0(outcome, " ~ ", controls, " | estrato4 + quarter | share_cooking_wood_charcoal ~ z_wood_lpg_estrato"))

  fs <- feols(f_fs, data = df, weights = ~weight_sum, cluster = ~estrato4)
  rf <- feols(f_rf, data = df, weights = ~weight_sum, cluster = ~estrato4)
  iv <- feols(f_iv, data = df, weights = ~weight_sum, cluster = ~estrato4)

  fs_t <- tidy(fs) %>% filter(term == "z_wood_lpg_estrato")
  rf_t <- tidy(rf) %>% filter(term == "z_wood_lpg_estrato")
  iv_t <- tidy(iv) %>% filter(grepl("^fit_", term))

  tibble(
    nobs = nobs(iv),
    fs_coef = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$estimate[[1]]),
    fs_p = ifelse(nrow(fs_t) == 0, NA_real_, fs_t$p.value[[1]]),
    rf_coef = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$estimate[[1]]),
    rf_p = ifelse(nrow(rf_t) == 0, NA_real_, rf_t$p.value[[1]]),
    iv_coef = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$estimate[[1]]),
    iv_se = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$std.error[[1]]),
    iv_p = ifelse(nrow(iv_t) == 0, NA_real_, iv_t$p.value[[1]]),
    ivf1 = safe_fitstat(iv, "ivf1"),
    ivfall = safe_fitstat(iv, "ivfall")
  )
}

main_rows <- list()
placebo_rows <- list()
im <- 1L
ip <- 1L

for (i in seq_len(nrow(specs))) {
  s <- specs[i, ]
  controls <- s$controls[[1]]

  dat_main <- panel %>% filter(!is.na(female_hours_effective_employed))
  res_main <- run_iv(dat_main, "female_hours_effective_employed", controls) %>%
    mutate(spec_name = s$spec_name, outcome = "female_hours_effective_employed")
  main_rows[[im]] <- res_main
  im <- im + 1L

  dat_male <- panel %>% filter(!is.na(male_hours_effective_employed))
  res_male <- run_iv(dat_male, "male_hours_effective_employed", controls) %>%
    mutate(spec_name = s$spec_name, outcome = "male_hours_effective_employed")
  placebo_rows[[ip]] <- res_male
  ip <- ip + 1L

  dat_lfp <- panel %>% filter(!is.na(female_lfp))
  res_lfp <- run_iv(dat_lfp, "female_lfp", controls) %>%
    mutate(spec_name = s$spec_name, outcome = "female_lfp")
  placebo_rows[[ip]] <- res_lfp
  ip <- ip + 1L
}

main_tbl <- bind_rows(main_rows) %>%
  select(spec_name, outcome, nobs, fs_coef, fs_p, rf_coef, rf_p, iv_coef, iv_se, iv_p, ivf1, ivfall)

placebo_tbl <- bind_rows(placebo_rows) %>%
  select(spec_name, outcome, nobs, fs_coef, fs_p, rf_coef, rf_p, iv_coef, iv_se, iv_p, ivf1, ivfall)

write_csv(main_tbl, main_out)
write_csv(placebo_tbl, placebo_out)

message("Wrote: ", baseline_path)
message("Wrote: ", main_out)
message("Wrote: ", placebo_out)
print(main_tbl)
print(placebo_tbl)
