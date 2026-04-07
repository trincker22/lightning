#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

root_dir <- here::here()

asset_split_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_urban_rural_2016_2024.parquet")
power_split_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_urban_rural_with_power_2016_2024.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_iv_assets", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

interaction_out_path <- here::here(out_dir, "lightning_iv_assets_rural_interaction_prepandemic.csv")
split_out_path <- here::here(out_dir, "lightning_iv_assets_rural_split_prepandemic.csv")

if (!file.exists(asset_split_path)) stop("Missing file: ", asset_split_path, call. = FALSE)
if (!file.exists(power_split_path)) stop("Missing file: ", power_split_path, call. = FALSE)
if (!file.exists(macro_path)) stop("Missing file: ", macro_path, call. = FALSE)
if (!file.exists(baseline_path)) stop("Missing file: ", baseline_path, call. = FALSE)

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

assets <- read_parquet(asset_split_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    urban_rural = as.character(urban_rural),
    uf = as.character(uf),
    uf_sigla = unname(uf_to_sigla[uf]),
    hh_weight_sum = as.numeric(weight_sum),
    share_refrigerator = as.numeric(share_refrigerator),
    share_washing_machine = as.numeric(share_washing_machine),
    share_computer = as.numeric(share_computer),
    share_vehicle = as.numeric(share_vehicle),
    mean_rooms = as.numeric(mean_rooms),
    mean_bedrooms = as.numeric(mean_bedrooms)
  )

power <- read_parquet(power_split_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    urban_rural = as.character(urban_rural),
    lightning_quarterly_total = as.numeric(lightning_quarterly_total),
    raw_quarterly_outage_hrs_per_1000 = as.numeric(raw_quarterly_outage_hrs_per_1000),
    raw_quarterly_events_per_1000 = as.numeric(raw_quarterly_events_per_1000)
  )

macro <- read_parquet(macro_path) %>%
  transmute(
    uf_sigla = as.character(uf_sigla),
    year,
    quarter,
    uf_unemployment_rate = as.numeric(uf_unemployment_rate),
    uf_labor_demand_proxy = as.numeric(uf_labor_demand_proxy),
    uf_real_wage_proxy = as.numeric(uf_real_wage_proxy),
    uf_share_secondaryplus = as.numeric(uf_share_secondaryplus),
    uf_share_tertiary = as.numeric(uf_share_tertiary)
  )

baseline <- read_parquet(baseline_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_female_age_25_34 = as.numeric(baseline_female_age_25_34),
    baseline_female_age_35_54 = as.numeric(baseline_female_age_35_54),
    baseline_female_age_55plus = as.numeric(baseline_female_age_55plus),
    baseline_child_dependency = as.numeric(baseline_child_dependency),
    baseline_female_share_secondaryplus = as.numeric(baseline_female_share_secondaryplus),
    baseline_female_share_tertiary = as.numeric(baseline_female_share_tertiary),
    baseline_log_income_p25 = as.numeric(baseline_log_income_p25),
    baseline_log_income_p50 = as.numeric(baseline_log_income_p50),
    baseline_log_income_p75 = as.numeric(baseline_log_income_p75),
    baseline_rural_share = as.numeric(baseline_rural_share)
  )

panel <- assets %>%
  left_join(power, by = c("estrato4", "year", "quarter", "urban_rural")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  filter(year <= 2019) %>%
  mutate(
    rural = if_else(urban_rural == "rural", 1, 0),
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
  )

outcomes <- tibble::tribble(
  ~outcome, ~outcome_label,
  "share_washing_machine", "Washing machine ownership",
  "share_computer", "Computer ownership",
  "share_vehicle", "Vehicle ownership"
)

endog_specs <- tibble::tribble(
  ~endog_var, ~endog_label,
  "raw_quarterly_outage_hrs_per_1000", "Outage hours per 1,000",
  "raw_quarterly_events_per_1000", "Outage events per 1,000"
)

base_controls <- c(
  "mean_rooms",
  "mean_bedrooms",
  "uf_unemployment_rate",
  "uf_labor_demand_proxy",
  "uf_real_wage_proxy",
  "uf_share_secondaryplus",
  "uf_share_tertiary",
  "b_age2534_t",
  "b_age3554_t",
  "b_age55plus_t",
  "b_dep_t",
  "b_secplus_t",
  "b_tertiary_t",
  "b_inc_p25_t",
  "b_inc_p50_t",
  "b_inc_p75_t",
  "b_rural_t",
  "rural"
)

interaction_rows <- list()
ir <- 1L
split_rows <- list()
sr <- 1L

for (ii in seq_len(nrow(endog_specs))) {
  endog_var <- endog_specs$endog_var[[ii]]
  endog_label <- endog_specs$endog_label[[ii]]

  for (jj in seq_len(nrow(outcomes))) {
    outcome <- outcomes$outcome[[jj]]
    outcome_label <- outcomes$outcome_label[[jj]]

    controls <- if (outcome == "share_vehicle") base_controls else c("share_vehicle", base_controls)

    dat <- panel %>%
      mutate(
        endog = .data[[endog_var]],
        instr = lightning_quarterly_total,
        endog_rural = .data[[endog_var]] * rural,
        instr_rural = lightning_quarterly_total * rural
      )

    needed <- unique(c("hh_weight_sum", "estrato4", "quarter", "rural", outcome, controls, "endog", "instr", "endog_rural", "instr_rural"))
    dat <- dat %>% filter(if_all(all_of(needed), ~ !is.na(.x)), hh_weight_sum > 0)

    if (nrow(dat) == 0) next

    controls_txt <- paste(controls, collapse = " + ")

    fs_endog <- feols(
      as.formula(paste0("endog ~ instr + instr_rural + ", controls_txt, " | estrato4 + quarter")),
      data = dat,
      weights = ~hh_weight_sum,
      cluster = ~estrato4
    )

    fs_endog_rural <- feols(
      as.formula(paste0("endog_rural ~ instr + instr_rural + ", controls_txt, " | estrato4 + quarter")),
      data = dat,
      weights = ~hh_weight_sum,
      cluster = ~estrato4
    )

    iv <- feols(
      as.formula(paste0(outcome, " ~ ", controls_txt, " | estrato4 + quarter | endog + endog_rural ~ instr + instr_rural")),
      data = dat,
      weights = ~hh_weight_sum,
      cluster = ~estrato4
    )

    fs_main_t <- tidy(fs_endog) %>% filter(term == "instr")
    fs_int_t <- tidy(fs_endog_rural) %>% filter(term == "instr_rural")
    iv_t <- tidy(iv) %>% filter(grepl("^fit_", term))

    coef_urban <- iv_t %>% filter(term == "fit_endog") %>% pull(estimate)
    se_urban <- iv_t %>% filter(term == "fit_endog") %>% pull(std.error)
    p_urban <- iv_t %>% filter(term == "fit_endog") %>% pull(p.value)
    coef_rural_diff <- iv_t %>% filter(term == "fit_endog_rural") %>% pull(estimate)
    se_rural_diff <- iv_t %>% filter(term == "fit_endog_rural") %>% pull(std.error)
    p_rural_diff <- iv_t %>% filter(term == "fit_endog_rural") %>% pull(p.value)

    if (length(coef_urban) == 0) coef_urban <- NA_real_
    if (length(se_urban) == 0) se_urban <- NA_real_
    if (length(p_urban) == 0) p_urban <- NA_real_
    if (length(coef_rural_diff) == 0) coef_rural_diff <- NA_real_
    if (length(se_rural_diff) == 0) se_rural_diff <- NA_real_
    if (length(p_rural_diff) == 0) p_rural_diff <- NA_real_

    interaction_rows[[ir]] <- tibble(
      outcome = outcome,
      outcome_label = outcome_label,
      endog_var = endog_var,
      endog_label = endog_label,
      nobs = nobs(iv),
      fs_instr_on_endog_coef = ifelse(nrow(fs_main_t) == 0, NA_real_, fs_main_t$estimate[[1]]),
      fs_instr_on_endog_p = ifelse(nrow(fs_main_t) == 0, NA_real_, fs_main_t$p.value[[1]]),
      fs_instrrural_on_endogrural_coef = ifelse(nrow(fs_int_t) == 0, NA_real_, fs_int_t$estimate[[1]]),
      fs_instrrural_on_endogrural_p = ifelse(nrow(fs_int_t) == 0, NA_real_, fs_int_t$p.value[[1]]),
      iv_urban_coef = coef_urban,
      iv_urban_se = se_urban,
      iv_urban_p = p_urban,
      iv_rural_diff_coef = coef_rural_diff,
      iv_rural_diff_se = se_rural_diff,
      iv_rural_diff_p = p_rural_diff,
      iv_rural_total_coef = coef_urban + coef_rural_diff,
      ivf1 = safe_fitstat(iv, "ivf1"),
      ivfall = safe_fitstat(iv, "ivfall")
    )
    ir <- ir + 1L

    for (group_name in c("urban", "rural")) {
      dat_g <- dat %>% filter(urban_rural == group_name)
      if (nrow(dat_g) == 0) next

      controls_g <- controls[controls != "rural"]
      controls_g_txt <- paste(controls_g, collapse = " + ")

      fs_g <- feols(
        as.formula(paste0("endog ~ instr + ", controls_g_txt, " | estrato4 + quarter")),
        data = dat_g,
        weights = ~hh_weight_sum,
        cluster = ~estrato4
      )

      iv_g <- feols(
        as.formula(paste0(outcome, " ~ ", controls_g_txt, " | estrato4 + quarter | endog ~ instr")),
        data = dat_g,
        weights = ~hh_weight_sum,
        cluster = ~estrato4
      )

      fs_g_t <- tidy(fs_g) %>% filter(term == "instr")
      iv_g_t <- tidy(iv_g) %>% filter(term == "fit_endog")

      split_rows[[sr]] <- tibble(
        outcome = outcome,
        outcome_label = outcome_label,
        endog_var = endog_var,
        endog_label = endog_label,
        sample = group_name,
        nobs = nobs(iv_g),
        fs_coef = ifelse(nrow(fs_g_t) == 0, NA_real_, fs_g_t$estimate[[1]]),
        fs_p = ifelse(nrow(fs_g_t) == 0, NA_real_, fs_g_t$p.value[[1]]),
        iv_coef = ifelse(nrow(iv_g_t) == 0, NA_real_, iv_g_t$estimate[[1]]),
        iv_se = ifelse(nrow(iv_g_t) == 0, NA_real_, iv_g_t$std.error[[1]]),
        iv_p = ifelse(nrow(iv_g_t) == 0, NA_real_, iv_g_t$p.value[[1]]),
        ivf1 = safe_fitstat(iv_g, "ivf1"),
        ivfall = safe_fitstat(iv_g, "ivfall")
      )
      sr <- sr + 1L
    }
  }
}

interaction_tbl <- bind_rows(interaction_rows) %>% arrange(endog_var, outcome)
split_tbl <- bind_rows(split_rows) %>% arrange(endog_var, outcome, sample)

write_csv(interaction_tbl, interaction_out_path)
write_csv(split_tbl, split_out_path)

message("Wrote: ", interaction_out_path)
message("Wrote: ", split_out_path)
print(interaction_tbl)
print(split_tbl)
