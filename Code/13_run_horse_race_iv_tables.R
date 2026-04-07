#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
  library(sidrar)
  library(tidyr)
})

root_dir <- here::here()

out_dir <- here::here("data", "powerIV", "regressions", "iv_horse_race")
table_dir <- here::here(out_dir, "tables")
macro_dir <- here::here("data", "powerIV", "macro")
dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(macro_dir, recursive = TRUE, showWarnings = FALSE)

table1_path <- here::here(table_dir, "table1_main_2sls_hours.csv")
table2_path <- here::here(table_dir, "table2_first_stage_diagnostics.csv")
table3_path <- here::here(table_dir, "table3_heterogeneity_outside_caprm.csv")
table4_path <- here::here(table_dir, "table4_placebo_secondary.csv")
macro_path <- here::here(macro_dir, "sidra_uf_quarter_macro_2016_2024.parquet")

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_quarter_estrato_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")

safe_num <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

safe_fitstat <- function(model, stat_name) {
  out <- tryCatch(fitstat(model, stat_name), error = function(e) NA_real_)
  safe_num(out)
}

get_term <- function(model, term_name) {
  tt <- tidy(model, conf.int = TRUE)
  rr <- tt[tt$term == term_name, , drop = FALSE]
  if (nrow(rr) == 0) return(NULL)
  rr[1, , drop = FALSE]
}

get_first_fit <- function(model) {
  tt <- tidy(model, conf.int = TRUE)
  rr <- tt[grepl("^fit_", tt$term), , drop = FALSE]
  if (nrow(rr) == 0) return(NULL)
  rr[1, , drop = FALSE]
}

macro_labor <- get_sidra(api = "/t/4093/n3/all/v/4099,4097/p/201601-202404") %>%
  transmute(
    uf_code = as.integer(`Unidade da Federação (Código)`),
    yq_code = as.integer(`Trimestre (Código)`),
    var_code = as.integer(`Variável (Código)`),
    value = as.numeric(Valor)
  ) %>%
  mutate(
    year = yq_code %/% 100L,
    quarter = yq_code %% 100L
  ) %>%
  filter(quarter %in% 1:4)

macro_wage <- get_sidra(api = "/t/5436/n3/all/v/5932/p/201601-202404") %>%
  transmute(
    uf_code = as.integer(`Unidade da Federação (Código)`),
    yq_code = as.integer(`Trimestre (Código)`),
    value = as.numeric(Valor)
  ) %>%
  mutate(
    year = yq_code %/% 100L,
    quarter = yq_code %% 100L
  ) %>%
  filter(quarter %in% 1:4)

macro_educ_raw <- get_sidra(
  x = 4095,
  variable = 1641,
  period = "201601-202404",
  geo = "State",
  classific = "c1568",
  category = "all"
) %>%
  transmute(
    uf_code = as.integer(`Unidade da Federação (Código)`),
    yq_code = as.integer(`Trimestre (Código)`),
    educ_code = as.integer(`Nível de instrução (Código)`),
    people_thousand = as.numeric(Valor)
  ) %>%
  mutate(
    year = yq_code %/% 100L,
    quarter = yq_code %% 100L
  ) %>%
  filter(quarter %in% 1:4)

uf_code_to_sigla <- c(
  "11" = "RO", "12" = "AC", "13" = "AM", "14" = "RR", "15" = "PA", "16" = "AP", "17" = "TO",
  "21" = "MA", "22" = "PI", "23" = "CE", "24" = "RN", "25" = "PB", "26" = "PE", "27" = "AL", "28" = "SE", "29" = "BA",
  "31" = "MG", "32" = "ES", "33" = "RJ", "35" = "SP",
  "41" = "PR", "42" = "SC", "43" = "RS",
  "50" = "MS", "51" = "MT", "52" = "GO", "53" = "DF"
)

macro_panel <- macro_labor %>%
  mutate(var_name = case_when(
    var_code == 4099L ~ "uf_unemployment_rate",
    var_code == 4097L ~ "uf_labor_demand_proxy",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(var_name)) %>%
  select(uf_code, year, quarter, var_name, value) %>%
  pivot_wider(names_from = var_name, values_from = value) %>%
  left_join(
    macro_wage %>% select(uf_code, year, quarter, uf_real_wage_proxy = value),
    by = c("uf_code", "year", "quarter")
  ) %>%
  left_join(
    macro_educ_raw %>%
      group_by(uf_code, year, quarter) %>%
      summarise(
        total_14plus = sum(people_thousand[educ_code == 120704L], na.rm = TRUE),
        n_secondary_complete = sum(people_thousand[educ_code == 11630L], na.rm = TRUE),
        n_tertiary_incomplete = sum(people_thousand[educ_code == 11631L], na.rm = TRUE),
        n_tertiary_complete = sum(people_thousand[educ_code == 11632L], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        uf_share_secondaryplus = dplyr::if_else(
          total_14plus > 0,
          (n_secondary_complete + n_tertiary_incomplete + n_tertiary_complete) / total_14plus,
          NA_real_
        ),
        uf_share_tertiary = dplyr::if_else(
          total_14plus > 0,
          (n_tertiary_incomplete + n_tertiary_complete) / total_14plus,
          NA_real_
        )
      ) %>%
      select(uf_code, year, quarter, uf_share_secondaryplus, uf_share_tertiary),
    by = c("uf_code", "year", "quarter")
  ) %>%
  mutate(uf_sigla = unname(uf_code_to_sigla[as.character(uf_code)]))

write_parquet(as.data.frame(macro_panel), macro_path)

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

lfp <- read_parquet(lfp_panel_path) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  transmute(
    estrato4, uf, uf_sigla = unname(uf_to_sigla[uf]), year, quarter, weight_sum,
    female_hours_effective_employed, male_hours_effective_employed, female_lfp,
    lightning_quarterly_total, raw_quarterly_outage_hrs_per_1000, dec_quarterly
  )

power <- read_parquet(power_panel_path) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  transmute(
    estrato4, year, quarter, capital, rm_ride,
    share_cooking_wood_charcoal,
    share_vehicle, mean_rooms, mean_bedrooms,
    share_internet_home, share_electricity_from_grid, share_grid_full_time
  )

lpg_estrato <- read_parquet(lpg_quarter_estrato_path) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  select(estrato4, year, quarter, log_lpg_price_quarterly_estrato)

baseline <- power %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = "drop")

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(baseline, by = "estrato4") %>%
  left_join(lpg_estrato, by = c("estrato4", "year", "quarter")) %>%
  left_join(
    macro_panel %>% select(
      uf_sigla, year, quarter,
      uf_unemployment_rate, uf_labor_demand_proxy, uf_real_wage_proxy,
      uf_share_secondaryplus, uf_share_tertiary
    ),
    by = c("uf_sigla", "year", "quarter")
  ) %>%
  mutate(
    z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato,
    outside_caprm = if_else((is.na(capital) | capital == "") & (is.na(rm_ride) | rm_ride == ""), 1, 0),
    raw_outage_x_outside = raw_quarterly_outage_hrs_per_1000 * outside_caprm,
    dec_x_outside = dec_quarterly * outside_caprm,
    wood_x_outside = share_cooking_wood_charcoal * outside_caprm,
    lightning_x_outside = lightning_quarterly_total * outside_caprm,
    z_wood_lpg_x_outside = z_wood_lpg_estrato * outside_caprm
  )

control_sets <- tibble::tribble(
  ~control_set, ~controls,
  "A_hh_plus_uf_series", "share_vehicle + mean_rooms + mean_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

designs <- tibble::tribble(
  ~design, ~endog, ~instr,
  "raw_lightning", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total",
  "dec_lightning", "dec_quarterly", "lightning_quarterly_total",
  "shiftshare_lpg", "share_cooking_wood_charcoal", "z_wood_lpg_estrato"
)

fe_formula <- "estrato4 + quarter"

table1_rows <- list()
table2_rows <- list()
t1 <- 1L
t2 <- 1L

for (ii in seq_len(nrow(control_sets))) {
  controls <- control_sets$controls[[ii]]
  control_set <- control_sets$control_set[[ii]]
  for (jj in seq_len(nrow(designs))) {
    design <- designs$design[[jj]]
    endog <- designs$endog[[jj]]
    instr <- designs$instr[[jj]]
    dat <- panel %>%
      filter(
        !is.na(weight_sum), weight_sum > 0,
        !is.na(female_hours_effective_employed),
        !is.na(.data[[endog]]),
        !is.na(.data[[instr]]),
        !is.na(share_vehicle), !is.na(mean_rooms), !is.na(mean_bedrooms),
        !is.na(uf_unemployment_rate), !is.na(uf_labor_demand_proxy), !is.na(uf_real_wage_proxy),
        !is.na(uf_share_secondaryplus), !is.na(uf_share_tertiary)
      )
    if (nrow(dat) == 0) next

    fs <- feols(
      as.formula(paste0(endog, " ~ ", instr, " + ", controls, " | ", fe_formula)),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )
    rf <- feols(
      as.formula(paste0("female_hours_effective_employed ~ ", instr, " + ", controls, " | ", fe_formula)),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )
    iv <- feols(
      as.formula(paste0("female_hours_effective_employed ~ ", controls, " | ", fe_formula, " | ", endog, " ~ ", instr)),
      data = dat,
      weights = ~weight_sum,
      cluster = ~estrato4
    )

    fs_term <- get_term(fs, instr)
    rf_term <- get_term(rf, instr)
    iv_term <- get_first_fit(iv)

    table1_rows[[t1]] <- tibble(
      control_set = control_set,
      design = design,
      outcome = "female_hours_effective_employed",
      nobs = nobs(iv),
      fs_coef = ifelse(is.null(fs_term), NA_real_, fs_term$estimate[[1]]),
      fs_p = ifelse(is.null(fs_term), NA_real_, fs_term$p.value[[1]]),
      rf_coef = ifelse(is.null(rf_term), NA_real_, rf_term$estimate[[1]]),
      rf_p = ifelse(is.null(rf_term), NA_real_, rf_term$p.value[[1]]),
      iv_coef = ifelse(is.null(iv_term), NA_real_, iv_term$estimate[[1]]),
      iv_se = ifelse(is.null(iv_term), NA_real_, iv_term$std.error[[1]]),
      iv_p = ifelse(is.null(iv_term), NA_real_, iv_term$p.value[[1]])
    )
    t1 <- t1 + 1L

    table2_rows[[t2]] <- tibble(
      control_set = control_set,
      design = design,
      nobs = nobs(iv),
      fs_f = safe_fitstat(fs, "f"),
      ivf1 = safe_fitstat(iv, "ivf1"),
      ivfall = safe_fitstat(iv, "ivfall"),
      kpr = safe_fitstat(iv, "kpr")
    )
    t2 <- t2 + 1L
  }
}

table1 <- bind_rows(table1_rows)
table2 <- bind_rows(table2_rows)

hetero_designs <- tibble::tribble(
  ~design, ~endog, ~endog_x, ~instr, ~instr_x,
  "raw_lightning", "raw_quarterly_outage_hrs_per_1000", "raw_outage_x_outside", "lightning_quarterly_total", "lightning_x_outside",
  "dec_lightning", "dec_quarterly", "dec_x_outside", "lightning_quarterly_total", "lightning_x_outside",
  "shiftshare_lpg", "share_cooking_wood_charcoal", "wood_x_outside", "z_wood_lpg_estrato", "z_wood_lpg_x_outside"
)

table3_rows <- list()
t3 <- 1L

for (jj in seq_len(nrow(hetero_designs))) {
  design <- hetero_designs$design[[jj]]
  endog <- hetero_designs$endog[[jj]]
  endog_x <- hetero_designs$endog_x[[jj]]
  instr <- hetero_designs$instr[[jj]]
  instr_x <- hetero_designs$instr_x[[jj]]
  controls <- control_sets$controls[[1]]
  dat <- panel %>%
    filter(
      !is.na(weight_sum), weight_sum > 0,
      !is.na(female_hours_effective_employed),
      !is.na(.data[[endog]]), !is.na(.data[[endog_x]]),
      !is.na(.data[[instr]]), !is.na(.data[[instr_x]]),
      !is.na(share_vehicle), !is.na(mean_rooms), !is.na(mean_bedrooms),
      !is.na(uf_unemployment_rate), !is.na(uf_labor_demand_proxy), !is.na(uf_real_wage_proxy),
      !is.na(uf_share_secondaryplus), !is.na(uf_share_tertiary)
    )
  if (nrow(dat) == 0) next

  iv <- feols(
    as.formula(paste0(
      "female_hours_effective_employed ~ ", controls, " | ", fe_formula, " | ",
      endog, " + ", endog_x, " ~ ", instr, " + ", instr_x
    )),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  tt <- tidy(iv, conf.int = TRUE)
  b_main <- tt %>% filter(term == paste0("fit_", endog))
  b_int <- tt %>% filter(term == paste0("fit_", endog_x))
  b <- coef(iv)
  V <- vcov(iv)
  n1 <- paste0("fit_", endog)
  n2 <- paste0("fit_", endog_x)
  implied <- NA_real_
  implied_p <- NA_real_
  if (all(c(n1, n2) %in% names(b))) {
    est <- b[[n1]] + b[[n2]]
    se <- sqrt(V[n1, n1] + V[n2, n2] + 2 * V[n1, n2])
    zt <- est / se
    implied <- est
    implied_p <- 2 * pnorm(-abs(zt))
  }

  table3_rows[[t3]] <- tibble(
    design = design,
    control_set = control_sets$control_set[[1]],
    nobs = nobs(iv),
    beta_main = ifelse(nrow(b_main) == 0, NA_real_, b_main$estimate[[1]]),
    p_main = ifelse(nrow(b_main) == 0, NA_real_, b_main$p.value[[1]]),
    beta_interaction = ifelse(nrow(b_int) == 0, NA_real_, b_int$estimate[[1]]),
    p_interaction = ifelse(nrow(b_int) == 0, NA_real_, b_int$p.value[[1]]),
    implied_outside_caprm = implied,
    p_implied_outside_caprm = implied_p,
    ivf1 = safe_fitstat(iv, "ivf1"),
    ivf2 = safe_fitstat(iv, "ivf2"),
    ivfall = safe_fitstat(iv, "ivfall")
  )
  t3 <- t3 + 1L
}

table3 <- bind_rows(table3_rows)

placebo_outcomes <- c("male_hours_effective_employed", "female_lfp")

table4_rows <- list()
t4 <- 1L

for (ii in seq_len(nrow(control_sets))) {
  controls <- control_sets$controls[[ii]]
  control_set <- control_sets$control_set[[ii]]
  for (jj in seq_len(nrow(designs))) {
    design <- designs$design[[jj]]
    endog <- designs$endog[[jj]]
    instr <- designs$instr[[jj]]
    for (yy in placebo_outcomes) {
      dat <- panel %>%
        filter(
          !is.na(weight_sum), weight_sum > 0,
          !is.na(.data[[yy]]),
          !is.na(.data[[endog]]),
          !is.na(.data[[instr]]),
          !is.na(share_vehicle), !is.na(mean_rooms), !is.na(mean_bedrooms),
          !is.na(uf_unemployment_rate), !is.na(uf_labor_demand_proxy), !is.na(uf_real_wage_proxy),
          !is.na(uf_share_secondaryplus), !is.na(uf_share_tertiary)
        )
      if (nrow(dat) == 0) next

      iv <- feols(
        as.formula(paste0(yy, " ~ ", controls, " | ", fe_formula, " | ", endog, " ~ ", instr)),
        data = dat,
        weights = ~weight_sum,
        cluster = ~estrato4
      )
      iv_term <- get_first_fit(iv)

      table4_rows[[t4]] <- tibble(
        control_set = control_set,
        design = design,
        outcome = yy,
        nobs = nobs(iv),
        iv_coef = ifelse(is.null(iv_term), NA_real_, iv_term$estimate[[1]]),
        iv_se = ifelse(is.null(iv_term), NA_real_, iv_term$std.error[[1]]),
        iv_p = ifelse(is.null(iv_term), NA_real_, iv_term$p.value[[1]]),
        ivf1 = safe_fitstat(iv, "ivf1"),
        ivfall = safe_fitstat(iv, "ivfall")
      )
      t4 <- t4 + 1L
    }
  }
}

table4 <- bind_rows(table4_rows)

write_csv(table1, table1_path)
write_csv(table2, table2_path)
write_csv(table3, table3_path)
write_csv(table4, table4_path)

message("Wrote: ", table1_path)
message("Wrote: ", table2_path)
message("Wrote: ", table3_path)
message("Wrote: ", table4_path)
message("Wrote: ", macro_path)

print(table1)
print(table2)
print(table3)
print(table4)
