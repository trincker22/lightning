#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here::here("data", "powerIV", "regressions", "lpg_state_price_alternatives")
table_dir <- here::here("data", "powerIV", "regressions", "lpg_state_price_alternatives", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_uf_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_uf_2016_2024.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")

uf_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP", "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

stars <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.01) return("***")
  if (p < 0.05) return("**")
  if (p < 0.1) return("*")
  ""
}

fmt_num <- function(x, digits = 3) {
  if (is.na(x)) return("")
  formatC(x, digits = digits, format = "f")
}

fmt_coef <- function(x, p, digits = 3) {
  if (is.na(x)) return("")
  paste0(formatC(x, digits = digits, format = "f"), stars(p))
}

fmt_se <- function(x, digits = 3) {
  if (is.na(x)) return("")
  paste0("(", formatC(x, digits = digits, format = "f"), ")")
}

term_row <- function(model, term) {
  tbl <- tidy(model)
  row <- tbl[tbl$term == term, , drop = FALSE]
  if (nrow(row) == 0) return(c(estimate = NA_real_, std.error = NA_real_, p.value = NA_real_))
  c(estimate = row$estimate[[1]], std.error = row$std.error[[1]], p.value = row$p.value[[1]])
}

wald_f <- function(model, term) {
  out <- tryCatch(wald(model, term), error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  stat <- suppressWarnings(as.numeric(unlist(out$stat)))
  stat <- stat[!is.na(stat)]
  if (length(stat) == 0) return(NA_real_)
  stat[[1]]
}

tex_wrap <- function(caption, label, body_lines, note) {
  c(
    "\\begin{table}[!htbp]",
    "\\centering",
    paste0("\\caption{", caption, "}"),
    paste0("\\label{", label, "}"),
    "\\small",
    body_lines,
    "\\vspace{0.3em}",
    paste0("\\parbox{0.94\\linewidth}{\\footnotesize ", note, "}"),
    "\\end{table}"
  )
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

pre_cap_uf <- lpg_uf %>%
  filter(year < 2022 | (year == 2022 & quarter <= 2)) %>%
  group_by(uf_sigla) %>%
  summarise(
    pre_cap_log_lpg_price_uf = mean(log_lpg_price_quarterly_uf, na.rm = TRUE),
    .groups = "drop"
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(lpg_uf, by = c("uf_sigla", "year", "quarter", "year_quarter")) %>%
  left_join(pre_cap_uf, by = "uf_sigla") %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    z_wood_lpg_uf = baseline_wood_2016 * log_lpg_price_quarterly_uf,
    rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - mean(log_lpg_price_quarterly_uf, na.rm = TRUE),
    post_icms_cap = as.integer(year > 2022 | (year == 2022 & quarter >= 3)),
    z_wood_icmscap_proxy = baseline_wood_2016 * pre_cap_log_lpg_price_uf * post_icms_cap
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

national_quarter <- lpg_uf %>%
  group_by(year, quarter, year_quarter) %>%
  summarise(log_lpg_price_quarterly_national_from_uf = mean(log_lpg_price_quarterly_uf, na.rm = TRUE), .groups = "drop")

panel <- panel %>%
  left_join(national_quarter, by = c("year", "quarter", "year_quarter")) %>%
  mutate(rel_log_lpg_price_uf = log_lpg_price_quarterly_uf - log_lpg_price_quarterly_national_from_uf) %>%
  group_by(uf_sigla) %>%
  mutate(
    rel_log_lpg_price_uf_dm = rel_log_lpg_price_uf - mean(rel_log_lpg_price_uf, na.rm = TRUE),
    z_wood_lpg_uf_dm = baseline_wood_2016 * rel_log_lpg_price_uf_dm
  ) %>%
  ungroup()

panel_pre2023q1 <- panel %>%
  filter(year < 2023 | (year == 2023 & quarter <= 1))

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time",
  "M3", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
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

needed_for <- function(outcome, instrument_var, controls) {
  unique(c(
    "estrato4", "year_quarter", "weight_sum", "share_cooking_wood_charcoal",
    instrument_var, outcome, all.vars(as.formula(paste("~", controls)))
  ))
}

diag_option1_nocontrols_data <- panel %>%
  filter(!is.na(log_lpg_price_quarterly_uf), !is.na(baseline_wood_2016))

diag_option1_nocontrols_data$lpg_price_resid_yq <- resid(
  feols(log_lpg_price_quarterly_uf ~ 1 | year_quarter, data = diag_option1_nocontrols_data)
)

diag_option1_nocontrols <- feols(
  lpg_price_resid_yq ~ baseline_wood_2016,
  data = diag_option1_nocontrols_data,
  cluster = ~estrato4
)

diag_option1_preferred_data <- panel %>%
  filter(!is.na(log_lpg_price_quarterly_uf), !is.na(baseline_wood_2016), !is.na(mean_rooms), !is.na(mean_bedrooms), !is.na(b_dep_t), !is.na(share_grid_full_time))

diag_option1_preferred <- feols(
  log_lpg_price_quarterly_uf ~ baseline_wood_2016 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | year_quarter,
  data = diag_option1_preferred_data,
  cluster = ~estrato4
)

diag_option1_pre2023_data <- panel_pre2023q1 %>%
  filter(!is.na(log_lpg_price_quarterly_uf), !is.na(baseline_wood_2016))

diag_option1_pre2023_data$lpg_price_resid_yq <- resid(
  feols(log_lpg_price_quarterly_uf ~ 1 | year_quarter, data = diag_option1_pre2023_data)
)

diag_option1_pre2023 <- feols(
  lpg_price_resid_yq ~ baseline_wood_2016,
  data = diag_option1_pre2023_data,
  cluster = ~estrato4
)

diag_option1_preferred_pre2023_data <- panel_pre2023q1 %>%
  filter(!is.na(log_lpg_price_quarterly_uf), !is.na(baseline_wood_2016), !is.na(mean_rooms), !is.na(mean_bedrooms), !is.na(b_dep_t), !is.na(share_grid_full_time))

diag_option1_preferred_pre2023 <- feols(
  log_lpg_price_quarterly_uf ~ baseline_wood_2016 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | year_quarter,
  data = diag_option1_preferred_pre2023_data,
  cluster = ~estrato4
)

diag1_nc <- term_row(diag_option1_nocontrols, "baseline_wood_2016")
diag1_pc <- term_row(diag_option1_preferred, "baseline_wood_2016")
diag1_pre <- term_row(diag_option1_pre2023, "baseline_wood_2016")
diag1_pc_pre <- term_row(diag_option1_preferred_pre2023, "baseline_wood_2016")

diag_body <- c(
  "\\begin{tabular}{lcccc}",
  "\\toprule",
  " & Full sample residualized & Full sample preferred & Pre-2023Q1 residualized & Pre-2023Q1 preferred \\\\",
  "\\midrule",
  paste0("Baseline wood share 2016 & ", fmt_coef(diag1_nc[["estimate"]], diag1_nc[["p.value"]]), " & ", fmt_coef(diag1_pc[["estimate"]], diag1_pc[["p.value"]]), " & ", fmt_coef(diag1_pre[["estimate"]], diag1_pre[["p.value"]]), " & ", fmt_coef(diag1_pc_pre[["estimate"]], diag1_pc_pre[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(diag1_nc[["std.error"]]), " & ", fmt_se(diag1_pc[["std.error"]]), " & ", fmt_se(diag1_pre[["std.error"]]), " & ", fmt_se(diag1_pc_pre[["std.error"]]), " \\\\"),
  paste0("Observations & ", format(nobs(diag_option1_nocontrols), big.mark = ",", scientific = FALSE), " & ", format(nobs(diag_option1_preferred), big.mark = ",", scientific = FALSE), " & ", format(nobs(diag_option1_pre2023), big.mark = ",", scientific = FALSE), " & ", format(nobs(diag_option1_preferred_pre2023), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "UF-Price Diagnostic",
    "tab:uf_price_diagnostic",
    diag_body,
    "Columns 1 and 3 residualize log UF-quarter LPG price on year-quarter fixed effects only and regress the residual on baseline 2016 wood share. Columns 2 and 4 regress log UF-quarter LPG price on baseline 2016 wood share with the preferred aggregate controls and year-quarter fixed effects. Pre-2023Q1 is the pre-harmonization sample used for the revised main UF-price design. Standard errors are clustered by estrato4."
  ),
  here::here(table_dir, "uf_price_diagnostic.tex")
)

main_diag_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & Residualized UF price & Preferred controls + year-quarter FE \\\\",
  "\\midrule",
  paste0("Baseline wood share 2016 & ", fmt_coef(diag1_nc[["estimate"]], diag1_nc[["p.value"]]), " & ", fmt_coef(diag1_pc[["estimate"]], diag1_pc[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(diag1_nc[["std.error"]]), " & ", fmt_se(diag1_pc[["std.error"]]), " \\\\"),
  paste0("Observations & ", format(nobs(diag_option1_nocontrols), big.mark = ",", scientific = FALSE), " & ", format(nobs(diag_option1_preferred), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "UF Prices and Initial Wood Share",
    "tab:uf_price_no_correlation",
    main_diag_body,
    "Column 1 residualizes log UF-quarter LPG price on year-quarter fixed effects only and regresses the residual on baseline 2016 wood share. Column 2 regresses log UF-quarter LPG price on baseline 2016 wood share with the preferred aggregate controls and year-quarter fixed effects. Standard errors are clustered by estrato4. The preferred column is the key diagnostic for whether residual UF price variation is mechanically aligned with initial wood dependence."
  ),
  here::here(table_dir, "uf_price_no_correlation.tex")
)

run_design <- function(instrument_var, tag) {
  rows <- list()
  ix <- 1L
  for (i in seq_len(nrow(specs))) {
    spec <- specs$spec[[i]]
    controls <- specs$controls[[i]]
    dat_emp <- panel %>% filter(if_all(all_of(needed_for("female_hours_effective_employed", instrument_var, controls)), ~ !is.na(.x)))
    dat_pop <- panel %>% filter(if_all(all_of(needed_for("female_hours_effective_population", instrument_var, controls)), ~ !is.na(.x)))
    fs_emp <- run_fs(dat_emp, instrument_var, controls)
    rf_emp <- run_rf(dat_emp, "female_hours_effective_employed", instrument_var, controls)
    iv_emp <- run_iv(dat_emp, "female_hours_effective_employed", instrument_var, controls)
    rf_pop <- run_rf(dat_pop, "female_hours_effective_population", instrument_var, controls)
    iv_pop <- run_iv(dat_pop, "female_hours_effective_population", instrument_var, controls)
    fs_term <- term_row(fs_emp, instrument_var)
    rf_emp_term <- term_row(rf_emp, instrument_var)
    iv_emp_term <- term_row(iv_emp, "fit_share_cooking_wood_charcoal")
    rf_pop_term <- term_row(rf_pop, instrument_var)
    iv_pop_term <- term_row(iv_pop, "fit_share_cooking_wood_charcoal")
    rows[[ix]] <- tibble(
      spec = spec,
      nobs_emp = nobs(iv_emp),
      nobs_pop = nobs(iv_pop),
      fs_coef = fs_term[["estimate"]],
      fs_se = fs_term[["std.error"]],
      fs_p = fs_term[["p.value"]],
      fs_f = wald_f(fs_emp, instrument_var),
      rf_emp_coef = rf_emp_term[["estimate"]],
      rf_emp_se = rf_emp_term[["std.error"]],
      rf_emp_p = rf_emp_term[["p.value"]],
      iv_emp_coef = iv_emp_term[["estimate"]],
      iv_emp_se = iv_emp_term[["std.error"]],
      iv_emp_p = iv_emp_term[["p.value"]],
      rf_pop_coef = rf_pop_term[["estimate"]],
      rf_pop_se = rf_pop_term[["std.error"]],
      rf_pop_p = rf_pop_term[["p.value"]],
      iv_pop_coef = iv_pop_term[["estimate"]],
      iv_pop_se = iv_pop_term[["std.error"]],
      iv_pop_p = iv_pop_term[["p.value"]]
    )
    ix <- ix + 1L
  }
  out <- bind_rows(rows)
  write.table(out, here::here(out_dir, paste0(tag, "_results.tsv")), sep = "\t", row.names = FALSE, quote = FALSE)
  out
}

option1 <- run_design("z_wood_lpg_uf", "option1_uf_price")
option1_dm <- run_design("z_wood_lpg_uf_dm", "option1_uf_price_demeaned")
panel <- panel_pre2023q1
option1_pre2023 <- run_design("z_wood_lpg_uf", "option1_uf_price_pre2023q1")
panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(lpg_uf, by = c("uf_sigla", "year", "quarter", "year_quarter")) %>%
  left_join(pre_cap_uf, by = "uf_sigla") %>%
  mutate(
    b_dep_t = baseline_child_dependency * (year - 2016),
    z_wood_lpg_uf = baseline_wood_2016 * log_lpg_price_quarterly_uf,
    post_icms_cap = as.integer(year > 2022 | (year == 2022 & quarter >= 3)),
    z_wood_icmscap_proxy = baseline_wood_2016 * pre_cap_log_lpg_price_uf * post_icms_cap
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0)
option2 <- run_design("z_wood_icmscap_proxy", "option2_icmscap_proxy")

make_design_tex <- function(tbl, caption, label, note) {
  body <- c(
    "\\begin{tabular}{lccc}",
    "\\toprule",
    " & M1 & M2 & M3 \\\\",
    "\\midrule",
    "\\multicolumn{4}{l}{\\emph{First stage: wood/charcoal cooking share}} \\\\",
    paste0("Instrument & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_coef(tbl$fs_coef[[i]], tbl$fs_p[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0(" & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_se(tbl$fs_se[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0("Robust first-stage F & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_num(tbl$fs_f[[i]], 2), character(1)), collapse = " & "), " \\\\"),
    "\\addlinespace",
    "\\multicolumn{4}{l}{\\emph{Reduced form: female effective hours (employed)}} \\\\",
    paste0("Instrument & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_coef(tbl$rf_emp_coef[[i]], tbl$rf_emp_p[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0(" & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_se(tbl$rf_emp_se[[i]]), character(1)), collapse = " & "), " \\\\"),
    "\\addlinespace",
    "\\multicolumn{4}{l}{\\emph{2SLS: female effective hours (employed)}} \\\\",
    paste0("Predicted wood share & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_coef(tbl$iv_emp_coef[[i]], tbl$iv_emp_p[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0(" & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_se(tbl$iv_emp_se[[i]]), character(1)), collapse = " & "), " \\\\"),
    "\\addlinespace",
    "\\multicolumn{4}{l}{\\emph{2SLS: female effective hours (population)}} \\\\",
    paste0("Predicted wood share & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_coef(tbl$iv_pop_coef[[i]], tbl$iv_pop_p[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0(" & ", paste(vapply(seq_len(nrow(tbl)), function(i) fmt_se(tbl$iv_pop_se[[i]]), character(1)), collapse = " & "), " \\\\"),
    "\\midrule",
    paste0("Observations (employed) & ", paste(vapply(seq_len(nrow(tbl)), function(i) format(tbl$nobs_emp[[i]], big.mark = ",", scientific = FALSE), character(1)), collapse = " & "), " \\\\"),
    paste0("Observations (population) & ", paste(vapply(seq_len(nrow(tbl)), function(i) format(tbl$nobs_pop[[i]], big.mark = ",", scientific = FALSE), character(1)), collapse = " & "), " \\\\"),
    "\\bottomrule",
    "\\end{tabular}"
  )
  writeLines(tex_wrap(caption, label, body, note), here::here(table_dir, paste0(label, ".tex")))
}

make_design_tex(
  option1_pre2023,
  "Option 1: UF-Quarter LPG Price Shift (Pre-2023Q1)",
  "option1_uf_price_shift_pre2023q1",
  "Instrument is baseline estrato wood share in 2016 multiplied by log UF-quarter LPG price. All models use estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4. The sample is restricted through 2023Q1, before the post-2023 national harmonization of GLP ICMS treatment becomes the dominant institutional setting. M2 adds mean rooms, mean bedrooms, baseline child dependency interacted with a linear time trend, and the estrato-quarter share of households with full-time grid electricity. M3 adds UF labor-market and education-composition controls."
)

make_design_tex(
  option1,
  "Option 1: UF-Quarter LPG Price Shift",
  "option1_uf_price_shift",
  "Instrument is baseline estrato wood share in 2016 multiplied by log UF-quarter LPG price. All models use estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4. M2 adds mean rooms, mean bedrooms, baseline child dependency interacted with a linear time trend, and the estrato-quarter share of households with full-time grid electricity. M3 adds UF labor-market and education-composition controls."
)

make_design_tex(
  option1_dm,
  "Option 1 Robustness: Demeaned UF Relative LPG Price Shift",
  "option1_uf_price_demeaned",
  "Instrument is baseline estrato wood share in 2016 multiplied by the UF's log LPG price relative to the national level, demeaned within UF over the full sample. This removes each state's persistent average relative price premium and uses only within-UF deviations from its normal relative price. All models use estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4."
)

main_option1_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & (1) FE Only & (2) Preferred \\\\",
  "\\midrule",
  "\\multicolumn{3}{l}{\\emph{Panel A: First stage --- dependent variable: wood/charcoal cooking share}} \\\\",
  "\\addlinespace",
  paste0("Baseline wood share $\\\\times$ log UF-quarter LPG price & ", fmt_coef(option1$fs_coef[[1]], option1$fs_p[[1]]), " & ", fmt_coef(option1$fs_coef[[2]], option1$fs_p[[2]]), " \\\\"),
  paste0(" & ", fmt_se(option1$fs_se[[1]]), " & ", fmt_se(option1$fs_se[[2]]), " \\\\"),
  "\\addlinespace",
  "Mean rooms &  & Yes \\\\",
  "Mean bedrooms &  & Yes \\\\",
  "Child dependency $\\\\times$ trend &  & Yes \\\\",
  "Full-time grid share &  & Yes \\\\",
  "\\addlinespace",
  paste0("Robust first-stage $F$ & ", fmt_num(option1$fs_f[[1]], 2), " & ", fmt_num(option1$fs_f[[2]], 2), " \\\\"),
  "\\midrule",
  "\\multicolumn{3}{l}{\\emph{Panel B: Reduced form --- dependent variable: female effective weekly hours (employed)}} \\\\",
  "\\addlinespace",
  paste0("Baseline wood share $\\\\times$ log UF-quarter LPG price & ", fmt_coef(option1$rf_emp_coef[[1]], option1$rf_emp_p[[1]]), " & ", fmt_coef(option1$rf_emp_coef[[2]], option1$rf_emp_p[[2]]), " \\\\"),
  paste0(" & ", fmt_se(option1$rf_emp_se[[1]]), " & ", fmt_se(option1$rf_emp_se[[2]]), " \\\\"),
  "\\midrule",
  "\\multicolumn{3}{l}{\\emph{Panel C: 2SLS --- dependent variable: female effective weekly hours (employed)}} \\\\",
  "\\addlinespace",
  paste0("Predicted wood/charcoal share & ", fmt_coef(option1$iv_emp_coef[[1]], option1$iv_emp_p[[1]]), " & ", fmt_coef(option1$iv_emp_coef[[2]], option1$iv_emp_p[[2]]), " \\\\"),
  paste0(" & ", fmt_se(option1$iv_emp_se[[1]]), " & ", fmt_se(option1$iv_emp_se[[2]]), " \\\\"),
  "\\midrule",
  paste0("Observations & ", format(option1$nobs_emp[[1]], big.mark = ",", scientific = FALSE), " & ", format(option1$nobs_emp[[2]], big.mark = ",", scientific = FALSE), " \\\\"),
  "Estrato FE & Yes & Yes \\\\",
  "Year-quarter FE & Yes & Yes \\\\",
  "Weights & Yes & Yes \\\\",
  "Clustered by estrato4 & Yes & Yes \\\\",
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Main UF-Quarter LPG IV",
    "tab:main_uf_price_iv",
    main_option1_body,
    "Notes: The instrument is baseline estrato wood/charcoal cooking share in 2016 multiplied by log UF-quarter LPG price from ANP retail price surveys. UF-quarter LPG prices vary across states over time due to state-level tax and administrative differences, together with differential pass-through into observed retail prices, and are collected independently of PNAD-C household labor outcomes. The preferred column adds mean rooms, mean bedrooms, baseline child dependency ratio interacted with a linear time trend, and the estrato-quarter share of households with full-time grid electricity. The aggregate reduced-form coefficient in Panel B differs from the analogous micro reduced-form coefficient because the unit of observation is the estrato-quarter rather than the individual woman-quarter, so the two specifications weight information differently. Robust first-stage $F$ is the cluster-robust excluded-instrument Wald statistic. Standard errors clustered by estrato4 in parentheses. Significance: $^{***}~p<0.01$, $^{**}~p<0.05$, $^{*}~p<0.1$."
  ),
  here::here(table_dir, "main_uf_price_iv.tex")
)

make_design_tex(
  option2,
  "Option 2: 2022 ICMS-Cap Proxy Shock",
  "option2_icms_cap_proxy",
  "Instrument is baseline estrato wood share in 2016 multiplied by a post-2022Q3 indicator and the UF's pre-cap average log LPG price, where the pre-cap average is computed over 2016Q1--2022Q2. This is a proxy design motivated by differential state exposure to the 2022 ICMS cap in the absence of a direct quarterly ICMS-rate panel on disk. All models use estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4."
)

cat("Wrote:\n")
cat(here::here(table_dir, "uf_price_diagnostic.tex"), "\n")
cat(here::here(table_dir, "uf_price_no_correlation.tex"), "\n")
cat(here::here(table_dir, "option1_uf_price_shift_pre2023q1.tex"), "\n")
cat(here::here(table_dir, "option1_uf_price_shift.tex"), "\n")
cat(here::here(table_dir, "option1_uf_price_demeaned.tex"), "\n")
cat(here::here(table_dir, "main_uf_price_iv.tex"), "\n")
cat(here::here(table_dir, "option2_icms_cap_proxy.tex"), "\n")
