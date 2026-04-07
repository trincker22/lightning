#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

main_dir <- here::here("data", "paper_tables", "main")
appendix_dir <- here::here("data", "paper_tables", "appendix")
dir.create(main_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(appendix_dir, recursive = TRUE, showWarnings = FALSE)

table1_out <- here::here("data", "paper_tables", "main", "table1_first_stage.tex")
table2_out <- here::here("data", "paper_tables", "main", "table2_main_2sls.tex")
table3_out <- here::here("data", "paper_tables", "main", "table3_reduced_form_labor_supply_margins.tex")
table4_out <- here::here("data", "paper_tables", "main", "table4_lightning_iv.tex")
appendix_a_out <- here::here("data", "paper_tables", "appendix", "appendixA_m3_robustness.tex")
appendix_b_out <- here::here("data", "paper_tables", "appendix", "appendixB_male_placebo.tex")
appendix_c_out <- here::here("data", "paper_tables", "appendix", "appendixC_heterogeneity_initial_wood.tex")
appendix_d_out <- here::here("data", "paper_tables", "appendix", "appendixD_excluding_2021_2022_price_spike.tex")
appendix_e_out <- here::here("data", "paper_tables", "appendix", "appendixE_ols_comparison.tex")

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")
linked_micro_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")
lightning_table_path <- here::here("data", "powerIV", "regressions", "lightning_iv_refresh", "tables", "lightning_iv_key_results.tex")

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

run_fs <- function(data, controls) {
  feols(
    as.formula(paste0("share_cooking_wood_charcoal ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_rf <- function(data, outcome, controls) {
  feols(
    as.formula(paste0(outcome, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_iv <- function(data, outcome, controls) {
  feols(
    as.formula(paste0(outcome, " ~ ", controls, " | estrato4 + year_quarter | share_cooking_wood_charcoal ~ z_wood_lpg_estrato")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

run_ols <- function(data, outcome, controls) {
  feols(
    as.formula(paste0(outcome, " ~ share_cooking_wood_charcoal + ", controls, " | estrato4 + year_quarter")),
    data = data,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
}

needed_for <- function(outcome, controls) {
  unique(c(
    "estrato4", "year_quarter", "weight_sum", "share_cooking_wood_charcoal",
    "z_wood_lpg_estrato", outcome, all.vars(as.formula(paste("~", controls)))
  ))
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
    female_hours_effective_population,
    male_hours_effective_employed
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

lpg <- read_parquet(lpg_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    log_lpg_price_quarterly_estrato
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

agg_panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  left_join(lpg, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  left_join(baseline_wood, by = "estrato4") %>%
  mutate(
    trend = year - 2016,
    b_dep_t = baseline_child_dependency * trend,
    z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

preferred_controls <- "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
m3_controls <- paste0(preferred_controls, " + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary")

dat_t1_c1 <- agg_panel %>% filter(if_all(all_of(needed_for("share_cooking_wood_charcoal", "1")), ~ !is.na(.x)))
dat_t1_c2 <- agg_panel %>% filter(if_all(all_of(needed_for("share_cooking_wood_charcoal", preferred_controls)), ~ !is.na(.x)))
fs_t1_c1 <- run_fs(dat_t1_c1, "1")
fs_t1_c2 <- run_fs(dat_t1_c2, preferred_controls)
fs1 <- term_row(fs_t1_c1, "z_wood_lpg_estrato")
fs2 <- term_row(fs_t1_c2, "z_wood_lpg_estrato")

table1_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & FE only & Preferred \\\\",
  "\\midrule",
  paste0("Instrument coefficient & ", fmt_coef(fs1[["estimate"]], fs1[["p.value"]]), " & ", fmt_coef(fs2[["estimate"]], fs2[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(fs1[["std.error"]]), " & ", fmt_se(fs2[["std.error"]]), " \\\\"),
  paste0("Robust first-stage F & ", fmt_num(wald_f(fs_t1_c1, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(fs_t1_c2, "z_wood_lpg_estrato"), 2), " \\\\"),
  paste0("Observations & ", format(nobs(fs_t1_c1), big.mark = ",", scientific = FALSE), " & ", format(nobs(fs_t1_c2), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Table 1: First Stage",
    "tab:table1_first_stage",
    table1_body,
    "Dependent variable is estrato-quarter wood/charcoal cooking share. The instrument is baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price. Both columns include estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4. The preferred column adds mean rooms, mean bedrooms, baseline child dependency interacted with a linear time trend, and the estrato-quarter share of households with full-time grid electricity."
  ),
  table1_out
)

dat_t2_c1 <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_employed", "1")), ~ !is.na(.x)))
dat_t2_c2 <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_employed", preferred_controls)), ~ !is.na(.x)))
dat_t2_c3 <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_population", preferred_controls)), ~ !is.na(.x)))

iv_t2_c1 <- run_iv(dat_t2_c1, "female_hours_effective_employed", "1")
iv_t2_c2 <- run_iv(dat_t2_c2, "female_hours_effective_employed", preferred_controls)
iv_t2_c3 <- run_iv(dat_t2_c3, "female_hours_effective_population", preferred_controls)
fs_t2_c1 <- run_fs(dat_t2_c1, "1")
fs_t2_c2 <- run_fs(dat_t2_c2, preferred_controls)
fs_t2_c3 <- run_fs(dat_t2_c3, preferred_controls)

iv1 <- term_row(iv_t2_c1, "fit_share_cooking_wood_charcoal")
iv2 <- term_row(iv_t2_c2, "fit_share_cooking_wood_charcoal")
iv3 <- term_row(iv_t2_c3, "fit_share_cooking_wood_charcoal")

table2_body <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & Employed hours: FE only & Employed hours: preferred & Population hours: preferred \\\\",
  "\\midrule",
  paste0("Predicted wood/charcoal share & ", fmt_coef(iv1[["estimate"]], iv1[["p.value"]]), " & ", fmt_coef(iv2[["estimate"]], iv2[["p.value"]]), " & ", fmt_coef(iv3[["estimate"]], iv3[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(iv1[["std.error"]]), " & ", fmt_se(iv2[["std.error"]]), " & ", fmt_se(iv3[["std.error"]]), " \\\\"),
  paste0("First-stage F & ", fmt_num(wald_f(fs_t2_c1, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(fs_t2_c2, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(fs_t2_c3, "z_wood_lpg_estrato"), 2), " \\\\"),
  paste0("Observations & ", format(nobs(iv_t2_c1), big.mark = ",", scientific = FALSE), " & ", format(nobs(iv_t2_c2), big.mark = ",", scientific = FALSE), " & ", format(nobs(iv_t2_c3), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Table 2: 2SLS Main Result",
    "tab:table2_main_2sls",
    table2_body,
    "Unit of observation is estrato-quarter. The endogenous regressor is estrato-quarter wood/charcoal cooking share. The instrument is baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price, so identification comes from differential exposure of initially wood-reliant strata to local LPG price movements over time. All columns include estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4. The preferred columns add mean rooms, mean bedrooms, baseline child dependency interacted with a linear time trend, and the estrato-quarter share of households with full-time grid electricity."
  ),
  table2_out
)

micro <- read_parquet(
  linked_micro_path,
  col_select = c(
    "year", "quarter", "year_quarter", "uf", "estrato4", "person_weight", "age",
    "female_secondaryplus", "female_tertiary", "hh_size", "n_children_0_14",
    "visit1_match_any", "visit1_n_rooms", "visit1_n_bedrooms",
    "wanted_to_work", "wants_more_hours", "available_more_hours", "worked_last_12m",
    "effective_hours"
  )
) %>%
  mutate(
    estrato4 = as.character(estrato4),
    age_sq = age^2
  )

micro_reg <- micro %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(lpg, by = c("estrato4", "year", "quarter")) %>%
  mutate(z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato)

micro_controls <- "1 + age + age_sq + female_secondaryplus + female_tertiary + hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms"
micro_outcomes <- c(
  "effective_hours" = "Effective weekly hours",
  "wanted_to_work" = "Wanted to work",
  "wants_more_hours" = "Wanted more hours",
  "available_more_hours" = "Available for more hours",
  "worked_last_12m" = "Worked in last 12 months"
)

micro_models <- list()
for (outcome_name in names(micro_outcomes)) {
  needed <- unique(c("estrato4", "year_quarter", "person_weight", "visit1_match_any", "z_wood_lpg_estrato", outcome_name, all.vars(as.formula(paste("~", micro_controls)))))
  dat <- micro_reg %>% filter(visit1_match_any, person_weight > 0, if_all(all_of(needed), ~ !is.na(.x)))
  micro_models[[outcome_name]] <- feols(
    as.formula(paste0(outcome_name, " ~ z_wood_lpg_estrato + ", micro_controls, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~person_weight,
    cluster = ~estrato4
  )
}

table3_body <- c(
  "\\begin{tabular}{lccccc}",
  "\\toprule",
  " & Effective weekly hours & Wanted to work & Wanted more hours & Available for more hours & Worked in last 12 months \\\\",
  "\\midrule",
  paste0(
    "Instrument coefficient & ",
    paste(vapply(names(micro_outcomes), function(x) {
      tr <- term_row(micro_models[[x]], "z_wood_lpg_estrato")
      fmt_coef(tr[["estimate"]], tr[["p.value"]])
    }, character(1)), collapse = " & "),
    " \\\\"
  ),
  paste0(
    " & ",
    paste(vapply(names(micro_outcomes), function(x) {
      tr <- term_row(micro_models[[x]], "z_wood_lpg_estrato")
      fmt_se(tr[["std.error"]])
    }, character(1)), collapse = " & "),
    " \\\\"
  ),
  paste0(
    "Observations & ",
    paste(vapply(names(micro_outcomes), function(x) format(nobs(micro_models[[x]]), big.mark = ",", scientific = FALSE), character(1)), collapse = " & "),
    " \\\\"
  ),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Table 3: Reduced Form Across Labor Supply Margins",
    "tab:table3_micro_reduced_form",
    table3_body,
    "Unit of observation is woman-quarter. All columns use the same M2 controls: age, age squared, secondary-plus education, tertiary education, household size, number of children aged 0--14, linked Visit 1 rooms, and linked Visit 1 bedrooms. All models include estrato4 and year-quarter fixed effects, survey weights, and standard errors clustered by estrato4. A positive coefficient on effective weekly hours means higher LPG exposure is associated with more realized work hours, while negative coefficients on wanted-to-work and wanted-more-hours indicate lower unmet labor supply on those margins."
  ),
  table3_out
)

lightning_lines <- readLines(lightning_table_path)
lightning_lines <- gsub("Lightning IV key results in readable units", "Table 4: Lightning IV", lightning_lines, fixed = TRUE)
lightning_lines <- gsub("tab:lightning_iv_key_results", "tab:table4_lightning_iv", lightning_lines, fixed = TRUE)
writeLines(lightning_lines, table4_out)

dat_a_fs <- agg_panel %>% filter(if_all(all_of(needed_for("share_cooking_wood_charcoal", m3_controls)), ~ !is.na(.x)))
dat_a_emp <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_employed", m3_controls)), ~ !is.na(.x)))
dat_a_pop <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_population", m3_controls)), ~ !is.na(.x)))
fs_a <- run_fs(dat_a_fs, m3_controls)
iv_a_emp <- run_iv(dat_a_emp, "female_hours_effective_employed", m3_controls)
iv_a_pop <- run_iv(dat_a_pop, "female_hours_effective_population", m3_controls)
fs_a_emp <- run_fs(dat_a_emp, m3_controls)
fs_a_pop <- run_fs(dat_a_pop, m3_controls)
row_a_fs <- term_row(fs_a, "z_wood_lpg_estrato")
row_a_emp <- term_row(iv_a_emp, "fit_share_cooking_wood_charcoal")
row_a_pop <- term_row(iv_a_pop, "fit_share_cooking_wood_charcoal")

appendix_a_body <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & First stage (M3) & 2SLS employed hours (M3) & 2SLS population hours (M3) \\\\",
  "\\midrule",
  paste0("Main coefficient & ", fmt_coef(row_a_fs[["estimate"]], row_a_fs[["p.value"]]), " & ", fmt_coef(row_a_emp[["estimate"]], row_a_emp[["p.value"]]), " & ", fmt_coef(row_a_pop[["estimate"]], row_a_pop[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(row_a_fs[["std.error"]]), " & ", fmt_se(row_a_emp[["std.error"]]), " & ", fmt_se(row_a_pop[["std.error"]]), " \\\\"),
  paste0("First-stage F & ", fmt_num(wald_f(fs_a, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(fs_a_emp, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(fs_a_pop, "z_wood_lpg_estrato"), 2), " \\\\"),
  paste0("Observations & ", format(nobs(fs_a), big.mark = ",", scientific = FALSE), " & ", format(nobs(iv_a_emp), big.mark = ",", scientific = FALSE), " & ", format(nobs(iv_a_pop), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Appendix A: Robustness to UF-Level Labor Market Controls",
    "tab:appendixA_m3_robustness",
    appendix_a_body,
    "This appendix reports the full M3 versions of the main first-stage and 2SLS specifications. In addition to the preferred controls, M3 adds UF-level unemployment, labor demand, real wage, and education composition controls."
  ),
  appendix_a_out
)

male_placebo_dat <- agg_panel %>% filter(if_all(all_of(needed_for("male_hours_effective_employed", preferred_controls)), ~ !is.na(.x)))
male_placebo <- run_rf(male_placebo_dat, "male_hours_effective_employed", preferred_controls)
male_row <- term_row(male_placebo, "z_wood_lpg_estrato")

appendix_b_body <- c(
  "\\begin{tabular}{lc}",
  "\\toprule",
  " & Male effective hours \\\\",
  "\\midrule",
  paste0("Instrument coefficient & ", fmt_coef(male_row[["estimate"]], male_row[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(male_row[["std.error"]]), " \\\\"),
  paste0("Observations & ", format(nobs(male_placebo), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Appendix B: Male Placebo",
    "tab:appendixB_male_placebo",
    appendix_b_body,
    "This placebo uses the same aggregate exposure and preferred controls as the main LPG reduced-form design, with male effective hours as the outcome. A fully linked male micro placebo matching Table 3 exactly is not yet cached in the current workflow, so this appendix reports the closest available placebo already supported by the data build."
  ),
  appendix_b_out
)

median_baseline_wood <- baseline_wood %>% summarise(med = median(baseline_wood_2016, na.rm = TRUE)) %>% pull(med)

split_results <- function(high_value) {
  dat <- agg_panel %>%
    left_join(baseline_wood, by = "estrato4", suffix = c("", ".dup")) %>%
    mutate(high_wood = baseline_wood_2016 >= median_baseline_wood) %>%
    filter(high_wood == high_value) %>%
    filter(if_all(all_of(needed_for("female_hours_effective_employed", preferred_controls)), ~ !is.na(.x)))
  list(iv = run_iv(dat, "female_hours_effective_employed", preferred_controls), fs = run_fs(dat, preferred_controls))
}

low_split <- split_results(FALSE)
high_split <- split_results(TRUE)
low_row <- term_row(low_split$iv, "fit_share_cooking_wood_charcoal")
high_row <- term_row(high_split$iv, "fit_share_cooking_wood_charcoal")

appendix_c_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & Low initial wood share & High initial wood share \\\\",
  "\\midrule",
  paste0("Predicted wood/charcoal share & ", fmt_coef(low_row[["estimate"]], low_row[["p.value"]]), " & ", fmt_coef(high_row[["estimate"]], high_row[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(low_row[["std.error"]]), " & ", fmt_se(high_row[["std.error"]]), " \\\\"),
  paste0("First-stage F & ", fmt_num(wald_f(low_split$fs, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(high_split$fs, "z_wood_lpg_estrato"), 2), " \\\\"),
  paste0("Observations & ", format(nobs(low_split$iv), big.mark = ",", scientific = FALSE), " & ", format(nobs(high_split$iv), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Appendix C: Heterogeneity by Initial Wood Share",
    "tab:appendixC_heterogeneity_initial_wood",
    appendix_c_body,
    "The estrato sample is split at the median 2016 wood/charcoal cooking share. Both columns use the Table 2 preferred employed-hours specification."
  ),
  appendix_c_out
)

full_pref_dat <- agg_panel %>% filter(if_all(all_of(needed_for("female_hours_effective_employed", preferred_controls)), ~ !is.na(.x)))
spike_drop_dat <- agg_panel %>%
  filter(!(year == 2021 | year == 2022)) %>%
  filter(if_all(all_of(needed_for("female_hours_effective_employed", preferred_controls)), ~ !is.na(.x)))
full_pref_iv <- run_iv(full_pref_dat, "female_hours_effective_employed", preferred_controls)
full_pref_fs <- run_fs(full_pref_dat, preferred_controls)
spike_drop_iv <- run_iv(spike_drop_dat, "female_hours_effective_employed", preferred_controls)
spike_drop_fs <- run_fs(spike_drop_dat, preferred_controls)
full_pref_row <- term_row(full_pref_iv, "fit_share_cooking_wood_charcoal")
spike_drop_row <- term_row(spike_drop_iv, "fit_share_cooking_wood_charcoal")

appendix_d_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & Full preferred sample & Excluding 2021Q1--2022Q4 \\\\",
  "\\midrule",
  paste0("Predicted wood/charcoal share & ", fmt_coef(full_pref_row[["estimate"]], full_pref_row[["p.value"]]), " & ", fmt_coef(spike_drop_row[["estimate"]], spike_drop_row[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(full_pref_row[["std.error"]]), " & ", fmt_se(spike_drop_row[["std.error"]]), " \\\\"),
  paste0("First-stage F & ", fmt_num(wald_f(full_pref_fs, "z_wood_lpg_estrato"), 2), " & ", fmt_num(wald_f(spike_drop_fs, "z_wood_lpg_estrato"), 2), " \\\\"),
  paste0("Observations & ", format(nobs(full_pref_iv), big.mark = ",", scientific = FALSE), " & ", format(nobs(spike_drop_iv), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Appendix D: Robustness to Excluding the 2021--2022 Price Spike",
    "tab:appendixD_excluding_2021_2022_price_spike",
    appendix_d_body,
    "Both columns use the Table 2 preferred employed-hours specification. The right column drops all observations from 2021Q1 through 2022Q4."
  ),
  appendix_d_out
)

ols_emp_c1 <- run_ols(dat_t2_c1, "female_hours_effective_employed", "1")
ols_emp_c2 <- run_ols(dat_t2_c2, "female_hours_effective_employed", preferred_controls)
ols_pop_c3 <- run_ols(dat_t2_c3, "female_hours_effective_population", preferred_controls)
ols1 <- term_row(ols_emp_c1, "share_cooking_wood_charcoal")
ols2 <- term_row(ols_emp_c2, "share_cooking_wood_charcoal")
ols3 <- term_row(ols_pop_c3, "share_cooking_wood_charcoal")

appendix_e_body <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & Employed hours: FE only & Employed hours: preferred & Population hours: preferred \\\\",
  "\\midrule",
  paste0("Observed wood/charcoal share & ", fmt_coef(ols1[["estimate"]], ols1[["p.value"]]), " & ", fmt_coef(ols2[["estimate"]], ols2[["p.value"]]), " & ", fmt_coef(ols3[["estimate"]], ols3[["p.value"]]), " \\\\"),
  paste0(" & ", fmt_se(ols1[["std.error"]]), " & ", fmt_se(ols2[["std.error"]]), " & ", fmt_se(ols3[["std.error"]]), " \\\\"),
  paste0("Observations & ", format(nobs(ols_emp_c1), big.mark = ",", scientific = FALSE), " & ", format(nobs(ols_emp_c2), big.mark = ",", scientific = FALSE), " & ", format(nobs(ols_pop_c3), big.mark = ",", scientific = FALSE), " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(
  tex_wrap(
    "Appendix E: OLS Comparison",
    "tab:appendixE_ols_comparison",
    appendix_e_body,
    "This appendix reports the OLS analog of Table 2. Table 1 is already a linear first-stage regression, so the first-stage comparison is identical by construction."
  ),
  appendix_e_out
)

cat("Wrote:\\n")
cat(table1_out, "\\n")
cat(table2_out, "\\n")
cat(table3_out, "\\n")
cat(table4_out, "\\n")
cat(appendix_a_out, "\\n")
cat(appendix_b_out, "\\n")
cat(appendix_c_out, "\\n")
cat(appendix_d_out, "\\n")
cat(appendix_e_out, "\\n")
