#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(fixest)
  library(broom)
})

out_dir <- here::here("data", "powerIV", "regressions", "headline_tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
lpg_path <- here::here("data", "powerIV", "prices", "anp_lpg_quarter_estrato_2016_2024.parquet")
macro_path <- here::here("data", "powerIV", "macro", "sidra_uf_quarter_macro_2016_2024.parquet")
baseline_path <- here::here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")
linked_micro_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")

headline_iv_tex_path <- here::here("data", "powerIV", "regressions", "headline_tables", "headline_lpg_iv_main.tex")
headline_micro_tex_path <- here::here("data", "powerIV", "regressions", "headline_tables", "headline_lpg_micro_reduced_form.tex")
headline_iv_csv_path <- here::here("data", "powerIV", "regressions", "headline_tables", "headline_lpg_iv_main.csv")
headline_micro_csv_path <- here::here("data", "powerIV", "regressions", "headline_tables", "headline_lpg_micro_reduced_form.csv")

canonical_iv_tex_path <- here::here("data", "powerIV", "regressions", "headline_tables", "canonical_lpg_iv_main.tex")
canonical_micro_tex_path <- here::here("data", "powerIV", "regressions", "headline_tables", "canonical_lpg_micro_reduced_form.tex")
canonical_iv_csv_path <- here::here("data", "powerIV", "regressions", "headline_tables", "canonical_lpg_iv_main.csv")
canonical_micro_csv_path <- here::here("data", "powerIV", "regressions", "headline_tables", "canonical_lpg_micro_reduced_form.csv")

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

extract_wald_f <- function(model, term) {
  out <- tryCatch(wald(model, term), error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  stat <- suppressWarnings(as.numeric(unlist(out$stat)))
  stat <- stat[!is.na(stat)]
  if (length(stat) == 0) return(NA_real_)
  stat[[1]]
}

term_lookup <- function(model, term) {
  tbl <- tidy(model)
  row <- tbl[tbl$term == term, , drop = FALSE]
  if (nrow(row) == 0) {
    return(c(coef = NA_real_, se = NA_real_, p = NA_real_))
  }
  c(coef = row$estimate[[1]], se = row$std.error[[1]], p = row$p.value[[1]])
}

make_panel_lines <- function(title, models, terms, labels, footer_rows = list()) {
  br <- "\\\\"
  lines <- c(
    paste0("\\multicolumn{4}{l}{\\emph{", title, "}} ", br),
    "\\midrule"
  )

  for (term in terms) {
    vals <- lapply(models, term_lookup, term = term)
    coefs <- vapply(vals, function(x) fmt_coef(x[["coef"]], x[["p"]]), character(1))
    ses <- vapply(vals, function(x) fmt_se(x[["se"]]), character(1))
    if (all(coefs == "")) next
    lines <- c(
      lines,
      paste0(labels[[term]], " & ", paste(coefs, collapse = " & "), " ", br),
      paste0(" & ", paste(ses, collapse = " & "), " ", br)
    )
  }

  if (length(footer_rows) > 0) {
    lines <- c(lines, "\\midrule")
    for (row_name in names(footer_rows)) {
      lines <- c(lines, paste0(row_name, " & ", paste(footer_rows[[row_name]], collapse = " & "), " ", br))
    }
  }

  lines
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
    mean_bedrooms
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

agg_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "mean_rooms + mean_bedrooms + b_dep_t",
  "M3", "mean_rooms + mean_bedrooms + b_dep_t + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

agg_fs_models <- list()
agg_rf_models <- list()
agg_iv_models <- list()
agg_rows <- list()

for (i in seq_len(nrow(agg_specs))) {
  spec <- agg_specs$spec[[i]]
  controls <- agg_specs$controls[[i]]
  needed_controls <- all.vars(as.formula(paste("~", controls)))
  needed <- unique(c(
    "estrato4", "year_quarter", "weight_sum", "female_hours_effective_employed",
    "share_cooking_wood_charcoal", "z_wood_lpg_estrato", needed_controls
  ))
  dat <- agg_panel %>% filter(if_all(all_of(needed), ~ !is.na(.x)))

  fs_model <- feols(
    as.formula(paste0("share_cooking_wood_charcoal ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  rf_model <- feols(
    as.formula(paste0("female_hours_effective_employed ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )
  iv_model <- feols(
    as.formula(paste0("female_hours_effective_employed ~ ", controls, " | estrato4 + year_quarter | share_cooking_wood_charcoal ~ z_wood_lpg_estrato")),
    data = dat,
    weights = ~weight_sum,
    cluster = ~estrato4
  )

  agg_fs_models[[spec]] <- fs_model
  agg_rf_models[[spec]] <- rf_model
  agg_iv_models[[spec]] <- iv_model

  agg_rows[[length(agg_rows) + 1L]] <- tidy(fs_model) %>% mutate(panel = "first_stage", spec = spec, nobs = nobs(fs_model))
  agg_rows[[length(agg_rows) + 1L]] <- tidy(rf_model) %>% mutate(panel = "reduced_form", spec = spec, nobs = nobs(rf_model))
  agg_rows[[length(agg_rows) + 1L]] <- tidy(iv_model) %>% mutate(panel = "iv", spec = spec, nobs = nobs(iv_model))
}

agg_results <- bind_rows(agg_rows)
write_csv(agg_results, canonical_iv_csv_path)

headline_iv_tbl <- tibble::tibble(
  spec = names(agg_fs_models),
  nobs = vapply(agg_iv_models, nobs, numeric(1)),
  fs_coef = vapply(agg_fs_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["coef"]], numeric(1)),
  fs_se = vapply(agg_fs_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["se"]], numeric(1)),
  fs_p = vapply(agg_fs_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["p"]], numeric(1)),
  fs_f = vapply(agg_fs_models, extract_wald_f, numeric(1), term = "z_wood_lpg_estrato"),
  rf_coef = vapply(agg_rf_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["coef"]], numeric(1)),
  rf_se = vapply(agg_rf_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["se"]], numeric(1)),
  rf_p = vapply(agg_rf_models, function(m) term_lookup(m, "z_wood_lpg_estrato")[["p"]], numeric(1)),
  iv_coef = vapply(agg_iv_models, function(m) term_lookup(m, "fit_share_cooking_wood_charcoal")[["coef"]], numeric(1)),
  iv_se = vapply(agg_iv_models, function(m) term_lookup(m, "fit_share_cooking_wood_charcoal")[["se"]], numeric(1)),
  iv_p = vapply(agg_iv_models, function(m) term_lookup(m, "fit_share_cooking_wood_charcoal")[["p"]], numeric(1))
)
write_csv(headline_iv_tbl, headline_iv_csv_path)

headline_iv_lines <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & M1 & M2 & M3 \\\\",
  "\\midrule",
  "\\multicolumn{4}{l}{\\emph{First Stage: Wood/charcoal cooking share}} \\\\",
  paste0("Instrument coef. & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_coef(headline_iv_tbl$fs_coef[[i]], headline_iv_tbl$fs_p[[i]]), character(1)), collapse = " & "), " \\\\"),
  paste0(" & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_se(headline_iv_tbl$fs_se[[i]]), character(1)), collapse = " & "), " \\\\"),
  paste0("Robust first-stage F & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_num(headline_iv_tbl$fs_f[[i]], 2), character(1)), collapse = " & "), " \\\\"),
  "\\addlinespace",
  "\\multicolumn{4}{l}{\\emph{Reduced Form: Female effective hours (employed)}} \\\\",
  paste0("Exposure coef. & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_coef(headline_iv_tbl$rf_coef[[i]], headline_iv_tbl$rf_p[[i]]), character(1)), collapse = " & "), " \\\\"),
  paste0(" & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_se(headline_iv_tbl$rf_se[[i]]), character(1)), collapse = " & "), " \\\\"),
  "\\addlinespace",
  "\\multicolumn{4}{l}{\\emph{2SLS: Female effective hours (employed)}} \\\\",
  paste0("Predicted wood/charcoal share & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_coef(headline_iv_tbl$iv_coef[[i]], headline_iv_tbl$iv_p[[i]]), character(1)), collapse = " & "), " \\\\"),
  paste0(" & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) fmt_se(headline_iv_tbl$iv_se[[i]]), character(1)), collapse = " & "), " \\\\"),
  "\\midrule",
  paste0("Observations & ", paste(vapply(seq_len(nrow(headline_iv_tbl)), function(i) format(headline_iv_tbl$nobs[[i]], big.mark = ",", scientific = FALSE), character(1)), collapse = " & "), " \\\\"),
  "Estrato FE & Yes & Yes & Yes \\\\",
  "Year-quarter FE & Yes & Yes & Yes \\\\",
  "Weights & Yes & Yes & Yes \\\\",
  "Clustered by estrato4 & Yes & Yes & Yes \\\\",
  "\\bottomrule",
  "\\multicolumn{4}{p{0.92\\linewidth}}{\\footnotesize Notes: Instrument is baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price. M1 includes fixed effects only. M2 adds mean rooms, mean bedrooms, and baseline child dependency interacted with a linear time trend. M3 adds UF-level unemployment, labor demand, real wage, and education composition controls. Robust first-stage F is the cluster-robust excluded-instrument Wald statistic. Significance: *** p<0.01, ** p<0.05, * p<0.1.} \\\\",
  "\\end{tabular}"
)
writeLines(headline_iv_lines, headline_iv_tex_path)

agg_labels <- c(
  "z_wood_lpg_estrato" = "Baseline wood share \\(\\times\\) log LPG price",
  "fit_share_cooking_wood_charcoal" = "Predicted wood/charcoal share",
  "mean_rooms" = "Mean rooms",
  "mean_bedrooms" = "Mean bedrooms",
  "b_dep_t" = "Baseline child dependency \\(\\times\\) trend",
  "uf_unemployment_rate" = "UF unemployment rate",
  "uf_labor_demand_proxy" = "UF labor demand proxy",
  "uf_real_wage_proxy" = "UF real wage proxy",
  "uf_share_secondaryplus" = "UF share secondary-plus",
  "uf_share_tertiary" = "UF share tertiary"
)

agg_common_terms <- c(
  "mean_rooms",
  "mean_bedrooms",
  "b_dep_t",
  "uf_unemployment_rate",
  "uf_labor_demand_proxy",
  "uf_real_wage_proxy",
  "uf_share_secondaryplus",
  "uf_share_tertiary"
)

canonical_iv_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Canonical LPG IV Results}",
  "\\small",
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & M1 & M2 & M3 \\\\"
)

canonical_iv_lines <- c(
  canonical_iv_lines,
  make_panel_lines(
    title = "Panel A: First stage (dependent variable: wood/charcoal cooking share)",
    models = agg_fs_models,
    terms = c("z_wood_lpg_estrato", agg_common_terms),
    labels = agg_labels,
    footer_rows = list(
      "Observations" = vapply(agg_fs_models, function(m) format(nobs(m), big.mark = ",", scientific = FALSE), character(1)),
      "Robust first-stage F" = vapply(agg_fs_models, function(m) fmt_num(extract_wald_f(m, "z_wood_lpg_estrato"), 2), character(1)),
      "Estrato FE" = rep("Yes", 3),
      "Year-quarter FE" = rep("Yes", 3),
      "Weights" = rep("Yes", 3),
      "Clustered by estrato4" = rep("Yes", 3)
    )
  ),
  "\\addlinespace",
  make_panel_lines(
    title = "Panel B: Reduced form (dependent variable: female effective hours, employed)",
    models = agg_rf_models,
    terms = c("z_wood_lpg_estrato", agg_common_terms),
    labels = agg_labels,
    footer_rows = list(
      "Observations" = vapply(agg_rf_models, function(m) format(nobs(m), big.mark = ",", scientific = FALSE), character(1)),
      "Estrato FE" = rep("Yes", 3),
      "Year-quarter FE" = rep("Yes", 3),
      "Weights" = rep("Yes", 3),
      "Clustered by estrato4" = rep("Yes", 3)
    )
  ),
  "\\addlinespace",
  make_panel_lines(
    title = "Panel C: 2SLS (dependent variable: female effective hours, employed)",
    models = agg_iv_models,
    terms = c("fit_share_cooking_wood_charcoal", agg_common_terms),
    labels = agg_labels,
    footer_rows = list(
      "Observations" = vapply(agg_iv_models, function(m) format(nobs(m), big.mark = ",", scientific = FALSE), character(1)),
      "Estrato FE" = rep("Yes", 3),
      "Year-quarter FE" = rep("Yes", 3),
      "Weights" = rep("Yes", 3),
      "Clustered by estrato4" = rep("Yes", 3)
    )
  ),
  "\\bottomrule",
  "\\multicolumn{4}{p{0.92\\linewidth}}{\\footnotesize Notes: This canonical table reports every estimated coefficient used in the headline aggregate IV models. Instrument is baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price. M1 includes fixed effects only. M2 adds mean rooms, mean bedrooms, and baseline child dependency interacted with a linear time trend. M3 adds UF-level unemployment, labor demand, real wage, and education composition controls. Standard errors clustered by estrato4 in parentheses. Significance: *** p<0.01, ** p<0.05, * p<0.1.} \\\\",
  "\\end{tabular}",
  "\\end{table}"
)
writeLines(canonical_iv_lines, canonical_iv_tex_path)

micro_cols_needed <- c(
  "year", "quarter", "year_quarter", "uf", "estrato4", "person_weight", "age", "female_secondaryplus", "female_tertiary",
  "employed", "wanted_to_work", "wants_more_hours", "available_more_hours", "search_long", "worked_last_12m", "effective_hours",
  "hh_size", "n_children_0_14", "visit1_match_any", "visit1_n_rooms", "visit1_n_bedrooms",
  "visit1_electricity_grid_full_time"
)

micro <- read_parquet(linked_micro_path, col_select = all_of(micro_cols_needed)) %>%
  mutate(
    estrato4 = as.character(estrato4),
    uf_sigla = unname(uf_to_sigla[uf]),
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  )

micro_reg <- micro %>%
  left_join(baseline_wood, by = "estrato4") %>%
  left_join(lpg, by = c("estrato4", "year", "quarter")) %>%
  left_join(macro, by = c("uf_sigla", "year", "quarter")) %>%
  mutate(z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato)

micro_specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "age + age_sq + female_secondaryplus + female_tertiary + hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms",
  "M3", "age + age_sq + female_secondaryplus + female_tertiary + hh_size + n_children_0_14 + visit1_n_rooms + visit1_n_bedrooms + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary"
)

micro_outcomes <- c(
  "effective_hours" = "Effective weekly hours",
  "wanted_to_work" = "Wanted work",
  "wants_more_hours" = "Wanted more hours",
  "available_more_hours" = "Available for more hours",
  "worked_last_12m" = "Worked in last 12 months"
)

micro_models <- list()
micro_rows <- list()

for (outcome_name in names(micro_outcomes)) {
  micro_models[[outcome_name]] <- list()
  for (i in seq_len(nrow(micro_specs))) {
    spec <- micro_specs$spec[[i]]
    controls <- micro_specs$controls[[i]]
    needed_controls <- all.vars(as.formula(paste("~", controls)))
    needed <- unique(c("estrato4", "year_quarter", "person_weight", "z_wood_lpg_estrato", outcome_name, needed_controls))
    dat <- micro_reg %>% filter(visit1_match_any, if_all(all_of(needed), ~ !is.na(.x)))
    model <- feols(
      as.formula(paste0(outcome_name, " ~ z_wood_lpg_estrato + ", controls, " | estrato4 + year_quarter")),
      data = dat,
      weights = ~person_weight,
      cluster = ~estrato4
    )
    micro_models[[outcome_name]][[spec]] <- model
    micro_rows[[length(micro_rows) + 1L]] <- tidy(model) %>% mutate(outcome = outcome_name, spec = spec, nobs = nobs(model))
  }
}

micro_results <- bind_rows(micro_rows)
write_csv(micro_results, canonical_micro_csv_path)

headline_micro_tbl <- micro_results %>%
  filter(term == "z_wood_lpg_estrato") %>%
  transmute(spec, outcome = recode(outcome, !!!micro_outcomes), nobs, coef = estimate, se = std.error, p = p.value)
write_csv(headline_micro_tbl, headline_micro_csv_path)

headline_micro_lines <- c(
  "\\begin{tabular}{lccccc}",
  "\\toprule",
  " & Effective weekly hours & Wanted work & Wanted more hours & Available for more hours & Worked in last 12 months \\\\",
  "\\midrule"
)

for (spec in micro_specs$spec) {
  rows <- headline_micro_tbl %>% filter(spec == !!spec)
  rows <- rows[match(unname(micro_outcomes), rows$outcome), ]
  headline_micro_lines <- c(
    headline_micro_lines,
    paste0(spec, " & ", paste(vapply(seq_len(nrow(rows)), function(i) fmt_coef(rows$coef[[i]], rows$p[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0(" & ", paste(vapply(seq_len(nrow(rows)), function(i) fmt_se(rows$se[[i]]), character(1)), collapse = " & "), " \\\\"),
    paste0("N & ", paste(vapply(seq_len(nrow(rows)), function(i) format(rows$nobs[[i]], big.mark = ",", scientific = FALSE), character(1)), collapse = " & "), " \\\\"),
    if (spec != tail(micro_specs$spec, 1)) "\\addlinespace" else NULL
  )
}

headline_micro_lines <- c(
  headline_micro_lines,
  "\\midrule",
  "Estrato FE & Yes & Yes & Yes & Yes & Yes \\\\",
  "Year-quarter FE & Yes & Yes & Yes & Yes & Yes \\\\",
  "Weights & Yes & Yes & Yes & Yes & Yes \\\\",
  "Clustered by estrato4 & Yes & Yes & Yes & Yes & Yes \\\\",
  "\\bottomrule",
  "\\multicolumn{6}{p{0.95\\linewidth}}{\\footnotesize Notes: Entries are reduced-form coefficients from pooled woman-quarter regressions on baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price. M1 includes fixed effects only. M2 adds age, age squared, education, household size, number of children, and linked rooms and bedrooms. M3 adds UF-level unemployment, labor demand, real wage, and education composition controls. Standard errors clustered by estrato4 in parentheses. Significance: *** p<0.01, ** p<0.05, * p<0.1.} \\\\",
  "\\end{tabular}"
)
writeLines(headline_micro_lines, headline_micro_tex_path)

micro_labels <- c(
  "z_wood_lpg_estrato" = "Baseline wood share \\(\\times\\) log LPG price",
  "age" = "Age",
  "age_sq" = "Age squared",
  "female_secondaryplusTRUE" = "Secondary-plus",
  "female_tertiaryTRUE" = "Tertiary",
  "hh_size" = "Household size",
  "n_children_0_14" = "Children aged 0--14",
  "visit1_n_rooms" = "Rooms",
  "visit1_n_bedrooms" = "Bedrooms",
  "uf_unemployment_rate" = "UF unemployment rate",
  "uf_labor_demand_proxy" = "UF labor demand proxy",
  "uf_real_wage_proxy" = "UF real wage proxy",
  "uf_share_secondaryplus" = "UF share secondary-plus",
  "uf_share_tertiary" = "UF share tertiary"
)

micro_terms <- c(
  "z_wood_lpg_estrato",
  "age",
  "age_sq",
  "female_secondaryplusTRUE",
  "female_tertiaryTRUE",
  "hh_size",
  "n_children_0_14",
  "visit1_n_rooms",
  "visit1_n_bedrooms",
  "uf_unemployment_rate",
  "uf_labor_demand_proxy",
  "uf_real_wage_proxy",
  "uf_share_secondaryplus",
  "uf_share_tertiary"
)

canonical_micro_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Canonical Pooled Woman-Quarter Reduced Forms}",
  "\\small"
)

panel_letters <- c("A", "B", "C", "D", "E")

for (i in seq_along(micro_outcomes)) {
  outcome_name <- names(micro_outcomes)[[i]]
  outcome_label <- unname(micro_outcomes[[i]])
  models <- micro_models[[outcome_name]]
  panel_lines <- c(
    "\\begin{tabular}{lccc}",
    "\\toprule",
    " & M1 & M2 & M3 \\\\",
    make_panel_lines(
      title = paste0("Panel ", panel_letters[[i]], ": ", outcome_label),
      models = models,
      terms = micro_terms,
      labels = micro_labels,
      footer_rows = list(
        "Observations" = vapply(models, function(m) format(nobs(m), big.mark = ",", scientific = FALSE), character(1)),
        "Estrato FE" = rep("Yes", 3),
        "Year-quarter FE" = rep("Yes", 3),
        "Weights" = rep("Yes", 3),
        "Clustered by estrato4" = rep("Yes", 3)
      )
    ),
    "\\bottomrule",
    "\\end{tabular}"
  )
  canonical_micro_lines <- c(canonical_micro_lines, panel_lines)
  if (i < length(micro_outcomes)) {
    canonical_micro_lines <- c(canonical_micro_lines, "\\vspace{0.75em}")
  }
}

canonical_micro_lines <- c(
  canonical_micro_lines,
  "\\vspace{0.5em}",
  "\\parbox{0.95\\linewidth}{\\footnotesize Notes: This canonical table reports every estimated coefficient used in the headline pooled woman-quarter reduced-form models. Regressions use baseline estrato wood share in 2016 multiplied by log estrato-quarter LPG price as the exposure. M1 includes fixed effects only. M2 adds age, age squared, education, household size, number of children, and linked rooms and bedrooms. M3 adds UF-level unemployment, labor demand, real wage, and education composition controls. Standard errors clustered by estrato4 in parentheses. Significance: *** p<0.01, ** p<0.05, * p<0.1.}",
  "\\end{table}"
)
writeLines(canonical_micro_lines, canonical_micro_tex_path)

cat("Wrote:\n")
cat(headline_iv_tex_path, "\n")
cat(headline_micro_tex_path, "\n")
cat(canonical_iv_tex_path, "\n")
cat(canonical_micro_tex_path, "\n")
cat(headline_iv_csv_path, "\n")
cat(headline_micro_csv_path, "\n")
cat(canonical_iv_csv_path, "\n")
cat(canonical_micro_csv_path, "\n")
