#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
})

linked_path <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")
adapt_path <- here::here("data", "powerIV", "regressions", "lightning_adaptation_checks", "tables", "adaptation_split_results.csv")
dependence_path <- here::here("data", "powerIV", "regressions", "lightning_preperiod_dependence_interactions", "tables", "dependence_interaction_results.csv")

out_dir <- here::here("data", "powerIV", "regressions", "lightning_best_tables", "tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

table1_out <- here::here(out_dir, "table_1_dependence_resilience_2016_2019.tex")
table2_out <- here::here(out_dir, "table_2_domestic_care_adaptation.tex")
table3_out <- here::here(out_dir, "table_3_dependence_interactions_2016_2019.tex")

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", formatC(x, digits = digits, format = "f"))
}

fmt_int <- function(x) {
  ifelse(is.na(x), "", format(x, big.mark = ",", scientific = FALSE, trim = TRUE))
}

fmt_est_se <- function(est, se, digits = 3) {
  paste0(fmt_num(est, digits), " (", fmt_num(se, digits), ")")
}

write_tex <- function(lines, path) {
  writeLines(lines, path)
  message("Wrote: ", path)
}

wm <- function(x, w) weighted.mean(x, w, na.rm = TRUE)

linked <- read_parquet(
  linked_path,
  col_select = c(
    "year", "person_weight", "outlf_domestic_care",
    "visit1_match_any", "visit1_same_quarter",
    "visit1_area_situation", "visit1_cooking_electricity",
    "visit1_appliance_count_basic", "visit1_computer",
    "visit1_internet_home", "visit1_n_rooms",
    "visit1_electricity_grid_full_time"
  )
) %>%
  transmute(
    year = as.integer(year),
    person_weight = as.numeric(person_weight),
    outlf_domestic_care = as.numeric(outlf_domestic_care),
    visit1_match_any = as.logical(visit1_match_any),
    visit1_same_quarter = as.logical(visit1_same_quarter),
    visit1_area_situation = as.character(visit1_area_situation),
    visit1_cooking_electricity = as.numeric(visit1_cooking_electricity),
    visit1_appliance_count_basic = as.numeric(visit1_appliance_count_basic),
    visit1_computer = as.numeric(visit1_computer),
    visit1_internet_home = as.numeric(visit1_internet_home),
    visit1_n_rooms = as.numeric(visit1_n_rooms),
    visit1_electricity_grid_full_time = as.numeric(visit1_electricity_grid_full_time)
  ) %>%
  filter(
    year >= 2016, year <= 2019,
    visit1_match_any == TRUE,
    visit1_same_quarter == TRUE,
    !is.na(person_weight),
    person_weight > 0
  )

table1_df <- bind_rows(
  linked %>%
    summarise(
      group = "All linked women",
      domcare = wm(outlf_domestic_care, person_weight),
      electric = wm(visit1_cooking_electricity, person_weight),
      appliance = wm(visit1_appliance_count_basic, person_weight),
      computer = wm(visit1_computer, person_weight),
      internet = wm(visit1_internet_home, person_weight),
      rooms = wm(visit1_n_rooms, person_weight),
      grid = wm(visit1_electricity_grid_full_time, person_weight),
      n = n()
    ),
  linked %>%
    filter(visit1_cooking_electricity == 1) %>%
    summarise(
      group = "Electric cooking",
      domcare = wm(outlf_domestic_care, person_weight),
      electric = wm(visit1_cooking_electricity, person_weight),
      appliance = wm(visit1_appliance_count_basic, person_weight),
      computer = wm(visit1_computer, person_weight),
      internet = wm(visit1_internet_home, person_weight),
      rooms = wm(visit1_n_rooms, person_weight),
      grid = wm(visit1_electricity_grid_full_time, person_weight),
      n = n()
    ),
  linked %>%
    filter(visit1_cooking_electricity == 0) %>%
    summarise(
      group = "No electric cooking",
      domcare = wm(outlf_domestic_care, person_weight),
      electric = wm(visit1_cooking_electricity, person_weight),
      appliance = wm(visit1_appliance_count_basic, person_weight),
      computer = wm(visit1_computer, person_weight),
      internet = wm(visit1_internet_home, person_weight),
      rooms = wm(visit1_n_rooms, person_weight),
      grid = wm(visit1_electricity_grid_full_time, person_weight),
      n = n()
    ),
  linked %>%
    mutate(area_grp = case_when(
      grepl("urb", tolower(visit1_area_situation)) ~ "Urban",
      grepl("rur", tolower(visit1_area_situation)) ~ "Rural",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(area_grp)) %>%
    group_by(group = area_grp) %>%
    summarise(
      domcare = wm(outlf_domestic_care, person_weight),
      electric = wm(visit1_cooking_electricity, person_weight),
      appliance = wm(visit1_appliance_count_basic, person_weight),
      computer = wm(visit1_computer, person_weight),
      internet = wm(visit1_internet_home, person_weight),
      rooms = wm(visit1_n_rooms, person_weight),
      grid = wm(visit1_electricity_grid_full_time, person_weight),
      n = n(),
      .groups = "drop"
    )
) %>%
  mutate(
    domcare = domcare * 100,
    electric = electric * 100,
    computer = computer * 100,
    internet = internet * 100,
    grid = grid * 100
  ) %>%
  slice(match(c("All linked women", "Electric cooking", "No electric cooking", "Rural", "Urban"), group))

table1_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Electricity Dependence and Resilience in the Pre-Period}",
  "\\label{tab:lightning_dependence_resilience}",
  "\\begin{tabular}{lrrrrrrrr}",
  "\\toprule",
  "Group & Domestic care baseline (\\%) & Electric cooking (\\%) & Basic appliance count & Computer (\\%) & Internet at home (\\%) & Rooms & Grid full-time (\\%) & $N$ \\\\",
  "\\midrule"
)

for (i in seq_len(nrow(table1_df))) {
  row <- table1_df[i, ]
  table1_lines <- c(
    table1_lines,
    sprintf(
      "%s & %s & %s & %s & %s & %s & %s & %s & %s \\\\",
      row$group,
      fmt_num(row$domcare, 1),
      fmt_num(row$electric, 1),
      fmt_num(row$appliance, 2),
      fmt_num(row$computer, 1),
      fmt_num(row$internet, 1),
      fmt_num(row$rooms, 2),
      fmt_num(row$grid, 1),
      fmt_int(row$n)
    )
  )
}

table1_lines <- c(
  table1_lines,
  "\\bottomrule",
  "\\end{tabular}",
  "\\vspace{0.4em}",
  "\\parbox{0.94\\linewidth}{\\footnotesize Notes: Weighted means in the same-quarter linked women sample for 2016--2019. Domestic care baseline is the share of women out of the labor force for domestic-care reasons.}",
  "\\end{table}"
)

write_tex(table1_lines, table1_out)

adapt <- read_csv(adapt_path, show_col_types = FALSE)

table2_df <- adapt %>%
  filter(sample %in% c("all_linked", "same_quarter_linked", "same_quarter_no_computer", "same_quarter_with_computer")) %>%
  mutate(sample_label = case_when(
    sample == "all_linked" ~ "All linked women",
    sample == "same_quarter_linked" ~ "Same-quarter linked women",
    sample == "same_quarter_no_computer" ~ "Same-quarter, no computer",
    sample == "same_quarter_with_computer" ~ "Same-quarter, with computer"
  )) %>%
  slice(match(c("All linked women", "Same-quarter linked women", "Same-quarter, no computer", "Same-quarter, with computer"), sample_label))

table2_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Domestic-Care IV Results and Post-Pandemic Attenuation}",
  "\\label{tab:lightning_domcare_adaptation}",
  "\\begin{tabular}{lrrrrrrr}",
  "\\toprule",
  "Sample & Pre effect & $p$-value & Post $-$ pre & $p$-value & Implied post effect & $p$-value & $N$ \\\\",
  "\\midrule"
)

for (i in seq_len(nrow(table2_df))) {
  row <- table2_df[i, ]
  table2_lines <- c(
    table2_lines,
    sprintf(
      "%s & %s & %s & %s & %s & %s & %s & %s \\\\",
      row$sample_label,
      fmt_est_se(row$pre_pp, row$pre_pp_se, 3),
      fmt_num(row$pre_p, 3),
      fmt_est_se(row$post_diff_pp, row$post_diff_pp_se, 3),
      fmt_num(row$post_diff_p, 3),
      fmt_est_se(row$post_total_pp, row$post_total_pp_se, 3),
      fmt_num(row$post_total_p, 3),
      fmt_int(row$nobs)
    )
  )
}

table2_lines <- c(
  table2_lines,
  "\\bottomrule",
  "\\end{tabular}",
  "\\vspace{0.4em}",
  "\\parbox{0.94\\linewidth}{\\footnotesize Notes: Entries are IV estimates in percentage points for the probability that a woman is out of the labor force for domestic-care reasons. The sample pools 2016--2019 and 2022--2024. All regressions include \\texttt{estrato4} and year-quarter fixed effects plus baseline characteristics interacted with a linear trend.}",
  "\\end{table}"
)

write_tex(table2_lines, table2_out)

dependence <- read_csv(dependence_path, show_col_types = FALSE)

table3_df <- dependence %>%
  filter(
    (dependence == "Electric cooking" & outcome_label %in% c("Domestic care", "Effective hours population")) |
      (dependence == "Basic appliance count (z)" & outcome_label %in% c("Domestic care", "Effective hours population"))
  ) %>%
  mutate(dep_order = case_when(
    dependence == "Electric cooking" ~ 1L,
    dependence == "Basic appliance count (z)" ~ 2L
  )) %>%
  mutate(outcome_order = case_when(
    outcome_label == "Domestic care" ~ 1L,
    outcome_label == "Effective hours population" ~ 2L
  )) %>%
  arrange(outcome_order, dep_order)

table3_lines <- c(
  "\\begin{table}[!htbp]",
  "\\centering",
  "\\caption{Where Electricity Dependence Amplifies Outage Effects, 2016--2019}",
  "\\label{tab:lightning_dependence_interactions}",
  "\\begin{tabular}{llrrrrrrr}",
  "\\toprule",
  "Outcome & Dependence measure & Base outage effect & $p$-value & Outage $\\times$ dependence & $p$-value & Implied high-dependence effect & $p$-value & $N$ \\\\",
  "\\midrule"
)

for (i in seq_len(nrow(table3_df))) {
  row <- table3_df[i, ]
  table3_lines <- c(
    table3_lines,
    sprintf(
      "%s & %s & %s & %s & %s & %s & %s & %s & %s \\\\",
      row$outcome_label,
      row$dependence,
      fmt_est_se(row$base_effect, row$base_se, 3),
      fmt_num(row$base_p, 3),
      fmt_est_se(row$interaction_effect, row$interaction_se, 3),
      fmt_num(row$interaction_p, 3),
      fmt_est_se(row$high_effect, row$high_se, 3),
      fmt_num(row$high_p, 3),
      fmt_int(row$nobs)
    )
  )
}

table3_lines <- c(
  table3_lines,
  "\\bottomrule",
  "\\end{tabular}",
  "\\vspace{0.4em}",
  "\\parbox{0.94\\linewidth}{\\footnotesize Notes: Pre-period only, same-quarter linked women sample. All regressions include \\texttt{estrato4} and year-quarter fixed effects plus baseline characteristics interacted with a linear trend. For electric cooking, the interaction compares electric-cooking households with non-electric-cooking households. For basic appliance count, the interaction shows the change in the outage effect for a one-standard-deviation increase in appliance count. Domestic-care effects are in percentage points; effective-hours-population effects are in minutes.}",
  "\\end{table}"
)

write_tex(table3_lines, table3_out)
