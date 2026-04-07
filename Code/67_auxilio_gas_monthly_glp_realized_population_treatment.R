suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_monthly_glp_realized_population_treatment")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

uf_population <- read_parquet(here("data", "powerIV", "auxilio_gas", "ibge_uf_population_2021_2025.parquet"))

auxgas_uf_month <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet")) %>%
  transmute(
    codigo_ibge = sprintf("%06d", as.integer(codigo_ibge)),
    year,
    month,
    anomes,
    reference_date,
    auxgas_families,
    auxgas_total_value,
    auxgas_people,
    uf_sigla = case_when(
      substr(codigo_ibge, 1, 2) == "11" ~ "RO",
      substr(codigo_ibge, 1, 2) == "12" ~ "AC",
      substr(codigo_ibge, 1, 2) == "13" ~ "AM",
      substr(codigo_ibge, 1, 2) == "14" ~ "RR",
      substr(codigo_ibge, 1, 2) == "15" ~ "PA",
      substr(codigo_ibge, 1, 2) == "16" ~ "AP",
      substr(codigo_ibge, 1, 2) == "17" ~ "TO",
      substr(codigo_ibge, 1, 2) == "21" ~ "MA",
      substr(codigo_ibge, 1, 2) == "22" ~ "PI",
      substr(codigo_ibge, 1, 2) == "23" ~ "CE",
      substr(codigo_ibge, 1, 2) == "24" ~ "RN",
      substr(codigo_ibge, 1, 2) == "25" ~ "PB",
      substr(codigo_ibge, 1, 2) == "26" ~ "PE",
      substr(codigo_ibge, 1, 2) == "27" ~ "AL",
      substr(codigo_ibge, 1, 2) == "28" ~ "SE",
      substr(codigo_ibge, 1, 2) == "29" ~ "BA",
      substr(codigo_ibge, 1, 2) == "31" ~ "MG",
      substr(codigo_ibge, 1, 2) == "32" ~ "ES",
      substr(codigo_ibge, 1, 2) == "33" ~ "RJ",
      substr(codigo_ibge, 1, 2) == "35" ~ "SP",
      substr(codigo_ibge, 1, 2) == "41" ~ "PR",
      substr(codigo_ibge, 1, 2) == "42" ~ "SC",
      substr(codigo_ibge, 1, 2) == "43" ~ "RS",
      substr(codigo_ibge, 1, 2) == "50" ~ "MS",
      substr(codigo_ibge, 1, 2) == "51" ~ "MT",
      substr(codigo_ibge, 1, 2) == "52" ~ "GO",
      substr(codigo_ibge, 1, 2) == "53" ~ "DF",
      TRUE ~ NA_character_
    )
  ) %>%
  group_by(uf_sigla, year, month, anomes, reference_date) %>%
  summarise(
    auxgas_families = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people = sum(auxgas_people, na.rm = TRUE),
    .groups = "drop"
  )

glp_monthly <- read_parquet(here("data", "powerIV", "monthly_source_audit", "anp_glp_vasilhame_monthly_uf_2007_2025.parquet")) %>%
  filter(vasilhame == "GLP - Até P13") %>%
  transmute(
    uf_sigla,
    year,
    month,
    date,
    year_month = sprintf("%04d-%02d", year, month),
    glp_p13_sales_m3 = sales_m3,
    log_glp_p13_sales = log(sales_m3)
  ) %>%
  filter(date >= as.Date("2021-12-01"), date <= as.Date("2025-12-01"))

panel <- glp_monthly %>%
  left_join(auxgas_uf_month, by = c("uf_sigla", "year", "month")) %>%
  left_join(uf_population, by = c("uf_sigla", "year")) %>%
  mutate(
    auxgas_families = coalesce(auxgas_families, 0),
    auxgas_total_value = coalesce(auxgas_total_value, 0),
    auxgas_people = coalesce(auxgas_people, 0),
    auxgas_people_per_1000 = 1000 * auxgas_people / population,
    auxgas_families_per_1000 = 1000 * auxgas_families / population,
    auxgas_value_per_capita = auxgas_total_value / population,
    month_id = year * 12 + month
  ) %>%
  arrange(uf_sigla, year, month) %>%
  group_by(uf_sigla) %>%
  mutate(
    lag1_auxgas_people_per_1000 = lag(auxgas_people_per_1000, 1),
    lag2_auxgas_people_per_1000 = lag(auxgas_people_per_1000, 2),
    lag1_auxgas_families_per_1000 = lag(auxgas_families_per_1000, 1),
    lag2_auxgas_families_per_1000 = lag(auxgas_families_per_1000, 2),
    lag1_auxgas_value_per_capita = lag(auxgas_value_per_capita, 1),
    lag2_auxgas_value_per_capita = lag(auxgas_value_per_capita, 2)
  ) %>%
  ungroup()

write_parquet(panel, file.path(out_dir, "monthly_glp_realized_population_panel.parquet"))

specs <- tribble(
  ~spec_name, ~treat0, ~treat1, ~treat2,
  "people_per_1000", "auxgas_people_per_1000", "lag1_auxgas_people_per_1000", "lag2_auxgas_people_per_1000",
  "families_per_1000", "auxgas_families_per_1000", "lag1_auxgas_families_per_1000", "lag2_auxgas_families_per_1000",
  "value_per_capita", "auxgas_value_per_capita", "lag1_auxgas_value_per_capita", "lag2_auxgas_value_per_capita"
)

outcomes <- tribble(
  ~outcome_name, ~outcome_var,
  "log_glp_p13_sales", "log_glp_p13_sales",
  "glp_p13_sales_m3", "glp_p13_sales_m3"
)

summary_out <- list()
terms_out <- list()

for (i in seq_len(nrow(specs))) {
  treat_name <- specs$spec_name[[i]]
  treat0 <- specs$treat0[[i]]
  treat1 <- specs$treat1[[i]]
  treat2 <- specs$treat2[[i]]

  for (j in seq_len(nrow(outcomes))) {
    outcome_name <- outcomes$outcome_name[[j]]
    outcome_var <- outcomes$outcome_var[[j]]

    reg_data <- panel %>%
      filter(!is.na(.data[[outcome_var]]), !is.na(.data[[treat1]]), !is.na(.data[[treat2]]))

    model <- feols(
      as.formula(paste0(outcome_var, " ~ ", treat0, " + ", treat1, " + ", treat2, " | uf_sigla + year_month")),
      data = reg_data,
      cluster = ~uf_sigla
    )

    tidy_tbl <- tidy(model) %>%
      filter(term %in% c(treat0, treat1, treat2)) %>%
      mutate(
        outcome = outcome_name,
        treatment = treat_name,
        lag_label = c("t", "t-1", "t-2"),
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(outcome, treatment, lag_label, term, estimate, std.error, conf_low, conf_high, p.value, n)

    summary_out[[length(summary_out) + 1]] <- tibble(
      outcome = outcome_name,
      treatment = treat_name,
      n = nobs(model),
      start_period = min(reg_data$year_month),
      end_period = max(reg_data$year_month),
      coef_t = tidy_tbl$estimate[tidy_tbl$lag_label == "t"],
      p_t = tidy_tbl$p.value[tidy_tbl$lag_label == "t"],
      coef_t1 = tidy_tbl$estimate[tidy_tbl$lag_label == "t-1"],
      p_t1 = tidy_tbl$p.value[tidy_tbl$lag_label == "t-1"],
      coef_t2 = tidy_tbl$estimate[tidy_tbl$lag_label == "t-2"],
      p_t2 = tidy_tbl$p.value[tidy_tbl$lag_label == "t-2"]
    )

    terms_out[[length(terms_out) + 1]] <- tidy_tbl
  }
}

summary_df <- bind_rows(summary_out) %>% arrange(outcome, treatment)
terms_df <- bind_rows(terms_out) %>% arrange(outcome, treatment, lag_label)

write.table(summary_df, file.path(out_dir, "monthly_realized_treatment_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(terms_df, file.path(out_dir, "monthly_realized_treatment_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~treatment, ~file_stub, ~title, ~subtitle,
  "log_glp_p13_sales", "people_per_1000", "monthly_realized_treatment_log_glp_people_per_1000", "Monthly GLP P13 Sales and Realized Auxilio Gas Intensity", "Outcome is log UF-month P13 sales; treatment is contemporaneous and lagged beneficiaries per 1,000 residents",
  "log_glp_p13_sales", "value_per_capita", "monthly_realized_treatment_log_glp_value_per_capita", "Monthly GLP P13 Sales and Realized Auxilio Gas Intensity", "Outcome is log UF-month P13 sales; treatment is contemporaneous and lagged reais per resident"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(outcome == plot_specs$outcome[[i]], treatment == plot_specs$treatment[[i]]) %>%
    mutate(lag_label = factor(lag_label, levels = c("t", "t-1", "t-2")))

  p <- ggplot(plot_df, aes(x = lag_label, y = estimate)) +
    geom_hline(yintercept = 0, color = "black") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#1f78b4") +
    geom_point(size = 2.4, color = "#1f78b4") +
    labs(
      x = "Treatment Month",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = plot_specs$subtitle[[i]]
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 8.5, height = 5.5, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 8.5, height = 5.5)
}

print(summary_df)
