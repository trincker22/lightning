suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_bimonthly_glp_realized_population_treatment")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

uf_population <- read_parquet(here("data", "powerIV", "auxilio_gas", "ibge_uf_population_2021_2025.parquet"))

auxgas_uf_month <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet")) %>%
  transmute(
    codigo_ibge = sprintf("%06d", as.integer(codigo_ibge)),
    year,
    month,
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
    ),
    bimester = ((month - 1) %/% 2) + 1
  ) %>%
  group_by(uf_sigla, year, bimester) %>%
  summarise(
    auxgas_families_bi = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value_bi = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people_bi = sum(auxgas_people, na.rm = TRUE),
    .groups = "drop"
  )

glp_uf_month <- read_parquet(here("data", "powerIV", "monthly_source_audit", "anp_glp_vasilhame_monthly_uf_2007_2025.parquet")) %>%
  filter(vasilhame == "GLP - Até P13") %>%
  transmute(
    uf_sigla,
    year,
    month,
    sales_m3,
    bimester = ((month - 1) %/% 2) + 1
  ) %>%
  filter(year <= 2025)

glp_uf_bimonth <- glp_uf_month %>%
  group_by(uf_sigla, year, bimester) %>%
  summarise(
    glp_p13_sales_m3_bi = sum(sales_m3, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    log_glp_p13_sales_bi = log(glp_p13_sales_m3_bi),
    year_bimester = sprintf("%04d-B%d", year, bimester)
  )

panel <- glp_uf_bimonth %>%
  left_join(auxgas_uf_month, by = c("uf_sigla", "year", "bimester")) %>%
  left_join(uf_population, by = c("uf_sigla", "year")) %>%
  mutate(
    auxgas_families_bi = coalesce(auxgas_families_bi, 0),
    auxgas_total_value_bi = coalesce(auxgas_total_value_bi, 0),
    auxgas_people_bi = coalesce(auxgas_people_bi, 0),
    auxgas_people_per_1000_bi = 1000 * auxgas_people_bi / population,
    auxgas_families_per_1000_bi = 1000 * auxgas_families_bi / population,
    auxgas_value_per_capita_bi = auxgas_total_value_bi / population,
    bi_index = year * 6 + bimester
  ) %>%
  arrange(uf_sigla, year, bimester) %>%
  group_by(uf_sigla) %>%
  mutate(
    lag1_auxgas_people_per_1000_bi = lag(auxgas_people_per_1000_bi, 1),
    lag1_auxgas_families_per_1000_bi = lag(auxgas_families_per_1000_bi, 1),
    lag1_auxgas_value_per_capita_bi = lag(auxgas_value_per_capita_bi, 1)
  ) %>%
  ungroup() %>%
  filter(year >= 2022)

write_parquet(panel, file.path(out_dir, "bimonthly_glp_realized_population_panel.parquet"))

specs <- tribble(
  ~treatment, ~t0, ~t1,
  "people_per_1000_bi", "auxgas_people_per_1000_bi", "lag1_auxgas_people_per_1000_bi",
  "families_per_1000_bi", "auxgas_families_per_1000_bi", "lag1_auxgas_families_per_1000_bi",
  "value_per_capita_bi", "auxgas_value_per_capita_bi", "lag1_auxgas_value_per_capita_bi"
)

outcomes <- tribble(
  ~outcome, ~outcome_var,
  "log_glp_p13_sales_bi", "log_glp_p13_sales_bi",
  "glp_p13_sales_m3_bi", "glp_p13_sales_m3_bi"
)

summary_out <- list()
terms_out <- list()

for (i in seq_len(nrow(specs))) {
  for (j in seq_len(nrow(outcomes))) {
    reg_data <- panel %>%
      filter(!is.na(.data[[outcomes$outcome_var[[j]]]]), !is.na(.data[[specs$t1[[i]]]]))

    model <- feols(
      as.formula(paste0(outcomes$outcome_var[[j]], " ~ ", specs$t0[[i]], " + ", specs$t1[[i]], " | uf_sigla + year_bimester")),
      data = reg_data,
      cluster = ~uf_sigla
    )

    tidy_tbl <- tidy(model) %>%
      filter(term %in% c(specs$t0[[i]], specs$t1[[i]])) %>%
      mutate(
        outcome = outcomes$outcome[[j]],
        treatment = specs$treatment[[i]],
        lag_label = c("b", "b-1"),
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(outcome, treatment, lag_label, term, estimate, std.error, conf_low, conf_high, p.value, n)

    summary_out[[length(summary_out) + 1]] <- tibble(
      outcome = outcomes$outcome[[j]],
      treatment = specs$treatment[[i]],
      n = nobs(model),
      start_period = min(reg_data$year_bimester),
      end_period = max(reg_data$year_bimester),
      coef_b = tidy_tbl$estimate[tidy_tbl$lag_label == "b"],
      p_b = tidy_tbl$p.value[tidy_tbl$lag_label == "b"],
      coef_b1 = tidy_tbl$estimate[tidy_tbl$lag_label == "b-1"],
      p_b1 = tidy_tbl$p.value[tidy_tbl$lag_label == "b-1"]
    )

    terms_out[[length(terms_out) + 1]] <- tidy_tbl
  }
}

summary_df <- bind_rows(summary_out) %>% arrange(outcome, treatment)
terms_df <- bind_rows(terms_out) %>% arrange(outcome, treatment, lag_label)

write.table(summary_df, file.path(out_dir, "bimonthly_realized_treatment_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(terms_df, file.path(out_dir, "bimonthly_realized_treatment_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~treatment, ~file_stub, ~title, ~subtitle,
  "log_glp_p13_sales_bi", "people_per_1000_bi", "bimonthly_realized_treatment_log_glp_people_per_1000", "Bimonthly GLP P13 Sales and Realized Auxilio Gas Intensity", "Outcome is log UF-bimester P13 sales; treatment is contemporaneous and lagged beneficiaries per 1,000 residents",
  "log_glp_p13_sales_bi", "value_per_capita_bi", "bimonthly_realized_treatment_log_glp_value_per_capita", "Bimonthly GLP P13 Sales and Realized Auxilio Gas Intensity", "Outcome is log UF-bimester P13 sales; treatment is contemporaneous and lagged reais per resident"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(outcome == plot_specs$outcome[[i]], treatment == plot_specs$treatment[[i]]) %>%
    mutate(lag_label = factor(lag_label, levels = c("b", "b-1")))

  p <- ggplot(plot_df, aes(x = lag_label, y = estimate)) +
    geom_hline(yintercept = 0, color = "black") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#1f78b4") +
    geom_point(size = 2.4, color = "#1f78b4") +
    labs(
      x = "Treatment Bimester",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = plot_specs$subtitle[[i]]
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 8.5, height = 5.5, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 8.5, height = 5.5)
}

print(summary_df)
