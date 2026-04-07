suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(readxl)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(stringr)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_population_normalization")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

pop_path <- here("data", "powerIV", "auxilio_gas", "ibge_uf_population_2021_2025.parquet")
if (!file.exists(pop_path)) {
  download.file(
    "https://ftp.ibge.gov.br/Projecao_da_Populacao/Projecao_da_Populacao_2024/projecoes_2024_tab1_idade_simples.xlsx",
    destfile = file.path(tempdir(), "projecoes_2024_tab1_idade_simples.xlsx"),
    mode = "wb",
    quiet = TRUE
  )

  uf_population <- read_excel(file.path(tempdir(), "projecoes_2024_tab1_idade_simples.xlsx"), sheet = 1, skip = 5) %>%
    filter(SEXO == "Ambos", CÓD. >= 11, CÓD. <= 53) %>%
    select(SIGLA, LOCAL, `2021`, `2022`, `2023`, `2024`, `2025`) %>%
    group_by(SIGLA, LOCAL) %>%
    summarise(across(`2021`:`2025`, ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    pivot_longer(`2021`:`2025`, names_to = "year", values_to = "population") %>%
    mutate(year = as.integer(year)) %>%
    rename(uf_sigla = SIGLA, uf_name = LOCAL)

  write_parquet(uf_population, pop_path)
} else {
  uf_population <- read_parquet(pop_path)
}

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
      str_sub(codigo_ibge, 1, 2) == "11" ~ "RO",
      str_sub(codigo_ibge, 1, 2) == "12" ~ "AC",
      str_sub(codigo_ibge, 1, 2) == "13" ~ "AM",
      str_sub(codigo_ibge, 1, 2) == "14" ~ "RR",
      str_sub(codigo_ibge, 1, 2) == "15" ~ "PA",
      str_sub(codigo_ibge, 1, 2) == "16" ~ "AP",
      str_sub(codigo_ibge, 1, 2) == "17" ~ "TO",
      str_sub(codigo_ibge, 1, 2) == "21" ~ "MA",
      str_sub(codigo_ibge, 1, 2) == "22" ~ "PI",
      str_sub(codigo_ibge, 1, 2) == "23" ~ "CE",
      str_sub(codigo_ibge, 1, 2) == "24" ~ "RN",
      str_sub(codigo_ibge, 1, 2) == "25" ~ "PB",
      str_sub(codigo_ibge, 1, 2) == "26" ~ "PE",
      str_sub(codigo_ibge, 1, 2) == "27" ~ "AL",
      str_sub(codigo_ibge, 1, 2) == "28" ~ "SE",
      str_sub(codigo_ibge, 1, 2) == "29" ~ "BA",
      str_sub(codigo_ibge, 1, 2) == "31" ~ "MG",
      str_sub(codigo_ibge, 1, 2) == "32" ~ "ES",
      str_sub(codigo_ibge, 1, 2) == "33" ~ "RJ",
      str_sub(codigo_ibge, 1, 2) == "35" ~ "SP",
      str_sub(codigo_ibge, 1, 2) == "41" ~ "PR",
      str_sub(codigo_ibge, 1, 2) == "42" ~ "SC",
      str_sub(codigo_ibge, 1, 2) == "43" ~ "RS",
      str_sub(codigo_ibge, 1, 2) == "50" ~ "MS",
      str_sub(codigo_ibge, 1, 2) == "51" ~ "MT",
      str_sub(codigo_ibge, 1, 2) == "52" ~ "GO",
      str_sub(codigo_ibge, 1, 2) == "53" ~ "DF",
      TRUE ~ NA_character_
    )
  ) %>%
  group_by(uf_sigla, year, month, anomes, reference_date) %>%
  summarise(
    auxgas_families = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people = sum(auxgas_people, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(uf_population, by = c("uf_sigla", "year")) %>%
  mutate(
    auxgas_people_per_1000 = 1000 * auxgas_people / population,
    auxgas_families_per_1000 = 1000 * auxgas_families / population,
    auxgas_value_per_capita = auxgas_total_value / population,
    auxgas_value_per_1000 = 1000 * auxgas_total_value / population
  )

pre_exposure <- auxgas_uf_month %>%
  filter((year == 2021 & month == 12) | (year == 2022 & month %in% c(2, 4, 6))) %>%
  group_by(uf_sigla) %>%
  summarise(
    pre_people_per_1000 = mean(auxgas_people_per_1000, na.rm = TRUE),
    pre_families_per_1000 = mean(auxgas_families_per_1000, na.rm = TRUE),
    pre_value_per_capita = mean(auxgas_value_per_capita, na.rm = TRUE),
    pre_value_per_1000 = mean(auxgas_value_per_1000, na.rm = TRUE),
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
  )

price_monthly <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_monthly_uf_2016_2025_repaired.parquet")) %>%
  transmute(
    uf_sigla,
    year,
    month,
    lpg_price_monthly_uf,
    n_station_obs
  )

panel <- glp_monthly %>%
  left_join(pre_exposure, by = "uf_sigla") %>%
  left_join(price_monthly, by = c("uf_sigla", "year", "month")) %>%
  mutate(event_time_m = (year - 2022) * 12 + (month - 8)) %>%
  filter(date >= as.Date("2021-12-01"), date <= as.Date("2025-12-01")) %>%
  filter(event_time_m >= -8, event_time_m <= 40)

outcomes <- tribble(
  ~outcome_name, ~outcome_var,
  "log_glp_p13_sales", "log_glp_p13_sales",
  "glp_p13_sales_m3", "glp_p13_sales_m3"
)

exposures <- tribble(
  ~exposure_name, ~exposure_var,
  "people_per_1000", "pre_people_per_1000",
  "families_per_1000", "pre_families_per_1000",
  "value_per_capita", "pre_value_per_capita"
)

specs <- tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + lpg_price_monthly_uf"
)

terms_out <- list()
summary_out <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]

  for (j in seq_len(nrow(exposures))) {
    exposure_name <- exposures$exposure_name[[j]]
    exposure_var <- exposures$exposure_var[[j]]

    for (k in seq_len(nrow(specs))) {
      spec_name <- specs$spec[[k]]
      controls <- specs$controls[[k]]

      vars_needed <- unique(c(
        "uf_sigla", "year_month", "event_time_m", outcome_var, exposure_var,
        all.vars(as.formula(paste("~", controls)))
      ))

      reg_data <- panel %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))

      model <- feols(
        as.formula(paste0(outcome_var, " ~ i(event_time_m, ", exposure_var, ", ref = -1) + ", controls, " | uf_sigla + year_month")),
        data = reg_data,
        cluster = ~uf_sigla
      )

      tidy_tbl <- tidy(model) %>%
        filter(str_detect(term, "event_time_m::")) %>%
        mutate(
          outcome = outcome_name,
          exposure = exposure_name,
          spec = spec_name,
          event_time_m = as.integer(str_match(term, "event_time_m::(-?[0-9]+)")[, 2]),
          conf_low = estimate - 1.96 * std.error,
          conf_high = estimate + 1.96 * std.error,
          n = nobs(model)
        ) %>%
        select(outcome, exposure, spec, event_time_m, estimate, std.error, conf_low, conf_high, p.value, n)

      lead_terms <- tidy_tbl %>% filter(event_time_m < 0)
      pretrend_terms <- paste0("event_time_m::", lead_terms$event_time_m, ":", exposure_var)

      pretrend_p <- NA_real_
      pretrend_f <- NA_real_
      if (length(pretrend_terms) > 0) {
        wald_out <- tryCatch({
          capture.output(w <- wald(model, pretrend_terms))
          w
        }, error = function(e) NULL)
        if (!is.null(wald_out)) {
          stat <- suppressWarnings(as.numeric(unlist(wald_out$stat)))
          pval <- suppressWarnings(as.numeric(unlist(wald_out$p)))
          stat <- stat[!is.na(stat)]
          pval <- pval[!is.na(pval)]
          if (length(stat) > 0) pretrend_f <- stat[[1]]
          if (length(pval) > 0) pretrend_p <- pval[[1]]
        }
      }

      post_peak <- tidy_tbl %>%
        filter(event_time_m >= 0) %>%
        arrange(p.value, desc(abs(estimate))) %>%
        slice(1)

      summary_out[[length(summary_out) + 1]] <- tibble(
        outcome = outcome_name,
        exposure = exposure_name,
        spec = spec_name,
        n = nobs(model),
        first_period = min(reg_data$year_month),
        last_period = max(reg_data$year_month),
        pretrend_f = pretrend_f,
        pretrend_p = pretrend_p,
        post_peak_event = if (nrow(post_peak) == 0) NA_integer_ else post_peak$event_time_m[[1]],
        post_peak_coef = if (nrow(post_peak) == 0) NA_real_ else post_peak$estimate[[1]],
        post_peak_se = if (nrow(post_peak) == 0) NA_real_ else post_peak$std.error[[1]],
        post_peak_p = if (nrow(post_peak) == 0) NA_real_ else post_peak$p.value[[1]]
      )

      terms_out[[length(terms_out) + 1]] <- tidy_tbl
    }
  }
}

terms_df <- bind_rows(terms_out) %>% arrange(outcome, exposure, spec, event_time_m)
summary_df <- bind_rows(summary_out) %>% arrange(outcome, exposure, spec)

write_parquet(uf_population, pop_path)
write_parquet(auxgas_uf_month, file.path(out_dir, "auxgas_uf_month_population_normalized.parquet"))
write.table(pre_exposure, file.path(out_dir, "monthly_pre_exposure_population_normalized.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(terms_df, file.path(out_dir, "monthly_event_study_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, file.path(out_dir, "monthly_event_study_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~exposure, ~spec, ~file_stub, ~title, ~subtitle,
  "log_glp_p13_sales", "people_per_1000", "M2", "monthly_event_study_log_glp_p13_sales_auxgas_people_per_1000", "Monthly GLP P13 Sales and Auxilio Gas Top-Up", "Outcome is log UF-month P13 sales; exposure is pre-top-up Auxilio Gas beneficiaries per 1,000 residents",
  "log_glp_p13_sales", "value_per_capita", "M2", "monthly_event_study_log_glp_p13_sales_auxgas_value_per_capita", "Monthly GLP P13 Sales and Auxilio Gas Top-Up", "Outcome is log UF-month P13 sales; exposure is pre-top-up Auxilio Gas reais per resident"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(
      outcome == plot_specs$outcome[[i]],
      exposure == plot_specs$exposure[[i]],
      spec == plot_specs$spec[[i]]
    )

  p <- ggplot(plot_df, aes(x = event_time_m, y = estimate)) +
    geom_hline(yintercept = 0, color = "black") +
    geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, color = "#1f78b4") +
    geom_point(size = 1.8, color = "#1f78b4") +
    scale_x_continuous(breaks = seq(min(plot_df$event_time_m), max(plot_df$event_time_m), by = 3)) +
    labs(
      x = "Months Relative to 2022-08",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = plot_specs$subtitle[[i]]
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 10, height = 5.75, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 10, height = 5.75)
}

print(summary_df)
