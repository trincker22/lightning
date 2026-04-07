suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(stringr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_event_study_gas")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

auxgas_muni <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet")) %>%
  transmute(
    codigo_ibge = as.integer(codigo_ibge),
    year,
    quarter,
    anomes,
    auxgas_families,
    auxgas_total_value,
    auxgas_people
  )

crosswalk <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_estrato_municipio_crosswalk.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    codigo_ibge = as.integer(municipio_code %/% 10)
  ) %>%
  distinct()

visit1 <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = gsub("-", "", year_quarter),
    weight_sum,
    share_cooking_gas_any,
    share_cooking_gas_bottled,
    mean_rooms,
    mean_bedrooms,
    share_grid_full_time
  )

baseline_shift <- read_parquet(here("data", "powerIV", "macro", "shiftshare_baseline_estrato_2016.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    baseline_child_dependency
  )

auxgas_q <- auxgas_muni %>%
  inner_join(crosswalk, by = "codigo_ibge") %>%
  group_by(estrato4, year, quarter) %>%
  summarise(
    year_quarter = sprintf("%dQ%d", year[[1]], quarter[[1]]),
    n_payments = n_distinct(anomes),
    auxgas_families_q = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value_q = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people_q = sum(auxgas_people, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    auxgas_families_per_payment = auxgas_families_q / n_payments,
    auxgas_total_value_per_payment = auxgas_total_value_q / n_payments,
    auxgas_people_per_payment = auxgas_people_q / n_payments
  )

panel <- visit1 %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  mutate(
    auxgas_families_per_weighted_household = auxgas_families_per_payment / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people_per_payment / weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value_per_payment / weight_sum,
    b_dep_t = baseline_child_dependency * (year - 2016),
    event_time_q = (year - 2022) * 4 + (quarter - 3)
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

pre_exposure <- panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_exposure = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    pre_families_exposure = mean(auxgas_families_per_weighted_household, na.rm = TRUE),
    pre_value_exposure = mean(auxgas_value_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

panel <- panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  filter(event_time_q >= -3, event_time_q <= 9)

outcomes <- tibble::tribble(
  ~outcome_name, ~outcome_var,
  "gas_any", "share_cooking_gas_any",
  "gas_bottled", "share_cooking_gas_bottled"
)

exposures <- tibble::tribble(
  ~exposure_name, ~exposure_var,
  "people", "pre_people_exposure",
  "families", "pre_families_exposure"
)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
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
        "estrato4", "year_quarter", "weight_sum", "event_time_q", outcome_var, exposure_var,
        all.vars(as.formula(paste("~", controls)))
      ))

      reg_data <- panel %>%
        filter(if_all(all_of(vars_needed), ~ !is.na(.)))

      model <- feols(
        as.formula(paste0(outcome_var, " ~ i(event_time_q, ", exposure_var, ", ref = -1) + ", controls, " | estrato4 + year_quarter")),
        data = reg_data,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      tidy_tbl <- tidy(model) %>%
        filter(str_detect(term, "event_time_q::")) %>%
        mutate(
          outcome = outcome_name,
          exposure = exposure_name,
          spec = spec_name,
          event_time_q = as.integer(str_match(term, "event_time_q::(-?[0-9]+)")[, 2]),
          conf_low = estimate - 1.96 * std.error,
          conf_high = estimate + 1.96 * std.error,
          n = nobs(model)
        ) %>%
        select(outcome, exposure, spec, event_time_q, estimate, std.error, conf_low, conf_high, p.value, n)

      lead_terms <- tidy_tbl %>% filter(event_time_q < 0)
      pretrend_terms <- paste0("event_time_q::", lead_terms$event_time_q, ":", exposure_var)

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
        filter(event_time_q >= 0) %>%
        arrange(p.value, desc(abs(estimate))) %>%
        slice(1)

      summary_out[[length(summary_out) + 1]] <- tibble(
        outcome = outcome_name,
        exposure = exposure_name,
        spec = spec_name,
        n = nobs(model),
        pretrend_f = pretrend_f,
        pretrend_p = pretrend_p,
        post_peak_event = if (nrow(post_peak) == 0) NA_integer_ else post_peak$event_time_q[[1]],
        post_peak_coef = if (nrow(post_peak) == 0) NA_real_ else post_peak$estimate[[1]],
        post_peak_se = if (nrow(post_peak) == 0) NA_real_ else post_peak$std.error[[1]],
        post_peak_p = if (nrow(post_peak) == 0) NA_real_ else post_peak$p.value[[1]]
      )

      terms_out[[length(terms_out) + 1]] <- tidy_tbl
    }
  }
}

terms_df <- bind_rows(terms_out) %>% arrange(outcome, exposure, spec, event_time_q)
summary_df <- bind_rows(summary_out) %>% arrange(pretrend_p, post_peak_p)

write.table(terms_df, here("data", "powerIV", "regressions", "auxilio_gas_event_study_gas", "event_study_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, here("data", "powerIV", "regressions", "auxilio_gas_event_study_gas", "event_study_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~exposure, ~spec, ~file_stub, ~title,
  "gas_any", "people", "M2", "event_study_gas_any_auxilio_gas", "Gas Any: Event Study Around Auxilio Gas Top-Up",
  "gas_bottled", "people", "M2", "event_study_gas_bottled_auxilio_gas", "Bottled Gas: Event Study Around Auxilio Gas Top-Up"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(
      outcome == plot_specs$outcome[[i]],
      exposure == plot_specs$exposure[[i]],
      spec == plot_specs$spec[[i]]
    )

  p <- ggplot(plot_df, aes(x = event_time_q, y = estimate)) +
    geom_hline(yintercept = 0, color = "black") +
    geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, color = "#1f78b4") +
    geom_point(size = 2.2, color = "#1f78b4") +
    scale_x_continuous(breaks = sort(unique(plot_df$event_time_q))) +
    labs(
      x = "Quarters Relative to 2022Q3",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = "Reference quarter is 2022Q2; exposure is pre-top-up people per weighted household"
    ) +
    theme_minimal(base_size = 12)

  ggsave(here("Figures", "powerIV", "auxilio_gas", paste0(plot_specs$file_stub[[i]], ".png")), p, width = 9, height = 5.5, dpi = 300)
  ggsave(here("Figures", "powerIV", "auxilio_gas", paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 9, height = 5.5)
}

print(summary_df)
