suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(stringr)
  library(tibble)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_monthly_novo_caged_event_study")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

novo <- read_parquet(here("data", "powerIV", "novo_caged", "novo_caged_estrato_month.parquet")) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter = ((month - 1) %/% 3) + 1,
    year_month = sprintf("%04d-%02d", year, month),
    event_time_m = (year - 2022) * 12 + (month - 8)
  )

auxgas <- read_parquet(here("data", "powerIV", "monthly_source_audit", "auxgas_estrato_month_2021_2025.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    month,
    quarter = ((month - 1) %/% 3) + 1,
    auxgas_people,
    auxgas_families,
    auxgas_total_value
  )

visit1 <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    weight_sum
  )

panel <- novo %>%
  left_join(visit1, by = c("estrato4", "year", "quarter")) %>%
  left_join(auxgas, by = c("estrato4", "year", "month", "quarter")) %>%
  mutate(
    female_net_per_weighted_household = female_net / weight_sum,
    female_admissions_per_weighted_household = female_admissions / weight_sum,
    female_separations_per_weighted_household = female_separations / weight_sum,
    total_net_per_weighted_household = total_net / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people / weight_sum,
    auxgas_families_per_weighted_household = auxgas_families / weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value / weight_sum
  ) %>%
  filter(event_time_m >= -8, event_time_m <= 14)

pre_exposure <- panel %>%
  filter((year == 2021 & month == 12) | (year == 2022 & month %in% c(2, 4, 6))) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_exposure = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    pre_families_exposure = mean(auxgas_families_per_weighted_household, na.rm = TRUE),
    pre_value_exposure = mean(auxgas_value_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

panel <- panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  filter(!is.na(weight_sum), weight_sum > 0)

outcomes <- tribble(
  ~outcome_name, ~outcome_var,
  "female_net", "female_net_per_weighted_household",
  "female_admissions", "female_admissions_per_weighted_household",
  "female_separations", "female_separations_per_weighted_household"
)

exposures <- tribble(
  ~exposure_name, ~exposure_var,
  "people", "pre_people_exposure",
  "families", "pre_families_exposure"
)

terms_out <- list()
summary_out <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]

  for (j in seq_len(nrow(exposures))) {
    exposure_name <- exposures$exposure_name[[j]]
    exposure_var <- exposures$exposure_var[[j]]

    vars_needed <- c("estrato4", "year_month", "event_time_m", outcome_var, exposure_var)

    reg_data <- panel %>%
      filter(if_all(all_of(vars_needed), ~ !is.na(.)))

    model <- feols(
      as.formula(paste0(outcome_var, " ~ i(event_time_m, ", exposure_var, ", ref = -1) | estrato4 + year_month")),
      data = reg_data,
      cluster = ~estrato4
    )

    tidy_tbl <- tidy(model) %>%
      filter(str_detect(term, "event_time_m::")) %>%
      mutate(
        outcome = outcome_name,
        exposure = exposure_name,
        event_time_m = as.integer(str_match(term, "event_time_m::(-?[0-9]+)")[, 2]),
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(outcome, exposure, event_time_m, estimate, std.error, conf_low, conf_high, p.value, n)

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

terms_df <- bind_rows(terms_out) %>% arrange(outcome, exposure, event_time_m)
summary_df <- bind_rows(summary_out) %>% arrange(outcome, exposure)

write.table(terms_df, file.path(out_dir, "monthly_novo_caged_event_study_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, file.path(out_dir, "monthly_novo_caged_event_study_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(pre_exposure, file.path(out_dir, "monthly_novo_caged_pre_exposure.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tribble(
  ~outcome, ~exposure, ~file_stub, ~title,
  "female_net", "people", "monthly_event_study_female_net_novo_caged_auxgas", "Female Net Formal Employment: Monthly Event Study",
  "female_admissions", "people", "monthly_event_study_female_admissions_novo_caged_auxgas", "Female Formal Admissions: Monthly Event Study",
  "female_separations", "people", "monthly_event_study_female_separations_novo_caged_auxgas", "Female Formal Separations: Monthly Event Study"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(outcome == plot_specs$outcome[[i]], exposure == plot_specs$exposure[[i]])

  p <- ggplot(plot_df, aes(x = event_time_m, y = estimate)) +
    geom_hline(yintercept = 0, color = "black", linewidth = 0.9) +
    geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray45", linewidth = 0.8) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#2C7FB8", linewidth = 0.7) +
    geom_point(size = 2.8, color = "#2C7FB8") +
    scale_x_continuous(breaks = seq(min(plot_df$event_time_m), max(plot_df$event_time_m), by = 2)) +
    labs(
      x = "Months Relative to 2022-08",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = "Outcome is per weighted household; exposure is pre-top-up people per weighted household"
    ) +
    theme_minimal(base_size = 16) +
    theme(
      plot.title = element_text(size = 21, face = "plain"),
      plot.subtitle = element_text(size = 12.5, color = "black"),
      axis.title = element_text(size = 16),
      axis.text = element_text(size = 11.5),
      panel.grid.major = element_line(color = "#DDDDDD", linewidth = 0.8),
      panel.grid.minor = element_line(color = "#E9E9E9", linewidth = 0.5)
    )

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 13.5, height = 7.2, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 13.5, height = 7.2)
}

print(summary_df)
