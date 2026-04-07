suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(tibble)
  library(seasonal)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_monthly_realized_intensity_x13_sa")
fig_dir <- here("Figures", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

novo <- read_parquet(here("data", "powerIV", "novo_caged", "novo_caged_estrato_month.parquet")) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter = ((month - 1) %/% 3) + 1,
    year_month = sprintf("%04d-%02d", year, month),
    date = as.Date(sprintf("%04d-%02d-01", year, month))
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
    auxgas_people = coalesce(auxgas_people, 0),
    auxgas_families = coalesce(auxgas_families, 0),
    auxgas_total_value = coalesce(auxgas_total_value, 0),
    female_net_per_weighted_household = female_net / weight_sum,
    female_admissions_per_weighted_household = female_admissions / weight_sum,
    female_separations_per_weighted_household = female_separations / weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people / weight_sum
  ) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  filter(date >= as.Date("2022-01-01"), date <= as.Date("2024-12-01")) %>%
  arrange(estrato4, date)

sa_one <- function(df, value_var, out_var) {
  x <- df[[value_var]]
  ts_x <- ts(x, start = c(df$year[[1]], df$month[[1]]), frequency = 12)
  fit <- seas(
    ts_x,
    x11 = "",
    transform.function = "none",
    regression.aictest = NULL,
    outlier = NULL,
    automdl = NULL,
    arima.model = "(0 1 1)(0 1 1)"
  )
  df[[out_var]] <- as.numeric(final(fit))
  df
}

sa_panel <- panel %>%
  group_by(estrato4) %>%
  group_modify(~ sa_one(.x, "female_net_per_weighted_household", "female_net_sa")) %>%
  group_modify(~ sa_one(.x, "female_admissions_per_weighted_household", "female_admissions_sa")) %>%
  group_modify(~ sa_one(.x, "female_separations_per_weighted_household", "female_separations_sa")) %>%
  ungroup() %>%
  group_by(estrato4) %>%
  mutate(
    lag1_auxgas_value_per_weighted_household = lag(auxgas_value_per_weighted_household, 1),
    lag2_auxgas_value_per_weighted_household = lag(auxgas_value_per_weighted_household, 2),
    lag1_auxgas_people_per_weighted_household = lag(auxgas_people_per_weighted_household, 1),
    lag2_auxgas_people_per_weighted_household = lag(auxgas_people_per_weighted_household, 2)
  ) %>%
  ungroup()

outcomes <- tribble(
  ~outcome_name, ~outcome_var,
  "female_net", "female_net_sa",
  "female_admissions", "female_admissions_sa",
  "female_separations", "female_separations_sa"
)

specs <- tribble(
  ~spec, ~rhs,
  "value_sa", "auxgas_value_per_weighted_household + lag1_auxgas_value_per_weighted_household + lag2_auxgas_value_per_weighted_household",
  "people_sa", "auxgas_people_per_weighted_household + lag1_auxgas_people_per_weighted_household + lag2_auxgas_people_per_weighted_household"
)

coef_map <- tribble(
  ~term, ~lag_label,
  "auxgas_value_per_weighted_household", "t",
  "lag1_auxgas_value_per_weighted_household", "t-1",
  "lag2_auxgas_value_per_weighted_household", "t-2",
  "auxgas_people_per_weighted_household", "t",
  "lag1_auxgas_people_per_weighted_household", "t-1",
  "lag2_auxgas_people_per_weighted_household", "t-2"
)

terms_out <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]

  for (j in seq_len(nrow(specs))) {
    spec_name <- specs$spec[[j]]
    rhs <- specs$rhs[[j]]

    reg_data <- sa_panel %>% filter(!is.na(.data[[outcome_var]]))

    model <- feols(
      as.formula(paste0(outcome_var, " ~ ", rhs, " | estrato4 + year_month")),
      data = reg_data,
      cluster = ~estrato4
    )

    terms_out[[length(terms_out) + 1]] <- tidy(model) %>%
      filter(term %in% coef_map$term) %>%
      left_join(coef_map, by = "term") %>%
      mutate(
        outcome = outcome_name,
        spec = spec_name,
        conf_low = estimate - 1.96 * std.error,
        conf_high = estimate + 1.96 * std.error,
        n = nobs(model)
      ) %>%
      select(outcome, spec, term, lag_label, estimate, std.error, conf_low, conf_high, p.value, n)
  }
}

terms_df <- bind_rows(terms_out) %>% arrange(outcome, spec, lag_label)
summary_df <- terms_df %>% select(outcome, spec, lag_label, estimate, std.error, p.value, n)

write.table(terms_df, file.path(out_dir, "monthly_realized_intensity_x13_sa_terms.tsv"), sep = "	", row.names = FALSE, quote = FALSE)
write.table(summary_df, file.path(out_dir, "monthly_realized_intensity_x13_sa_summary.tsv"), sep = "	", row.names = FALSE, quote = FALSE)
write_parquet(sa_panel, file.path(out_dir, "monthly_realized_intensity_x13_sa_panel.parquet"))

plot_specs <- tribble(
  ~outcome, ~file_stub, ~title,
  "female_net", "monthly_realized_value_x13_sa_female_net_novo_caged_full", "Female Net Formal Employment and Auxilio Gas Intensity",
  "female_admissions", "monthly_realized_value_x13_sa_female_admissions_novo_caged_full", "Female Formal Admissions and Auxilio Gas Intensity",
  "female_separations", "monthly_realized_value_x13_sa_female_separations_novo_caged_full", "Female Formal Separations and Auxilio Gas Intensity"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>% filter(outcome == plot_specs$outcome[[i]], spec == "value_sa")

  p <- ggplot(plot_df, aes(x = lag_label, y = estimate)) +
    geom_hline(yintercept = 0, color = "black", linewidth = 0.9) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, color = "#2C7FB8", linewidth = 0.8) +
    geom_point(size = 3, color = "#2C7FB8") +
    labs(
      x = "Auxilio Gas Timing",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = "Outcome is X-13 seasonally adjusted at the estrato-month level; full monthly Novo Caged panel through 2024-12"
    ) +
    theme_minimal(base_size = 16) +
    theme(
      plot.title = element_text(size = 21, face = "plain"),
      plot.subtitle = element_text(size = 12.5, color = "black"),
      axis.title = element_text(size = 16),
      axis.text = element_text(size = 12),
      panel.grid.major = element_line(color = "#DDDDDD", linewidth = 0.8),
      panel.grid.minor = element_line(color = "#E9E9E9", linewidth = 0.5)
    )

  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".png")), p, width = 10.5, height = 6.2, dpi = 300)
  ggsave(file.path(fig_dir, paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 10.5, height = 6.2)
}

print(summary_df)
