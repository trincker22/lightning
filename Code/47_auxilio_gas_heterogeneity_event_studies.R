suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
  library(stringr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_heterogeneity")
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
    share_cooking_wood_charcoal,
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

baseline_wood <- visit1 %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(
    baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE),
    .groups = "drop"
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
  left_join(baseline_wood, by = "estrato4") %>%
  mutate(
    auxgas_families_per_weighted_household = auxgas_families_per_payment / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people_per_payment / weight_sum,
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
    .groups = "drop"
  )

panel <- panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  mutate(
    baseline_wood_quartile = ntile(baseline_wood_2016, 4),
    pre_exposure_quartile = ntile(pre_people_exposure, 4)
  ) %>%
  filter(event_time_q >= -3, event_time_q <= 9)

outcomes <- tibble::tribble(
  ~outcome_name, ~outcome_var,
  "gas_any", "share_cooking_gas_any",
  "gas_bottled", "share_cooking_gas_bottled",
  "wood", "share_cooking_wood_charcoal"
)

groups <- tibble::tribble(
  ~group_name, ~group_var,
  "baseline_wood", "baseline_wood_quartile",
  "pre_exposure", "pre_exposure_quartile"
)

terms_out <- list()
summary_out <- list()

for (g in seq_len(nrow(groups))) {
  group_name <- groups$group_name[[g]]
  group_var <- groups$group_var[[g]]

  for (q in 1:4) {
    group_data <- panel %>% filter(.data[[group_var]] == q)

    for (i in seq_len(nrow(outcomes))) {
      outcome_name <- outcomes$outcome_name[[i]]
      outcome_var <- outcomes$outcome_var[[i]]

      vars_needed <- c(
        "estrato4", "year_quarter", "weight_sum", "event_time_q", "pre_people_exposure",
        outcome_var, "mean_rooms", "mean_bedrooms", "b_dep_t", "share_grid_full_time"
      )

      reg_data <- group_data %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))
      if (nrow(reg_data) == 0) next

      model <- feols(
        as.formula(paste0(outcome_var, " ~ i(event_time_q, pre_people_exposure, ref = -1) + 1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time | estrato4 + year_quarter")),
        data = reg_data,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      tidy_tbl <- tidy(model) %>%
        filter(str_detect(term, "event_time_q::")) %>%
        mutate(
          grouping = group_name,
          quartile = q,
          outcome = outcome_name,
          event_time_q = as.integer(str_match(term, "event_time_q::(-?[0-9]+)")[, 2]),
          conf_low = estimate - 1.96 * std.error,
          conf_high = estimate + 1.96 * std.error,
          n = nobs(model)
        ) %>%
        select(grouping, quartile, outcome, event_time_q, estimate, std.error, conf_low, conf_high, p.value, n)

      lead_terms <- tidy_tbl %>% filter(event_time_q < 0)
      pretrend_terms <- paste0("event_time_q::", lead_terms$event_time_q, ":pre_people_exposure")

      pretrend_p <- NA_real_
      pretrend_f <- NA_real_
      if (length(pretrend_terms) > 0) {
        wald_out <- tryCatch({
          capture.output(wald_obj <- wald(model, pretrend_terms))
          wald_obj
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

      strongest_post <- tidy_tbl %>%
        filter(event_time_q >= 0) %>%
        arrange(p.value, desc(abs(estimate))) %>%
        slice(1)

      summary_out[[length(summary_out) + 1]] <- tibble(
        grouping = group_name,
        quartile = q,
        outcome = outcome_name,
        n = nobs(model),
        pretrend_f = pretrend_f,
        pretrend_p = pretrend_p,
        strongest_post_event = if (nrow(strongest_post) == 0) NA_integer_ else strongest_post$event_time_q[[1]],
        strongest_post_coef = if (nrow(strongest_post) == 0) NA_real_ else strongest_post$estimate[[1]],
        strongest_post_se = if (nrow(strongest_post) == 0) NA_real_ else strongest_post$std.error[[1]],
        strongest_post_p = if (nrow(strongest_post) == 0) NA_real_ else strongest_post$p.value[[1]]
      )

      terms_out[[length(terms_out) + 1]] <- tidy_tbl
    }
  }
}

terms_df <- bind_rows(terms_out) %>% arrange(grouping, outcome, quartile, event_time_q)
summary_df <- bind_rows(summary_out) %>% arrange(grouping, outcome, quartile)

write.table(terms_df, here("data", "powerIV", "regressions", "auxilio_gas_heterogeneity", "heterogeneity_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, here("data", "powerIV", "regressions", "auxilio_gas_heterogeneity", "heterogeneity_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

plot_specs <- tibble::tribble(
  ~grouping, ~outcome, ~file_stub, ~title,
  "baseline_wood", "gas_any", "heterogeneity_baseline_wood_gas_any", "Gas Any by Baseline Wood Quartile",
  "baseline_wood", "gas_bottled", "heterogeneity_baseline_wood_gas_bottled", "Bottled Gas by Baseline Wood Quartile",
  "pre_exposure", "gas_any", "heterogeneity_pre_exposure_gas_any", "Gas Any by Pre-Top-Up Exposure Quartile",
  "pre_exposure", "gas_bottled", "heterogeneity_pre_exposure_gas_bottled", "Bottled Gas by Pre-Top-Up Exposure Quartile"
)

for (i in seq_len(nrow(plot_specs))) {
  plot_df <- terms_df %>%
    filter(grouping == plot_specs$grouping[[i]], outcome == plot_specs$outcome[[i]])

  p <- ggplot(plot_df, aes(x = event_time_q, y = estimate)) +
    geom_hline(yintercept = 0, color = "black") +
    geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, color = "#1f78b4") +
    geom_point(size = 1.8, color = "#1f78b4") +
    facet_wrap(~quartile, ncol = 2) +
    scale_x_continuous(breaks = sort(unique(plot_df$event_time_q))) +
    labs(
      x = "Quarters Relative to 2022Q3",
      y = "Coefficient",
      title = plot_specs$title[[i]],
      subtitle = "Reference quarter is 2022Q2; exposure is pre-top-up people per weighted household"
    ) +
    theme_minimal(base_size = 12)

  ggsave(here("Figures", "powerIV", "auxilio_gas", paste0(plot_specs$file_stub[[i]], ".png")), p, width = 10, height = 7, dpi = 300)
  ggsave(here("Figures", "powerIV", "auxilio_gas", paste0(plot_specs$file_stub[[i]], ".pdf")), p, width = 10, height = 7)
}

print(summary_df)
