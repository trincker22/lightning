suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(ggplot2)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_normalized_topup_checks")
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
    n_households,
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
    auxgas_value_per_family = auxgas_total_value_per_payment / auxgas_families_per_payment,
    b_dep_t = baseline_child_dependency * (year - 2016),
    post_topup_2022q3 = as.integer(year > 2022 | (year == 2022 & quarter >= 3))
  ) %>%
  arrange(estrato4, year, quarter) %>%
  group_by(estrato4) %>%
  mutate(
    quarter_index = year * 4 + quarter,
    wood_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_wood_charcoal), NA_real_),
    gas_any_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_gas_any), NA_real_),
    gas_bottled_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_gas_bottled), NA_real_)
  ) %>%
  ungroup() %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

pre_exposure <- panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_auxgas_families_per_weighted_household = mean(auxgas_families_per_weighted_household, na.rm = TRUE),
    pre_auxgas_people_per_weighted_household = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    pre_auxgas_value_per_weighted_household = mean(auxgas_value_per_weighted_household, na.rm = TRUE),
    pre_auxgas_value_per_family = mean(auxgas_value_per_family, na.rm = TRUE),
    .groups = "drop"
  )

panel <- panel %>%
  left_join(pre_exposure, by = "estrato4") %>%
  mutate(
    z_families_hh_post = pre_auxgas_families_per_weighted_household * post_topup_2022q3,
    z_people_hh_post = pre_auxgas_people_per_weighted_household * post_topup_2022q3,
    z_value_hh_post = pre_auxgas_value_per_weighted_household * post_topup_2022q3
  )

validation <- panel %>%
  filter(!is.na(pre_auxgas_value_per_weighted_household), !is.na(auxgas_value_per_weighted_household)) %>%
  mutate(exposure_quartile = ntile(pre_auxgas_value_per_weighted_household, 4)) %>%
  group_by(year, quarter, year_quarter, exposure_quartile) %>%
  summarise(
    mean_value_per_weighted_household = weighted.mean(auxgas_value_per_weighted_household, w = weight_sum, na.rm = TRUE),
    mean_families_per_weighted_household = weighted.mean(auxgas_families_per_weighted_household, w = weight_sum, na.rm = TRUE),
    estratos = n(),
    .groups = "drop"
  ) %>%
  mutate(quarter_date = as.Date(sprintf("%d-%02d-01", year, c(1, 4, 7, 10)[quarter])))

plot_value <- ggplot(validation, aes(x = quarter_date, y = mean_value_per_weighted_household, color = factor(exposure_quartile))) +
  geom_line(linewidth = 0.9) +
  geom_vline(xintercept = as.Date("2022-07-01"), linetype = "dashed", color = "black") +
  scale_color_brewer(palette = "Dark2", name = "Pre-top-up quartile") +
  labs(
    x = NULL,
    y = "Auxilio Gas transfer per weighted household",
    title = "Auxilio Gas Intensity by Pre-Top-Up Exposure Quartile"
  ) +
  theme_minimal(base_size = 12)

ggsave(here("Figures", "powerIV", "auxilio_gas", "auxilio_gas_value_per_weighted_household_quartiles_2021_2024.png"), plot_value, width = 10, height = 6, dpi = 300)

ggsave(here("Figures", "powerIV", "auxilio_gas", "auxilio_gas_value_per_weighted_household_quartiles_2021_2024.pdf"), plot_value, width = 10, height = 6)

outcomes <- tibble::tribble(
  ~outcome_name, ~outcome_var,
  "wood_t", "share_cooking_wood_charcoal",
  "wood_l1", "wood_l1",
  "gas_any_t", "share_cooking_gas_any",
  "gas_any_l1", "gas_any_l1",
  "gas_bottled_t", "share_cooking_gas_bottled",
  "gas_bottled_l1", "gas_bottled_l1"
)

treatments <- tibble::tribble(
  ~treatment_name, ~treatment_var,
  "families_hh_post", "z_families_hh_post",
  "people_hh_post", "z_people_hh_post",
  "value_hh_post", "z_value_hh_post"
)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
)

term_row <- function(model, term) {
  tbl <- tidy(model)
  row <- tbl[tbl$term == term, , drop = FALSE]
  if (nrow(row) == 0) return(tibble(estimate = NA_real_, std.error = NA_real_, p.value = NA_real_))
  tibble(estimate = row$estimate[[1]], std.error = row$std.error[[1]], p.value = row$p.value[[1]])
}

wald_f <- function(model, term) {
  out <- tryCatch({
    capture.output(w <- wald(model, term))
    w
  }, error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  stat <- suppressWarnings(as.numeric(unlist(out$stat)))
  stat <- stat[!is.na(stat)]
  if (length(stat) == 0) return(NA_real_)
  stat[[1]]
}

results <- list()

for (i in seq_len(nrow(outcomes))) {
  outcome_name <- outcomes$outcome_name[[i]]
  outcome_var <- outcomes$outcome_var[[i]]

  for (j in seq_len(nrow(treatments))) {
    treatment_name <- treatments$treatment_name[[j]]
    treatment_var <- treatments$treatment_var[[j]]

    for (k in seq_len(nrow(specs))) {
      spec_name <- specs$spec[[k]]
      controls <- specs$controls[[k]]

      vars_needed <- unique(c(
        "estrato4", "year_quarter", "weight_sum", outcome_var, treatment_var,
        all.vars(as.formula(paste("~", controls)))
      ))

      reg_data <- panel %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))

      model <- feols(
        as.formula(paste0(outcome_var, " ~ ", treatment_var, " + ", controls, " | estrato4 + year_quarter")),
        data = reg_data,
        weights = ~weight_sum,
        cluster = ~estrato4
      )

      term <- term_row(model, treatment_var)

      results[[length(results) + 1]] <- tibble(
        outcome = outcome_name,
        treatment = treatment_name,
        spec = spec_name,
        n = nobs(model),
        coef = term$estimate,
        se = term$std.error,
        p = term$p.value,
        f = wald_f(model, treatment_var)
      )
    }
  }
}

results_df <- bind_rows(results) %>% arrange(p)
write.table(results_df, here("data", "powerIV", "regressions", "auxilio_gas_normalized_topup_checks", "normalized_topup_results.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

coverage_df <- panel %>%
  summarise(
    n_rows = n(),
    n_estratos = n_distinct(estrato4),
    mean_pre_families_per_weighted_household = mean(pre_auxgas_families_per_weighted_household, na.rm = TRUE),
    sd_pre_families_per_weighted_household = sd(pre_auxgas_families_per_weighted_household, na.rm = TRUE),
    mean_pre_value_per_weighted_household = mean(pre_auxgas_value_per_weighted_household, na.rm = TRUE),
    sd_pre_value_per_weighted_household = sd(pre_auxgas_value_per_weighted_household, na.rm = TRUE),
    mean_pre_value_per_family = mean(pre_auxgas_value_per_family, na.rm = TRUE),
    sd_pre_value_per_family = sd(pre_auxgas_value_per_family, na.rm = TRUE)
  )
write.table(coverage_df, here("data", "powerIV", "regressions", "auxilio_gas_normalized_topup_checks", "normalized_topup_coverage.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(validation, here("data", "powerIV", "regressions", "auxilio_gas_normalized_topup_checks", "value_per_weighted_household_quartile_series.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

print(coverage_df)
print(results_df)
