suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_topup_spec_changes")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

auxgas_q <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_estrato_quarter_2021_2025.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter,
    auxgas_families,
    auxgas_total_value,
    auxgas_people,
    auxgas_avg_benefit
  )

lfp <- read_parquet(here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    weight_sum,
    female_hours_effective_employed
  )

power <- read_parquet(here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
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

pre_exposure <- auxgas_q %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_auxgas_families = mean(auxgas_families, na.rm = TRUE),
    pre_auxgas_total_value = mean(auxgas_total_value, na.rm = TRUE),
    pre_auxgas_people = mean(auxgas_people, na.rm = TRUE),
    pre_auxgas_avg_benefit = mean(auxgas_avg_benefit, na.rm = TRUE),
    .groups = "drop"
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(pre_exposure, by = "estrato4") %>%
  left_join(baseline_shift, by = "estrato4") %>%
  mutate(
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
  mutate(
    z_families_post = pre_auxgas_families * post_topup_2022q3,
    z_value_post = pre_auxgas_total_value * post_topup_2022q3,
    z_benefit_post = pre_auxgas_avg_benefit * post_topup_2022q3
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0)

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
  "families_post", "z_families_post",
  "value_post", "z_value_post",
  "benefit_post", "z_benefit_post"
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
  out <- tryCatch(wald(model, term), error = function(e) NULL)
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
write.table(results_df, here("data", "powerIV", "regressions", "auxilio_gas_topup_spec_changes", "spec_change_results.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
print(results_df)
