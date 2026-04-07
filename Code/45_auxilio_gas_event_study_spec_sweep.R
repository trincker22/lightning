suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(stringr)
})

out_dir <- here("data", "powerIV", "regressions", "auxilio_gas_event_study_spec_sweep")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

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

lfp <- read_parquet(here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    year_quarter = sprintf("%dQ%d", year, quarter),
    female_hours_effective_employed
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
  left_join(lfp, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(auxgas_q, by = c("estrato4", "year", "quarter", "year_quarter")) %>%
  left_join(baseline_shift, by = "estrato4") %>%
  mutate(
    quarter_index = year * 4 + quarter,
    auxgas_families_per_weighted_household = auxgas_families_per_payment / weight_sum,
    auxgas_people_per_weighted_household = auxgas_people_per_payment / weight_sum,
    auxgas_value_per_weighted_household = auxgas_total_value_per_payment / weight_sum,
    b_dep_t = baseline_child_dependency * (year - 2016)
  ) %>%
  filter(year >= 2021, year <= 2024) %>%
  filter(!is.na(weight_sum), weight_sum > 0) %>%
  arrange(estrato4, quarter_index) %>%
  group_by(estrato4) %>%
  mutate(
    wood_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_wood_charcoal), NA_real_),
    gas_any_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_gas_any), NA_real_),
    gas_bottled_l1 = if_else(quarter_index - lag(quarter_index) == 1, lag(share_cooking_gas_bottled), NA_real_)
  ) %>%
  ungroup()

pre_exposure <- panel %>%
  filter((year == 2021 & quarter == 4) | (year == 2022 & quarter <= 2)) %>%
  group_by(estrato4) %>%
  summarise(
    pre_people_exposure = mean(auxgas_people_per_weighted_household, na.rm = TRUE),
    pre_families_exposure = mean(auxgas_families_per_weighted_household, na.rm = TRUE),
    .groups = "drop"
  )

panel <- panel %>% left_join(pre_exposure, by = "estrato4")

quarter_index_from_label <- function(label) {
  yr <- as.integer(substr(label, 1, 4))
  q <- as.integer(substr(label, 6, 6))
  yr * 4 + q
}

bin_event_time <- function(x) {
  case_when(
    x <= -2 ~ -2,
    x == -1 ~ -1,
    x %in% c(0, 1) ~ 0,
    x %in% c(2, 3, 4) ~ 2,
    x >= 5 ~ 5,
    TRUE ~ NA_real_
  )
}

outcomes <- tibble::tribble(
  ~outcome_name, ~outcome_var,
  "gas_any", "share_cooking_gas_any",
  "gas_bottled", "share_cooking_gas_bottled",
  "wood", "share_cooking_wood_charcoal",
  "female_hours_employed", "female_hours_effective_employed"
)

exposures <- tibble::tribble(
  ~exposure_name, ~exposure_var,
  "people", "pre_people_exposure"
)

specs <- tibble::tribble(
  ~spec, ~controls,
  "M1", "1",
  "M2", "1 + mean_rooms + mean_bedrooms + b_dep_t + share_grid_full_time"
)

anchors <- tibble::tribble(
  ~anchor_name, ~anchor_label,
  "topup_q3", "2022Q3",
  "cleanpost_q4", "2022Q4"
)

windows <- tibble::tribble(
  ~window_name, ~window_start,
  "w2021q4", "2021Q4",
  "w2022q1", "2022Q1"
)

dynamics <- tibble::tribble(
  ~dynamic_name,
  "quarterly",
  "binned"
)

terms_out <- list()
summary_out <- list()

for (a in seq_len(nrow(anchors))) {
  anchor_label <- anchors$anchor_label[[a]]
  anchor_name <- anchors$anchor_name[[a]]
  anchor_index <- quarter_index_from_label(anchor_label)

  for (w in seq_len(nrow(windows))) {
    window_name <- windows$window_name[[w]]
    window_start <- quarter_index_from_label(windows$window_start[[w]])

    base_data <- panel %>%
      mutate(event_time_q = quarter_index - anchor_index) %>%
      filter(quarter_index >= window_start, quarter_index <= quarter_index_from_label("2024Q4"))

    for (d in seq_len(nrow(dynamics))) {
      dynamic_name <- dynamics$dynamic_name[[d]]

      data_dyn <- base_data
      event_var <- "event_time_q"
      if (dynamic_name == "quarterly") {
        data_dyn <- data_dyn %>% filter(event_time_q >= -3, event_time_q <= 8)
      } else {
        data_dyn <- data_dyn %>%
          filter(event_time_q >= -3, event_time_q <= 8) %>%
          mutate(event_bin = bin_event_time(event_time_q))
        event_var <- "event_bin"
      }

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
              "estrato4", "year_quarter", "weight_sum", event_var, outcome_var, exposure_var,
              all.vars(as.formula(paste("~", controls)))
            ))

            reg_data <- data_dyn %>% filter(if_all(all_of(vars_needed), ~ !is.na(.)))
            if (nrow(reg_data) == 0) next

            model <- feols(
              as.formula(paste0(outcome_var, " ~ i(", event_var, ", ", exposure_var, ", ref = -1) + ", controls, " | estrato4 + year_quarter")),
              data = reg_data,
              weights = ~weight_sum,
              cluster = ~estrato4
            )

            tidy_tbl <- tidy(model) %>%
              filter(str_detect(term, paste0(event_var, "::"))) %>%
              mutate(
                anchor = anchor_name,
                window = window_name,
                dynamic = dynamic_name,
                outcome = outcome_name,
                exposure = exposure_name,
                spec = spec_name,
                event_value = as.integer(str_match(term, paste0(event_var, "::(-?[0-9]+)"))[, 2]),
                conf_low = estimate - 1.96 * std.error,
                conf_high = estimate + 1.96 * std.error,
                n = nobs(model)
              ) %>%
              select(anchor, window, dynamic, outcome, exposure, spec, event_value, estimate, std.error, conf_low, conf_high, p.value, n)

            lead_terms <- tidy_tbl %>% filter(event_value < 0)
            pretrend_terms <- paste0(event_var, "::", lead_terms$event_value, ":", exposure_var)

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

            first_post <- tidy_tbl %>% filter(event_value == 0) %>% slice(1)
            medium_post <- tidy_tbl %>% filter(event_value == 2) %>% slice(1)
            strongest_post <- tidy_tbl %>% filter(event_value >= 0) %>% arrange(p.value, desc(abs(estimate))) %>% slice(1)

            summary_out[[length(summary_out) + 1]] <- tibble(
              anchor = anchor_name,
              window = window_name,
              dynamic = dynamic_name,
              outcome = outcome_name,
              exposure = exposure_name,
              spec = spec_name,
              n = nobs(model),
              pretrend_f = pretrend_f,
              pretrend_p = pretrend_p,
              first_post_event = if (nrow(first_post) == 0) NA_integer_ else first_post$event_value[[1]],
              first_post_coef = if (nrow(first_post) == 0) NA_real_ else first_post$estimate[[1]],
              first_post_se = if (nrow(first_post) == 0) NA_real_ else first_post$std.error[[1]],
              first_post_p = if (nrow(first_post) == 0) NA_real_ else first_post$p.value[[1]],
              medium_post_event = if (nrow(medium_post) == 0) NA_integer_ else medium_post$event_value[[1]],
              medium_post_coef = if (nrow(medium_post) == 0) NA_real_ else medium_post$estimate[[1]],
              medium_post_se = if (nrow(medium_post) == 0) NA_real_ else medium_post$std.error[[1]],
              medium_post_p = if (nrow(medium_post) == 0) NA_real_ else medium_post$p.value[[1]],
              strongest_post_event = if (nrow(strongest_post) == 0) NA_integer_ else strongest_post$event_value[[1]],
              strongest_post_coef = if (nrow(strongest_post) == 0) NA_real_ else strongest_post$estimate[[1]],
              strongest_post_p = if (nrow(strongest_post) == 0) NA_real_ else strongest_post$p.value[[1]]
            )

            terms_out[[length(terms_out) + 1]] <- tidy_tbl
          }
        }
      }
    }
  }
}

terms_df <- bind_rows(terms_out) %>% arrange(outcome, anchor, window, dynamic, spec, event_value)
summary_df <- bind_rows(summary_out) %>% arrange(pretrend_p, strongest_post_p)

write.table(terms_df, file.path(out_dir, "spec_sweep_terms.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(summary_df, file.path(out_dir, "spec_sweep_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

print(summary_df)
