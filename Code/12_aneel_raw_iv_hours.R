#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(fixest)
  library(broom)
  library(readr)
})

root_dir <- here::here()

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")

out_dir <- here::here("data", "powerIV", "regressions", "aneel_raw_iv_hours")
table_dir <- here::here(out_dir, "tables")
dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

results_path <- here::here(table_dir, "aneel_raw_iv_hours_results.csv")
qa_path <- here::here(table_dir, "aneel_raw_iv_hours_qa.csv")

lfp <- read_parquet(lfp_panel_path) %>%
  mutate(estrato4 = as.character(estrato4))

power_controls <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_vehicle,
    share_internet_home,
    share_electricity_from_grid,
    share_grid_full_time
  )

panel <- lfp %>%
  left_join(power_controls, by = c("estrato4", "year", "quarter")) %>%
  filter(
    !is.na(weight_sum),
    weight_sum > 0,
    !is.na(share_vehicle),
    !is.na(share_internet_home),
    !is.na(share_electricity_from_grid),
    !is.na(share_grid_full_time)
  )

controls <- "share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time"
hours_outcomes <- c("female_hours_effective_employed", "female_hours_effective_population")

fe_specs <- tibble::tribble(
  ~fe_name, ~fe_formula,
  "year_quarter", "year + quarter",
  "uf_year_quarter", "uf + year + quarter",
  "estrato_year_quarter", "estrato4 + year + quarter",
  "estrato_year", "estrato4 + year",
  "estrato_quarter", "estrato4 + quarter"
)

lag_specs <- tibble::tribble(
  ~lag_name, ~endog_var, ~instr_var,
  "current", "raw_quarterly_outage_hrs_per_1000", "lightning_quarterly_total",
  "lag4", "raw_quarterly_outage_hrs_per_1000_l4", "lightning_quarterly_total_l4"
)

safe_stat <- function(x) {
  y <- suppressWarnings(as.numeric(unlist(x)))
  y <- y[!is.na(y)]
  if (length(y) == 0) NA_real_ else y[[1]]
}

rows <- list()
ix <- 1L

for (ll in seq_len(nrow(lag_specs))) {
  lag_name <- lag_specs$lag_name[[ll]]
  endog_var <- lag_specs$endog_var[[ll]]
  instr_var <- lag_specs$instr_var[[ll]]

  for (ii in seq_len(nrow(fe_specs))) {
    fe_name <- fe_specs$fe_name[[ii]]
    fe_formula <- fe_specs$fe_formula[[ii]]

    dat_fs <- panel %>% filter(!is.na(.data[[endog_var]]), !is.na(.data[[instr_var]]))
    if (nrow(dat_fs) == 0) next

    fs <- feols(
      as.formula(paste0(endog_var, " ~ ", instr_var, " + ", controls, " | ", fe_formula)),
      data = dat_fs,
      weights = ~weight_sum,
      cluster = ~estrato4
    )

    fs_row <- tidy(fs, conf.int = TRUE) %>%
      filter(term == instr_var) %>%
      mutate(
        model = "first_stage",
        lag = lag_name,
        fe = fe_name,
        outcome = endog_var,
        endogenous = endog_var,
        nobs = nobs(fs),
        fsf = safe_stat(fitstat(fs, "f")),
        ivf1 = NA_real_
      )
    rows[[ix]] <- fs_row
    ix <- ix + 1L

    for (yy in hours_outcomes) {
      dat <- panel %>% filter(!is.na(.data[[yy]]), !is.na(.data[[endog_var]]), !is.na(.data[[instr_var]]))
      if (nrow(dat) == 0) next

      rf <- feols(
        as.formula(paste0(yy, " ~ ", instr_var, " + ", controls, " | ", fe_formula)),
        data = dat,
        weights = ~weight_sum,
        cluster = ~estrato4
      )
      rf_row <- tidy(rf, conf.int = TRUE) %>%
        filter(term == instr_var) %>%
        mutate(
          model = "reduced_form",
          lag = lag_name,
          fe = fe_name,
          outcome = yy,
          endogenous = "none",
          nobs = nobs(rf),
          fsf = NA_real_,
          ivf1 = NA_real_
        )
      rows[[ix]] <- rf_row
      ix <- ix + 1L

      iv <- feols(
        as.formula(paste0(yy, " ~ ", controls, " | ", fe_formula, " | ", endog_var, " ~ ", instr_var)),
        data = dat,
        weights = ~weight_sum,
        cluster = ~estrato4
      )
      iv_row <- tidy(iv, conf.int = TRUE) %>%
        filter(grepl("^fit_", term)) %>%
        mutate(
          model = "iv",
          lag = lag_name,
          fe = fe_name,
          outcome = yy,
          endogenous = endog_var,
          nobs = nobs(iv),
          fsf = NA_real_,
          ivf1 = safe_stat(fitstat(iv, "ivf1"))
        )
      rows[[ix]] <- iv_row
      ix <- ix + 1L
    }
  }
}

results <- bind_rows(rows) %>%
  select(model, lag, fe, outcome, endogenous, term, estimate, std.error, statistic, p.value, conf.low, conf.high, nobs, fsf, ivf1)

write_csv(results, results_path)

qa <- tibble(
  metric = c(
    "panel_rows",
    "panel_years",
    "raw_current_nonmissing",
    "raw_lag4_nonmissing"
  ),
  value = c(
    as.character(nrow(panel)),
    paste(sort(unique(panel$year)), collapse = ","),
    as.character(sum(!is.na(panel$raw_quarterly_outage_hrs_per_1000) & !is.na(panel$lightning_quarterly_total))),
    as.character(sum(!is.na(panel$raw_quarterly_outage_hrs_per_1000_l4) & !is.na(panel$lightning_quarterly_total_l4)))
  )
)
write_csv(qa, qa_path)

message("Wrote: ", results_path)
message("Wrote: ", qa_path)
print(results %>% filter(model == "iv"))
