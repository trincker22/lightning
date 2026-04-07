#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(PNADcIBGE)
  library(arrow)
  library(dplyr)
  library(stringr)
  library(readr)
  library(lubridate)
})

root_dir <- here::here()

raw_dir <- here::here("data", "PNAD-C", "raw")
power_dir <- here::here("data", "powerIV", "pnadc_power", "panels")
out_dir <- here::here("data", "powerIV", "pnadc_lfp")
panel_dir <- here::here(out_dir, "panels")
qa_dir <- here::here(out_dir, "qa")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_out <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_2016_2024.parquet")
lfp_split_out <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_urban_rural_2016_2024.parquet")
lfp_power_out <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
lfp_split_power_out <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_urban_rural_with_power_2016_2024.parquet")
qa_out <- here::here(qa_dir, "pnadc_lfp_processing_qa.csv")
seed_lfp_panel_path <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_2016_2021.parquet")
seed_lfp_split_path <- here::here(panel_dir, "pnadc_estrato4_quarter_lfp_urban_rural_2016_2021.parquet")
target_years <- 2016:2024
hours_cols <- c(
  "female_hours_usual_employed",
  "male_hours_usual_employed",
  "female_hours_effective_employed",
  "male_hours_effective_employed",
  "female_hours_usual_population",
  "male_hours_usual_population",
  "female_hours_effective_population",
  "male_hours_effective_population"
)

legacy_out_dir <- here::here("data", "BrazilPowerPipeline", "pnadc_lfp")
legacy_panel_dir <- here::here(legacy_out_dir, "panels")
legacy_qa_dir <- here::here(legacy_out_dir, "qa")

light_path <- here::here(power_dir, "lightning_estrato4_quarter.parquet")
raw_power_path <- here::here(power_dir, "raw_event_estrato4_quarter.parquet")
cont_power_path <- here::here(power_dir, "continuity_estrato4_quarter.parquet")

new_outputs <- c(lfp_panel_out, lfp_split_out, lfp_power_out, lfp_split_power_out, qa_out)
legacy_outputs <- c(
  here::here(legacy_panel_dir, basename(lfp_panel_out)),
  here::here(legacy_panel_dir, basename(lfp_split_out)),
  here::here(legacy_panel_dir, basename(lfp_power_out)),
  here::here(legacy_panel_dir, basename(lfp_split_power_out)),
  here::here(legacy_qa_dir, basename(qa_out))
)

has_hours_columns <- function(path, required_cols) {
  if (!file.exists(path)) return(FALSE)
  nm <- names(read_parquet(path, as_data_frame = FALSE))
  all(required_cols %in% nm)
}

max_nonmissing_year <- function(path, var_name) {
  if (!file.exists(path)) return(NA_real_)
  dat <- read_parquet(path, col_select = c("year", var_name), as_data_frame = TRUE)
  if (!(var_name %in% names(dat))) return(NA_real_)
  yrs <- dat$year[!is.na(dat[[var_name]])]
  if (length(yrs) == 0) return(NA_real_)
  suppressWarnings(max(yrs, na.rm = TRUE))
}

power_coverage_current <- function(path, target_years) {
  source_raw_max <- max_nonmissing_year(raw_power_path, "raw_quarterly_outage_hrs_per_1000")
  source_dec_max <- max_nonmissing_year(cont_power_path, "dec_quarterly")
  source_light_max <- max_nonmissing_year(light_path, "lightning_quarterly_total")

  out_raw_max <- max_nonmissing_year(path, "raw_quarterly_outage_hrs_per_1000")
  out_dec_max <- max_nonmissing_year(path, "dec_quarterly")
  out_light_max <- max_nonmissing_year(path, "lightning_quarterly_total")

  expected_raw_max <- min(source_raw_max, max(target_years), na.rm = TRUE)
  expected_dec_max <- min(source_dec_max, max(target_years), na.rm = TRUE)
  expected_light_max <- min(source_light_max, max(target_years), na.rm = TRUE)

  isTRUE(!is.na(out_raw_max) && out_raw_max >= expected_raw_max) &&
    isTRUE(!is.na(out_dec_max) && out_dec_max >= expected_dec_max) &&
    isTRUE(!is.na(out_light_max) && out_light_max >= expected_light_max)
}

panel_hours_complete <- function(path, required_cols, target_years) {
  if (!has_hours_columns(path, required_cols)) return(FALSE)
  chk <- read_parquet(path, as_data_frame = TRUE) %>%
    select(year, all_of(required_cols))
  if (!all(target_years %in% unique(chk$year))) return(FALSE)
  all(sapply(target_years, function(y) {
    any(!is.na(chk$female_hours_effective_employed[chk$year == y]))
  }))
}

if (all(file.exists(legacy_outputs)) &&
    has_hours_columns(legacy_outputs[1], hours_cols) &&
    has_hours_columns(legacy_outputs[2], hours_cols) &&
    has_hours_columns(legacy_outputs[3], hours_cols) &&
    has_hours_columns(legacy_outputs[4], hours_cols) &&
    panel_hours_complete(legacy_outputs[1], hours_cols, target_years) &&
    power_coverage_current(legacy_outputs[3], target_years)) {
  file.copy(legacy_outputs[1], lfp_panel_out, overwrite = TRUE)
  file.copy(legacy_outputs[2], lfp_split_out, overwrite = TRUE)
  file.copy(legacy_outputs[3], lfp_power_out, overwrite = TRUE)
  file.copy(legacy_outputs[4], lfp_split_power_out, overwrite = TRUE)
  file.copy(legacy_outputs[5], qa_out, overwrite = TRUE)
  message("Copied existing LFP outputs from legacy pipeline into data/powerIV.")
  stop("SKIP_ALREADY_EXISTS: copied current legacy LFP outputs.", call. = FALSE)
}

if (all(file.exists(new_outputs)) &&
    has_hours_columns(lfp_panel_out, hours_cols) &&
    has_hours_columns(lfp_split_out, hours_cols) &&
    has_hours_columns(lfp_power_out, hours_cols) &&
    has_hours_columns(lfp_split_power_out, hours_cols) &&
    panel_hours_complete(lfp_panel_out, hours_cols, target_years) &&
    power_coverage_current(lfp_power_out, target_years)) {
  message("All LFP outputs already exist in data/powerIV. Skipping rebuild.")
  stop("SKIP_ALREADY_EXISTS: LFP outputs are current.", call. = FALSE)
}

weighted_share <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
}

weighted_mean_num <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  weighted.mean(x[keep], w[keep], na.rm = TRUE)
}

get_pnadc_quiet <- function(...) {
  out <- NULL
  invisible(suppressWarnings(suppressMessages(capture.output(
    out <- get_pnadc(...),
    type = "output"
  ))))
  out
}

fetch_pnadc_quarter <- function(year, quarter, vars, savedir) {
  args <- list(
    year = year,
    quarter = quarter,
    vars = vars,
    labels = TRUE,
    deflator = FALSE,
    design = FALSE,
    savedir = savedir
  )

  tryCatch(
    do.call(get_pnadc_quiet, c(args, list(reload = FALSE))),
    error = function(e) {
      message("Quarter ", year, " Q", quarter, " failed with reload=FALSE. Retrying with reload=TRUE.")
      do.call(get_pnadc_quiet, c(args, list(reload = TRUE)))
    }
  )
}

extract_pnadc_data <- function(x) {
  if (is.data.frame(x)) {
    return(as_tibble(x))
  }
  if (is.list(x)) {
    if (!is.null(x$variables) && is.data.frame(x$variables)) {
      return(as_tibble(x$variables))
    }
    if (!is.null(x$data) && is.data.frame(x$data)) {
      return(as_tibble(x$data))
    }
    data_frames <- x[vapply(x, is.data.frame, logical(1))]
    if (length(data_frames) > 0) {
      return(as_tibble(data_frames[[1]]))
    }
  }
  stop("PNAD-C quarter fetch did not return a tabular object.", call. = FALSE)
}

vars <- c(
  "Ano", "Trimestre", "UF", "Estrato", "V1022", "V1033",
  "V1008", "V2003", "V2007", "V2009",
  "VD4001", "VD4002", "VD4005", "VD4007",
  "V4078", "V4078A", "VD4030",
  "V4039", "V4039C"
)

years_done <- integer(0)
seed_lfp_panel <- NULL
seed_lfp_split <- NULL
if (file.exists(lfp_panel_out) && file.exists(lfp_split_out) &&
    has_hours_columns(lfp_panel_out, hours_cols) &&
    has_hours_columns(lfp_split_out, hours_cols)) {
  seed_lfp_panel <- read_parquet(lfp_panel_out) %>%
    mutate(estrato4 = as.character(estrato4))
  seed_lfp_split <- read_parquet(lfp_split_out) %>%
    mutate(estrato4 = as.character(estrato4))
  years_done <- sort(unique(seed_lfp_panel$year))
} else if (file.exists(seed_lfp_panel_path) && file.exists(seed_lfp_split_path)) {
  if (has_hours_columns(seed_lfp_panel_path, hours_cols) &&
      has_hours_columns(seed_lfp_split_path, hours_cols)) {
    seed_lfp_panel <- read_parquet(seed_lfp_panel_path) %>%
      mutate(estrato4 = as.character(estrato4))
    seed_lfp_split <- read_parquet(seed_lfp_split_path) %>%
      mutate(estrato4 = as.character(estrato4))
    years_done <- sort(unique(seed_lfp_panel$year))
  }
}

years <- setdiff(target_years, years_done)
quarters <- 1:4

if (length(years) == 0 && !is.null(seed_lfp_panel) && !is.null(seed_lfp_split)) {
  message("All target years already available in existing LFP core panels. Skipping PNAD-C core downloads.")
  person_df <- tibble()
  lfp_panel <- seed_lfp_panel
  lfp_split <- seed_lfp_split
} else {
  quarterly_list <- list()
  message("Downloading and processing quarterly PNAD-C core microdata for years: ", paste(years, collapse = ", "))
  for (yr in years) {
    for (qtr in quarters) {
      message("Year ", yr, " quarter ", qtr)
      dat <- fetch_pnadc_quarter(
        year = yr,
        quarter = qtr,
        vars = vars,
        savedir = raw_dir
      )
      dat <- extract_pnadc_data(dat)

      missing_vars <- setdiff(vars, names(dat))
      if (length(missing_vars) > 0) {
        for (v in missing_vars) dat[[v]] <- NA_character_
      }

      quarter_df <- dat %>%
        mutate(across(everything(), as.character)) %>%
        transmute(
          year = as.integer(Ano),
          quarter = as.integer(Trimestre),
          year_quarter = paste0(Ano, "-Q", Trimestre),
          uf = UF,
          estrato = Estrato,
          estrato4 = substr(Estrato, 1, 4),
          area_situation = V1022,
          weight = suppressWarnings(as.numeric(V1033)),
          household_id = paste(Ano, Trimestre, V1008, sep = "_"),
          person_order = suppressWarnings(as.integer(V2003)),
          sex = V2007,
          age = suppressWarnings(as.integer(V2009)),
          female = sex == "Mulher",
          male = sex == "Homem",
          working_age = !is.na(age) & age >= 18 & age <= 64,
          in_labor_force = VD4001 == "Pessoas na força de trabalho",
          out_labor_force = VD4001 == "Pessoas fora da força de trabalho",
          employed = VD4002 == "Pessoas ocupadas",
          unemployed = VD4002 == "Pessoas desocupadas",
          discouraged = VD4005 == "Pessoas desalentadas",
          domestic_reason_text = coalesce(VD4030, V4078A, V4078),
          domestic_care_reason = str_detect(coalesce(VD4030, V4078A, V4078, ""), regex("afazeres domésticos|afazeres domesticos|filho|parente|dependente", ignore_case = TRUE)),
          domestic_worker = str_detect(coalesce(VD4007, ""), regex("doméstico|domestico", ignore_case = TRUE)),
          usual_hours = suppressWarnings(as.numeric(V4039)),
          effective_hours = suppressWarnings(as.numeric(V4039C))
        )

      quarterly_list[[paste0(yr, "_", qtr)]] <- quarter_df
    }
  }

  person_df <- bind_rows(quarterly_list)

  message("Aggregating pooled estrato-quarter labor outcomes.")
  lfp_panel <- person_df %>%
    filter(working_age, !is.na(weight), weight > 0) %>%
    group_by(year, quarter, year_quarter, estrato4) %>%
    summarise(
      uf = dplyr::first(uf),
      n_persons = n(),
      weight_sum = sum(weight, na.rm = TRUE),
      female_pop_weight = sum(weight[female], na.rm = TRUE),
      male_pop_weight = sum(weight[male], na.rm = TRUE),
      female_lfp = weighted_share(in_labor_force[female], weight[female]),
      male_lfp = weighted_share(in_labor_force[male], weight[male]),
      female_employment = weighted_share(employed[female], weight[female]),
      male_employment = weighted_share(employed[male], weight[male]),
      female_unemployment = weighted_share(unemployed[female], weight[female]),
      male_unemployment = weighted_share(unemployed[male], weight[male]),
      female_discouraged = weighted_share(discouraged[female], weight[female]),
      male_discouraged = weighted_share(discouraged[male], weight[male]),
      female_domestic_care_reason = weighted_share(domestic_care_reason[female], weight[female]),
      male_domestic_care_reason = weighted_share(domestic_care_reason[male], weight[male]),
      female_outlf_domestic_care = weighted_share(domestic_care_reason[female & out_labor_force], weight[female & out_labor_force]),
      male_outlf_domestic_care = weighted_share(domestic_care_reason[male & out_labor_force], weight[male & out_labor_force]),
      female_domestic_worker = weighted_share(domestic_worker[female], weight[female]),
      male_domestic_worker = weighted_share(domestic_worker[male], weight[male]),
      female_hours_usual_employed = weighted_mean_num(usual_hours[female & employed], weight[female & employed]),
      male_hours_usual_employed = weighted_mean_num(usual_hours[male & employed], weight[male & employed]),
      female_hours_effective_employed = weighted_mean_num(effective_hours[female & employed], weight[female & employed]),
      male_hours_effective_employed = weighted_mean_num(effective_hours[male & employed], weight[male & employed]),
      female_hours_usual_population = weighted_mean_num(ifelse(employed[female], usual_hours[female], 0), weight[female]),
      male_hours_usual_population = weighted_mean_num(ifelse(employed[male], usual_hours[male], 0), weight[male]),
      female_hours_effective_population = weighted_mean_num(ifelse(employed[female], effective_hours[female], 0), weight[female]),
      male_hours_effective_population = weighted_mean_num(ifelse(employed[male], effective_hours[male], 0), weight[male]),
      gender_lfp_gap = female_lfp - male_lfp,
      gender_employment_gap = female_employment - male_employment,
      gender_hours_usual_employed_gap = female_hours_usual_employed - male_hours_usual_employed,
      gender_hours_effective_employed_gap = female_hours_effective_employed - male_hours_effective_employed,
      gender_hours_usual_population_gap = female_hours_usual_population - male_hours_usual_population,
      gender_hours_effective_population_gap = female_hours_effective_population - male_hours_effective_population,
      .groups = "drop"
    )

  message("Aggregating estrato-quarter labor outcomes by urban/rural.")
  lfp_split <- person_df %>%
    filter(working_age, !is.na(weight), weight > 0) %>%
    mutate(
      urban_rural = case_when(
        area_situation == "Urbana" ~ "urban",
        area_situation == "Rural" ~ "rural",
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(urban_rural)) %>%
    group_by(year, quarter, year_quarter, estrato4, urban_rural) %>%
    summarise(
      uf = dplyr::first(uf),
      n_persons = n(),
      weight_sum = sum(weight, na.rm = TRUE),
      female_pop_weight = sum(weight[female], na.rm = TRUE),
      male_pop_weight = sum(weight[male], na.rm = TRUE),
      female_lfp = weighted_share(in_labor_force[female], weight[female]),
      male_lfp = weighted_share(in_labor_force[male], weight[male]),
      female_employment = weighted_share(employed[female], weight[female]),
      male_employment = weighted_share(employed[male], weight[male]),
      female_unemployment = weighted_share(unemployed[female], weight[female]),
      male_unemployment = weighted_share(unemployed[male], weight[male]),
      female_discouraged = weighted_share(discouraged[female], weight[female]),
      male_discouraged = weighted_share(discouraged[male], weight[male]),
      female_domestic_care_reason = weighted_share(domestic_care_reason[female], weight[female]),
      male_domestic_care_reason = weighted_share(domestic_care_reason[male], weight[male]),
      female_outlf_domestic_care = weighted_share(domestic_care_reason[female & out_labor_force], weight[female & out_labor_force]),
      male_outlf_domestic_care = weighted_share(domestic_care_reason[male & out_labor_force], weight[male & out_labor_force]),
      female_domestic_worker = weighted_share(domestic_worker[female], weight[female]),
      male_domestic_worker = weighted_share(domestic_worker[male], weight[male]),
      female_hours_usual_employed = weighted_mean_num(usual_hours[female & employed], weight[female & employed]),
      male_hours_usual_employed = weighted_mean_num(usual_hours[male & employed], weight[male & employed]),
      female_hours_effective_employed = weighted_mean_num(effective_hours[female & employed], weight[female & employed]),
      male_hours_effective_employed = weighted_mean_num(effective_hours[male & employed], weight[male & employed]),
      female_hours_usual_population = weighted_mean_num(ifelse(employed[female], usual_hours[female], 0), weight[female]),
      male_hours_usual_population = weighted_mean_num(ifelse(employed[male], usual_hours[male], 0), weight[male]),
      female_hours_effective_population = weighted_mean_num(ifelse(employed[female], effective_hours[female], 0), weight[female]),
      male_hours_effective_population = weighted_mean_num(ifelse(employed[male], effective_hours[male], 0), weight[male]),
      gender_lfp_gap = female_lfp - male_lfp,
      gender_employment_gap = female_employment - male_employment,
      gender_hours_usual_employed_gap = female_hours_usual_employed - male_hours_usual_employed,
      gender_hours_effective_employed_gap = female_hours_effective_employed - male_hours_effective_employed,
      gender_hours_usual_population_gap = female_hours_usual_population - male_hours_usual_population,
      gender_hours_effective_population_gap = female_hours_effective_population - male_hours_effective_population,
      .groups = "drop"
    )

  if (!is.null(seed_lfp_panel)) {
    lfp_panel <- bind_rows(seed_lfp_panel, lfp_panel) %>%
      mutate(estrato4 = as.character(estrato4)) %>%
      arrange(year, quarter, estrato4) %>%
      distinct(year, quarter, estrato4, .keep_all = TRUE)
  }

  if (!is.null(seed_lfp_split)) {
    lfp_split <- bind_rows(seed_lfp_split, lfp_split) %>%
      mutate(estrato4 = as.character(estrato4)) %>%
      arrange(year, quarter, estrato4, urban_rural) %>%
      distinct(year, quarter, estrato4, urban_rural, .keep_all = TRUE)
  }
}

message("Preparing power panel with lags.")
power_panel <- read_parquet(light_path) %>%
  select(estrato4, year, quarter, lightning_quarterly_total) %>%
  left_join(read_parquet(raw_power_path) %>% select(estrato4, year, quarter, raw_quarterly_outage_hrs_per_1000, raw_quarterly_events_per_1000), by = c("estrato4", "year", "quarter")) %>%
  left_join(read_parquet(cont_power_path) %>% select(estrato4, year, quarter, dec_quarterly, fec_quarterly), by = c("estrato4", "year", "quarter")) %>%
  mutate(
    estrato4 = as.character(estrato4),
    quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  ) %>%
  arrange(estrato4, quarter_start) %>%
  group_by(estrato4) %>%
  mutate(
    lightning_quarterly_total_l1 = lag(lightning_quarterly_total, 1),
    lightning_quarterly_total_l4 = lag(lightning_quarterly_total, 4),
    raw_quarterly_outage_hrs_per_1000_l1 = lag(raw_quarterly_outage_hrs_per_1000, 1),
    raw_quarterly_outage_hrs_per_1000_l4 = lag(raw_quarterly_outage_hrs_per_1000, 4),
    dec_quarterly_l1 = lag(dec_quarterly, 1),
    dec_quarterly_l4 = lag(dec_quarterly, 4),
    fec_quarterly_l1 = lag(fec_quarterly, 1),
    fec_quarterly_l4 = lag(fec_quarterly, 4)
  ) %>%
  ungroup() %>%
  select(-quarter_start)

message("Merging power exposures into LFP panels.")
lfp_power <- lfp_panel %>% left_join(power_panel, by = c("estrato4", "year", "quarter"))
lfp_split_power <- lfp_split %>% left_join(power_panel, by = c("estrato4", "year", "quarter"))

write_parquet(lfp_panel, lfp_panel_out)
write_parquet(lfp_split, lfp_split_out)
write_parquet(lfp_power, lfp_power_out)
write_parquet(lfp_split_power, lfp_split_power_out)

qa <- tibble(
  metric = c(
    "person_rows",
    "lfp_panel_rows",
    "lfp_split_rows",
    "lfp_power_rows",
    "lfp_split_power_rows",
    "raw_power_nonmissing",
    "continuity_nonmissing",
    "female_lfp_mean",
    "female_domestic_care_mean",
    "female_hours_effective_employed_mean",
    "female_hours_effective_population_mean",
    "year_range"
  ),
  value = c(
    as.character(nrow(person_df)),
    as.character(nrow(lfp_panel)),
    as.character(nrow(lfp_split)),
    as.character(nrow(lfp_power)),
    as.character(nrow(lfp_split_power)),
    as.character(sum(!is.na(lfp_power$raw_quarterly_outage_hrs_per_1000))),
    as.character(sum(!is.na(lfp_power$dec_quarterly))),
    as.character(mean(lfp_panel$female_lfp, na.rm = TRUE)),
    as.character(mean(lfp_panel$female_domestic_care_reason, na.rm = TRUE)),
    as.character(mean(lfp_panel$female_hours_effective_employed, na.rm = TRUE)),
    as.character(mean(lfp_panel$female_hours_effective_population, na.rm = TRUE)),
    paste(min(lfp_panel$year, na.rm = TRUE), max(lfp_panel$year, na.rm = TRUE), sep = "-")
  )
)
write_csv(qa, qa_out)

coverage_script <- here::here("Code", "07a_power_coverage_audit.R")
if (file.exists(coverage_script)) {
  cov_status <- tryCatch(system2("Rscript", coverage_script), error = function(e) 1L)
  if (!identical(cov_status, 0L)) {
    warning("Coverage audit refresh failed in Script 08.")
  }
}

message("Done.")
message("Pooled LFP panel: ", lfp_panel_out)
message("Urban/rural LFP panel: ", lfp_split_out)
message("Pooled LFP panel with power: ", lfp_power_out)
message("Urban/rural LFP panel with power: ", lfp_split_power_out)
message("QA: ", qa_out)
