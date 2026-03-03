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

raw_dir <- file.path(root_dir, "data", "PNAD-C", "raw")
power_dir <- file.path(root_dir, "data", "powerIV", "pnadc_power", "panels")
out_dir <- file.path(root_dir, "data", "powerIV", "pnadc_lfp")
panel_dir <- file.path(out_dir, "panels")
qa_dir <- file.path(out_dir, "qa")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

lfp_panel_out <- file.path(panel_dir, "pnadc_estrato4_quarter_lfp_2016_2021.parquet")
lfp_split_out <- file.path(panel_dir, "pnadc_estrato4_quarter_lfp_urban_rural_2016_2021.parquet")
lfp_power_out <- file.path(panel_dir, "pnadc_estrato4_quarter_lfp_with_power_2016_2021.parquet")
lfp_split_power_out <- file.path(panel_dir, "pnadc_estrato4_quarter_lfp_urban_rural_with_power_2016_2021.parquet")
qa_out <- file.path(qa_dir, "pnadc_lfp_processing_qa.csv")

legacy_out_dir <- file.path(root_dir, "data", "BrazilPowerPipeline", "pnadc_lfp")
legacy_panel_dir <- file.path(legacy_out_dir, "panels")
legacy_qa_dir <- file.path(legacy_out_dir, "qa")

light_path <- file.path(power_dir, "lightning_estrato4_quarter.parquet")
raw_power_path <- file.path(power_dir, "raw_event_estrato4_quarter.parquet")
cont_power_path <- file.path(power_dir, "continuity_estrato4_quarter.parquet")

new_outputs <- c(lfp_panel_out, lfp_split_out, lfp_power_out, lfp_split_power_out, qa_out)
legacy_outputs <- c(
  file.path(legacy_panel_dir, basename(lfp_panel_out)),
  file.path(legacy_panel_dir, basename(lfp_split_out)),
  file.path(legacy_panel_dir, basename(lfp_power_out)),
  file.path(legacy_panel_dir, basename(lfp_split_power_out)),
  file.path(legacy_qa_dir, basename(qa_out))
)

if (all(file.exists(new_outputs))) {
  message("All LFP outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

if (all(file.exists(legacy_outputs))) {
  file.copy(legacy_outputs[1], lfp_panel_out, overwrite = TRUE)
  file.copy(legacy_outputs[2], lfp_split_out, overwrite = TRUE)
  file.copy(legacy_outputs[3], lfp_power_out, overwrite = TRUE)
  file.copy(legacy_outputs[4], lfp_split_power_out, overwrite = TRUE)
  file.copy(legacy_outputs[5], qa_out, overwrite = TRUE)
  message("Copied existing LFP outputs from legacy pipeline into data/powerIV.")
  quit(save = "no", status = 0)
}

vars <- c(
  "Ano", "Trimestre", "UF", "Estrato", "V1022", "V1033",
  "V1008", "V2003", "V2007", "V2009",
  "VD4001", "VD4002", "VD4005", "VD4007",
  "V4078", "V4078A", "VD4030"
)

years <- 2016:2021
quarters <- 1:4

quarterly_list <- list()
message("Downloading and processing quarterly PNAD-C core microdata.")
for (yr in years) {
  for (qtr in quarters) {
    message("Year ", yr, " quarter ", qtr)
    dat <- get_pnadc(
      year = yr,
      quarter = qtr,
      vars = vars,
      labels = TRUE,
      deflator = FALSE,
      design = FALSE,
      savedir = raw_dir,
      reload = FALSE
    )

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
        domestic_worker = str_detect(coalesce(VD4007, ""), regex("doméstico|domestico", ignore_case = TRUE))
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
    gender_lfp_gap = female_lfp - male_lfp,
    gender_employment_gap = female_employment - male_employment,
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
    gender_lfp_gap = female_lfp - male_lfp,
    gender_employment_gap = female_employment - male_employment,
    .groups = "drop"
  )

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
    paste(min(person_df$year, na.rm = TRUE), max(person_df$year, na.rm = TRUE), sep = "-")
  )
)
write_csv(qa, qa_out)

message("Done.")
message("Pooled LFP panel: ", lfp_panel_out)
message("Urban/rural LFP panel: ", lfp_split_out)
message("Pooled LFP panel with power: ", lfp_power_out)
message("Urban/rural LFP panel with power: ", lfp_split_power_out)
message("QA: ", qa_out)
