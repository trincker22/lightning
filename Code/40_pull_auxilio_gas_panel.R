suppressPackageStartupMessages({
  library(here)
  library(jsonlite)
  library(arrow)
  library(dplyr)
})

out_dir <- here("data", "powerIV", "auxilio_gas")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

meta_url <- "https://dados.gov.br/api/publico/conjuntos-dados/programa-auxilio-gas-dos-brasileiros"
meta <- fromJSON(meta_url, simplifyDataFrame = TRUE)

resources <- as_tibble(meta$resources) %>%
  filter(format == "CSV") %>%
  mutate(
    source_year = as.integer(sub(".*anomes_s:([0-9]{4}).*", "\\1", url)),
    source_label = coalesce(description, name, as.character(source_year))
  ) %>%
  arrange(source_year)

pull_one_year <- function(url, source_year) {
  read.csv(url, stringsAsFactors = FALSE, colClasses = "character") %>%
    as_tibble() %>%
    mutate(source_year = source_year)
}

auxgas_raw <- bind_rows(lapply(seq_len(nrow(resources)), function(i) {
  pull_one_year(resources$url[[i]], resources$source_year[[i]])
}))

auxgas_panel <- auxgas_raw %>%
  transmute(
    codigo_ibge = sprintf("%06s", codigo_ibge),
    anomes = sprintf("%06s", anomes),
    year = as.integer(substr(anomes, 1, 4)),
    month = as.integer(substr(anomes, 5, 6)),
    reference_date = as.Date(paste0(substr(anomes, 1, 4), "-", substr(anomes, 5, 6), "-01")),
    quarter = ((month - 1L) %/% 3L) + 1L,
    bimester = ((month - 1L) %/% 2L) + 1L,
    source_year = as.integer(source_year),
    auxgas_families = as.integer(qtd_fam_benef_aux_gas),
    auxgas_total_value = as.numeric(valor_total_fam_benef_aux_gas),
    auxgas_avg_benefit = as.numeric(valor_medio_aux_gas),
    auxgas_people = as.integer(qtd_pessoas_benef_aux_gas),
    auxgas_female_rf = as.integer(qtd_rf_feminino_benef_aux_gas),
    auxgas_female_rf_share = as.numeric(perc_rf_feminino_aux_gas),
    phase = case_when(
      anomes == "202112" ~ "launch",
      anomes %in% c("202202", "202204", "202206") ~ "regular_pre_topup",
      anomes %in% c("202208", "202210", "202212") ~ "emergency_topup_2022",
      year >= 2023 ~ "continued_topup_2023_onward",
      TRUE ~ "other"
    )
  ) %>%
  arrange(anomes, codigo_ibge)

national_bimonth <- auxgas_panel %>%
  group_by(anomes, year, month, reference_date, phase) %>%
  summarise(
    auxgas_families = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people = sum(auxgas_people, na.rm = TRUE),
    auxgas_female_rf = sum(auxgas_female_rf, na.rm = TRUE),
    auxgas_avg_benefit_implied = if_else(auxgas_families > 0, auxgas_total_value / auxgas_families, NA_real_),
    municipalities_reporting = n_distinct(codigo_ibge),
    .groups = "drop"
  )

coverage_summary <- auxgas_panel %>%
  group_by(anomes, year, month) %>%
  summarise(
    municipalities_reporting = n_distinct(codigo_ibge),
    total_families = sum(auxgas_families, na.rm = TRUE),
    total_value = sum(auxgas_total_value, na.rm = TRUE),
    mean_benefit_weighted = if_else(sum(auxgas_families, na.rm = TRUE) > 0, sum(auxgas_total_value, na.rm = TRUE) / sum(auxgas_families, na.rm = TRUE), NA_real_),
    .groups = "drop"
  ) %>%
  arrange(anomes)

panel_summary <- tibble(
  metric = c(
    "n_rows",
    "n_municipalities",
    "first_anomes",
    "last_anomes",
    "n_unique_anomes",
    "all_even_months_only",
    "n_csv_resources",
    "metadata_modified"
  ),
  value = c(
    as.character(nrow(auxgas_panel)),
    as.character(n_distinct(auxgas_panel$codigo_ibge)),
    min(auxgas_panel$anomes, na.rm = TRUE),
    max(auxgas_panel$anomes, na.rm = TRUE),
    as.character(n_distinct(auxgas_panel$anomes)),
    as.character(all(auxgas_panel$month %in% c(2L, 4L, 6L, 8L, 10L, 12L))),
    as.character(nrow(resources)),
    as.character(meta$metadata_modified)
  )
)

write_parquet(auxgas_panel, here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet"))
write_parquet(national_bimonth, here("data", "powerIV", "auxilio_gas", "auxilio_gas_national_bimonth_2021_2025.parquet"))
write.csv(resources, here("data", "powerIV", "auxilio_gas", "auxilio_gas_resource_manifest.csv"), row.names = FALSE)
write.table(panel_summary, here("data", "powerIV", "auxilio_gas", "auxilio_gas_panel_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(coverage_summary, here("data", "powerIV", "auxilio_gas", "auxilio_gas_coverage_by_anomes.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

cat(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet"), "\n")
cat(here("data", "powerIV", "auxilio_gas", "auxilio_gas_national_bimonth_2021_2025.parquet"), "\n")
print(panel_summary)
print(head(coverage_summary, 8))
print(tail(coverage_summary, 8))
