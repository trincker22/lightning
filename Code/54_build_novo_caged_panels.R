suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
})

out_dir <- here('data', 'powerIV', 'novo_caged')
part_dir <- file.path(out_dir, 'municipality_month_parts')

part_files <- list.files(part_dir, pattern = '^novo_caged_municipality_month_[0-9]{6}\\.tsv$', full.names = TRUE)
if (length(part_files) == 0) {
  stop('No municipality-month TSV parts found in ', part_dir)
}

municipality_month <- bind_rows(lapply(part_files, function(path) {
  read_tsv(path, show_col_types = FALSE, progress = FALSE, col_types = cols(
    competencia = col_integer(),
    year = col_integer(),
    month = col_integer(),
    codigo_ibge = col_integer(),
    total_net = col_double(),
    female_net = col_double(),
    total_admissions = col_double(),
    female_admissions = col_double(),
    total_separations = col_double(),
    female_separations = col_double(),
    total_records = col_double(),
    female_records = col_double(),
    mov_records = col_double(),
    for_records = col_double(),
    exc_records = col_double()
  ))
})) %>%
  mutate(date = as.Date(sprintf('%04d-%02d-01', year, month))) %>%
  arrange(competencia, codigo_ibge)

write_parquet(municipality_month, file.path(out_dir, 'novo_caged_municipality_month.parquet'))

crosswalk <- read_parquet(here('data', 'powerIV', 'pnadc', 'pnadc_estrato_municipio_crosswalk.parquet')) %>%
  transmute(
    estrato4 = as.character(estrato4),
    codigo_ibge = as.integer(municipio_code %/% 10)
  ) %>%
  distinct()

estrato_month <- municipality_month %>%
  inner_join(crosswalk, by = 'codigo_ibge') %>%
  group_by(estrato4, competencia, year, month, date) %>%
  summarise(
    total_net = sum(total_net, na.rm = TRUE),
    female_net = sum(female_net, na.rm = TRUE),
    total_admissions = sum(total_admissions, na.rm = TRUE),
    female_admissions = sum(female_admissions, na.rm = TRUE),
    total_separations = sum(total_separations, na.rm = TRUE),
    female_separations = sum(female_separations, na.rm = TRUE),
    total_records = sum(total_records, na.rm = TRUE),
    female_records = sum(female_records, na.rm = TRUE),
    mov_records = sum(mov_records, na.rm = TRUE),
    for_records = sum(for_records, na.rm = TRUE),
    exc_records = sum(exc_records, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  arrange(competencia, estrato4)

write_parquet(estrato_month, file.path(out_dir, 'novo_caged_estrato_month.parquet'))

summary_tbl <- tibble::tribble(
  ~dataset, ~n_rows, ~n_units, ~first_competencia, ~last_competencia,
  'municipality_month', nrow(municipality_month), n_distinct(municipality_month$codigo_ibge), min(municipality_month$competencia), max(municipality_month$competencia),
  'estrato_month', nrow(estrato_month), n_distinct(estrato_month$estrato4), min(estrato_month$competencia), max(estrato_month$competencia)
)

write.table(summary_tbl, file.path(out_dir, 'novo_caged_summary.tsv'), sep = '\t', row.names = FALSE, quote = FALSE)
print(summary_tbl)
