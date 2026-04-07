suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
})

out_dir <- here("data", "powerIV", "monthly_source_audit")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

caged_attempts <- tibble::tribble(
  ~source, ~url, ~status, ~detail,
  "Novo Caged main page", "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged", "reachable", "Official page is public and lists monthly releases.",
  "Novo Caged microdados page", "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged/microdados", "blocked", "Returns restricted-content message in unauthenticated requests.",
  "BGCAGED portal", "https://bi.trabalho.gov.br/bgcaged/", "blocked", "Public landing page is reachable but data access requires login credentials.",
  "PDET bases de dados", "https://acesso.mte.gov.br/portal-pdet/o-pdet/portifolio-de-produtos/bases-de-dados.htm", "legacy route", "Official text confirms monthly CAGED data exist, but no clean direct bulk municipality CSV was exposed in this pass."
)
write.table(caged_attempts, file.path(out_dir, "caged_attempt_log.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

uf_name_to_sigla <- c(
  "Rondônia" = "RO", "Acre" = "AC", "Amazonas" = "AM", "Roraima" = "RR", "Pará" = "PA", "Amapá" = "AP", "Tocantins" = "TO",
  "Maranhão" = "MA", "Piauí" = "PI", "Ceará" = "CE", "Rio Grande do Norte" = "RN", "Paraíba" = "PB", "Pernambuco" = "PE", "Alagoas" = "AL", "Sergipe" = "SE", "Bahia" = "BA",
  "Minas Gerais" = "MG", "Espírito Santo" = "ES", "Rio de Janeiro" = "RJ", "São Paulo" = "SP",
  "Paraná" = "PR", "Santa Catarina" = "SC", "Rio Grande do Sul" = "RS",
  "Mato Grosso do Sul" = "MS", "Mato Grosso" = "MT", "Goiás" = "GO", "Distrito Federal" = "DF"
)

estrato_uf <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_visit1_estrato_quarter_2016_2024.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    uf_name = uf,
    uf_sigla = unname(uf_name_to_sigla[uf])
  ) %>%
  distinct()

auxgas_muni <- read_parquet(here("data", "powerIV", "auxilio_gas", "auxilio_gas_municipality_bimonth_2021_2025.parquet")) %>%
  transmute(
    codigo_ibge = as.integer(codigo_ibge),
    anomes,
    year,
    month,
    auxgas_families,
    auxgas_total_value,
    auxgas_avg_benefit,
    auxgas_people,
    reference_date
  )

crosswalk <- read_parquet(here("data", "powerIV", "pnadc", "pnadc_estrato_municipio_crosswalk.parquet")) %>%
  transmute(
    estrato4 = as.character(estrato4),
    codigo_ibge = as.integer(municipio_code %/% 10)
  ) %>%
  distinct()

auxgas_estrato_month <- auxgas_muni %>%
  inner_join(crosswalk, by = "codigo_ibge") %>%
  group_by(estrato4, year, month, anomes, reference_date) %>%
  summarise(
    auxgas_families = sum(auxgas_families, na.rm = TRUE),
    auxgas_total_value = sum(auxgas_total_value, na.rm = TRUE),
    auxgas_people = sum(auxgas_people, na.rm = TRUE),
    auxgas_avg_benefit = auxgas_total_value / auxgas_families,
    .groups = "drop"
  )
write_parquet(auxgas_estrato_month, file.path(out_dir, "auxgas_estrato_month_2021_2025.parquet"))

glp_vasilhame_uf <- read_parquet(file.path(out_dir, "anp_glp_vasilhame_monthly_uf_2007_2025.parquet")) %>%
  filter(vasilhame == "GLP - Até P13") %>%
  transmute(
    uf_sigla,
    year,
    month,
    date,
    glp_p13_sales_m3 = sales_m3
  )

glp_p13_estrato_month <- estrato_uf %>%
  inner_join(glp_vasilhame_uf, by = "uf_sigla")
write_parquet(glp_p13_estrato_month, file.path(out_dir, "anp_glp_p13_estrato_month_2007_2025.parquet"))

uf_price_monthly <- read_parquet(here("data", "powerIV", "prices", "anp_lpg_monthly_uf_2016_2024.parquet")) %>%
  transmute(
    uf_sigla,
    year,
    month,
    lpg_price_monthly_uf,
    n_station_obs,
    date = as.Date(sprintf("%d-%02d-01", year, month))
  )

uf_price_estrato_month <- estrato_uf %>%
  inner_join(uf_price_monthly, by = "uf_sigla")
write_parquet(uf_price_estrato_month, file.path(out_dir, "anp_lpg_price_estrato_month_2016_2024.parquet"))

combined_monthly <- auxgas_estrato_month %>%
  left_join(glp_p13_estrato_month, by = c("estrato4", "year", "month")) %>%
  left_join(uf_price_estrato_month %>% select(estrato4, year, month, lpg_price_monthly_uf, n_station_obs), by = c("estrato4", "year", "month"))
write_parquet(combined_monthly, file.path(out_dir, "auxgas_glp_monthly_estrato_combined_2021_2025.parquet"))

summary_tbl <- tibble::tribble(
  ~dataset, ~n_rows, ~n_estratos, ~first_period, ~last_period,
  "auxgas_estrato_month", nrow(auxgas_estrato_month), n_distinct(auxgas_estrato_month$estrato4), min(auxgas_estrato_month$anomes), max(auxgas_estrato_month$anomes),
  "glp_p13_estrato_month", nrow(glp_p13_estrato_month), n_distinct(glp_p13_estrato_month$estrato4), min(sprintf("%d%02d", glp_p13_estrato_month$year, glp_p13_estrato_month$month)), max(sprintf("%d%02d", glp_p13_estrato_month$year, glp_p13_estrato_month$month)),
  "uf_price_estrato_month", nrow(uf_price_estrato_month), n_distinct(uf_price_estrato_month$estrato4), min(sprintf("%d%02d", uf_price_estrato_month$year, uf_price_estrato_month$month)), max(sprintf("%d%02d", uf_price_estrato_month$year, uf_price_estrato_month$month)),
  "combined_monthly", nrow(combined_monthly), n_distinct(combined_monthly$estrato4), min(sprintf("%d%02d", combined_monthly$year, combined_monthly$month)), max(sprintf("%d%02d", combined_monthly$year, combined_monthly$month))
)
write.table(summary_tbl, file.path(out_dir, "monthly_fallback_panel_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

print(caged_attempts)
print(summary_tbl)
