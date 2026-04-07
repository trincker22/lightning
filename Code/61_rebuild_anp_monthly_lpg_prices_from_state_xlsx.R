suppressPackageStartupMessages({
  library(here)
  library(readxl)
  library(dplyr)
  library(arrow)
  library(tibble)
})

price_dir <- here('data', 'powerIV', 'prices')
dir.create(price_dir, recursive = TRUE, showWarnings = FALSE)

xlsx_path <- file.path(price_dir, 'mensal-estados-desde-jan2013.xlsx')
if (!file.exists(xlsx_path)) {
  download.file(
    'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-estados-desde-jan2013.xlsx',
    destfile = xlsx_path,
    mode = 'wb',
    quiet = FALSE
  )
}

uf_map <- c(
  'ACRE' = 'AC', 'ALAGOAS' = 'AL', 'AMAPA' = 'AP', 'AMAZONAS' = 'AM', 'BAHIA' = 'BA',
  'CEARA' = 'CE', 'DISTRITO FEDERAL' = 'DF', 'ESPIRITO SANTO' = 'ES', 'GOIAS' = 'GO',
  'MARANHAO' = 'MA', 'MATO GROSSO' = 'MT', 'MATO GROSSO DO SUL' = 'MS', 'MINAS GERAIS' = 'MG',
  'PARA' = 'PA', 'PARAIBA' = 'PB', 'PARANA' = 'PR', 'PERNAMBUCO' = 'PE', 'PIAUI' = 'PI',
  'RIO DE JANEIRO' = 'RJ', 'RIO GRANDE DO NORTE' = 'RN', 'RIO GRANDE DO SUL' = 'RS',
  'RONDONIA' = 'RO', 'RORAIMA' = 'RR', 'SANTA CATARINA' = 'SC', 'SAO PAULO' = 'SP',
  'SERGIPE' = 'SE', 'TOCANTINS' = 'TO'
)

df <- read_excel(xlsx_path, sheet = 1, skip = 16) %>%
  transmute(
    date = as.Date(`MÊS`),
    year = as.integer(format(date, '%Y')),
    month = as.integer(format(date, '%m')),
    produto = as.character(PRODUTO),
    estado = toupper(trimws(as.character(ESTADO))),
    uf_sigla = unname(uf_map[toupper(trimws(as.character(ESTADO)))]),
    n_station_obs = suppressWarnings(as.integer(`NÚMERO DE POSTOS PESQUISADOS`)),
    lpg_price_monthly_uf = suppressWarnings(as.numeric(`PREÇO MÉDIO REVENDA`)),
    lpg_distribution_price_monthly_uf = suppressWarnings(as.numeric(`PREÇO MÉDIO DISTRIBUIÇÃO`))
  ) %>%
  filter(produto == 'GLP', !is.na(uf_sigla), !is.na(date)) %>%
  filter(year >= 2016, year <= 2025) %>%
  arrange(uf_sigla, year, month)

national <- df %>%
  group_by(year, month, date) %>%
  summarise(
    lpg_price_monthly = weighted.mean(lpg_price_monthly_uf, w = n_station_obs, na.rm = TRUE),
    lpg_distribution_price_monthly = weighted.mean(lpg_distribution_price_monthly_uf, w = n_station_obs, na.rm = TRUE),
    n_station_obs = sum(n_station_obs, na.rm = TRUE),
    .groups = 'drop'
  )

audit <- bind_rows(
  df %>% count(year, month, name = 'n_ufs') %>% mutate(dataset = 'uf'),
  national %>% mutate(n_ufs = NA_integer_, dataset = 'national') %>% select(dataset, year, month, n_ufs)
)

write_parquet(df, file.path(price_dir, 'anp_lpg_monthly_uf_2016_2025_repaired.parquet'))
write_parquet(national, file.path(price_dir, 'anp_lpg_monthly_national_2016_2025_repaired.parquet'))
write.table(audit, file.path(price_dir, 'anp_lpg_monthly_2016_2025_repaired_audit.tsv'), sep = '\t', row.names = FALSE, quote = FALSE)

print(df %>% count(year) %>% arrange(year))
print(df %>% filter(year %in% c(2021, 2022)) %>% count(year, month) %>% arrange(year, month))
