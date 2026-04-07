suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
})

out_dir <- here("data", "powerIV", "monthly_source_audit")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

source_audit <- tibble::tribble(
  ~source_name, ~category, ~frequency, ~geography, ~url, ~status, ~notes,
  "Novo Caged statistics page", "labor outcome", "monthly", "municipality candidate", "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged", "needs deeper pull", "Official monthly formal employment program page; useful entry point for municipality-month labor outcomes.",
  "Novo Caged microdados page", "labor outcome", "monthly", "micro/municipality candidate", "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged/microdados", "restricted", "Official page exists but currently returns restricted-content message in unauthenticated requests.",
  "PDET bases de dados", "labor outcome", "monthly", "municipality candidate", "https://acesso.mte.gov.br/portal-pdet/o-pdet/portifolio-de-produtos/bases-de-dados.htm", "legacy access point", "Official PDET page states CAGED statistical data are monthly and available online via Acesso Online/Internet.",
  "ANP GLP by vasilhame", "mechanism outcome", "monthly", "UF and national", "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-glp-tipo-vasilhame-m3-2007-2025.csv", "pulled", "Official monthly GLP sales by type of cylinder; immediately usable.",
  "ANP municipality GLP sales", "mechanism outcome", "annual", "municipality", "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vaehdpm/glp/vendas-anuais-de-glp-por-municipio.csv", "available but annual", "Useful geography, but not monthly.",
  "Auxilio Gas municipality panel", "treatment", "bimonthly", "municipality", "https://dados.gov.br/dados/conjuntos-dados/programa-auxilio-gas-dos-brasileiros", "already pulled", "Official municipality-bimonth treatment panel through 2025."
)

write.table(source_audit, file.path(out_dir, "monthly_source_audit.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

url_glp <- "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-glp-tipo-vasilhame-m3-2007-2025.csv"
raw_glp <- read.csv2(url_glp)

month_map <- c(JAN = 1, FEV = 2, MAR = 3, ABR = 4, MAI = 5, JUN = 6, JUL = 7, AGO = 8, SET = 9, OUT = 10, NOV = 11, DEZ = 12)
uf_map <- c(
  "RONDÔNIA" = "RO", "ACRE" = "AC", "AMAZONAS" = "AM", "RORAIMA" = "RR", "PARÁ" = "PA", "AMAPÁ" = "AP", "TOCANTINS" = "TO",
  "MARANHÃO" = "MA", "PIAUÍ" = "PI", "CEARÁ" = "CE", "RIO GRANDE DO NORTE" = "RN", "PARAÍBA" = "PB", "PERNAMBUCO" = "PE", "ALAGOAS" = "AL", "SERGIPE" = "SE", "BAHIA" = "BA",
  "MINAS GERAIS" = "MG", "ESPÍRITO SANTO" = "ES", "RIO DE JANEIRO" = "RJ", "SÃO PAULO" = "SP",
  "PARANÁ" = "PR", "SANTA CATARINA" = "SC", "RIO GRANDE DO SUL" = "RS",
  "MATO GROSSO DO SUL" = "MS", "MATO GROSSO" = "MT", "GOIÁS" = "GO", "DISTRITO FEDERAL" = "DF"
)

glp_monthly <- raw_glp %>%
  transmute(
    year = as.integer(ANO),
    month = unname(month_map[MÊS]),
    uf_name = as.character(UNIDADE.DA.FEDERAÇÃO),
    uf_sigla = unname(uf_map[UNIDADE.DA.FEDERAÇÃO]),
    region = as.character(GRANDE.REGIÃO),
    vasilhame = as.character(VASILHAME),
    sales_m3 = as.numeric(VENDAS),
    date = as.Date(sprintf("%d-%02d-01", year, month))
  )

write_parquet(glp_monthly, file.path(out_dir, "anp_glp_vasilhame_monthly_2007_2025.parquet"))

uf_monthly <- glp_monthly %>%
  group_by(uf_sigla, year, month, date, vasilhame) %>%
  summarise(sales_m3 = sum(sales_m3, na.rm = TRUE), .groups = "drop")
write_parquet(uf_monthly, file.path(out_dir, "anp_glp_vasilhame_monthly_uf_2007_2025.parquet"))

national_monthly <- glp_monthly %>%
  group_by(year, month, date, vasilhame) %>%
  summarise(sales_m3 = sum(sales_m3, na.rm = TRUE), .groups = "drop")
write_parquet(national_monthly, file.path(out_dir, "anp_glp_vasilhame_monthly_national_2007_2025.parquet"))

p13_summary <- national_monthly %>%
  filter(vasilhame == "GLP - Até P13") %>%
  summarise(
    first_date = min(date),
    last_date = max(date),
    n_months = n(),
    mean_sales_m3 = mean(sales_m3, na.rm = TRUE),
    sd_sales_m3 = sd(sales_m3, na.rm = TRUE)
  )

write.table(p13_summary, file.path(out_dir, "anp_glp_vasilhame_monthly_national_p13_summary.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
print(source_audit)
print(p13_summary)
