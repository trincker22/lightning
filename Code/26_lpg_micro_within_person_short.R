#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(fixest)
  library(broom)
})

linked_in <- here::here('data', 'powerIV', 'pnadc_lfp', 'micro', 'pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet')
power_in <- here::here('data', 'powerIV', 'pnadc_power', 'panels', 'pnadc_estrato4_quarter_with_power.parquet')
lpg_in <- here::here('data', 'powerIV', 'prices', 'anp_lpg_quarter_estrato_2016_2024.parquet')
macro_in <- here::here('data', 'powerIV', 'macro', 'sidra_uf_quarter_macro_2016_2024.parquet')
out_path <- here::here('data', 'powerIV', 'regressions', 'lpg_micro_panel', 'within_person_short.csv')
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

uf_to_sigla <- c(
  'Rondônia' = 'RO', 'Acre' = 'AC', 'Amazonas' = 'AM', 'Roraima' = 'RR', 'Pará' = 'PA', 'Amapá' = 'AP', 'Tocantins' = 'TO',
  'Maranhão' = 'MA', 'Piauí' = 'PI', 'Ceará' = 'CE', 'Rio Grande do Norte' = 'RN', 'Paraíba' = 'PB', 'Pernambuco' = 'PE', 'Alagoas' = 'AL', 'Sergipe' = 'SE', 'Bahia' = 'BA',
  'Minas Gerais' = 'MG', 'Espírito Santo' = 'ES', 'Rio de Janeiro' = 'RJ', 'São Paulo' = 'SP', 'Paraná' = 'PR', 'Santa Catarina' = 'SC', 'Rio Grande do Sul' = 'RS',
  'Mato Grosso do Sul' = 'MS', 'Mato Grosso' = 'MT', 'Goiás' = 'GO', 'Distrito Federal' = 'DF'
)

cols_needed <- c('woman_id','year','quarter','year_quarter','uf','estrato4','upa','panel','household_number','person_number','person_weight','age','employed','wanted_to_work','wants_more_hours','available_more_hours','search_long','worked_last_12m','effective_hours','visit1_match_any')

women <- read_parquet(linked_in, col_select = all_of(cols_needed)) %>%
  mutate(
    estrato4 = as.character(estrato4),
    uf_sigla = unname(uf_to_sigla[uf]),
    base_woman_id = if_else(!is.na(person_number), paste(upa, panel, household_number, person_number, sep = '_'), woman_id),
    woman_quarter_id = year * 4L + quarter,
    age_sq = age^2,
    effective_hours_population = if_else(employed & !is.na(effective_hours), effective_hours, 0)
  ) %>%
  arrange(base_woman_id, year, quarter) %>%
  group_by(base_woman_id) %>%
  mutate(
    woman_spell_start = if_else(row_number() == 1L | woman_quarter_id - lag(woman_quarter_id, default = first(woman_quarter_id)) > 1L, 1L, 0L),
    woman_spell_index = cumsum(woman_spell_start),
    woman_spell_id = paste(base_woman_id, sprintf('%03d', woman_spell_index), sep = '_')
  ) %>%
  ungroup()

baseline_power <- read_parquet(power_in, col_select = c('estrato4','year','share_cooking_wood_charcoal')) %>%
  mutate(estrato4 = as.character(estrato4)) %>%
  filter(year == 2016) %>%
  group_by(estrato4) %>%
  summarise(baseline_wood_2016 = mean(share_cooking_wood_charcoal, na.rm = TRUE), .groups = 'drop')

lpg_quarter_estrato <- read_parquet(lpg_in, col_select = c('estrato4','year','quarter','log_lpg_price_quarterly_estrato')) %>%
  mutate(estrato4 = as.character(estrato4))

macro_panel <- read_parquet(macro_in, col_select = c('uf_sigla','year','quarter','uf_unemployment_rate','uf_labor_demand_proxy','uf_real_wage_proxy','uf_share_secondaryplus','uf_share_tertiary')) %>%
  mutate(uf_sigla = as.character(uf_sigla))

women_reg <- women %>%
  filter(visit1_match_any) %>%
  left_join(baseline_power, by = 'estrato4') %>%
  left_join(lpg_quarter_estrato, by = c('estrato4','year','quarter')) %>%
  left_join(macro_panel, by = c('uf_sigla','year','quarter')) %>%
  mutate(z_wood_lpg_estrato = baseline_wood_2016 * log_lpg_price_quarterly_estrato)

repeat_women <- women_reg %>% count(woman_spell_id, name = 'n_woman_q') %>% filter(n_woman_q >= 2)
dat0 <- women_reg %>% inner_join(repeat_women, by = 'woman_spell_id')

control_sets <- tibble::tribble(
  ~spec, ~controls,
  'M1', '1',
  'M2', 'age + age_sq',
  'M3', 'age + age_sq + uf_unemployment_rate + uf_labor_demand_proxy + uf_real_wage_proxy + uf_share_secondaryplus + uf_share_tertiary'
)

outcomes <- c('effective_hours','effective_hours_population','wanted_to_work','wants_more_hours','available_more_hours','search_long','worked_last_12m')
rows <- list()
idx <- 1L

for (i in seq_len(nrow(control_sets))) {
  spec <- control_sets$spec[[i]]
  controls <- control_sets$controls[[i]]
  needed_controls <- all.vars(as.formula(paste('~', controls)))
  for (y in outcomes) {
    needed <- unique(c('woman_spell_id','year_quarter','estrato4','person_weight','z_wood_lpg_estrato', y, needed_controls))
    dat <- dat0 %>% filter(if_all(all_of(needed), ~ !is.na(.x)))
    model <- feols(as.formula(paste0(y, ' ~ z_wood_lpg_estrato + ', controls, ' | woman_spell_id + year_quarter')), data = dat, weights = ~person_weight, cluster = ~estrato4)
    term <- tidy(model) %>% filter(term == 'z_wood_lpg_estrato')
    rows[[idx]] <- tibble(spec = spec, outcome = y, nobs = nobs(model), n_women = n_distinct(dat$woman_spell_id), coef = term$estimate[[1]], se = term$std.error[[1]], p = term$p.value[[1]])
    idx <- idx + 1L
  }
}

res <- bind_rows(rows)
write_csv(res, out_path)
print(res)
