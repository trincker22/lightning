#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(PNADcIBGE)
  library(arrow)
  library(dplyr)
  library(stringr)
  library(readr)
})

raw_dir <- here::here('data', 'PNAD-C', 'raw')
input_txt <- here::here('data', 'PNAD-C', 'raw', 'input_PNADC_trimestral.txt')
dict_file <- here::here('data', 'PNAD-C', 'raw', 'dicionario_PNADC_microdados_trimestral.xls')
micro_out <- here::here('data', 'powerIV', 'pnadc_lfp', 'micro', 'pnadc_female_domcare_micro_2016_2024.parquet')
validation_out <- here::here('data', 'powerIV', 'pnadc_lfp', 'micro', 'pnadc_female_domcare_woman_id_validation_2016_2024.csv')

dir.create(dirname(micro_out), recursive = TRUE, showWarnings = FALSE)

vars <- c(
  'Ano', 'Trimestre', 'UF', 'Estrato', 'UPA', 'V1008', 'V1014', 'V1033', 'V2003',
  'V2007', 'V2009', 'VD3004', 'VD4001', 'VD4002', 'V4078', 'V4078A', 'VD4030',
  'V4063A', 'V4064A', 'V4073', 'V4076', 'V4082', 'V4039C'
)

zip_files <- sort(Sys.glob(here::here('data', 'PNAD-C', 'raw', 'PNADC_*20*.zip')))
zip_files <- zip_files[grepl('PNADC_[0-9]{2}20[0-9]{2}_', basename(zip_files))]

education_secondaryplus <- c('Médio completo ou equivalente', 'Superior incompleto ou equivalente', 'Superior completo')
education_tertiary <- c('Superior incompleto ou equivalente', 'Superior completo')

q_list <- vector('list', length(zip_files))
for (i in seq_along(zip_files)) {
  zf <- zip_files[[i]]
  message('Reading ', basename(zf))
  dat <- read_pnadc(zf, input_txt, vars = vars)
  dat <- pnadc_labeller(dat, dict_file)
  q_list[[i]] <- dat %>%
    mutate(across(everything(), as.character)) %>%
    transmute(
      year = as.integer(Ano),
      quarter = as.integer(Trimestre),
      year_quarter = sprintf('%sQ%s', Ano, Trimestre),
      uf = UF,
      estrato4 = substr(Estrato, 1, 4),
      upa = UPA,
      panel = V1014,
      household_number = V1008,
      person_number = V2003,
      hh_match_key = paste(Ano, Trimestre, UPA, V1014, V1008, sep = '_'),
      person_weight = suppressWarnings(as.numeric(V1033)),
      sex = V2007,
      age = suppressWarnings(as.integer(V2009)),
      education = VD3004,
      female = V2007 == 'Mulher',
      working_age = !is.na(age) & age >= 18 & age <= 64,
      out_labor_force = VD4001 == 'Pessoas fora da força de trabalho',
      employed = VD4002 == 'Pessoas ocupadas',
      domestic_reason_text = coalesce(VD4030, V4078A, V4078),
      domestic_care_reason = str_detect(coalesce(VD4030, V4078A, V4078, ''), regex('afazeres domésticos|afazeres domesticos|filho|parente|dependente', ignore_case = TRUE)),
      female_secondaryplus = !is.na(education) & education %in% education_secondaryplus,
      female_tertiary = !is.na(education) & education %in% education_tertiary,
      child_0_14 = !is.na(age) & age <= 14,
      wants_more_hours = V4063A == 'Sim',
      available_more_hours = V4064A == 'Sim',
      wanted_to_work = V4073 == 'Sim',
      search_duration_text = V4076,
      search_long = V4076 %in% c('De 1 ano a menos de 2 anos', '2 anos ou mais'),
      worked_last_12m = V4082 == 'Sim',
      effective_hours = suppressWarnings(as.numeric(V4039C))
    )
}

person_df <- bind_rows(q_list)

hh_composition <- person_df %>%
  group_by(hh_match_key) %>%
  summarise(hh_size = n(), n_children_0_14 = sum(child_0_14, na.rm = TRUE), .groups = 'drop')

women <- person_df %>%
  filter(female, working_age, !is.na(person_weight), person_weight > 0) %>%
  mutate(
    base_woman_id = paste(upa, panel, household_number, person_number, sep = '_'),
    woman_quarter_id = year * 4L + quarter,
    outlf_domestic_care = as.integer(domestic_care_reason & out_labor_force)
  ) %>%
  arrange(base_woman_id, year, quarter) %>%
  group_by(base_woman_id) %>%
  mutate(
    woman_spell_start = if_else(row_number() == 1L | woman_quarter_id - lag(woman_quarter_id, default = first(woman_quarter_id)) > 1L, 1L, 0L),
    woman_spell_index = cumsum(woman_spell_start),
    woman_spell_id = paste(base_woman_id, sprintf('%03d', woman_spell_index), sep = '_'),
    woman_id = woman_spell_id
  ) %>%
  ungroup() %>%
  left_join(hh_composition, by = 'hh_match_key')

validation_base <- women %>%
  arrange(base_woman_id, year, quarter) %>%
  group_by(base_woman_id) %>%
  summarise(
    n_obs = n(),
    n_distinct_sex = n_distinct(sex),
    age_min = min(age, na.rm = TRUE),
    age_max = max(age, na.rm = TRUE),
    age_span = age_max - age_min,
    .groups = 'drop'
  ) %>%
  summarise(
    id_type = 'base_woman_id',
    n_women = n(),
    share_multi_q = mean(n_obs >= 2),
    share_sex_inconsistent = mean(n_distinct_sex > 1),
    share_age_span_gt2 = mean(age_span > 2, na.rm = TRUE),
    share_age_span_gt4 = mean(age_span > 4, na.rm = TRUE),
    max_age_span = max(age_span, na.rm = TRUE)
  )

validation_spell <- women %>%
  arrange(woman_spell_id, year, quarter) %>%
  group_by(woman_spell_id) %>%
  summarise(
    n_obs = n(),
    n_distinct_sex = n_distinct(sex),
    age_min = min(age, na.rm = TRUE),
    age_max = max(age, na.rm = TRUE),
    age_span = age_max - age_min,
    .groups = 'drop'
  ) %>%
  summarise(
    id_type = 'woman_spell_id',
    n_women = n(),
    share_multi_q = mean(n_obs >= 2),
    share_sex_inconsistent = mean(n_distinct_sex > 1),
    share_age_span_gt2 = mean(age_span > 2, na.rm = TRUE),
    share_age_span_gt4 = mean(age_span > 4, na.rm = TRUE),
    max_age_span = max(age_span, na.rm = TRUE)
  )

validation <- bind_rows(validation_base, validation_spell)

write_parquet(women, micro_out)
write_csv(validation, validation_out)

print(validation)
