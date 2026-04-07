#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(data.table)
})

micro_in <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_2016_2024.parquet")
visit1_in <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
linked_out <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_linked_2016_2024.parquet")
match_out <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_link_summary_2016_2024.csv")
year_out <- here::here("data", "powerIV", "pnadc_lfp", "micro", "pnadc_female_domcare_micro_visit1_link_by_year_quarter_2016_2024.csv")

dir.create(dirname(linked_out), recursive = TRUE, showWarnings = FALSE)

women <- as.data.table(read_parquet(micro_in))
visit1 <- as.data.table(read_parquet(visit1_in))

women[, `:=`(
  upa = as.character(upa),
  panel = as.character(panel),
  household_number = as.character(household_number),
  base_hh_id = paste(as.character(upa), as.character(panel), as.character(household_number), sep = "_"),
  quarter_id = as.integer(year) * 4L + as.integer(quarter)
)]

setorder(women, base_hh_id, quarter_id)
women[, spell_index := cumsum(c(1L, diff(quarter_id) > 1L)), by = base_hh_id]
women[, spell_hh_id := paste(base_hh_id, sprintf("%03d", spell_index), sep = "_")]

spell_ranges <- unique(
  women[, .(
    base_hh_id,
    spell_index,
    spell_hh_id,
    spell_start_q = min(quarter_id),
    spell_end_q = max(quarter_id)
  ), by = .(base_hh_id, spell_index, spell_hh_id)]
)

visit1[, `:=`(
  upa = as.character(upa),
  panel = as.character(panel),
  household_number = as.character(household_number),
  base_hh_id = paste(as.character(upa), as.character(panel), as.character(household_number), sep = "_"),
  visit1_quarter_id = as.integer(year) * 4L + as.integer(quarter)
)]

visit1_cols <- setdiff(names(visit1), "base_hh_id")
setnames(visit1, visit1_cols, paste0("visit1_", visit1_cols))
setnames(visit1, "visit1_visit1_quarter_id", "visit1_quarter_id")

setkey(spell_ranges, base_hh_id)
setkey(visit1, base_hh_id)

spell_candidates <- spell_ranges[visit1, allow.cartesian = TRUE]
spell_candidates[, dist_to_spell := fifelse(
  visit1_quarter_id < spell_start_q,
  spell_start_q - visit1_quarter_id,
  fifelse(visit1_quarter_id > spell_end_q, visit1_quarter_id - spell_end_q, 0L)
)]

setorder(spell_candidates, base_hh_id, dist_to_spell, spell_start_q)
spell_best <- spell_candidates[, .SD[1], by = base_hh_id]
spell_best[, visit1_obs_in_women_spell := dist_to_spell == 0L]

spell_link <- spell_best[, !"base_hh_id"]
setkey(spell_link, spell_hh_id)

women_linked <- merge(
  women,
  spell_link,
  by = "spell_hh_id",
  all.x = TRUE,
  sort = FALSE
)

setorder(women_linked, year, quarter, upa, panel, household_number)
women_linked[, `:=`(
  visit1_match_any = !is.na(visit1_year),
  visit1_same_quarter = !is.na(visit1_year) & year == visit1_year & quarter == visit1_quarter,
  carry_distance_q = fifelse(!is.na(visit1_quarter_id), quarter_id - visit1_quarter_id, NA_integer_),
  carry_abs_distance_q = fifelse(!is.na(visit1_quarter_id), abs(quarter_id - visit1_quarter_id), NA_integer_)
)]

matched <- women_linked[visit1_match_any == TRUE]

match_summary <- data.table(
  metric = c(
    "n_women_rows",
    "n_distinct_base_households",
    "n_distinct_spell_households",
    "spell_match_rate",
    "same_quarter_match_rate",
    "share_visit1_obs_in_women_spell",
    "mean_abs_carry_distance_q",
    "median_abs_carry_distance_q",
    "p90_abs_carry_distance_q",
    "max_abs_carry_distance_q",
    "share_carry_distance_0",
    "share_carry_distance_1",
    "share_carry_distance_2",
    "share_carry_distance_3",
    "share_carry_distance_4"
  ),
  value = c(
    nrow(women_linked),
    uniqueN(women_linked$base_hh_id),
    uniqueN(women_linked$spell_hh_id),
    mean(women_linked$visit1_match_any),
    mean(women_linked$visit1_same_quarter),
    mean(matched$visit1_obs_in_women_spell),
    mean(matched$carry_abs_distance_q),
    median(matched$carry_abs_distance_q),
    as.numeric(quantile(matched$carry_abs_distance_q, 0.9)),
    max(matched$carry_abs_distance_q),
    mean(matched$carry_abs_distance_q == 0L),
    mean(matched$carry_abs_distance_q == 1L),
    mean(matched$carry_abs_distance_q == 2L),
    mean(matched$carry_abs_distance_q == 3L),
    mean(matched$carry_abs_distance_q == 4L)
  )
)

match_by_year_quarter <- women_linked[, .(
  women_n = .N,
  spell_match_rate = mean(visit1_match_any),
  same_quarter_match_rate = mean(visit1_same_quarter),
  share_visit1_obs_in_women_spell = mean(visit1_obs_in_women_spell, na.rm = TRUE),
  mean_abs_carry_distance_q = mean(carry_abs_distance_q, na.rm = TRUE),
  median_abs_carry_distance_q = median(carry_abs_distance_q, na.rm = TRUE)
), by = .(year, quarter)][order(year, quarter)]

write_parquet(women_linked, linked_out, compression = "snappy")
fwrite(match_summary, match_out)
fwrite(match_by_year_quarter, year_out)

print(match_summary)
print(match_by_year_quarter)
