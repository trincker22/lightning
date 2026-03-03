#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(readr)
  library(lubridate)
})

root_dir <- here::here()

in_path <- file.path(root_dir, "data", "ANEEL", "Feather", "out_all.feather")
out_dir <- file.path(root_dir, "data", "powerIV", "outages")
panel_dir <- file.path(out_dir, "panels")
qa_dir <- file.path(out_dir, "qa")
dir.create(panel_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

conj_month_out <- file.path(panel_dir, "raw_event_conj_month.parquet")
qa_out <- file.path(qa_dir, "raw_event_conj_month_qa.csv")

if (all(file.exists(c(conj_month_out, qa_out)))) {
  message("All raw outage outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

out_ds <- open_dataset(in_path, format = "feather")
conj_month <- out_ds %>%
  select(consumer_unit_group_id, interruption_start, customer_outage_min, group_consumer_count) %>%
  mutate(year = year(interruption_start), month = month(interruption_start)) %>%
  group_by(consumer_unit_group_id, year, month) %>%
  summarise(
    total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
    n_events = n(),
    group_consumer_count_avg = mean(group_consumer_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  collect() %>%
  transmute(
    conj_code = consumer_unit_group_id,
    year = as.integer(year),
    month = as.integer(month),
    date = as.Date(sprintf("%04d-%02d-01", year, month)),
    total_customer_outage_hrs = as.numeric(total_customer_outage_hrs),
    n_events = as.integer(n_events),
    group_consumer_count_avg = as.numeric(group_consumer_count_avg)
  ) %>%
  filter(date >= as.Date("2017-01-01"), date <= as.Date("2021-12-01"))

write_parquet(conj_month, conj_month_out)

qa <- tibble(
  metric = c("rows", "distinct_conj", "min_date", "max_date"),
  value = c(
    as.character(nrow(conj_month)),
    as.character(n_distinct(conj_month$conj_code)),
    as.character(min(conj_month$date, na.rm = TRUE)),
    as.character(max(conj_month$date, na.rm = TRUE))
  )
)
write_csv(qa, qa_out)

message("Wrote: ", conj_month_out)
message("QA: ", qa_out)
