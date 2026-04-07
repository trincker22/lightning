#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(sf)
  library(ggplot2)
  library(scales)
  library(readr)
})

hh_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
shape_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")

fig_dir <- here::here("Figures", "powerIV", "appliances")
out_dir <- here::here("data", "powerIV", "regressions", "appliances")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

washer_map_path <- here::here(fig_dir, "brazil_washing_machine_prevalence_map_2024.png")
appliance_availability_path <- here::here(out_dir, "appliance_availability_2024.csv")
washer_estrato_path <- here::here(out_dir, "washing_machine_prevalence_estrato_2024.csv")

weighted_share <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  out <- weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
  if (is.nan(out)) NA_real_ else out
}

make_map <- function(data_sf, value_col, title_text, subtitle_text, out_path) {
  p <- ggplot(data_sf) +
    geom_sf(aes(fill = .data[[value_col]]), color = "grey60", linewidth = 0.1) +
    scale_fill_viridis_c(labels = percent_format(accuracy = 1), na.value = "grey92") +
    labs(title = title_text, subtitle = subtitle_text, fill = "Share") +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )
  ggsave(out_path, p, width = 10, height = 7, dpi = 300)
}

hh <- read_parquet(
  hh_path,
  col_select = c("year", "estrato4", "weight_calibrated", "washing_machine")
) %>%
  mutate(estrato4 = as.character(estrato4))

washer_2024 <- hh %>%
  filter(year == 2024) %>%
  group_by(estrato4) %>%
  summarise(
    share_washing_machine = weighted_share(washing_machine, weight_calibrated),
    weight_sum = sum(weight_calibrated, na.rm = TRUE),
    .groups = "drop"
  )

estrato_sf <- st_read(shape_path, quiet = TRUE) %>%
  mutate(estrato4 = as.character(estrato4))

map_sf <- estrato_sf %>%
  left_join(washer_2024, by = "estrato4")

make_map(
  map_sf,
  "share_washing_machine",
  "Brazil 2024: washing machine prevalence",
  "Weighted estrato-level PNAD-C household shares",
  washer_map_path
)

appliance_availability <- tibble::tribble(
  ~appliance, ~status_2024, ~source, ~detail,
  "washing_machine", "available", "PNAD-C annual visit 1", "Mapped from processed household variable washing_machine, sourced from raw PNAD-C variable S01024.",
  "dishwasher", "not_found", "PNAD-C annual visit 1", "Not present in the processed household parquet and not present in the 2024 PNAD-C appliance block around S01023-S01031 in input_PNADC_2024_visita1_20251119.txt.",
  "dryer", "not_found", "PNAD-C annual visit 1", "Not present in the processed household parquet and not present in the 2024 PNAD-C appliance block around S01023-S01031 in input_PNADC_2024_visita1_20251119.txt."
)

write_csv(appliance_availability, appliance_availability_path)
write_csv(washer_2024, washer_estrato_path)

cat("Saved outputs to:\n")
cat(washer_map_path, "\n")
cat(appliance_availability_path, "\n")
cat(washer_estrato_path, "\n")
