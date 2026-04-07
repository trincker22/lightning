#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(ggplot2)
  library(sf)
  library(scales)
  library(readr)
})

root_dir <- here::here()

hh_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
shape_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")

fig_dir <- here::here("Figures", "powerIV", "cooking_summary")
out_dir <- here::here("data", "powerIV", "regressions", "cooking_summary")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

all_fuels_plot_path <- here::here(fig_dir, "brazil_cooking_all_reported_trend_2016_2024.png")
primary_plot_path <- here::here(fig_dir, "brazil_cooking_primary_trend_2022_2024.png")
area_plot_path <- here::here(fig_dir, "brazil_cooking_rural_urban_2024.png")
region_plot_path <- here::here(fig_dir, "brazil_cooking_region_2024.png")
wood_any_map_path <- here::here(fig_dir, "brazil_cooking_wood_any_map_2024.png")
wood_primary_map_path <- here::here(fig_dir, "brazil_cooking_wood_primary_map_2024.png")
electricity_any_map_path <- here::here(fig_dir, "brazil_cooking_electricity_any_map_2024.png")
gas_primary_map_path <- here::here(fig_dir, "brazil_cooking_gas_primary_map_2024.png")

key_numbers_path <- here::here(out_dir, "brazil_cooking_key_numbers_2024.csv")
all_fuels_table_path <- here::here(out_dir, "brazil_cooking_all_reported_trend_2016_2024.csv")
primary_table_path <- here::here(out_dir, "brazil_cooking_primary_trend_2022_2024.csv")
area_table_path <- here::here(out_dir, "brazil_cooking_rural_urban_2024.csv")
region_table_path <- here::here(out_dir, "brazil_cooking_region_2024.csv")
slides_path <- here::here(out_dir, "brazil_cooking_slide_summary.md")

weighted_share <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  out <- weighted.mean(x[keep], w[keep], na.rm = TRUE)
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

region_map <- c(
  "Rondônia" = "North",
  "Acre" = "North",
  "Amazonas" = "North",
  "Roraima" = "North",
  "Pará" = "North",
  "Amapá" = "North",
  "Tocantins" = "North",
  "Maranhão" = "Northeast",
  "Piauí" = "Northeast",
  "Ceará" = "Northeast",
  "Rio Grande do Norte" = "Northeast",
  "Paraíba" = "Northeast",
  "Pernambuco" = "Northeast",
  "Alagoas" = "Northeast",
  "Sergipe" = "Northeast",
  "Bahia" = "Northeast",
  "Minas Gerais" = "Southeast",
  "Espírito Santo" = "Southeast",
  "Rio de Janeiro" = "Southeast",
  "São Paulo" = "Southeast",
  "Paraná" = "South",
  "Santa Catarina" = "South",
  "Rio Grande do Sul" = "South",
  "Mato Grosso do Sul" = "Center-West",
  "Mato Grosso" = "Center-West",
  "Goiás" = "Center-West",
  "Distrito Federal" = "Center-West"
)

hh <- read_parquet(
  hh_path,
  col_select = c(
    "year",
    "quarter",
    "uf",
    "estrato4",
    "area_situation",
    "weight_calibrated",
    "cooking_gas_any",
    "cooking_wood_charcoal",
    "cooking_electricity",
    "cooking_other_fuel",
    "cooking_main_fuel"
  )
) %>%
  mutate(
    estrato4 = as.character(estrato4),
    urban_rural = case_when(
      area_situation == "Urbana" ~ "Urban",
      area_situation == "Rural" ~ "Rural",
      TRUE ~ NA_character_
    ),
    region = unname(region_map[uf]),
    main_fuel = case_when(
      str_detect(coalesce(cooking_main_fuel, ""), regex("g[aá]s", ignore_case = TRUE)) ~ "Gas",
      str_detect(coalesce(cooking_main_fuel, ""), regex("lenha|carv", ignore_case = TRUE)) ~ "Wood/charcoal",
      str_detect(coalesce(cooking_main_fuel, ""), regex("energia el[eé]trica", ignore_case = TRUE)) ~ "Electricity",
      !is.na(cooking_main_fuel) ~ "Other",
      TRUE ~ NA_character_
    )
  )

all_fuels_table <- hh %>%
  group_by(year, quarter) %>%
  summarise(
    gas = weighted_share(cooking_gas_any, weight_calibrated),
    wood_charcoal = weighted_share(cooking_wood_charcoal, weight_calibrated),
    electricity = weighted_share(cooking_electricity, weight_calibrated),
    other = weighted_share(cooking_other_fuel, weight_calibrated),
    .groups = "drop"
  ) %>%
  mutate(quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L)))

primary_table <- hh %>%
  filter(year >= 2022, !is.na(main_fuel)) %>%
  group_by(year, quarter, main_fuel) %>%
  summarise(weight = sum(weight_calibrated, na.rm = TRUE), .groups = "drop_last") %>%
  mutate(share = weight / sum(weight, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L)))

all_fuels_long <- bind_rows(
  all_fuels_table %>% transmute(quarter_start, fuel = "Gas", share = gas),
  all_fuels_table %>% transmute(quarter_start, fuel = "Wood/charcoal", share = wood_charcoal),
  all_fuels_table %>% transmute(quarter_start, fuel = "Electricity", share = electricity),
  all_fuels_table %>% transmute(quarter_start, fuel = "Other", share = other)
) %>%
  mutate(fuel = factor(fuel, levels = c("Gas", "Wood/charcoal", "Electricity", "Other")))

primary_long <- primary_table %>%
  filter(main_fuel %in% c("Gas", "Wood/charcoal", "Electricity")) %>%
  mutate(main_fuel = factor(main_fuel, levels = c("Gas", "Wood/charcoal", "Electricity")))

area_any_2024 <- hh %>%
  filter(year == 2024, !is.na(urban_rural)) %>%
  group_by(urban_rural) %>%
  summarise(
    Gas = weighted_share(cooking_gas_any, weight_calibrated),
    `Wood/charcoal` = weighted_share(cooking_wood_charcoal, weight_calibrated),
    Electricity = weighted_share(cooking_electricity, weight_calibrated),
    .groups = "drop"
  ) %>%
  pivot_longer(-urban_rural, names_to = "fuel", values_to = "share") %>%
  mutate(measure = "Any reported use")

area_primary_2024 <- hh %>%
  filter(year == 2024, !is.na(urban_rural), !is.na(main_fuel), main_fuel %in% c("Gas", "Wood/charcoal", "Electricity")) %>%
  group_by(urban_rural, main_fuel) %>%
  summarise(weight = sum(weight_calibrated, na.rm = TRUE), .groups = "drop_last") %>%
  mutate(share = weight / sum(weight, na.rm = TRUE)) %>%
  ungroup() %>%
  transmute(urban_rural, fuel = main_fuel, share, measure = "Primary fuel")

area_plot_data <- bind_rows(area_any_2024, area_primary_2024) %>%
  mutate(
    urban_rural = factor(urban_rural, levels = c("Urban", "Rural")),
    fuel = factor(fuel, levels = c("Gas", "Wood/charcoal", "Electricity")),
    measure = factor(measure, levels = c("Any reported use", "Primary fuel"))
  )

region_any_2024 <- hh %>%
  filter(year == 2024, !is.na(region)) %>%
  group_by(region) %>%
  summarise(
    Gas = weighted_share(cooking_gas_any, weight_calibrated),
    `Wood/charcoal` = weighted_share(cooking_wood_charcoal, weight_calibrated),
    Electricity = weighted_share(cooking_electricity, weight_calibrated),
    .groups = "drop"
  ) %>%
  pivot_longer(-region, names_to = "fuel", values_to = "share") %>%
  mutate(measure = "Any reported use")

region_primary_2024 <- hh %>%
  filter(year == 2024, !is.na(region), !is.na(main_fuel), main_fuel %in% c("Gas", "Wood/charcoal", "Electricity")) %>%
  group_by(region, main_fuel) %>%
  summarise(weight = sum(weight_calibrated, na.rm = TRUE), .groups = "drop_last") %>%
  mutate(share = weight / sum(weight, na.rm = TRUE)) %>%
  ungroup() %>%
  transmute(region, fuel = main_fuel, share, measure = "Primary fuel")

region_plot_data <- bind_rows(region_any_2024, region_primary_2024) %>%
  mutate(
    region = factor(region, levels = c("North", "Northeast", "Center-West", "Southeast", "South")),
    fuel = factor(fuel, levels = c("Gas", "Wood/charcoal", "Electricity")),
    measure = factor(measure, levels = c("Any reported use", "Primary fuel"))
  )

key_numbers <- bind_rows(
  hh %>%
    filter(year == 2024, quarter == 4, !is.na(main_fuel), main_fuel %in% c("Gas", "Wood/charcoal", "Electricity")) %>%
    group_by(metric = "Primary fuel 2024 Q4", category = main_fuel) %>%
    summarise(weight = sum(weight_calibrated, na.rm = TRUE), .groups = "drop_last") %>%
    mutate(share = weight / sum(weight, na.rm = TRUE)) %>%
    ungroup(),
  hh %>%
    filter(year == 2024) %>%
    summarise(
      Gas = weighted_share(cooking_gas_any, weight_calibrated),
      `Wood/charcoal` = weighted_share(cooking_wood_charcoal, weight_calibrated),
      Electricity = weighted_share(cooking_electricity, weight_calibrated)
    ) %>%
    pivot_longer(everything(), names_to = "category", values_to = "share") %>%
    mutate(metric = "Any reported fuel 2024", weight = NA_real_) %>%
    select(metric, category, weight, share)
) %>%
  mutate(share_percent = 100 * share)

estrato_sf <- st_read(shape_path, quiet = TRUE) %>%
  mutate(estrato4 = as.character(estrato4))

map_base_2024 <- hh %>%
  filter(year == 2024) %>%
  group_by(estrato4) %>%
  summarise(
    wood_any = weighted_share(cooking_wood_charcoal, weight_calibrated),
    electricity_any = weighted_share(cooking_electricity, weight_calibrated),
    gas_primary = weighted_share(as.integer(main_fuel == "Gas"), weight_calibrated),
    wood_primary = weighted_share(as.integer(main_fuel == "Wood/charcoal"), weight_calibrated),
    .groups = "drop"
  )

map_sf <- estrato_sf %>%
  left_join(map_base_2024, by = "estrato4")

all_fuels_plot <- ggplot(all_fuels_long, aes(x = quarter_start, y = share, color = fuel)) +
  geom_line(linewidth = 0.95, na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_color_manual(values = c("Gas" = "#01665e", "Wood/charcoal" = "#8c510a", "Electricity" = "#2c7fb8", "Other" = "#636363")) +
  labs(
    title = "Brazil cooking fuels: all reported fuels over time",
    subtitle = "Weighted PNAD-C household shares; households can report more than one fuel",
    x = NULL,
    y = "Household share",
    color = NULL
  ) +
  theme_minimal(base_size = 11)

primary_plot <- ggplot(primary_long, aes(x = quarter_start, y = share, color = main_fuel)) +
  geom_line(linewidth = 0.95, na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_color_manual(values = c("Gas" = "#01665e", "Wood/charcoal" = "#8c510a", "Electricity" = "#2c7fb8")) +
  labs(
    title = "Brazil cooking fuels: primary fuel over time",
    subtitle = "Weighted PNAD-C household shares; primary fuel variable available from 2022 onward",
    x = NULL,
    y = "Household share",
    color = NULL
  ) +
  theme_minimal(base_size = 11)

area_plot <- ggplot(area_plot_data, aes(x = fuel, y = share, fill = urban_rural)) +
  geom_col(position = position_dodge(width = 0.72), width = 0.64) +
  facet_wrap(~measure) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_fill_manual(values = c("Urban" = "#4d4d4d", "Rural" = "#c51b7d")) +
  labs(
    title = "Cooking fuel prevalence is far more wood-intensive in rural Brazil",
    subtitle = "2024 weighted PNAD-C household shares",
    x = NULL,
    y = "Household share",
    fill = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.major.x = element_blank())

region_plot <- ggplot(region_plot_data, aes(x = region, y = share, fill = fuel)) +
  geom_col(position = "stack", width = 0.72) +
  facet_wrap(~measure) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_fill_manual(values = c("Gas" = "#01665e", "Wood/charcoal" = "#8c510a", "Electricity" = "#2c7fb8")) +
  labs(
    title = "Regional concentration matters mostly for wood/charcoal",
    subtitle = "2024 weighted PNAD-C household shares",
    x = NULL,
    y = "Household share",
    fill = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle = 20, hjust = 1))

ggsave(all_fuels_plot_path, all_fuels_plot, width = 10, height = 6, dpi = 300)
ggsave(primary_plot_path, primary_plot, width = 10, height = 6, dpi = 300)
ggsave(area_plot_path, area_plot, width = 10, height = 6, dpi = 300)
ggsave(region_plot_path, region_plot, width = 10, height = 6, dpi = 300)

make_map(
  map_sf,
  "wood_any",
  "Brazil cooking fuels: any reported wood or charcoal use",
  "Weighted estrato-level household shares, 2024",
  wood_any_map_path
)

make_map(
  map_sf,
  "wood_primary",
  "Brazil cooking fuels: wood or charcoal as primary fuel",
  "Weighted estrato-level household shares, 2024",
  wood_primary_map_path
)

make_map(
  map_sf,
  "electricity_any",
  "Brazil cooking fuels: any reported electricity use",
  "Weighted estrato-level household shares, 2024",
  electricity_any_map_path
)

make_map(
  map_sf,
  "gas_primary",
  "Brazil cooking fuels: gas as primary fuel",
  "Weighted estrato-level household shares, 2024",
  gas_primary_map_path
)

write_csv(key_numbers, key_numbers_path)
write_csv(all_fuels_table, all_fuels_table_path)
write_csv(primary_table, primary_table_path)
write_csv(area_plot_data, area_table_path)
write_csv(region_plot_data, region_table_path)

slide_lines <- c(
  "# Brazil Cooking Fuels",
  "",
  "## Slide 1",
  "Brazil is a gas-dominant cooking country on primary fuel, but many households also report electricity use for cooking.",
  paste0("Visuals: ", basename(primary_plot_path), "; ", basename(all_fuels_plot_path)),
  "",
  "## Slide 2",
  "Primary fuel shifted modestly away from wood/charcoal from 2022 to 2024, while gas became even more dominant.",
  paste0("Visuals: ", basename(primary_plot_path), "; ", basename(gas_primary_map_path)),
  "",
  "## Slide 3",
  "Wood/charcoal is heavily concentrated in rural Brazil when we look at primary reliance, even though urban households also report some secondary wood use.",
  paste0("Visuals: ", basename(area_plot_path), "; ", basename(wood_primary_map_path)),
  "",
  "## Slide 4",
  "Regional concentration is strongest in the Northeast and North for wood/charcoal, while electricity use is broader and often secondary.",
  paste0("Visuals: ", basename(region_plot_path), "; ", basename(wood_any_map_path), "; ", basename(electricity_any_map_path)),
  "",
  "## Slide 5",
  "Work takeaway: gas dominates the main cooking system nationally, electricity is often additive, and wood/charcoal remains a targeted rural and regional issue."
)

write_lines(slide_lines, slides_path)

cat("Saved outputs to:\n")
cat(fig_dir, "\n")
cat(out_dir, "\n")
