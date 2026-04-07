#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(sf)
  library(ggplot2)
  library(scales)
  library(fixest)
  library(broom)
  library(readr)
  library(tidyr)
})

root_dir <- here::here()
coverage_script <- here::here("Code", "07a_power_coverage_audit.R")
if (file.exists(coverage_script)) {
  cov_status <- tryCatch(system2("Rscript", coverage_script), error = function(e) 1L)
  if (!identical(cov_status, 0L)) {
    warning("Coverage audit refresh failed in Script 10.")
  }
}

lfp_panel_path <- here::here("data", "powerIV", "pnadc_lfp", "panels", "pnadc_estrato4_quarter_lfp_with_power_2016_2024.parquet")
power_panel_path <- here::here("data", "powerIV", "pnadc_power", "panels", "pnadc_estrato4_quarter_with_power.parquet")
visit1_hh_path <- here::here("data", "powerIV", "pnadc", "pnadc_visit1_household_2016_2024.parquet")
estrato_shape_path <- here::here("data", "powerIV", "pnadc", "shapes", "pnadc_estrato_146_2020.gpkg")

fig_dir <- here::here("Figures", "powerIV", "lfp")
out_dir <- here::here("data", "powerIV", "regressions", "lfp")
tab_dir <- here::here(out_dir, "tables")
qa_dir <- here::here("data", "powerIV", "pnadc_lfp", "qa")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

map_start_path <- here::here(fig_dir, "map_female_lfp_2016q1.png")
map_end_path <- here::here(fig_dir, "map_female_lfp_2024q4.png")
reg_csv_path <- here::here(tab_dir, "flfp_on_charcoal_basic.csv")
reg_tex_path <- here::here(tab_dir, "flfp_on_charcoal_basic.tex")
reg_fe_controls_tex_path <- here::here(tab_dir, "flfp_on_charcoal_fe_controls_compare.tex")
hours_reg_csv_path <- here::here(tab_dir, "hours_on_cooking_fuels.csv")
hours_reg_tex_path <- here::here(tab_dir, "hours_on_cooking_fuels.tex")
hours_gas_reg_csv_path <- here::here(tab_dir, "hours_on_cooking_fuels_with_gas.csv")
hours_gas_reg_tex_path <- here::here(tab_dir, "hours_on_cooking_fuels_with_gas.tex")
hours_wood_fe_tex_path <- here::here(tab_dir, "female_hours_effective_employed_wood_fe_sensitivity.tex")
map_stats_path <- here::here(qa_dir, "female_lfp_map_summary_2016q1_2024q4.csv")
fuel_trend_fig_path <- here::here(fig_dir, "fuel_shares_national_quarterly.png")
main_fuel_trend_fig_path <- here::here(fig_dir, "main_fuel_shares_national_quarterly_2022_2024.png")
main_fuel_lfp_results_path <- here::here(tab_dir, "main_fuel_lfp_results.csv")

lfp <- read_parquet(lfp_panel_path) %>%
  mutate(estrato4 = as.character(estrato4))

power <- read_parquet(power_panel_path) %>%
  transmute(
    estrato4 = as.character(estrato4),
    year,
    quarter,
    share_cooking_wood_charcoal,
    share_cooking_electricity,
    share_cooking_gas_any,
    share_cooking_other_fuel,
    share_vehicle,
    share_internet_home,
    share_electricity_from_grid,
    share_grid_full_time
  )

panel <- lfp %>%
  left_join(power, by = c("estrato4", "year", "quarter")) %>%
  mutate(
    quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  ) %>%
  arrange(estrato4, quarter_start) %>%
  group_by(estrato4) %>%
  mutate(
    share_cooking_wood_charcoal_l1 = lag(share_cooking_wood_charcoal, 1),
    share_cooking_electricity_l1 = lag(share_cooking_electricity, 1),
    share_cooking_gas_any_l1 = lag(share_cooking_gas_any, 1)
  ) %>%
  ungroup() %>%
  select(-quarter_start)

estrato_sf <- st_read(estrato_shape_path, quiet = TRUE) %>%
  mutate(estrato4 = as.character(estrato4))

make_map <- function(data_sf, value_col, title_text, subtitle_text, out_path) {
  p <- ggplot(data_sf) +
    geom_sf(aes(fill = .data[[value_col]]), color = "grey60", linewidth = 0.1) +
    scale_fill_viridis_c(labels = percent_format(accuracy = 1), na.value = "grey90") +
    labs(title = title_text, subtitle = subtitle_text, fill = "Share") +
    theme_void() +
    theme(plot.title = element_text(face = "bold"), legend.position = "right")
  ggsave(out_path, p, width = 10, height = 7, dpi = 300)
}

start_panel <- panel %>% filter(year == 2016, quarter == 1)
end_panel <- panel %>% filter(year == 2024, quarter == 4)

start_sf <- estrato_sf %>% left_join(start_panel, by = "estrato4")
end_sf <- estrato_sf %>% left_join(end_panel, by = "estrato4")

make_map(
  start_sf,
  "female_lfp",
  "PNAD-C strata: female labor force participation",
  "2016 Q1",
  map_start_path
)

make_map(
  end_sf,
  "female_lfp",
  "PNAD-C strata: female labor force participation",
  "2024 Q4",
  map_end_path
)

summarize_period <- function(df, y, q, label) {
  d <- df %>% filter(year == y, quarter == q)
  tibble(
    period = label,
    n_estrato = nrow(d),
    pop_weighted_mean = weighted.mean(d$female_lfp, d$weight_sum, na.rm = TRUE),
    estrato_mean = mean(d$female_lfp, na.rm = TRUE),
    estrato_median = median(d$female_lfp, na.rm = TRUE),
    estrato_sd = sd(d$female_lfp, na.rm = TRUE),
    p10 = quantile(d$female_lfp, 0.10, na.rm = TRUE, names = FALSE),
    p90 = quantile(d$female_lfp, 0.90, na.rm = TRUE, names = FALSE),
    min = min(d$female_lfp, na.rm = TRUE),
    max = max(d$female_lfp, na.rm = TRUE)
  )
}

map_stats <- bind_rows(
  summarize_period(panel, 2016, 1, "2016 Q1"),
  summarize_period(panel, 2024, 4, "2024 Q4")
)
write_csv(map_stats, map_stats_path)

reg_data <- panel %>%
  filter(
    !is.na(female_lfp),
    !is.na(share_cooking_wood_charcoal),
    !is.na(share_vehicle),
    !is.na(share_internet_home),
    !is.na(share_electricity_from_grid),
    !is.na(share_grid_full_time),
    !is.na(weight_sum),
    weight_sum > 0
  )

m1 <- feols(
  female_lfp ~ share_cooking_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

m2 <- feols(
  female_lfp ~ share_cooking_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

m0 <- feols(
  female_lfp ~ share_cooking_wood_charcoal | year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

m3 <- feols(
  female_lfp ~ share_cooking_wood_charcoal + share_vehicle + share_internet_home | year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

m4 <- feols(
  female_lfp ~ share_cooking_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | uf + year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

m5 <- feols(
  female_lfp ~ share_cooking_wood_charcoal + share_vehicle + share_internet_home | estrato4 + year + quarter,
  data = reg_data,
  weights = ~weight_sum,
  cluster = ~estrato4
)

etable(
  m1,
  m2,
  tex = TRUE,
  file = reg_tex_path,
  replace = TRUE,
  fitstat = ~n + wr2,
  headers = c("Controls + time FE", "Controls + estrato FE + time FE"),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female labor force participation and wood/charcoal cooking"
)

etable(
  m0,
  m3,
  m1,
  m4,
  m5,
  m2,
  tex = TRUE,
  file = reg_fe_controls_tex_path,
  replace = TRUE,
  fitstat = ~n + wr2,
  headers = c(
    "Time FE only",
    "Time FE + socio controls",
    "Time FE + full controls",
    "UF FE + time FE + full controls",
    "Estrato FE + time FE + socio controls",
    "Estrato FE + time FE + full controls"
  ),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female labor force participation and wood/charcoal cooking: FE and controls sensitivity"
)

reg_terms <- bind_rows(
  tidy(m1, conf.int = TRUE) %>% mutate(model = "controls_time_fe"),
  tidy(m2, conf.int = TRUE) %>% mutate(model = "controls_estrato_time_fe")
) %>%
  filter(term == "share_cooking_wood_charcoal") %>%
  mutate(
    nobs = c(nobs(m1), nobs(m2)),
    sample_years = paste(sort(unique(reg_data$year)), collapse = ",")
  )

write_csv(reg_terms, reg_csv_path)

fuel_trend <- panel %>%
  mutate(quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))) %>%
  group_by(quarter_start) %>%
  summarise(
    wood_charcoal = weighted.mean(share_cooking_wood_charcoal, weight_sum, na.rm = TRUE),
    gas = weighted.mean(share_cooking_gas_any, weight_sum, na.rm = TRUE),
    electricity = weighted.mean(share_cooking_electricity, weight_sum, na.rm = TRUE),
    other = weighted.mean(share_cooking_other_fuel, weight_sum, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    wood_charcoal = ifelse(is.nan(wood_charcoal), NA_real_, wood_charcoal),
    gas = ifelse(is.nan(gas), NA_real_, gas),
    electricity = ifelse(is.nan(electricity), NA_real_, electricity),
    other = ifelse(is.nan(other), NA_real_, other)
  )

fuel_trend_long <- bind_rows(
  fuel_trend %>% transmute(quarter_start, fuel = "Wood/charcoal", share = wood_charcoal),
  fuel_trend %>% transmute(quarter_start, fuel = "Gas (any)", share = gas),
  fuel_trend %>% transmute(quarter_start, fuel = "Electricity", share = electricity),
  fuel_trend %>% transmute(quarter_start, fuel = "Other fuel", share = other)
)

fuel_plot <- ggplot(fuel_trend_long, aes(x = quarter_start, y = share, color = fuel)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_color_manual(values = c("Wood/charcoal" = "#8c510a", "Gas (any)" = "#01665e", "Electricity" = "#2c7fb8", "Other fuel" = "#636363")) +
  labs(
    title = "National cooking fuel shares over time",
    subtitle = "PNAD-C weighted quarterly means (pandemic gap reflects module interruption)",
    x = NULL,
    y = "Household share",
    color = NULL
  ) +
  theme_minimal(base_size = 11)

ggsave(fuel_trend_fig_path, fuel_plot, width = 10, height = 6, dpi = 300)

hh_visit1 <- read_parquet(visit1_hh_path) %>%
  transmute(
    year,
    quarter,
    estrato4 = as.character(estrato4),
    weight_calibrated,
    cooking_main_fuel
  ) %>%
  mutate(
    main_fuel_gas = case_when(
      is.na(cooking_main_fuel) ~ NA_integer_,
      grepl("g[aá]s", cooking_main_fuel, ignore.case = TRUE, perl = TRUE) ~ 1L,
      TRUE ~ 0L
    ),
    main_fuel_wood_charcoal = case_when(
      is.na(cooking_main_fuel) ~ NA_integer_,
      grepl("lenha|carv", cooking_main_fuel, ignore.case = TRUE, perl = TRUE) ~ 1L,
      TRUE ~ 0L
    ),
    main_fuel_electricity = case_when(
      is.na(cooking_main_fuel) ~ NA_integer_,
      grepl("energia el[eé]trica", cooking_main_fuel, ignore.case = TRUE, perl = TRUE) ~ 1L,
      TRUE ~ 0L
    ),
    main_fuel_other = case_when(
      is.na(cooking_main_fuel) ~ NA_integer_,
      main_fuel_gas == 1L | main_fuel_wood_charcoal == 1L | main_fuel_electricity == 1L ~ 0L,
      TRUE ~ 1L
    )
  )

weighted_share_na <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) return(NA_real_)
  out <- weighted.mean(x[keep], w[keep], na.rm = TRUE)
  if (is.nan(out)) NA_real_ else out
}

main_fuel_panel <- hh_visit1 %>%
  group_by(year, quarter, estrato4) %>%
  summarise(
    main_weight_sum = sum(weight_calibrated, na.rm = TRUE),
    share_main_fuel_gas = weighted_share_na(main_fuel_gas, weight_calibrated),
    share_main_fuel_wood_charcoal = weighted_share_na(main_fuel_wood_charcoal, weight_calibrated),
    share_main_fuel_electricity = weighted_share_na(main_fuel_electricity, weight_calibrated),
    share_main_fuel_other = weighted_share_na(main_fuel_other, weight_calibrated),
    .groups = "drop"
  )

main_fuel_national <- main_fuel_panel %>%
  filter(year >= 2022) %>%
  mutate(quarter_start = as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))) %>%
  group_by(quarter_start) %>%
  summarise(
    share_main_fuel_gas = weighted.mean(share_main_fuel_gas, main_weight_sum, na.rm = TRUE),
    share_main_fuel_wood_charcoal = weighted.mean(share_main_fuel_wood_charcoal, main_weight_sum, na.rm = TRUE),
    share_main_fuel_electricity = weighted.mean(share_main_fuel_electricity, main_weight_sum, na.rm = TRUE),
    share_main_fuel_other = weighted.mean(share_main_fuel_other, main_weight_sum, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    across(starts_with("share_main_fuel_"), ~ ifelse(is.nan(.x), NA_real_, .x)),
    total_share = share_main_fuel_gas + share_main_fuel_wood_charcoal + share_main_fuel_electricity + share_main_fuel_other
  )

main_fuel_long <- main_fuel_national %>%
  select(quarter_start, share_main_fuel_gas, share_main_fuel_wood_charcoal, share_main_fuel_electricity, share_main_fuel_other) %>%
  pivot_longer(-quarter_start, names_to = "fuel", values_to = "share") %>%
  mutate(
    fuel = recode(
      fuel,
      share_main_fuel_gas = "Main fuel: gas",
      share_main_fuel_wood_charcoal = "Main fuel: wood/charcoal",
      share_main_fuel_electricity = "Main fuel: electricity",
      share_main_fuel_other = "Main fuel: other"
    )
  )

main_fuel_plot <- ggplot(main_fuel_long, aes(x = quarter_start, y = share, color = fuel)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_color_manual(values = c(
    "Main fuel: gas" = "#01665e",
    "Main fuel: wood/charcoal" = "#8c510a",
    "Main fuel: electricity" = "#2c7fb8",
    "Main fuel: other" = "#636363"
  )) +
  labs(
    title = "National main cooking fuel shares over time",
    subtitle = "PNAD-C weighted quarterly means, 2022 Q1 to 2024 Q4",
    x = NULL,
    y = "Household share",
    color = NULL
  ) +
  theme_minimal(base_size = 11)

ggsave(main_fuel_trend_fig_path, main_fuel_plot, width = 10, height = 6, dpi = 300)

main_fuel_reg_data <- panel %>%
  left_join(main_fuel_panel, by = c("year", "quarter", "estrato4")) %>%
  filter(
    year >= 2022,
    !is.na(female_lfp),
    !is.na(weight_sum),
    weight_sum > 0,
    !is.na(share_vehicle),
    !is.na(share_internet_home),
    !is.na(share_electricity_from_grid),
    !is.na(share_grid_full_time)
  )

wood_main_fe_yq <- feols(
  female_lfp ~ share_main_fuel_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
wood_main_fe_ufyq <- feols(
  female_lfp ~ share_main_fuel_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | uf + year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
wood_main_fe_eyq <- feols(
  female_lfp ~ share_main_fuel_wood_charcoal + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
gas_main_fe_eyq <- feols(
  female_lfp ~ share_main_fuel_gas + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_gas)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
electric_main_fe_eyq <- feols(
  female_lfp ~ share_main_fuel_electricity + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
joint_main_fe_eyq <- feols(
  female_lfp ~ share_main_fuel_wood_charcoal + share_main_fuel_gas + share_main_fuel_electricity + share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time | estrato4 + year + quarter,
  data = main_fuel_reg_data %>% filter(!is.na(share_main_fuel_wood_charcoal), !is.na(share_main_fuel_gas), !is.na(share_main_fuel_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)

main_fuel_results <- bind_rows(
  tidy(wood_main_fe_yq, conf.int = TRUE) %>% filter(term == "share_main_fuel_wood_charcoal") %>% mutate(model = "female_lfp_main_wood_year_quarter"),
  tidy(wood_main_fe_ufyq, conf.int = TRUE) %>% filter(term == "share_main_fuel_wood_charcoal") %>% mutate(model = "female_lfp_main_wood_uf_year_quarter"),
  tidy(wood_main_fe_eyq, conf.int = TRUE) %>% filter(term == "share_main_fuel_wood_charcoal") %>% mutate(model = "female_lfp_main_wood_estrato_year_quarter"),
  tidy(gas_main_fe_eyq, conf.int = TRUE) %>% filter(term == "share_main_fuel_gas") %>% mutate(model = "female_lfp_main_gas_estrato_year_quarter"),
  tidy(electric_main_fe_eyq, conf.int = TRUE) %>% filter(term == "share_main_fuel_electricity") %>% mutate(model = "female_lfp_main_electricity_estrato_year_quarter"),
  tidy(joint_main_fe_eyq, conf.int = TRUE) %>% filter(term %in% c("share_main_fuel_wood_charcoal", "share_main_fuel_gas", "share_main_fuel_electricity")) %>% mutate(model = "female_lfp_main_joint_estrato_year_quarter")
) %>%
  mutate(
    nobs = c(
      nobs(wood_main_fe_yq),
      nobs(wood_main_fe_ufyq),
      nobs(wood_main_fe_eyq),
      nobs(gas_main_fe_eyq),
      nobs(electric_main_fe_eyq),
      rep(nobs(joint_main_fe_eyq), 3)
    )
  )

write_csv(main_fuel_results, main_fuel_lfp_results_path)

cat("\nMain fuel LFP check (2022-2024):\n")
print(main_fuel_results %>% select(model, term, estimate, std.error, p.value, nobs))
cat(sprintf("Main fuel national share sum min/max: %.4f / %.4f\n", min(main_fuel_national$total_share, na.rm = TRUE), max(main_fuel_national$total_share, na.rm = TRUE)))

hours_controls <- "share_vehicle + share_internet_home + share_electricity_from_grid + share_grid_full_time"

hours_data <- panel %>%
  filter(
    !is.na(weight_sum),
    weight_sum > 0,
    !is.na(share_vehicle),
    !is.na(share_internet_home),
    !is.na(share_electricity_from_grid),
    !is.na(share_grid_full_time)
  )

h1 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h2 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal_l1 + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal_l1)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h3 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_electricity + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h4 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_electricity_l1 + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_electricity_l1)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h5 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_wood_charcoal + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h6 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_wood_charcoal_l1 + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_wood_charcoal_l1)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h7 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_electricity + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h8 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_electricity_l1 + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_electricity_l1)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h9 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_gas_any + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_gas_any)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h10 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + share_cooking_gas_any + share_cooking_electricity + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal), !is.na(share_cooking_gas_any), !is.na(share_cooking_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h11 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_gas_any + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_gas_any)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
h12 <- feols(
  as.formula(paste0("female_hours_effective_population ~ share_cooking_wood_charcoal + share_cooking_gas_any + share_cooking_electricity + ", hours_controls, " | estrato4 + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_population), !is.na(share_cooking_wood_charcoal), !is.na(share_cooking_gas_any), !is.na(share_cooking_electricity)),
  weights = ~weight_sum,
  cluster = ~estrato4
)

etable(
  h1, h2, h3, h4, h5, h6, h7, h8,
  tex = TRUE,
  file = hours_reg_tex_path,
  replace = TRUE,
  fitstat = ~n + wr2,
  headers = c(
    "Eff. hrs employed: wood t",
    "Eff. hrs employed: wood t-1",
    "Eff. hrs employed: electric t",
    "Eff. hrs employed: electric t-1",
    "Eff. hrs pop: wood t",
    "Eff. hrs pop: wood t-1",
    "Eff. hrs pop: electric t",
    "Eff. hrs pop: electric t-1"
  ),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_cooking_wood_charcoal_l1" = "Wood/charcoal cooking share (lag 1q)",
    "share_cooking_electricity" = "Electric cooking share",
    "share_cooking_electricity_l1" = "Electric cooking share (lag 1q)",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female hours outcomes and cooking fuels"
)

etable(
  h1, h3, h9, h10, h5, h7, h11, h12,
  tex = TRUE,
  file = hours_gas_reg_tex_path,
  replace = TRUE,
  fitstat = ~n + wr2,
  headers = c(
    "Eff. hrs employed: wood",
    "Eff. hrs employed: electric",
    "Eff. hrs employed: gas",
    "Eff. hrs employed: joint fuels",
    "Eff. hrs pop: wood",
    "Eff. hrs pop: electric",
    "Eff. hrs pop: gas",
    "Eff. hrs pop: joint fuels"
  ),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_cooking_gas_any" = "Gas cooking share",
    "share_cooking_electricity" = "Electric cooking share",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female hours outcomes and cooking fuels, including gas"
)

wood_hours_fe_1 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + ", hours_controls, " | year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
wood_hours_fe_2 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + ", hours_controls, " | uf + year + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
wood_hours_fe_3 <- h1
wood_hours_fe_4 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + ", hours_controls, " | estrato4 + year")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)
wood_hours_fe_5 <- feols(
  as.formula(paste0("female_hours_effective_employed ~ share_cooking_wood_charcoal + ", hours_controls, " | estrato4 + quarter")),
  data = hours_data %>% filter(!is.na(female_hours_effective_employed), !is.na(share_cooking_wood_charcoal)),
  weights = ~weight_sum,
  cluster = ~estrato4
)

etable(
  wood_hours_fe_1, wood_hours_fe_2, wood_hours_fe_3, wood_hours_fe_4, wood_hours_fe_5,
  tex = TRUE,
  file = hours_wood_fe_tex_path,
  replace = TRUE,
  fitstat = ~n + wr2,
  headers = c(
    "year + quarter",
    "uf + year + quarter",
    "estrato4 + year + quarter",
    "estrato4 + year",
    "estrato4 + quarter"
  ),
  dict = c(
    "share_cooking_wood_charcoal" = "Wood/charcoal cooking share",
    "share_vehicle" = "Vehicle share",
    "share_internet_home" = "Internet at home share",
    "share_electricity_from_grid" = "Grid electricity share",
    "share_grid_full_time" = "Full-time grid share"
  ),
  title = "Female effective hours (employed) on wood/charcoal cooking share: FE sensitivity"
)

hours_terms <- bind_rows(
  tidy(h1, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_wood_t"),
  tidy(h2, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_wood_t1"),
  tidy(h3, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_elec_t"),
  tidy(h4, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_elec_t1"),
  tidy(h5, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_wood_t"),
  tidy(h6, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_wood_t1"),
  tidy(h7, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_elec_t"),
  tidy(h8, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_elec_t1"),
  tidy(h9, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_gas_t"),
  tidy(h10, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_joint_t"),
  tidy(h11, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_gas_t"),
  tidy(h12, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_joint_t")
) %>%
  filter(term %in% c(
    "share_cooking_wood_charcoal",
    "share_cooking_wood_charcoal_l1",
    "share_cooking_electricity",
    "share_cooking_electricity_l1",
    "share_cooking_gas_any"
  ))

write_csv(hours_terms, hours_reg_csv_path)

hours_gas_terms <- bind_rows(
  tidy(h1, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_wood_t"),
  tidy(h3, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_elec_t"),
  tidy(h9, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_gas_t"),
  tidy(h10, conf.int = TRUE) %>% mutate(model = "eff_hrs_employed_joint_t"),
  tidy(h5, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_wood_t"),
  tidy(h7, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_elec_t"),
  tidy(h11, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_gas_t"),
  tidy(h12, conf.int = TRUE) %>% mutate(model = "eff_hrs_pop_joint_t")
) %>%
  filter(term %in% c(
    "share_cooking_wood_charcoal",
    "share_cooking_electricity",
    "share_cooking_gas_any"
  ))

write_csv(hours_gas_terms, hours_gas_reg_csv_path)

cat("Wrote\n")
cat(map_start_path, "\n")
cat(map_end_path, "\n")
cat(map_stats_path, "\n")
cat(fuel_trend_fig_path, "\n")
cat(main_fuel_trend_fig_path, "\n")
cat(reg_csv_path, "\n")
cat(reg_tex_path, "\n")
cat(reg_fe_controls_tex_path, "\n")
cat(hours_reg_csv_path, "\n")
cat(hours_reg_tex_path, "\n")
cat(hours_gas_reg_csv_path, "\n")
cat(hours_gas_reg_tex_path, "\n")
cat(hours_wood_fe_tex_path, "\n")
cat(main_fuel_lfp_results_path, "\n")
