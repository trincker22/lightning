#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(PNADcIBGE)
  library(dplyr)
  library(stringr)
  library(readr)
  library(readxl)
  library(arrow)
  library(tidyr)
  library(sf)
  library(geobr)
  library(ggplot2)
  library(scales)
})

sf::sf_use_s2(FALSE)

root_dir <- here::here()

pnad_dir <- here::here("data", "PNAD-C")
raw_dir <- here::here(pnad_dir, "raw")
out_dir <- here::here("data", "powerIV", "pnadc")
fig_dir <- here::here("Figures", "powerIV", "pnadc")
shape_dir <- here::here(out_dir, "shapes")
report_dir <- here::here(out_dir, "reports")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(shape_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(report_dir, recursive = TRUE, showWarnings = FALSE)

crosswalk_path <- here::here(pnad_dir, "Municipios_por_Estratos.csv")
varbook_path <- here::here(raw_dir, "Variaveis_PNADC_Anual_Visita.xls")
hh_out <- here::here(out_dir, "pnadc_visit1_household_2016_2024.parquet")
panel_out <- here::here(out_dir, "pnadc_visit1_estrato_quarter_2016_2024.parquet")
split_panel_out <- here::here(out_dir, "pnadc_visit1_estrato_quarter_urban_rural_2016_2024.parquet")
crosswalk_out <- here::here(out_dir, "pnadc_estrato_municipio_crosswalk.parquet")
shape_out <- here::here(shape_dir, "pnadc_estrato_146_2020.gpkg")
continuity_out <- here::here(report_dir, "pnadc_visit1_variable_continuity.csv")
continuity_md_out <- here::here(report_dir, "pnadc_visit1_variable_continuity.md")
qa_out <- here::here(out_dir, "pnadc_visit1_processing_qa.csv")
map_outputs <- c(
  here::here(fig_dir, "map_1_electricity_from_grid_2016q1.png"),
  here::here(fig_dir, "map_1_electricity_from_grid_2024q4.png"),
  here::here(fig_dir, "map_2_grid_full_time_2016q1.png"),
  here::here(fig_dir, "map_2_grid_full_time_2024q4.png"),
  here::here(fig_dir, "map_3_washing_machine_2016q1.png"),
  here::here(fig_dir, "map_3_washing_machine_2024q4.png"),
  here::here(fig_dir, "map_4_cooking_wood_charcoal_2016q1.png"),
  here::here(fig_dir, "map_4_cooking_wood_charcoal_2024q4.png")
)

required_outputs <- c(
  hh_out,
  panel_out,
  split_panel_out,
  crosswalk_out,
  shape_out,
  continuity_out,
  continuity_md_out,
  qa_out,
  map_outputs
)

as_yes_no_binary <- function(x) {
  x_chr <- str_to_lower(str_trim(as.character(x)))
  case_when(
    is.na(x_chr) | x_chr == "" ~ NA_integer_,
    str_detect(x_chr, regex("\\bsim\\b|^1\\b", ignore_case = TRUE)) ~ 1L,
    str_detect(x_chr, regex("\\bnao\\b|\\bnão\\b|^2\\b", ignore_case = TRUE)) ~ 0L,
    TRUE ~ NA_integer_
  )
}

first_non_missing <- function(x) {
  idx <- which(!is.na(x) & x != "")
  if (length(idx) == 0) NA_character_ else as.character(x[[idx[[1]]]])
}

weighted_share <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
}

weighted_mean_num <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  weighted.mean(as.numeric(x[keep]), w[keep], na.rm = TRUE)
}

make_map <- function(data_sf, value_col, title_text, subtitle_text, out_path) {
  p <- ggplot(data_sf) +
    geom_sf(aes(fill = .data[[value_col]]), color = "grey60", linewidth = 0.1) +
    scale_fill_viridis_c(labels = scales::percent_format(accuracy = 1), na.value = "grey90") +
    labs(title = title_text, subtitle = subtitle_text, fill = "Share") +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )
  ggsave(out_path, p, width = 10, height = 7, dpi = 300)
}

if (all(file.exists(required_outputs))) {
  message("All PNAD-C visit 1 outputs already exist in data/powerIV. Skipping rebuild.")
  quit(save = "no", status = 0)
}

if (!file.exists(crosswalk_path)) stop("Missing crosswalk: ", crosswalk_path)
if (!file.exists(varbook_path)) {
  download.file(
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Anual/Microdados/Visita/Documentacao_Geral/Variaveis_PNADC_Anual_Visita.xls",
    varbook_path,
    mode = "wb"
  )
}

message("Reading estrato-municipality crosswalk.")
estrato_crosswalk <- read.csv(crosswalk_path, sep = ";", header = TRUE, fileEncoding = "UTF-8")
names(estrato_crosswalk) <- c("estrato_name", "estrato_code", "municipio_name", "municipio_code")
estrato_crosswalk <- estrato_crosswalk %>%
  mutate(
    estrato4 = sprintf("%04d", as.integer(estrato_code)),
    municipio_code = as.integer(municipio_code)
  )
write_parquet(estrato_crosswalk, crosswalk_out)

years <- c(2016:2019, 2022:2024)
vars <- c(
  "Ano", "Trimestre", "UF", "Capital", "RM_RIDE", "UPA", "Estrato", "posest", "posest_sxi",
  "V1008", "V1014", "V1022", "V1023", "V1031", "V1032",
  "S01001", "S01005", "S01006", "S01007", "S01008",
  "S01014", "S010141", "S010142", "S01015",
  "S01016", "S010161", "S010162", "S010163", "S010164",
  "S01016A", "S01016A1", "S01016A2", "S01016A3", "S01016A4", "S01016A5", "S01016B",
  "S01021", "S01022", "S01023", "S01024", "S01027", "S01028", "S01029", "S01030", "S01031"
)

message("Downloading and processing PNAD-C annual visit 1 microdata.")
hh_list <- lapply(years, function(yr) {
  message("Year ", yr)
  dat <- get_pnadc(
    year = yr,
    interview = 1,
    vars = vars,
    labels = TRUE,
    deflator = FALSE,
    design = FALSE,
    savedir = raw_dir,
    reload = FALSE
  )

  missing_vars <- setdiff(vars, names(dat))
  if (length(missing_vars) > 0) {
    for (v in missing_vars) dat[[v]] <- NA_character_
  }

  dat %>%
    mutate(across(everything(), as.character)) %>%
    distinct(ID_DOMICILIO, .keep_all = TRUE) %>%
    transmute(
      year = as.integer(Ano),
      quarter = as.integer(Trimestre),
      year_quarter = paste0(Ano, "-Q", Trimestre),
      hh_id = ID_DOMICILIO,
      uf = UF,
      capital = Capital,
      rm_ride = RM_RIDE,
      upa = UPA,
      estrato = Estrato,
      estrato4 = substr(Estrato, 1, 4),
      posest = posest,
      posest_sxi = posest_sxi,
      household_number = V1008,
      panel = V1014,
      area_situation = V1022,
      area_type = V1023,
      weight_uncalibrated = suppressWarnings(as.numeric(V1031)),
      weight_calibrated = suppressWarnings(as.numeric(V1032)),
      dwelling_type = S01001,
      n_rooms = suppressWarnings(as.integer(S01005)),
      n_bedrooms = suppressWarnings(as.integer(S01006)),
      water_source = S01007,
      water_frequency = S01008,
      electricity_source_any_text = S01014,
      electricity_from_grid = as_yes_no_binary(S010141),
      electricity_other_source = as_yes_no_binary(S010142),
      electricity_grid_full_time = case_when(
        is.na(S01015) ~ NA_integer_,
        str_detect(S01015, regex("tempo integral", ignore_case = TRUE)) ~ 1L,
        str_detect(S01015, regex("diariamente", ignore_case = TRUE)) ~ 0L,
        TRUE ~ 0L
      ),
      electricity_grid_frequency = S01015,
      cooking_fuel_any_text = coalesce(S01016, S01016A),
      cooking_gas_any = if (yr <= 2018) {
        as_yes_no_binary(S010161)
      } else {
        case_when(
          !is.na(S01016A1) | !is.na(S01016A2) ~ as.integer(coalesce(as_yes_no_binary(S01016A1), 0L) == 1L | coalesce(as_yes_no_binary(S01016A2), 0L) == 1L),
          TRUE ~ NA_integer_
        )
      },
      cooking_gas_bottled = as_yes_no_binary(S01016A1),
      cooking_gas_piped = as_yes_no_binary(S01016A2),
      cooking_wood_charcoal = coalesce(as_yes_no_binary(S010162), as_yes_no_binary(S01016A3)),
      cooking_electricity = coalesce(as_yes_no_binary(S010163), as_yes_no_binary(S01016A4)),
      cooking_other_fuel = coalesce(as_yes_no_binary(S010164), as_yes_no_binary(S01016A5)),
      cooking_main_fuel = S01016B,
      n_mobile_phones = suppressWarnings(as.integer(S01021)),
      landline = as_yes_no_binary(S01022),
      refrigerator = as_yes_no_binary(S01023),
      refrigerator_detail = S01023,
      washing_machine = as_yes_no_binary(S01024),
      satellite_dish_tv = as_yes_no_binary(S01027),
      computer = as_yes_no_binary(S01028),
      internet_home = as_yes_no_binary(S01029),
      internet_by_mobile = as_yes_no_binary(S01030),
      vehicle = as_yes_no_binary(S01031)
    ) %>%
    left_join(estrato_crosswalk %>% distinct(estrato4, estrato_name), by = "estrato4") %>%
    mutate(
      electricity_any = case_when(
        !is.na(electricity_from_grid) | !is.na(electricity_other_source) ~ as.integer(coalesce(electricity_from_grid, 0L) == 1L | coalesce(electricity_other_source, 0L) == 1L),
        is.na(electricity_source_any_text) ~ NA_integer_,
        str_detect(electricity_source_any_text, regex("não utiliza|nao utiliza", ignore_case = TRUE)) ~ 0L,
        TRUE ~ 1L
      ),
      appliance_count_basic = rowSums(cbind(refrigerator, washing_machine, computer, vehicle), na.rm = TRUE),
      appliance_count_connected = rowSums(cbind(landline, satellite_dish_tv, computer, internet_home, internet_by_mobile), na.rm = TRUE)
    )
})

hh_all <- bind_rows(hh_list)

message("Checking estrato crosswalk coverage.")
unmatched_estrato <- hh_all %>% distinct(estrato4) %>% anti_join(estrato_crosswalk %>% distinct(estrato4), by = "estrato4")
if (nrow(unmatched_estrato) > 0) {
  warning("Unmatched estrato4 codes found: ", paste(unmatched_estrato$estrato4, collapse = ", "))
}

message("Writing household-level PNAD-C output.")
write_parquet(hh_all, hh_out)

message("Aggregating to estrato-quarter panel.")
panel <- hh_all %>%
  group_by(year, quarter, year_quarter, estrato4, estrato_name) %>%
  summarise(
    uf = first_non_missing(uf),
    capital = first_non_missing(capital),
    rm_ride = first_non_missing(rm_ride),
    posest = first_non_missing(posest),
    n_households = n(),
    weight_sum = sum(weight_calibrated, na.rm = TRUE),
    share_electricity_any = weighted_share(electricity_any, weight_calibrated),
    share_electricity_from_grid = weighted_share(electricity_from_grid, weight_calibrated),
    share_electricity_other_source = weighted_share(electricity_other_source, weight_calibrated),
    share_grid_full_time = weighted_share(electricity_grid_full_time, weight_calibrated),
    share_cooking_gas_any = weighted_share(cooking_gas_any, weight_calibrated),
    share_cooking_gas_bottled = weighted_share(cooking_gas_bottled, weight_calibrated),
    share_cooking_gas_piped = weighted_share(cooking_gas_piped, weight_calibrated),
    share_cooking_wood_charcoal = weighted_share(cooking_wood_charcoal, weight_calibrated),
    share_cooking_electricity = weighted_share(cooking_electricity, weight_calibrated),
    share_cooking_other_fuel = weighted_share(cooking_other_fuel, weight_calibrated),
    share_landline = weighted_share(landline, weight_calibrated),
    share_refrigerator = weighted_share(refrigerator, weight_calibrated),
    share_washing_machine = weighted_share(washing_machine, weight_calibrated),
    share_satellite_dish_tv = weighted_share(satellite_dish_tv, weight_calibrated),
    share_computer = weighted_share(computer, weight_calibrated),
    share_internet_home = weighted_share(internet_home, weight_calibrated),
    share_internet_by_mobile = weighted_share(internet_by_mobile, weight_calibrated),
    share_vehicle = weighted_share(vehicle, weight_calibrated),
    mean_rooms = weighted_mean_num(n_rooms, weight_calibrated),
    mean_bedrooms = weighted_mean_num(n_bedrooms, weight_calibrated),
    mean_mobile_phones = weighted_mean_num(n_mobile_phones, weight_calibrated),
    mean_appliance_count_basic = weighted_mean_num(appliance_count_basic, weight_calibrated),
    mean_appliance_count_connected = weighted_mean_num(appliance_count_connected, weight_calibrated),
    .groups = "drop"
  )
write_parquet(panel, panel_out)

message("Aggregating to estrato-quarter by urban/rural.")
split_panel <- hh_all %>%
  mutate(urban_rural = case_when(
    area_situation == "Urbana" ~ "urban",
    area_situation == "Rural" ~ "rural",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(urban_rural)) %>%
  group_by(year, quarter, year_quarter, estrato4, estrato_name, urban_rural) %>%
  summarise(
    uf = first_non_missing(uf),
    capital = first_non_missing(capital),
    rm_ride = first_non_missing(rm_ride),
    posest = first_non_missing(posest),
    n_households = n(),
    weight_sum = sum(weight_calibrated, na.rm = TRUE),
    share_electricity_any = weighted_share(electricity_any, weight_calibrated),
    share_electricity_from_grid = weighted_share(electricity_from_grid, weight_calibrated),
    share_electricity_other_source = weighted_share(electricity_other_source, weight_calibrated),
    share_grid_full_time = weighted_share(electricity_grid_full_time, weight_calibrated),
    share_cooking_gas_any = weighted_share(cooking_gas_any, weight_calibrated),
    share_cooking_gas_bottled = weighted_share(cooking_gas_bottled, weight_calibrated),
    share_cooking_gas_piped = weighted_share(cooking_gas_piped, weight_calibrated),
    share_cooking_wood_charcoal = weighted_share(cooking_wood_charcoal, weight_calibrated),
    share_cooking_electricity = weighted_share(cooking_electricity, weight_calibrated),
    share_cooking_other_fuel = weighted_share(cooking_other_fuel, weight_calibrated),
    share_landline = weighted_share(landline, weight_calibrated),
    share_refrigerator = weighted_share(refrigerator, weight_calibrated),
    share_washing_machine = weighted_share(washing_machine, weight_calibrated),
    share_satellite_dish_tv = weighted_share(satellite_dish_tv, weight_calibrated),
    share_computer = weighted_share(computer, weight_calibrated),
    share_internet_home = weighted_share(internet_home, weight_calibrated),
    share_internet_by_mobile = weighted_share(internet_by_mobile, weight_calibrated),
    share_vehicle = weighted_share(vehicle, weight_calibrated),
    mean_rooms = weighted_mean_num(n_rooms, weight_calibrated),
    mean_bedrooms = weighted_mean_num(n_bedrooms, weight_calibrated),
    mean_mobile_phones = weighted_mean_num(n_mobile_phones, weight_calibrated),
    mean_appliance_count_basic = weighted_mean_num(appliance_count_basic, weight_calibrated),
    mean_appliance_count_connected = weighted_mean_num(appliance_count_connected, weight_calibrated),
    .groups = "drop"
  )
write_parquet(split_panel, split_panel_out)

message("Building continuity report.")
varbook <- read_excel(varbook_path, sheet = "PNADC_anual_visita1")

top20 <- tibble::tribble(
  ~display_var, ~source_var, ~grouping, ~change_note,
  "Water source", "S01007", "stable", "Stable wording 2016-2024.",
  "Water frequency", "S01008", "stable", "Stable wording 2016-2024.",
  "Any electricity source used", "S01014", "stable", "Stable wording 2016-2024.",
  "Electricity from main grid", "S010141", "stable", "Stable wording 2016-2024.",
  "Other electricity source", "S010142", "stable", "Stable wording 2016-2024.",
  "Grid availability frequency", "S01015", "stable", "Stable wording 2016-2024.",
  "Any fuel used for food preparation", "S01016/S01016A", "changed_2019", "2016-2018 uses S01016. In 2019 the fuel block changed to S01016A with more detailed follow-ups.",
  "Cooking gas (any)", "S010161/S01016A1/S01016A2", "changed_2019", "2016-2018 combines bottled and piped gas in S010161. From 2019 onward they split bottled gas (A1) and piped gas (A2).",
  "Cooking gas (bottled)", "S01016A1", "added_2019", "Introduced in 2019 when gas was split by type.",
  "Cooking gas (piped)", "S01016A2", "added_2019", "Introduced in 2019 when gas was split by type.",
  "Cooking wood/charcoal", "S010162/S01016A3", "changed_2019", "Concept is stable; variable code changes in 2019.",
  "Cooking electricity", "S010163/S01016A4", "changed_2019", "Concept is stable; variable code changes in 2019.",
  "Cooking other fuel", "S010164/S01016A5", "changed_2019", "Concept is stable; variable code changes in 2019.",
  "Main cooking fuel", "S01016B", "added_2022", "Added in 2022 to identify the most used cooking fuel.",
  "Mobile phones count", "S01021", "stable", "Stable wording 2016-2024.",
  "Landline", "S01022", "stable", "Stable wording 2016-2024.",
  "Refrigerator", "S01023", "stable", "Stable wording 2016-2024.",
  "Washing machine", "S01024", "stable", "Stable wording 2016-2024.",
  "Computer", "S01028", "stable", "Stable wording 2016-2024.",
  "Internet at home", "S01029", "stable", "Stable wording 2016-2024."
)

availability_long <- varbook %>%
  select(Variável, `2016`, `2017`, `2018`, `2019`, `2022`, `2023`, `2024`) %>%
  pivot_longer(-Variável, names_to = "year", values_to = "present") %>%
  mutate(year = as.integer(year), present = present == "X")

source_lookup <- function(source_var) {
  unlist(strsplit(gsub(" ", "", source_var), "/", fixed = TRUE))
}

format_year_segments <- function(years_present) {
  yrs <- sort(unique(years_present))
  if (length(yrs) == 0) return(NA_character_)
  breaks <- c(TRUE, diff(yrs) > 1)
  grp <- cumsum(breaks)
  segs <- split(yrs, grp)
  paste(vapply(segs, function(s) {
    if (length(s) == 1) as.character(s[[1]]) else paste0(min(s), "-", max(s))
  }, character(1)), collapse = ", ")
}

continuity <- bind_rows(lapply(seq_len(nrow(top20)), function(i) {
  row <- top20[i, ]
  source_vars <- source_lookup(row$source_var)
  av <- availability_long %>%
    filter(Variável %in% source_vars) %>%
    group_by(year) %>%
    summarise(present = any(present), .groups = "drop")
  yrs_present <- av$year[av$present %in% TRUE]
  tibble(
    display_var = row$display_var,
    source_var = row$source_var,
    available_years = format_year_segments(yrs_present),
    first_year = min(yrs_present),
    last_year = max(yrs_present),
    continuity = case_when(
      identical(yrs_present, c(2016L, 2017L, 2018L, 2019L, 2022L, 2023L, 2024L)) ~ "2016-2019, 2022-2024",
      TRUE ~ format_year_segments(yrs_present)
    ),
    question_change = row$change_note
  )
}))
write.csv(continuity, continuity_out, row.names = FALSE)

mermaid_lines <- c(
  "```mermaid",
  "gantt",
  "    title PNAD-C visit 1 continuity for 20 key housing/appliance variables",
  "    dateFormat  YYYY-MM-DD",
  "    axisFormat  %Y",
  "    section Stable through 2024",
  "    Water source (1) :a1, 2016-01-01, 2019-12-31",
  "    Water source (2) :a2, 2022-01-01, 2024-12-31",
  "    Water frequency (1) :a3, 2016-01-01, 2019-12-31",
  "    Water frequency (2) :a4, 2022-01-01, 2024-12-31",
  "    Any electricity source (1) :a5, 2016-01-01, 2019-12-31",
  "    Any electricity source (2) :a6, 2022-01-01, 2024-12-31",
  "    Main-grid electricity (1) :a7, 2016-01-01, 2019-12-31",
  "    Main-grid electricity (2) :a8, 2022-01-01, 2024-12-31",
  "    Other electricity source (1) :a9, 2016-01-01, 2019-12-31",
  "    Other electricity source (2) :a10, 2022-01-01, 2024-12-31",
  "    Grid availability frequency (1) :a11, 2016-01-01, 2019-12-31",
  "    Grid availability frequency (2) :a12, 2022-01-01, 2024-12-31",
  "    Mobile phones count (1) :a13, 2016-01-01, 2019-12-31",
  "    Mobile phones count (2) :a14, 2022-01-01, 2024-12-31",
  "    Landline (1) :a15, 2016-01-01, 2019-12-31",
  "    Landline (2) :a16, 2022-01-01, 2024-12-31",
  "    Refrigerator (1) :a17, 2016-01-01, 2019-12-31",
  "    Refrigerator (2) :a18, 2022-01-01, 2024-12-31",
  "    Washing machine (1) :a19, 2016-01-01, 2019-12-31",
  "    Washing machine (2) :a20, 2022-01-01, 2024-12-31",
  "    Computer (1) :a21, 2016-01-01, 2019-12-31",
  "    Computer (2) :a22, 2022-01-01, 2024-12-31",
  "    Internet at home (1) :a23, 2016-01-01, 2019-12-31",
  "    Internet at home (2) :a24, 2022-01-01, 2024-12-31",
  "    section Cooking fuel block",
  "    Any cooking fuel (S01016) :b1, 2016-01-01, 2018-12-31",
  "    Any cooking fuel (S01016A-1) :b2, 2019-01-01, 2019-12-31",
  "    Any cooking fuel (S01016A-2) :b3, 2022-01-01, 2024-12-31",
  "    Cooking gas combined :b3, 2016-01-01, 2018-12-31",
  "    Cooking gas bottled (1) :b4, 2019-01-01, 2019-12-31",
  "    Cooking gas bottled (2) :b5, 2022-01-01, 2024-12-31",
  "    Cooking gas piped (1) :b6, 2019-01-01, 2019-12-31",
  "    Cooking gas piped (2) :b7, 2022-01-01, 2024-12-31",
  "    Cooking wood/charcoal (1) :b8, 2016-01-01, 2019-12-31",
  "    Cooking wood/charcoal (2) :b9, 2022-01-01, 2024-12-31",
  "    Cooking electricity (1) :b10, 2016-01-01, 2019-12-31",
  "    Cooking electricity (2) :b11, 2022-01-01, 2024-12-31",
  "    Cooking other fuel (1) :b12, 2016-01-01, 2019-12-31",
  "    Cooking other fuel (2) :b13, 2022-01-01, 2024-12-31",
  "    Main cooking fuel :b14, 2022-01-01, 2024-12-31",
  "```",
  "",
  "Question-change notes:",
  "- The electricity block (S01014, S010141, S010142, S01015) is stable from 2016 through 2024.",
  "- The cooking-fuel block changes in 2019: S01016 is replaced by S01016A and gas splits into bottled (A1) and piped (A2).",
  "- S01016B is added in 2022 to identify the main cooking fuel.",
  "- Pandemic-era annual visit files are absent for 2020 and 2021."
)
writeLines(c("# PNAD-C Visit 1 Variable Continuity", "", mermaid_lines), continuity_md_out)

message("Building estrato geometry and maps.")
muni_sf <- read_municipality(code_muni = "all", year = 2020, simplified = FALSE, showProgress = FALSE) %>%
  transmute(municipio_code = as.integer(code_muni), geom) %>%
  st_transform(5880) %>%
  st_make_valid() %>%
  suppressWarnings(st_buffer(0))

estrato_sf <- muni_sf %>%
  inner_join(estrato_crosswalk %>% distinct(estrato4, estrato_name, municipio_code), by = "municipio_code") %>%
  group_by(estrato4, estrato_name) %>%
  summarise(do_union = TRUE, .groups = "drop") %>%
  st_make_valid() %>%
  suppressWarnings(st_buffer(0)) %>%
  st_transform(4674)

st_write(estrato_sf, shape_out, delete_dsn = TRUE, quiet = TRUE)

start_panel <- panel %>% filter(year == 2016, quarter == 1)
start_map_sf <- estrato_sf %>% left_join(start_panel, by = c("estrato4", "estrato_name"))
latest_panel <- panel %>% filter(year == 2024, quarter == 4)
latest_map_sf <- estrato_sf %>% left_join(latest_panel, by = c("estrato4", "estrato_name"))

make_map(
  start_map_sf,
  "share_electricity_from_grid",
  "PNAD-C 2016 Q1: electricity from main grid",
  "146 geographic strata",
  here::here(fig_dir, "map_1_electricity_from_grid_2016q1.png")
)
make_map(
  latest_map_sf,
  "share_electricity_from_grid",
  "PNAD-C 2024 Q4: electricity from main grid",
  "146 geographic strata",
  here::here(fig_dir, "map_1_electricity_from_grid_2024q4.png")
)
make_map(
  start_map_sf,
  "share_grid_full_time",
  "PNAD-C 2016 Q1: full-time grid availability",
  "146 geographic strata",
  here::here(fig_dir, "map_2_grid_full_time_2016q1.png")
)
make_map(
  latest_map_sf,
  "share_grid_full_time",
  "PNAD-C 2024 Q4: full-time grid availability",
  "146 geographic strata",
  here::here(fig_dir, "map_2_grid_full_time_2024q4.png")
)
make_map(
  start_map_sf,
  "share_washing_machine",
  "PNAD-C 2016 Q1: washing machine ownership",
  "146 geographic strata",
  here::here(fig_dir, "map_3_washing_machine_2016q1.png")
)
make_map(
  latest_map_sf,
  "share_washing_machine",
  "PNAD-C 2024 Q4: washing machine ownership",
  "146 geographic strata",
  here::here(fig_dir, "map_3_washing_machine_2024q4.png")
)
make_map(
  start_map_sf,
  "share_cooking_wood_charcoal",
  "PNAD-C 2016 Q1: wood/charcoal used for food preparation",
  "146 geographic strata",
  here::here(fig_dir, "map_4_cooking_wood_charcoal_2016q1.png")
)
make_map(
  latest_map_sf,
  "share_cooking_wood_charcoal",
  "PNAD-C 2024 Q4: wood/charcoal used for food preparation",
  "146 geographic strata",
  here::here(fig_dir, "map_4_cooking_wood_charcoal_2024q4.png")
)

qa <- bind_rows(
  tibble(metric = "years_processed", value = paste(years, collapse = ",")),
  tibble(metric = "household_rows", value = as.character(nrow(hh_all))),
  tibble(metric = "estrato_quarter_rows", value = as.character(nrow(panel))),
  tibble(metric = "estrato_quarter_urban_rural_rows", value = as.character(nrow(split_panel))),
  tibble(metric = "unique_estrato4", value = as.character(n_distinct(hh_all$estrato4))),
  tibble(metric = "unmatched_estrato4", value = as.character(nrow(unmatched_estrato))),
  tibble(metric = "quarter_range", value = paste(min(hh_all$year_quarter), max(hh_all$year_quarter), sep = " to "))
)
write.csv(qa, qa_out, row.names = FALSE)

message("Done.")
message("Household output: ", hh_out)
message("Estrato-quarter panel: ", panel_out)
message("Estrato-quarter urban/rural panel: ", split_panel_out)
message("Continuity CSV: ", continuity_out)
message("Continuity MD: ", continuity_md_out)
message("Geometry: ", shape_out)
message("Figures: ", fig_dir)
message("QA: ", qa_out)
