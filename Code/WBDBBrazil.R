library(tidyverse)
library(janitor)
library(lubridate)
library(arrow)
library(units)
library(stringr)
library(sf)
library(terra)
library(tidyterra)
library(geodata)
library(exactextractr)
library(blackmarbler)
library(MODIStsp)
library(ggplot2)
library(ggpubr)
library(tmap)
library(leaflet)
library(leaflet.providers)
library(ncdf4)
library(rhdf5)
library(readxl)
library(DT)
library(kableExtra)
library(gtools)
library(fixest)
library(WDI)
library(httr)
library(fasttime)
library(here)
library(geobr)
library(rmapshaper)
options(scipen=999)


wb <- read_excel(here("WBDB", "WB-DB.xlsx"))
br <- wb %>% 
  filter(wb$`Economy Name` =="Brazil")

saidi <- br %>% 
  filter(br$`Indicator ID` == "WB.DB.58" |
           br$`Indicator ID` == "WB.DB.55" | 
           br$`Indicator ID` == "WB.DB.56" |
           br$`Indicator ID` == "WB.DB.57" )

# Getting electricity : System average interruption duration index (SAIDI) (DB16-20 methodology)
# Getting electricity : System average interruption frequency index (SAIFI) (DB16-20 methodology)
# Getting electricity : Minimum outage time (in minutes)  (DB16-20 methodology)
# Getting electricity : Price of electricity (US cents per kWh) (DB16-20 methodology)


brwb <- data.frame(
  year = as.numeric(colnames(saidi)[20:25]), 
  SAIDI = as.numeric(saidi[1, 20:25]),
  SAIFI = as.numeric(saidi[2, 20:25]), 
  minout = as.numeric(saidi[3, 20:25]), 
  price = as.numeric(saidi[4, 20:25])
)

ggplot(brwb, aes(x = year)) +
  geom_line(aes(y = SAIDI, color = "SAIDI"), linewidth = 1) +
  geom_line(aes(y = SAIFI, color = "SAIFI"), linewidth = 1) +
  geom_line(aes(y = minout, color = "Minimum Outage Duration"), linewidth = 1) +
  geom_line(aes(y = price, color = "Avg Price--US cents per kWh"), linewidth = 1) +
  labs(title = "Utility Performance Indicators",
       y = "Value",
       color = "Indicator") +
  theme_minimal()


############################################################
# Trial w small df

out24 <- read_delim(here("ANEEL", "CSVs", "interrupcoes-energia-eletrica-2024.csv"), 
                  delim = ";", 
                  locale = locale(encoding = "ISO-8859-1"))


colnames(out24) <- c(
  "data_generated",               # DatGeracaoConjuntoDados
  "consumer_unit_group_id",      # IdeConjuntoUnidadeConsumidora
  "consumer_unit_group_desc",    # DscConjuntoUnidadeConsumidora
  "feeder_description",          # DscAlimentadorSubestacao
  "substation_description",      # DscSubestacaoDistribuicao
  "interruption_order_id",       # NumOrdemInterrupcao
  "interruption_type",           # DscTipoInterrupcao
  "interruption_reason_id",      # IdeMotivoInterrupcao (note: was listed as IdeMotivoExpurgo in doc, assumed equivalent)
  "interruption_start",          # DatInicioInterrupcao
  "interruption_end",            # DatFimInterrupcao
  "interruption_cause",          # DscFatoGeradorInterrupcao
  "voltage_level",               # NumNivelTensao
  "affected_units",              # NumUnidadeConsumidora
  "group_consumer_count",        # NumConsumidorConjunto
  "year",                        # NumAno
  "regulated_agent_name",        # NomAgenteRegulado
  "regulated_agent_abbr",        # SigAgente
  "cpf_cnpj"                     # NumCPFCNPJ
)


out24short <- out24[1:400,]

summary(out24$consumer_unit_group_id)

out24 %>%  
  filter(affected_units >= 10 & affected_units <= 10000) %>% 
  ggplot(aes(x = affected_units)) + 
  geom_histogram()



############################################################

# Define output path and file name
output_path <- here("ANEEL")
output_file <- file.path(output_path, "out_all.rds")

if (!file.exists(output_file)) {
  
  message("Generating full outage dataset from raw CSVs")
  
  # List of all CSVs to load
  file_paths <- list.files(here("ANEEL", "CSVs"), full.names = TRUE, pattern = "\\.csv$")
  
  # Labels for interruption causes
  cause_labels <- c(
    "Waived",                                      # 0
    "Failure at consumer unit (no 3rd party impact)",  # 1
    "Customer-side work only",                        # 2
    "Emergency situation",                            # 3
    "Suspension (default or safety)",                 # 4
    "Rationing program",                              # 5
    "Critical day event",                             # 6
    "Load shedding (ONS)",                            # 7
    "External to distribution system"                 # 8
  )
  
  out_list <- list()
  
  for (file in file_paths) {
    year <- str_extract(file, "\\d{4}")
    
    df <- read_delim(file,
                     delim = ";",
                     locale = locale(encoding = "ISO-8859-1"))
    
    # Rename columns
    colnames(df) <- c(
      "data_generated",               # DatGeracaoConjuntoDados
      "consumer_unit_group_id",      # IdeConjuntoUnidadeConsumidora
      "consumer_unit_group_desc",    # DscConjuntoUnidadeConsumidora
      "feeder_description",          # DscAlimentadorSubestacao
      "substation_description",      # DscSubestacaoDistribuicao
      "interruption_order_id",       # NumOrdemInterrupcao
      "interruption_type",           # DscTipoInterrupcao
      "interruption_reason_id",      # IdeMotivoInterrupcao
      "interruption_start",          # DatInicioInterrupcao
      "interruption_end",            # DatFimInterrupcao
      "interruption_cause",          # DscFatoGeradorInterrupcao
      "voltage_level",               # NumNivelTensao
      "affected_units",              # NumUnidadeConsumidora
      "group_consumer_count",        # NumConsumidorConjunto
      "year",                        # NumAno
      "regulated_agent_name",        # NomAgenteRegulado
      "regulated_agent_abbr",        # SigAgente
      "cpf_cnpj"                     # NumCPFCNPJ
    )
    
    # Clean and process
    df <- df %>%
      distinct() %>%
      mutate(
        interruption_type = factor(
          interruption_type,
          levels = c("Não Programada", "Programada"),
          labels = c("Unplanned", "Planned")
        )
      ) %>%
      filter(interruption_type != "Planned") %>%
      mutate(
        interruption_cause = factor(
          interruption_reason_id,
          levels = 0:8,
          labels = cause_labels
        ),
        interruption_start = fastPOSIXct(interruption_start),
        interruption_end   = fastPOSIXct(interruption_end),
        outage_duration_min = as.numeric(difftime(interruption_end, interruption_start, units = "mins")),
        customer_outage_min = round(outage_duration_min * affected_units, 2),
        outage_duration_min = round(outage_duration_min, 2)
      )
    
    out_list[[paste0("out", year)]] <- df
  }
  
  # Bind all years into one dataframe
  out_all <- bind_rows(out_list, .id = "source")
  
  # Save to RDS for future use
  saveRDS(out_all, output_file)
  cat("Cleaned and merged dataset saved to:", output_file, "\n")
  
} else {
  cat("File already exists at:", output_file, "\nLoading from disk.\n")
  out_all <- readRDS(output_file)
}

##########################################
# Loads the CONJ-to-municipality lookup table from ANEEL.
# Renames columns to English for clarity/ compatibility
# This has a m:m join with the CONJ codes, so it is used as a middle step
# to map to the intermediate region
##########################################

loc <- read_delim(
  here("ANEEL", "LocationKey", "indqual-municipio.csv"), 
  delim = ";", 
  locale = locale(encoding = "ISO-8859-1")
)

loc <- loc %>%
  rename(
    data_generated = DatGeracaoConjuntoDados,
    consumer_unit_group_id = IdeConjUnidConsumidoras,
    municipality_code = CodMunicipio,
    municipality_name = NomMunicipio,
    state_abbr = SigUF
  )

##########################################
# Loads or builds spatial centroid coordinates for each CONJ (consumer unit group)
# using preprocessed RDS or raw *_pj.csv point files from ANEEL.
# The _pj files do not include all CONJ codes, just those that primarily serve businesses
# This chunk takes a while to load, one of the csvs is 4GB
##########################################

cleaned_path <- here("ANEEL", "PointKey", "Cleaned", "consumer_conj_centroids.rds")

if (file.exists(cleaned_path)) {
  message("Loading preprocessed CONJ centroids from RDS file")
  consumer_conj_centroids <- readRDS(cleaned_path)
} else {
  message("Preprocessed file not found. Reading raw PJ files and processing.")
  
  pointcsv <- list.files(here("ANEEL", "PointKey", "Raw"), pattern = "_pj.csv", full.names = TRUE)
  consumer_list <- list()
  
  for (file_path in pointcsv) {
    df <- read_delim(file_path, delim = ";", locale = locale(encoding = "ISO-8859-1")) %>%
      select(CONJ, POINT_X, POINT_Y)
    consumer_list[[file_path]] <- df
  }
  
  consumer_points <- bind_rows(consumer_list)
  
  consumer_sf <- st_as_sf(consumer_points, coords = c("POINT_X", "POINT_Y"), crs = 4326)
  
  consumer_conj_centroids <- consumer_sf %>%
    group_by(CONJ) %>%
    summarise(geometry = st_centroid(st_union(geometry)), .groups = "drop")
  
  saveRDS(consumer_conj_centroids, cleaned_path)
  message("Saved processed CONJ centroids to: ", cleaned_path)
}

##########################################
# Loads or generates a lookup between each CONJ and its intermediate region.
# Uses three strategies: unique mapping, spatial join via centroid, and majority rule.
##########################################

conj_int_path <- here("ANEEL", "IntermediateKey", "conj_int.rds")
muni_meso_lookup <- lookup_muni(name_muni = "all")

intermediate_regions <- read_intermediate_region(year = 2020) %>%
  st_transform(4326)

if (file.exists(conj_int_path)) {
  message("Loading CONJ to intermediate region key")
  conj_int <- readRDS(conj_int_path)
} else {
  message("Generating CONJ to intermediate region key")
  
  int_key <- muni_meso_lookup %>%
    select(code_muni, name_muni, code_intermediate, name_intermediate, abbrev_state) %>%
    left_join(intermediate_regions %>% select(code_intermediate, geom), by = "code_intermediate") %>%
    st_as_sf(crs = 4326)
  
  int_key <- int_key %>%
    left_join(loc, join_by(code_muni == municipality_code)) %>%
    st_drop_geometry()
  
  int_sim <- int_key %>%
    select(consumer_unit_group_id, code_intermediate) %>%
    distinct()
  
  ##########################################
  # Handles CONJs mapped to more than one intermediate region.
  # 1. Assign the intermediate region where there is a unique 1:1:1 relationship between conj code, 
  # municipality, and intermediate region
  # 2. For codes that have a m:m match between conj code and intermediate region, but the conj code matches 
  # a point from the _pj dataset, find the intermediate region associated with the point, which will have to fall 
  # into a single intermediate region
  # 3. For codes that match to many intermediate regions, and that do not match a point from the _pj 
  # dataset, use majority rule- ( ex. say a conj code maps to 4 municipalities. if 3 municipalities are in 
  # one intermediate region, then map the whole conj code to that region). For codes where there 
  # is not a majority, randomly pick one of the int codes that it matches with.
  
  # I have included the method variable which will say by which method each conj is assigned its code. 
  # the random match is a very small portion of total conj codes
  ##########################################
  
  conj_region_check <- int_sim %>%
    group_by(consumer_unit_group_id) %>%
    summarise(n_intermediates = n_distinct(code_intermediate), .groups = "drop") %>%
    filter(n_intermediates > 1)
  
  conjdup <- conj_region_check$consumer_unit_group_id
  
  conj_unique_assigned <- int_sim %>%
    group_by(consumer_unit_group_id) %>%
    filter(n() == 1) %>%
    ungroup() %>%
    mutate(method = "unique_mapping", tie_flag = 0L)
  
  pj_mapped <- consumer_conj_centroids %>%
    filter(CONJ %in% conjdup) %>%
    st_transform(4326) %>%
    st_join(intermediate_regions %>% select(code_intermediate)) %>%
    select(consumer_unit_group_id = CONJ, code_intermediate) %>%
    mutate(method = "point_geometry", tie_flag = 1L)
  
  remaining_conjs <- setdiff(conjdup, pj_mapped$consumer_unit_group_id)
  
  majority_raw <- int_sim %>%
    filter(consumer_unit_group_id %in% remaining_conjs) %>%
    count(consumer_unit_group_id, code_intermediate)
  
  conj_tie_check <- majority_raw %>%
    group_by(consumer_unit_group_id) %>%
    filter(n == max(n)) %>%
    mutate(is_tie = n() > 1)
  
  set.seed(42)
  majority_assigned <- conj_tie_check %>%
    group_by(consumer_unit_group_id) %>%
    slice_sample(n = 1) %>%
    ungroup() %>%
    mutate(method = "majority_rule_or_random", tie_flag = as.integer(is_tie)) %>%
    select(-is_tie)
  
  ##########################################
  # Handles 4 unmatched CONJs 
  # Two are resolved via spatial centroid join, two via manual municipality name match.
  ##########################################
  unmatched_geo_resolved <- consumer_conj_centroids %>%
    filter(CONJ %in% c("16719", "16721")) %>%
    st_transform(4326) %>%
    st_join(intermediate_regions %>% select(code_intermediate)) %>%
    select(consumer_unit_group_id = CONJ, code_intermediate) %>%
    mutate(
      consumer_unit_group_id = as.character(consumer_unit_group_id),
      method = "spatial_resolution_unmatched",
      tie_flag = 1L
    )
  
  manual_ids <- tibble(
    consumer_unit_group_id = as.character(c(13646, 16720)),
    name_muni = c("Natividade", "Alvorada")
  )
  
  manual_assigned <- manual_ids %>%
    left_join(
      int_key %>%
        select(name_muni, code_intermediate) %>%
        distinct(),
      by = "name_muni"
    ) %>%
    group_by(consumer_unit_group_id) %>%
    slice_sample(n = 1) %>%  # randomly pick one if there are multiple
    ungroup() %>%
    mutate(method = "manual_name_match", tie_flag = 1L)
  
  final_manual_resolved <- bind_rows(
    unmatched_geo_resolved,
    manual_assigned
  )
  
  ##########################################
  # Combines all assignment methods into a final CONJ-to-region key
  # and saves to RDS for future use.
  ##########################################
  
  conj_int <- bind_rows(
    conj_unique_assigned %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    pj_mapped %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    majority_assigned %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    final_manual_resolved
  ) %>%
    distinct(consumer_unit_group_id, .keep_all = TRUE) %>%
    select(consumer_unit_group_id, code_intermediate, method)
  
  saveRDS(conj_int, conj_int_path)
  message("Saved CONJ → intermediate region mapping to ", conj_int_path)
}

##########################################
# ALL CONJ CODES MAP CHECK
##########################################

sum(!(unique(out_all$consumer_unit_group_id) %in% conj_int$consumer_unit_group_id))

##########################################
# Joins CONJ → region mapping to the full outage dataset.
# Aggregates customer outage hours by region and month.
##########################################

####################################################
### STILL WORKING--- need to figure out how to join without hitting vector memory limit 

out_all <- out_all %>%
  mutate(consumer_unit_group_id = as.character(consumer_unit_group_id))


library(data.table)

setDT(out_all)
setDT(conj_int)

out_with_region <- merge(out_all, conj_int, by = "consumer_unit_group_id", all.x = TRUE)


out_with_region <- out_all %>%
  left_join(conj_int, by = "consumer_unit_group_id")


names(out_all)

monthly_outages <- out_with_region %>%
  mutate(
    year_month = floor_date(interruption_start, unit = "month")
  ) %>%
  group_by(code_intermediate, year_month) %>%
  summarise(
    total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
    n_events = n(),
    .groups = "drop"
  )

n_missing_regions <- sum(is.na(out_with_region$code_intermediate))
cat("Missing region codes:", n_missing_regions, "\n")


conj_int <- conj_int %>%
  mutate(consumer_unit_group_id = as.character(consumer_unit_group_id))

chunks <- split(out_all, ceiling(seq_len(nrow(out_all)) / 5e6))  # 5M rows per chunk

results <- lapply(chunks, function(chunk) {
  chunk %>%
    mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)) %>%
    left_join(conj_int, by = "consumer_unit_group_id") %>%
    mutate(year_month = floor_date(interruption_start, unit = "month")) %>%
    group_by(code_intermediate, year_month) %>%
    summarise(
      total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
      n_events = n(),
      .groups = "drop"
    )
})

monthly_outages <- bind_rows(results)


write_feather(out_all, "out_all.feather")
write_feather(conj_int, "conj_int.feather")

out_all_arrow <- open_dataset("out_all.feather")
conj_int_arrow <- open_dataset("conj_int.feather")

monthly_outages <- out_all_arrow %>%
  left_join(conj_int_arrow, by = "consumer_unit_group_id") %>%
  mutate(year_month = floor_date(interruption_start, unit = "month")) %>%
  group_by(code_intermediate, year_month) %>%
  summarise(
    total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
    n_events = n()
  ) %>%
  collect()

##############################################################

iso3_code <- "BRA"
gadm_dir <- here("GADM")


for (i in 0:2) {
  file_check <- file.path(gadm_dir, "gadm", paste0("gadm41_", iso3_code, "_", i, "_pk.rds"))
  
  if (!file.exists(file_check)) {
    message("Downloading GADM level ", i, "...")
    gadm(iso3_code, level = i, path = gadm_dir)
  } else {
    message("GADM level ", i, " already exists — skipping download.")
  }
}


load_gadm <- function(i) {
  list.files(
    path = file.path(gadm_dir, "gadm"),
    pattern = paste0("_", i, "_pk\\.rds$"),
    full.names = TRUE
  ) %>%
    map(~ readRDS(.x) %>% st_as_sf()) %>%
    list_rbind()
}

roi <- load_gadm(1)
brazil <- load_gadm(0) %>%
  st_transform(crs = 4326)

roi_vect <- vect(roi) %>%
  project("EPSG:4326")  


###########################################################



# =============================================
# Lightning Raster Processing Using IBGE Intermediate Regions
# =============================================


lightout_dir <- here("Lightning", "Masked")
raster_dir <- here("Lightning", "Rasters")
dir.create(lightout_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(raster_dir, showWarnings = FALSE, recursive = TRUE)

# File paths
lightning_sf_path <- file.path(lightout_dir, "lightning_sf.rds")
all_lightning_path <- file.path(lightout_dir, "all_lightning.rds")

# Check if processing is needed
needs_processing <- !all(file.exists(c(
  lightning_sf_path,
  all_lightning_path,
  file.path(raster_dir, "density_5m.tif"),
  file.path(raster_dir, "power_mean.tif"),
  file.path(raster_dir, "power_median.tif"), 
  file.path(raster_dir, "power_sd.tif")
)))

if (!needs_processing) {
  tryCatch({
    lightning_sf <- readRDS(lightning_sf_path)
    all_lightning <- readRDS(all_lightning_path)
    density_5m <- rast(file.path(raster_dir, "density_5m.tif"))
    power_mean <- rast(file.path(raster_dir, "power_mean.tif")) 
    power_median <- rast(file.path(raster_dir, "power_median.tif"))
    power_sd <- rast(file.path(raster_dir, "power_sd.tif"))
    message("Successfully loaded all pre-processed data")
  }, error = function(e) {
    message("Error loading saved files: ", e$message)
    needs_processing <<- TRUE
  })
}

if (needs_processing) {
  message("Processing data...")
  
  # ---------------------------------------------
  # 1. Load IBGE intermediate regions
  # ---------------------------------------------
  brazil <- read_country(year = 2020) %>% st_transform(4326)
  roi <- read_intermediate_region(year = 2020) %>% st_transform(4326)
  roi_vect <- vect(roi)
  roi_vect <- project(roi_vect, "EPSG:4326")

  
  # ---------------------------------------------
  # 2. Load and subset lightning density raster
  # ---------------------------------------------
  density_5m <- rast(here("Lightning", "WGLC", "wglc_timeseries_05m.nc"))
  density_5m <- mask(crop(density_5m, brazil), brazil)
  dates <- seq.Date(from = as.Date("2010-01-01"), by = "month", length.out = nlyr(density_5m))
  i_2017 <- which(dates == as.Date("2017-01-01"))
  density_5m <- density_5m[[i_2017:nlyr(density_5m)]]
  dates <- dates[i_2017:length(dates)]
  days <- days_in_month(dates)
  for (i in 1:nlyr(density_5m)) {
    density_5m[[i]] <- density_5m[[i]] * days[i]
  }
  names(density_5m) <- format(dates, "%Y_%m")
  
  # ---------------------------------------------
  # 3. Load stroke power rasters (30 arc-min)
  # ---------------------------------------------
  load_power_var <- function(varname) {
    r_all <- rast(here("Lightning", "WGLC", "wglc_timeseries_30m.nc"))
    r <- subset(r_all, grep(varname, names(r_all), value = TRUE))
    r <- mask(crop(r, brazil), brazil)
    r <- r[[i_2017:nlyr(r)]]
    names(r) <- format(dates, "%Y_%m")
    return(r)
  }
  
  power_mean   <- load_power_var("power_mean")
  power_median <- load_power_var("power_median")
  power_sd     <- load_power_var("power_SD")
  
  # ---------------------------------------------
  # 4. Extract Mean Values by Intermediate Region
  # ---------------------------------------------
  
  extract_mean_df <- function(raster_obj, value_name) {
    df <- terra::extract(raster_obj, roi_vect, fun = mean, na.rm = TRUE)
    
    # Join metadata from roi
    roi_meta <- roi %>%
      st_drop_geometry() %>%
      mutate(ID = row_number()) %>%
      select(ID, code_intermediate, name_intermediate, abbrev_state)
    
    df <- df %>%
      left_join(roi_meta, by = "ID")
    
    # Find which columns contain date-layer values (assumes all non-ID columns are raster layers)
    date_cols <- setdiff(names(df), c("ID", "code_intermediate", "name_intermediate", "abbrev_state"))
    
    df_long <- df %>%
      pivot_longer(
        cols = all_of(date_cols),
        names_to = "month_str",
        values_to = value_name
      ) %>%
      mutate(
        # Clean date string (remove X if present)
        date = lubridate::ym(str_remove(month_str, "^X"))
      ) %>%
      select(code_intermediate, name_intermediate, abbrev_state, date, all_of(value_name))
    
    return(df_long)
  }
  
  
  density_df     <- extract_mean_df(density_5m, "density")
  powermean_df   <- extract_mean_df(power_mean, "power_mean")
  powermedian_df <- extract_mean_df(power_median, "power_median")
  powersd_df     <- extract_mean_df(power_sd, "power_sd")
  
  # ---------------------------------------------
  # 5. Combine into One Long Table
  # ---------------------------------------------
  all_lightning <- reduce(
    list(density_df, powermean_df, powermedian_df, powersd_df),
    ~ left_join(.x, .y, by = c("code_intermediate", "name_intermediate", "abbrev_state", "date"))
  )

  
  # ---------------------------------------------
  # 6. Convert to sf and Add Geometry
  # ---------------------------------------------
  lightning_sf <- all_lightning %>%
    left_join(roi %>% select(code_intermediate, geom), by = "code_intermediate") %>%
    st_as_sf()
  
  
  
  # Save all processed outputs
  saveRDS(lightning_sf, lightning_sf_path, compress = "xz")
  saveRDS(all_lightning, all_lightning_path, compress = "xz")
  writeRaster(density_5m, file.path(raster_dir, "density_5m.tif"), overwrite = TRUE)
  writeRaster(power_mean, file.path(raster_dir, "power_mean.tif"), overwrite = TRUE)
  writeRaster(power_median, file.path(raster_dir, "power_median.tif"), overwrite = TRUE)
  writeRaster(power_sd, file.path(raster_dir, "power_sd.tif"), overwrite = TRUE)
  message("Data processing complete and saved")
}



#########################################

# ---------------------------------------------
# Plotting Functions 
# ---------------------------------------------


plot_lightning_density_fast <- function(lightning_sf, year, month) {
  stopifnot(month %in% 1:12)
  
  target_date <- as.Date(sprintf("%04d-%02d-01", year, month))
  
  plot_data <- lightning_sf %>%
    filter(date == target_date, !is.na(density)) %>%
    st_as_sf()  # ensure it's still sf
  
  tmap_mode("plot")
  tm_shape(plot_data) +
    tm_polygons("density",
                style = "quantile") +
    tm_layout(title = "Lightning Density by Intermediate Region",
              frame = FALSE,
              title.position = c("bottom", "right"), 
              title.size = 4,
              legend.frame = FALSE,
              legend.outside = TRUE)
}

plot_lightning_density_fast(lightning_sf, 2019, 1)


plot_power_mean <- function(lightning_sf, year, month) {
  stopifnot(month %in% 1:12)
  
  target_date <- as.Date(sprintf("%04d-%02d-01", year, month))
  
  plot_data <- lightning_sf %>%
    filter(date == target_date, !is.na(power_mean)) %>%
    st_as_sf()
  
  tmap_mode("plot")
  tm_shape(plot_data) +
    tm_polygons("power_mean",
                title = glue::glue("Mean Stroke Power\n{year}-{sprintf('%02d', month)}"),
                palette = "brewer.purples",
                style = "quantile") +
    tm_layout(title = "Mean Stroke Power by Intermediate Region",
              title.position = c("bottom", "right"), 
              title.size = 4,
              frame = FALSE,
              legend.frame = FALSE,
              legend.outside = TRUE)
}

plot_power_mean(lightning_sf, 2019, 1)
plot_power_mean(lightning_sf, 2021, 7)


