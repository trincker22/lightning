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
library(fixest)  
library(sidrar)
library(reticulate)
library(jsonlite)
<<<<<<< HEAD
library(marginaleffects)
=======
>>>>>>> f2645f1daa1c9e78bf245c57f3630ba3dbb4abdb

options(scipen=999)

############################################################

# SECTION 1: ANEEL

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

#ANEEL Full Outage Processing 

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



############################################################

# ANEEL Building Join Key

############################################################

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
  
  int_ba <- int_key %>% filter(abbrev_state == "BA" | 
                                 abbrev_state == "MG"|
                                 abbrev_state == "ES")
  
  int_key_simple <- ms_simplify(int_ba, keep = 0.05)
  
  ggplot(int_key_simple) +
    geom_sf(aes(fill = name_intermediate), color = NA) +
    guides(fill = "none") +
    theme_minimal()
  
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
  
  # double check matches 
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
  
  check <- out_all %>% 
    filter(consumer_unit_group_id %in% c(16719,16721,13646,16720 ))
  
  conj_int %>%  
    filter(consumer_unit_group_id == 13646)
    
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
  # Final CONJ → intermediate region mapping with customer count
  conj_int <- bind_rows(
    conj_unique_assigned %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    pj_mapped %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    majority_assigned %>% mutate(consumer_unit_group_id = as.character(consumer_unit_group_id)),
    final_manual_resolved
  ) %>%
    distinct(consumer_unit_group_id, .keep_all = TRUE) %>%
    select(consumer_unit_group_id, code_intermediate, method) %>%
    left_join(served_customers, by = "consumer_unit_group_id")
  
  saveRDS(conj_int, conj_int_path)
  message("Saved CONJ → intermediate region mapping to ", conj_int_path)
}


sum(is.na(out_all$group_consumer_count))
sum(is.na(conj_int$group_consumer_count_avg))

no_match <- setdiff(conj_int$consumer_unit_group_id, out_all$consumer_unit_group_id)


conj_comparison <- conj_int %>%
  mutate(
    match_status = if_else(consumer_unit_group_id %in% no_match, "not_matched", "matched")
  )

# Compare tie_flag distribution by match status
tie_flag_summary <- conj_comparison %>%
  group_by(match_status, method) %>%
  summarise(n = n()) %>%
  mutate(pct = round(100 * n / sum(n), 1)) %>%
  arrange(match_status, desc(method))

out_all %>%  
  filter(consumer_unit_group_id == "6459")

ggplot(tie_flag_summary, aes(x = as.factor(method), y = pct, fill = match_status)) +
  geom_col(position = "dodge") +
  labs(x = "Tie Flag", y = "Percent", fill = "Match Status", title = "Tie Flag Distribution by CONJ Match Status") +
  theme_minimal()


##########################################
# ALL CONJ CODES MAP CHECK
##########################################

sum(!(unique(out_all$consumer_unit_group_id) %in% conj_int$consumer_unit_group_id))

##########################################
# Joins CONJ → region mapping to the full outage dataset.
# Aggregates customer outage hours by region and month.
##########################################

# using .feather files and joining on the disk is the best strategy because I hit vector limit on a normal join 
# but, with arrow I have a mac and the installations of GDAL and arrow packages can error if GDAL installed with homebrew 
# (which I did). So this is best to run on windows. 


feather_path <- here("ANEEL", "Feather")
dir.create(feather_path, showWarnings = FALSE, recursive = TRUE)

rds_path <- here("ANEEL", "monthly_outages.rds")

if (file.exists(rds_path)) {
  monthly_outages <- readRDS(rds_path)
} else {
  # Ensure ID is character
  out_all <- out_all %>%
    mutate(consumer_unit_group_id = as.character(consumer_unit_group_id))

  write_feather(out_all, file.path(feather_path, "out_all.feather"))
  write_feather(conj_int, file.path(feather_path, "conj_int.feather"))
  

  out_all_arrow <- open_dataset(file.path(feather_path, "out_all.feather"))
  conj_int_arrow <- open_dataset(file.path(feather_path, "conj_int.feather"))
  
  monthly_outages <- out_all_arrow %>%
    left_join(conj_int_arrow, by = "consumer_unit_group_id") %>%
    mutate(year_month = floor_date(interruption_start, unit = "month")) %>%
    group_by(code_intermediate, year_month) %>%
    summarise(
      total_customer_outage_hrs = sum(customer_outage_min, na.rm = TRUE) / 60,
      n_events = n()
    ) %>%
    collect()
  
  saveRDS(monthly_outages, rds_path)
}



####################################################

# Population by Intermediate Regions

###################################################

pop_data <- get_sidra(x = 6579, variable = 9324, period = "2020", geo = "City")

pop_muni <- pop_data %>%
  transmute(
    code_muni = as.character(`Município (Código)`),
    pop_2020 = as.numeric(`Valor`)
  )

muni_lookup <- lookup_muni(name_muni = "all") %>%
  select(code_muni, code_intermediate) %>% 
  mutate(code_muni = as.character(code_muni))


# Manual assignment for unmatched municipalities
manual_fixes <- tibble(
  code_muni = c("1504752", "4212650", "4220000", "4314548", "5006275"),
  code_intermediate = c(1505, 4202, 4202, 4307, 5001)
)

muni_lookup_fixed <- bind_rows(muni_lookup, manual_fixes)

pop_intermediate <- pop_muni %>%
  left_join(muni_lookup_fixed, by = "code_muni") %>%
  group_by(code_intermediate) %>%
  summarise(int_pop_2020 = sum(pop_2020, na.rm = TRUE)) %>%
  arrange(code_intermediate)

conj_int <- conj_int %>%  
  left_join(pop_intermediate, join_by(code_intermediate))


monthly_outages <- monthly_outages %>%
  left_join(
    conj_int %>% select(code_intermediate, int_pop_2020) %>% distinct(),
    by = "code_intermediate"
  ) %>% 
  mutate( outage_hrs = total_customer_outage_hrs*1000/int_pop_2020
  ) %>%  
  select(-int_pop_2020)

monthly_outages <- monthly_outages %>%
  mutate(date = as.Date(year_month)) %>%  
  select(-year_month)

region_ids <- unique(monthly_outages$code_intermediate)
month_seq <- seq.Date(
  from = as.Date("2017-01-01"),
  to = max(monthly_outages$date),
  by = "month"
)


full_grid <- expand_grid(
  code_intermediate = region_ids,
  date = month_seq
)


monthly_outages <- full_grid %>%
  left_join(monthly_outages, by = c("code_intermediate", "date")) %>%
  group_by(code_intermediate) %>%
  fill(int_pop_2020, .direction = "downup") %>%  # ensure population is filled within region
  ungroup() %>%
  mutate(
    total_customer_outage_hrs = replace_na(total_customer_outage_hrs, 0),
    n_events = replace_na(n_events, 0),
    outage_hrs = if_else(is.na(outage_hrs), 0, outage_hrs)
  ) %>% filter(!is.na(code_intermediate))

##################################################################

# Lightning Raster Processing Using IBGE Intermediate Regions

#####################################################################



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

###############################################

# ---------------------------------------------
# Lightning Plotting Functions 
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



##############################################################


# Creating Control Vars: Rainfall, Temperature, Land Cover, Elevation


#############################################################



# 1. Rainfall-----------------------------------------------------------


dir.create("Rainfall", showWarnings = F, recursive = T)
dir.create("Rainfall/TIFs", showWarnings = FALSE, recursive = TRUE)


download_chirps_month <- function(year, month) {
  yyyymm <- sprintf("%04d.%02d", year, month)
  url <- paste0("https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.", yyyymm, ".tif.gz")
  dest_gz <- file.path("Rainfall", "TIFs", basename(url))
  dest_tif <- sub(".gz$", "", dest_gz)
  
  if (!file.exists(dest_tif)) {
    download.file(url, dest_gz, mode = "wb")
    R.utils::gunzip(dest_gz, remove = TRUE)
  }
  
  return(dest_tif)
}

years <- 2017:2021
months <- 1:12
tif_paths <- unlist(lapply(years, function(y) sapply(months, function(m) download_chirps_month(y, m))))


intermediates <- read_intermediate_region(year = 2020) %>%
  st_transform(4326)  # Match CHIRPS CRS

# List all unzipped .tif files
tif_files <- list.files("Rainfall/TIFs", pattern = "\\.tif$", full.names = TRUE)

# Initialize an output data frame
rainfall_df <- data.frame()

for (file in tif_files) {
  r <- rast(file)
  
  # Set -9999 as NA
  r[r == -9999] <- NA
  
  date <- ymd(paste0(str_extract(basename(file), "\\d{4}\\.\\d{2}"), ".01"))
  mean_vals <- exact_extract(r, intermediates, 'mean')
  
  rainfall_df <- bind_rows(rainfall_df, tibble(
    code_intermediate = intermediates$code_intermediate,
    date = date,
    precip_mm = mean_vals
  ))
}

rainfall_df <- rainfall_df %>%
  mutate(precip_mm = ifelse(precip_mm < 0, NA, precip_mm))


joined_df <- joined_df %>%
  left_join(rainfall_df, by = c("code_intermediate", "date"))



# 2. Weather  -----------------------------------------------------------
ee <- import("ee")
ee$Initialize()

# Setup 
if (!dir.exists("Temp")) dir.create("Temp")
if (!dir.exists(file.path("Temp", "TIFs"))) dir.create(file.path("Temp", "TIFs"))

era5_df_path <- file.path("Temp", "era5_df.rds")

if (file.exists(era5_df_path)) {
  message("Loading ERA5 data from file...")
  era5_df <- readRDS(era5_df_path)
  
} else {
  # Geometry setup
  bbox <- st_bbox(brazil)
  brazil_ee <- ee$Geometry$Rectangle(
    coords = as.numeric(c(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"])),
    proj = "EPSG:4326"
  )
  
  dates <- seq(as.Date("2017-01-01"), as.Date("2021-12-01"), by = "month")
  
  bands <- c(
    "temperature_2m",
    "total_precipitation_sum",
    "dewpoint_temperature_2m",
    "surface_pressure",
    "total_evaporation_sum",
    "runoff_sum"
  )
  
  # Download function
  download_era5 <- function(date, out_dir = "Temp/TIFs") {
    fname <- paste0(format(date, "%Y%m"), ".tif")
    tif_file <- file.path(out_dir, fname)
    if (file.exists(tif_file)) return(tif_file)
    
    img <- ee$ImageCollection('ECMWF/ERA5_LAND/MONTHLY_AGGR')$
      filter(ee$Filter$date(format(date, "%Y-%m-%d")))$
      first()$
      select(bands)$
      clip(brazil_ee)
    
    url <- img$getDownloadURL(list(
      scale = 10000,
      region = brazil_ee$bounds()$getInfo()$coordinates,
      format = "GEO_TIFF",
      filePerBand = FALSE
    ))
    
    tryCatch({
      download.file(url, tif_file, mode = "wb")
      message("Downloaded: ", basename(tif_file))
      return(tif_file)
    }, error = function(e) {
      message("Download failed for ", format(date, "%Y-%m"))
      return(NULL)
    })
  }
  
  # Download and extract
  all_dfs <- map_dfr(dates, function(date) {
    tif_path <- download_era5(date)
    if (is.null(tif_path)) return(NULL)
    
    r <- rast(tif_path)
    names(r) <- bands
    
    extracted <- exact_extract(
      r,
      st_transform(intermediate_regions, crs(r)),
      fun = "mean",
      append_cols = "code_intermediate"
    ) %>%
      pivot_longer(-code_intermediate, names_to = "variable", values_to = "value") %>%
      mutate(
        date = date
      )
    
    return(extracted)
  })
  
  # Clean and convert units
  era5_df <- all_dfs %>%
    pivot_wider(names_from = variable, values_from = value) %>%
    mutate(
      avg_temp     = (temperature_2m - 273.15) * 9/5 + 32,
      precip_mm    = total_precipitation_sum * 1000,
      dewpoint_f   = (dewpoint_temperature_2m - 273.15) * 9/5 + 32,
      pressure_hpa = surface_pressure / 100,
      evap_mm      = total_evaporation_sum * 1000,
      runoff_mm    = runoff_sum * 1000
    ) %>%
    select(code_intermediate, date, avg_temp, precip_mm, dewpoint_f, pressure_hpa, evap_mm, runoff_mm) %>%
    arrange(code_intermediate, date)
  
  saveRDS(era5_df, era5_df_path)
  message("ERA5 data saved to: ", era5_df_path)
}


# 3. Land Cover -----------------------------------------------------

# File path to save/load processed RDS
landcover_rds_path <- here("LandCover", "landcover_final.rds")

if (file.exists(landcover_rds_path)) {
  message("Loading processed landcover data from RDS...")
  landcover_final <- readRDS(landcover_rds_path)
} else {
  message("Processing landcover data...")
  
  landcover_dir <- here("LandCover", "Raw")
  all_years <- list.files(path = landcover_dir, pattern = "Brazil_LandCover_\\d{4}_summary.csv", full.names = TRUE)
  
  igbp_classes <- tibble(
    code = as.character(1:17),
    name = c(
      "evergreen_needleleaf_forest",
      "evergreen_broadleaf_forest",
      "deciduous_needleleaf_forest",
      "deciduous_broadleaf_forest",
      "mixed_forest",
      "closed_shrublands",
      "open_shrublands",
      "woody_savannas",
      "savannas",
      "grasslands",
      "permanent_wetlands",
      "croplands",
      "urban_and_built_up",
      "cropland_natural_vegetation_mosaic",
      "permanent_snow_and_ice",
      "barren_or_sparsely_vegetated",
      "unclassified"
    )
  )
  
  process_landcover_csv <- function(file_path) {
    df <- read_csv(file_path, locale = locale(encoding = "UTF-8"))
    
    hist_expanded <- df %>%
      mutate(
        clean_histogram = str_replace_all(histogram, "(\\d+)=", '"\\1":'),
        clean_histogram = ifelse(!str_starts(clean_histogram, "\\{"), paste0("{", clean_histogram, "}"), clean_histogram),
        hist_json = map(clean_histogram, ~fromJSON(.) %>% enframe(name = "code", value = "count"))
      ) %>%
      unnest(hist_json) %>%
      mutate(count = as.numeric(count)) %>%
      group_by(`system:index`, cd_ntrm) %>%
      mutate(total = sum(count)) %>%
      mutate(percent = (count / total) * 100) %>%
      left_join(igbp_classes, by = "code") %>%
      select(cd_ntrm, name, percent) %>%
      pivot_wider(names_from = name, values_from = percent, values_fill = 0)
    
    return(hist_expanded)
  }
  
  landcover_list <- map(all_years, process_landcover_csv)
  
  landcover_final <- bind_rows(
    map2(landcover_list, all_years, ~ mutate(.x, year = str_extract(.y, "\\d{4}")))
  )
  
  landcover_final <- landcover_final %>%
    janitor::clean_names() %>%
    ungroup() %>%
    select(-system_index) %>%
    rename(code_intermediate = cd_ntrm) %>%
    mutate(year = as.integer(year)) %>%
    mutate(across(-c(code_intermediate, year), ~replace_na(., 0))) %>%
    rowwise() %>%
    mutate(total_percent = sum(c_across(-c(code_intermediate, year)))) %>%
    ungroup() %>%
    mutate(across(-c(code_intermediate, year, total_percent), ~ ifelse(. < 0.001, 0, .))) %>%
    mutate(across(-c(code_intermediate, year, total_percent), ~ . / 100))  # convert to share
  
  saveRDS(landcover_final, landcover_rds_path)
  message("Saved processed landcover data to RDS.")
}

# plot shares over time for selected intermediate codes
plot_codes <- c(1101, 1201, 1301)

landcover_long <- landcover_final %>%
  filter(code_intermediate %in% plot_codes) %>%
  pivot_longer(
    cols = -c(code_intermediate, year, total_percent),
    names_to = "landcover_type",
    values_to = "share"
  )

landcover_long_filtered <- landcover_long %>%
  group_by(code_intermediate, landcover_type) %>%
  filter(max(share) >= 0.01) %>%
  ungroup()


ggplot(landcover_long_filtered, aes(x = year, y = share, color = landcover_type)) +
  geom_line() +
  facet_wrap(~ code_intermediate) +
  labs(
    title = "Land Cover Shares Over Time",
    y = "Share (0–1)",
    x = "Year",
    color = "Land Cover Type"
  ) +
  theme_minimal()


# 4. Elevation ----------------------------------------------------------------------

elevation <- read_csv(here("Elevation", "Raw", "Intermediate_Region_Elevation.csv"))

elevation <- elevation %>%  
  rename(code_intermediate = cd_ntrm)

##################################################################

# WBDB data 

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


##############################################################

# GADM


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

# 7980 OBSERVATIONS
joined_df <- monthly_outages %>%
  mutate(year = year(date))  # needed for land cover join

joined_df <- joined_df %>%
  left_join(all_lightning, by = c("code_intermediate", "date"))

sapply(joined_df, function(x) sum(is.na(x)))

joined_df <- joined_df %>%
  left_join(era5_df, by = c("code_intermediate", "date"))

sapply(joined_df, function(x) sum(is.na(x)))

joined_df <- joined_df %>%
  left_join(landcover_final, by = c("code_intermediate", "year"))

sapply(joined_df, function(x) sum(is.na(x)))

joined_df <- joined_df %>%
  left_join(elevation, by = "code_intermediate")

sapply(joined_df, function(x) sum(is.na(x)))


saveRDS(joined_df, "joined_df.rds")

#############################################

joined_df$month <- factor(lubridate::month(joined_df$date),
                          levels = 1:12,
                          labels = month.name)

joined_df <- joined_df %>%
  select(-matches("\\.y$")) %>%  
  rename_with(~ sub("\\.x$", "", .), ends_with(".x")) %>% 
  rename(mean_elev = mean,
         stdDev_elev = stdDev)


feols(
  outage_hrs ~ density + precip_mm + avg_temp + mean_elev + 
    stdDev_elev + evergreen_broadleaf_forest + month,
  data = joined_df, cluster = ~ code_intermediate
)

ols <- feols(
  outage_hrs ~ density + precip_mm + avg_temp +
    mean_elev + stdDev_elev +
    power_mean + power_median + power_sd +
    evergreen_broadleaf_forest + closed_shrublands +
    deciduous_broadleaf_forest + croplands +
    urban_and_built_up + evergreen_needleleaf_forest +
    mixed_forest + barren_or_sparsely_vegetated +
    month +
    dewpoint_f + pressure_hpa + evap_mm + runoff_mm,
  data = joined_df,
  cluster = ~ code_intermediate
)


fe_model <- feols(
  outage_hrs ~ density + precip_mm + avg_temp + year +
    power_mean + power_median + power_sd +
    evergreen_broadleaf_forest + closed_shrublands +
    deciduous_broadleaf_forest + croplands +
    urban_and_built_up + woody_savannas + savannas +
    permanent_wetlands + grasslands + evergreen_needleleaf_forest +
    mixed_forest + barren_or_sparsely_vegetated + open_shrublands +
    cropland_natural_vegetation_mosaic + deciduous_needleleaf_forest +
    month +
    dewpoint_f + pressure_hpa + evap_mm + runoff_mm | code_intermediate,
  data = joined_df,
  cluster = ~ code_intermediate
)

fe_year_model <- feols(
  outage_hrs ~ density + density*urban_and_built_up + precip_mm + avg_temp +
    power_mean + power_median + power_sd +
    evergreen_broadleaf_forest + closed_shrublands +
    deciduous_broadleaf_forest + croplands +
    urban_and_built_up + woody_savannas + savannas +
    permanent_wetlands + grasslands + evergreen_needleleaf_forest +
    mixed_forest + barren_or_sparsely_vegetated + open_shrublands +
    cropland_natural_vegetation_mosaic + deciduous_needleleaf_forest +
    month +
    dewpoint_f + pressure_hpa + evap_mm + runoff_mm | code_intermediate + year,
  data = joined_df,
  cluster = ~ code_intermediate
)

plot_slopes(
  model = fe_year_model,
  variable = "density",
  condition = "urban_and_built_up"
)



feols(
  outage_hrs ~ density + precip_mm + avg_temp + mean_elev + 
    stdDev_elev + evergreen_broadleaf_forest + month | code_intermediate,
  data = joined_df,
  cluster = ~ code_intermediate
)

feols(
  outage_hrs ~ density + power_mean + precip_mm| code_intermediate + date,
  data = joined_df
)


ggplot(joined_df, aes(x = density, y = outage_hrs)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", color = "red") +
  labs(title = "Outage Hours vs. Density (Raw)",
       x = "Population Density",
       y = "Outage Hours per 1000 People")

joined_df <- joined_df %>%
  group_by(code_intermediate) %>%
  mutate(
    outage_hrs_demeaned = outage_hrs - mean(outage_hrs, na.rm = TRUE),
    density_demeaned = density - mean(density, na.rm = TRUE)
  ) %>%
  ungroup()

ggplot(joined_df, aes(x = density_demeaned, y = outage_hrs_demeaned)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", color = "blue") +
  labs(title = "Within-Region Relationship (Fixed Effects)",
       x = "Density (demeaned by region)",
       y = "Outage Hours (demeaned by region)")

joined_df <- joined_df %>%
  mutate(urban_bin = cut(urban_and_built_up,
                         breaks = quantile(urban_and_built_up, probs = c(0, 0.25, 0.5, 0.75, 1), na.rm = TRUE),
                         labels = c("Low", "Mid-Low", "Mid-High", "High"),
                         include.lowest = TRUE))

ggplot(joined_df, aes(x = urban_bin, y = outage_hrs)) +
  geom_boxplot() +
  scale_y_log10() +
  labs(title = "Outage Hours by Urban Bin (Log Scale)",
       x = "Urban Bin",
       y = "Outage Hours per 1000 People (log10 scale)")



joined_df %>%
  filter(total_customer_outage_hrs > 0, density > 0) %>%  # remove 0s for log scale
  ggplot(aes(x = density, y = total_customer_outage_hrs)) +
  geom_point(alpha = 0.5) +
  scale_x_log10() +
  scale_y_log10() +
  labs(
    x = "Lightning Strike Density (log)",
    y = "Total Customer Outage Hours (log)",
    title = "Log-Log Scatter: Lightning vs. Outage Hours"
  ) +
  theme_minimal()

  ggplot(aes(x = z_density, y = z_outage)) +
  geom_point(alpha = 0.5) +
  labs(
    x = "Standardized Lightning Density",
    y = "Standardized Outage Hours",
    title = "Standardized Scatter: Lightning vs. Outages"
  ) +
  geom_smooth()+
  theme_minimal()


