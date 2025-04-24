library(tidyverse)
library(janitor)
library(lubridate)
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

# Changed encoding to read accents properly 
# ---------------------------------------------
# Check if final RDS file already exists
# ---------------------------------------------
output_path <- here("ANEEL")
output_file <- file.path(output_path, "all_outages.rds")

if (!file.exists(output_file)) {
  
  # ---------------------------------------------
  # Load, Clean, and Merge Individual CSVs
  # ---------------------------------------------
  file_paths <- list.files(here("ANEEL", "CSVs"), full.names = TRUE, pattern = "\\.csv$")
  
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
    
    # Clean and transform
    df <- df %>%
      distinct() %>% # remove duplicates
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
  
  out_all <- bind_rows(out_list, .id = "source")
  saveRDS(out_all, output_file)
  cat("Cleaned and merged dataset saved to:", output_file, "\n")
  
} else {
  cat("File already exists at:", output_file, "\nSkipping import and merge, loading in data.\n")
  outage <- readRDS(output_file)
}


#################################################
# location key 
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



#################################################

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
# Lightning Raster Processing 
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
    # Load all pre-processed data
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
  # 1. Load GADM level 1 (Brazil states)
  # ---------------------------------------------
  brazil <- load_gadm(0)
  brazil <- st_transform(brazil, crs = 4326)
  
  roi <- load_gadm(1)  # 27 administrative regions
  roi_vect <- vect(roi)
  roi_vect <- project(roi_vect, "EPSG:4326")  # Match CRS
  
  # ---------------------------------------------
  # 2. Load and subset density (5 arc-min resolution)
  # ---------------------------------------------
  density_5m <- rast(here("Lightning", "WGLC", "wglc_timeseries_05m.nc"))
  density_5m <- mask(crop(density_5m, brazil), brazil)
  
  # Subset to 2017-2021
  dates <- seq.Date(from = as.Date("2010-01-01"), by = "month", length.out = nlyr(density_5m))
  i_2017 <- which(dates == as.Date("2017-01-01"))
  density_5m <- density_5m[[i_2017:nlyr(density_5m)]]
  # Update dates to match the subset
  dates <- dates[i_2017:length(dates)]
  
  # Scale by days in month
  days <- days_in_month(dates)
  for (i in 1:nlyr(density_5m)) {
    density_5m[[i]] <- density_5m[[i]] * days[i]
  }
  
  # Clean date-only names like "2017_01"
  names(density_5m) <- format(dates, "%Y_%m")
  
  # ---------------------------------------------
  # 3. Load and subset stroke power rasters (30 arc-min)
  # ---------------------------------------------
  load_power_var <- function(varname) {
    r_all <- rast(here("Lightning", "WGLC", "wglc_timeseries_30m.nc"))
    r <- subset(r_all, grep(varname, names(r_all), value = TRUE))
    r <- mask(crop(r, brazil), brazil)
    # Use the same date subsetting as density_5m
    r <- r[[i_2017:nlyr(r)]]
    names(r) <- format(dates, "%Y_%m")  # Use the same dates vector
    return(r)
  }
  
  power_mean   <- load_power_var("power_mean")
  power_median <- load_power_var("power_median")
  power_sd     <- load_power_var("power_SD")
  
  # ---------------------------------------------
  # 4. Extract Mean Values by Region (GADM 1)
  # ---------------------------------------------
  extract_mean_df <- function(raster_obj, value_name) {
    df <- terra::extract(raster_obj, roi_vect, fun = mean, na.rm = TRUE)
    df$NAME_1 <- roi$NAME_1  
    
    df_long <- df %>%
      pivot_longer(
        cols = -c(ID, NAME_1),
        names_to = "month_str",
        values_to = value_name
      ) %>%
      mutate(
        date = lubridate::ym(month_str)
      ) %>%
      select(NAME_1, date, all_of(value_name))
    
    return(df_long)
  }
  
  # Apply the function to each raster
  density_df     <- extract_mean_df(density_5m, "density")
  powermean_df   <- extract_mean_df(power_mean, "power_mean")
  powermedian_df <- extract_mean_df(power_median, "power_median")
  powersd_df     <- extract_mean_df(power_sd, "power_sd")
  
  # ---------------------------------------------
  # 5. Combine into One Long Table
  # ---------------------------------------------
  all_lightning <- reduce(
    list(density_df, powermean_df, powermedian_df, powersd_df),
    ~ left_join(.x, .y, by = c("NAME_1", "date"))
  )
  
  # ---------------------------------------------
  # 6. Convert to sf and Add Geometry
  # ---------------------------------------------
  roi_df <- st_as_sf(roi)
  lightning_sf <- left_join(roi_df, all_lightning, by = "NAME_1")
  
  # After processing, save results
  saveRDS(lightning_sf, lightning_sf_path, compress = "xz")
  saveRDS(all_lightning, all_lightning_path, compress = "xz")
  
  # Save rasters
  writeRaster(density_5m, file.path(raster_dir, "density_5m.tif"), overwrite = TRUE)
  writeRaster(power_mean, file.path(raster_dir, "power_mean.tif"), overwrite = TRUE)
  writeRaster(power_median, file.path(raster_dir, "power_median.tif"), overwrite = TRUE)
  writeRaster(power_sd, file.path(raster_dir, "power_sd.tif"), overwrite = TRUE)
  
  message("Data processing complete and saved")
}

########################

# ---------------------------------------------
# Visualization for Jan 2017
# ---------------------------------------------
r_tmap <- density_5m[[1]]
names(r_tmap) <- "density2017_01"  # Optional rename for display

tm_shape(r_tmap) +
  tm_raster(title = "Strokes/km²", style = "cont") +
  tm_shape(brazil) +
  tm_borders(col = "black")

# ---------------------------------------------
# NA Analysis 
# ---------------------------------------------
na_map <- is.na(density_5m[[1]])
plot(na_map, main = "NA regions in Jan 2017 (white = NA)")
lines(brazil, col = "red")

vals <- values(density_5m[[1]])
cat("Percent NA:", round(sum(is.na(vals)) / length(vals) * 100, 2), "%\n")

# Check temporal coverage
summary(lightning_sf$date)

# ---------------------------------------------
# Yearly summary and map
# ---------------------------------------------
yearly_means <- lightning_sf %>%
  st_drop_geometry() %>%
  mutate(year = year(date)) %>%
  group_by(NAME_1, year) %>%
  summarize(
    mean_density = mean(density, na.rm = TRUE),
    .groups = "drop"
  )

mean_2019 <- yearly_means %>%
  filter(year == 2019) %>%
  select(NAME_1, mean_density)

plot_data <- lightning_sf %>%
  distinct(NAME_1, .keep_all = TRUE) %>%
  left_join(mean_2019, by = "NAME_1")

tmap_mode("plot")
tm_shape(plot_data) +
  tm_polygons("mean_density", 
              title = "Avg Lightning Density\n(2019, strokes/km²)") +
  tm_layout(frame = FALSE)
