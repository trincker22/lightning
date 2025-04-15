rm(list = ls())

setwd("~/Outages")

library(blackmarbler)
library(DT)
library(dplyr)
library(exactextractr)
library(fixest)
library(geodata)
library(ggplot2)
library(ggpubr)
library(gtools)
library(janitor)
library(kableExtra)
library(leaflet)
library(leaflet.providers)
library(lubridate)
library(ncdf4)
library(readxl)
library(rhdf5)
library(sf)
library(tidyterra)
library(tidyverse)
library(tmap)
library(units)
library(WDI)
library(stringr)
library(MODIStsp)
library(httr)
library(tidyverse)
library(readxl)
library(terra)




wb <- read_excel("~/Outages/WBDB/WB-DB.xlsx")
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


out24 <- read.csv("~/Outages/ANEEL/CSVs/interrupcoes-energia-eletrica-2024.csv", 
                  sep = ";")

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


# ---------------------------------------------
# Check if final RDS file already exists
# ---------------------------------------------
output_path <- "~/Outages/ANEEL"

if (!file.exists(output_path)) {
  
  # ---------------------------------------------
  # 1. Load and Merge Individual CSVs
  # ---------------------------------------------
  file_paths <- list.files("~/Outages/ANEEL/CSVs", full.names = TRUE, pattern = "\\.csv$")
  
  out_list <- list()
  for (file in file_paths) {
    year <- str_extract(file, "\\d{4}")
    out_list[[paste0("out", year)]] <- read.csv(file, sep = ";")
  }
  
  out_all <- bind_rows(out_list, .id = "source")
  
  # ---------------------------------------------
  # 2. Rename Columns for Clarity
  # ---------------------------------------------
  colnames(out_all) <- c(
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
  
  # ---------------------------------------------
  # 3. Save the Combined Dataset
  # ---------------------------------------------
  saveRDS(out_all, output_path)
  cat("Merged dataset saved to:", output_path, "\n")
  
} else {
  cat("File already exists at:", output_path, "\nSkipping import and merge.\n")
}



#############################################################
# ---------------------------------------------
# Load Required Libraries
# ---------------------------------------------
library(terra)
library(lubridate)
library(sf)
library(tmap)

# ---------------------------------------------
# 0. Set Output Path and Check If File Already Exists
# ---------------------------------------------
processed_path <- "~/Outages/Lightning/Masked/lightning_brazil_2017.tif"

if (!file.exists(processed_path)) {
  
  # ---------------------------------------------
  # 1. Load Lightning Raster Data (NetCDF)
  # ---------------------------------------------
  lightning <- rast("~/Outages/Lightning/WGLC/wglc_timeseries_05m.nc")
  names(lightning)
  print(lightning)
  
  # ---------------------------------------------
  # 2. Load, Transform, and Convert Brazil Boundary
  # ---------------------------------------------
  brazil <- st_read("~/NTL/data/gadm/shp") |>
    st_transform(crs = 4326) |>
    vect()
  
  # ---------------------------------------------
  # 3. Crop and Mask Lightning Data to Brazil
  # ---------------------------------------------
  lightning <- mask(crop(lightning, brazil), brazil)
  
  # ---------------------------------------------
  # 4. Subset Data from Jan 2017 Onward and Convert to Monthly Strokes
  # ---------------------------------------------
  dates <- seq.Date(from = as.Date("2010-01-01"), by = "month", length.out = nlyr(lightning))
  start_index <- which(dates == as.Date("2017-01-01"))
  dates <- dates[start_index:nlyr(lightning)]
  days <- days_in_month(dates)
  
  lightning <- lightning[[start_index:nlyr(lightning)]]
  
  for (i in 1:nlyr(lightning)) {
    lightning[[i]] <- lightning[[i]] * days[i]
  }
  
  # ---------------------------------------------
  # 5. Save Processed Raster
  # ---------------------------------------------
  writeRaster(lightning, processed_path, overwrite = TRUE)
  cat("Processed lightning raster saved to:", processed_path, "\n")
  
} else {
  # ---------------------------------------------
  # Load Existing File
  # ---------------------------------------------
  lightning <- rast(processed_path)
  cat("Processed lightning raster already exists. Loaded from:", processed_path, "\n")
}

# ---------------------------------------------
# 6. Quick Visualization for Jan 2017
# ---------------------------------------------
r_tmap <- lightning[[1]]
names(r_tmap) <- "density"

tm_shape(r_tmap) +
  tm_raster(title = "Strokes/km²", style = "cont") +
  tm_shape(brazil) +
  tm_borders(col = "black")

# ---------------------------------------------
# 7. NA Analysis 
# ---------------------------------------------
na_map <- is.na(lightning[[1]])
plot(na_map, main = "NA regions in Jan 2017 (white = NA)")
lines(brazil, col = "red")

vals <- values(lightning[[1]])
cat("Percent NA:", round(sum(is.na(vals)) / length(vals) * 100, 2), "%\n")




#################################################

iso3_code <- "BRA"
iso2_code <- "BR"
gadm_dir <- "~/Outages/GADM"  


for (i in 0:3) {
  gadm(iso3_code, level = i, path = gadm_dir)
}


load_gadm <- function(i) {
  file.path(gadm_dir, 'gadm') %>%
    list.files(full.names = TRUE, pattern = paste0("_", i, "_pk.rds")) %>%
    map(~ readRDS(.x) %>% st_as_sf()) %>%
    list_rbind()
}

class(load_gadm(1))  # sf

tmap_mode("view") 
qtm(load_gadm(1)) 




###################################################

# Convert roi (sf) to SpatVector and match CRS
roi_vect <- vect(roi)
roi_vect <- project(roi_vect, crs(lightning))

# Extract mean lightning density per polygon, per month
lightning_data <- terra::extract(lightning, roi_vect, fun = mean, na.rm = TRUE)

# Prepare column names (region ID + 60 months from 2017–2021)
dates <- seq.Date(from = as.Date("2017-01-01"), by = "month", length.out = nlyr(lightning))
names(lightning_data) <- c("region", paste0("density", format(dates, "%Y_%m")))

# Combine lightning data back with the geometry
lightning_sf <- cbind(roi_vect, lightning_data)
lightning_sf <- st_as_sf(lightning_sf)  # convert to sf for plotting

# Compute yearly mean for 2019
lightning_sf$mean_2019 <- lightning_sf %>%
  st_drop_geometry() %>%
  select(starts_with("density2019_")) %>%
  rowMeans(na.rm = TRUE)

# Plot!
tmap_mode("plot")  # or "view" for interactive mode

tm_shape(lightning_sf) +
  tm_polygons("mean_2019", 
              title = "Avg Lightning Density\n(2019, strokes/km²)") +
  tm_layout(frame = FALSE)


lightning_sf$mean_2020 <- lightning_sf %>%
  st_drop_geometry() %>%
  select(starts_with("density2020_")) %>%
  rowMeans(na.rm = TRUE)

colnames(lightning_sf)


# Plot!
tmap_mode("plot")  # or "view" for interactive mode

tm_shape(lightning_sf) +
  tm_polygons("mean_2020", 
              title = "Avg Lightning Density\n(2020, strokes/km²)") +
  tm_layout(frame = FALSE)


tm_shape(lightning_sf) +
  tm_polygons("density2018_09", 
              title = "Avg Lightning Density\n(2020, strokes/km²)") +
  tm_layout(frame = FALSE)



