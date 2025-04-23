library(here)
library(httr)
library(jsonlite)
library(future)
library(furrr)
library(curl)


csv_dir <- here("ANEEL", "CSVNew")
dir.create(csv_dir, recursive = TRUE, showWarnings = FALSE)

# Retrieve resource list from ANEEL API
package_id <- "ccb25653-f07b-4f28-84c2-62a89d1f5a56"
url <- "https://dadosabertos.aneel.gov.br/api/3/action/package_show"

res <- GET(url, query = list(id = package_id))
dataset <- fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)
resources <- dataset$result$resources


csv_resources <- resources[
  grepl("\\.csv$", tolower(resources$url)) &
    grepl("interrupcao|energia|distribuicao|\\d{4}", tolower(resources$name)),
]


plan(multisession, workers = parallel::detectCores() - 1)


safe_download <- function(url, dest_folder = csv_dir) {
  file_name <- basename(url)
  dest_path <- file.path(dest_folder, file_name)
  
  if (file.exists(dest_path)) {
    message("✔ Already downloaded: ", file_name)
    return(dest_path)
  }
  
  tryCatch({
    curl_download(
      url,
      destfile = dest_path,
      mode = "wb",
      handle = new_handle()  # No timeout
    )
    message("Downloaded: ", file_name)
    return(dest_path)
  }, error = function(e) {
    warning("Failed: ", file_name, " — ", e$message)
    return(NA)
  })
}

# Start downloading
csv_urls <- csv_resources$url
downloaded_files <- future_map(csv_urls, safe_download)
