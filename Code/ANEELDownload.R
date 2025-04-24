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


safe_download <- function(url, dest_folder = here("ANEEL", "CSVNew")) {
  file_name <- basename(url)
  dest_path <- file.path(dest_folder, file_name)
  
  if (file.exists(dest_path)) return(dest_path)
  
  tryCatch({
    suppressMessages(
      download.file(url, destfile = dest_path, mode = "wb", quiet = TRUE)
    )
    if (file.exists(dest_path) && file.info(dest_path)$size > 1e6) {
      return(dest_path)
    } else {
      return(NA)
    }
  }, error = function(e) {
    return(NA)
  })
}

# test_url <- csv_resources$url[1]
# safe_download(test_url)


csv_urls <- csv_resources$url
library(progressr)
handlers(global = TRUE)
handlers("txtprogressbar")

with_progress({
  downloaded_files <- map(csv_urls, safe_download)
})

csv_urls


