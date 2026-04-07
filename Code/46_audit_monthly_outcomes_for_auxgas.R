suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tibble)
})

root <- "/Users/theresarincker/Documents/GitHub/lightning/data/powerIV"
out_dir <- file.path(root, "regressions", "auxilio_gas_monthly_audit")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

files <- list.files(root, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
files <- files[str_detect(tolower(files), "month|monthly")]

inspect_one <- function(path) {
  tryCatch({
    x <- read_parquet(path)
    cols <- names(x)
    tibble(
      path = path,
      n_rows = nrow(x),
      n_cols = ncol(x),
      has_year = "year" %in% cols,
      has_month = "month" %in% cols,
      has_date = "date" %in% cols,
      has_estrato = "estrato4" %in% cols,
      has_municipio = any(cols %in% c("codigo_ibge", "municipio_code", "mun_code")),
      has_labor = any(str_detect(cols, "female_hours|employed|labor|lfp|work|hours")),
      has_cooking = any(str_detect(cols, "share_cooking|cooking_wood|cooking_gas|cooking_fuel")),
      has_program = any(str_detect(cols, "auxgas|benef|valor|transfer|aux_gas")),
      columns = paste(cols, collapse = " | ")
    )
  }, error = function(e) {
    tibble(
      path = path,
      n_rows = NA_integer_,
      n_cols = NA_integer_,
      has_year = NA,
      has_month = NA,
      has_date = NA,
      has_estrato = NA,
      has_municipio = NA,
      has_labor = NA,
      has_cooking = NA,
      has_program = NA,
      columns = paste("ERROR:", conditionMessage(e))
    )
  })
}

audit <- map_dfr(files, inspect_one)
if (nrow(audit) == 0) audit <- tibble(path = character(), n_rows = integer(), n_cols = integer(), has_year = logical(), has_month = logical(), has_date = logical(), has_estrato = logical(), has_municipio = logical(), has_labor = logical(), has_cooking = logical(), has_program = logical(), columns = character())

audit <- audit %>% arrange(desc(has_labor | has_cooking | has_program), path)
candidates <- audit %>% filter(has_month | has_date)

write.table(audit, file.path(out_dir, "monthly_file_audit.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
write.table(candidates, file.path(out_dir, "monthly_candidate_files.tsv"), sep = "\t", row.names = FALSE, quote = FALSE)

print(candidates)
