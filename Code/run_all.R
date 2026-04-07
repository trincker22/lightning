library(here)
scripts <- c(
  "01_process_lightning.R",
  "02_process_aneel_continuity.R",
  "03_process_conj_geometries.R",
  "04_process_raw_outages.R",
  "05_process_pnadc_visit1.R",
  "06_merge_power_pnadc.R",
  "07_run_pnadc_cooking_regressions.R",
  "08_process_pnadc_lfp.R",
  "09_run_pnadc_lfp_regressions.R"
)

# if data already built, starts at 7

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
code_dir <- here("Code")

for (s in scripts) {
  path <- here::here(code_dir, s)
  message("Running ", basename(path))
  status <- system2("Rscript", path)
  if (!identical(status, 0L)) stop("Failed: ", s)
}
