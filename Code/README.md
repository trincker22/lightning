# powerIV Pipeline

Run order:
1. `01_process_lightning.R`
2. `02_process_aneel_continuity.R`
3. `03_process_conj_geometries.R`
4. `04_process_raw_outages.R`
5. `05_process_pnadc_visit1.R`
6. `06_merge_power_pnadc.R`
7. `07_run_pnadc_cooking_regressions.R`
8. `08_process_pnadc_lfp.R`
9. `09_run_pnadc_lfp_regressions.R`

Raw inputs are read from the existing top-level `data` directories and are not duplicated. Processed outputs are written under `data/powerIV`. Figures are written under `Figures/powerIV`.
