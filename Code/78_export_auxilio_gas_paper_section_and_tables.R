#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
})

main_dir <- here::here("data", "paper_tables", "main")
dir.create(main_dir, recursive = TRUE, showWarnings = FALSE)

fuel_domcare_path <- here::here(
  "data", "powerIV", "regressions",
  "auxilio_gas_vd5008_no_bpc_primary_fuel_sweep",
  "primary_fuel_domcare_paper_table.tsv"
)

domcare_triple_diff_path <- here::here(
  "data", "powerIV", "regressions",
  "auxilio_gas_domcare_triple_diff",
  "domcare_triple_diff_paper_table.tsv"
)

labor_path <- here::here(
  "data", "powerIV", "regressions",
  "auxilio_gas_income_eligibility_sweep",
  "income_eligibility_cross_results.tsv"
)

table5_out <- here::here("data", "paper_tables", "main", "table5_auxilio_gas_primary_fuel.tex")
table6_out <- here::here("data", "paper_tables", "main", "table6_auxilio_gas_domestic_care.tex")
table7_out <- here::here("data", "paper_tables", "main", "table7_auxilio_gas_broader_labor.tex")
section_out <- here::here("data", "paper_tables", "main", "auxilio_gas_section_draft.tex")

fuel_domcare <- read.delim(fuel_domcare_path, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE)
domcare_triple_diff <- read.delim(domcare_triple_diff_path, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE)
labor <- read.delim(labor_path, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE)

stars <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.01) return("***")
  if (p < 0.05) return("**")
  if (p < 0.1) return("*")
  ""
}

fmt_num <- function(x, digits = 3) {
  formatC(x, digits = digits, format = "f")
}

fmt_coef <- function(x, p, digits = 3) {
  paste0(fmt_num(x, digits = digits), stars(p))
}

fmt_se <- function(x, digits = 3) {
  paste0("(", fmt_num(x, digits = digits), ")")
}

fmt_int <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

pick_row <- function(df, filters) {
  out <- df
  for (nm in names(filters)) {
    out <- out[out[[nm]] == filters[[nm]], , drop = FALSE]
  }
  if (nrow(out) != 1) {
    stop(paste("Expected one row but found", nrow(out), "for", paste(names(filters), filters, sep = "=", collapse = ", ")), call. = FALSE)
  }
  out[1, , drop = FALSE]
}

tex_wrap <- function(caption, label, body_lines, note) {
  c(
    "\\begin{table}[!htbp]",
    "\\centering",
    paste0("\\caption{", caption, "}"),
    paste0("\\label{", label, "}"),
    "\\small",
    body_lines,
    "\\vspace{0.3em}",
    paste0("\\parbox{0.94\\linewidth}{\\footnotesize ", note, "}"),
    "\\end{table}"
  )
}

fuel_samples <- c(
  "all_households",
  "eligible_vd5008_only_households",
  "eligible_vd5008_only_no_bpc_households",
  "eligible_vd5008_bolsa_no_bpc_households"
)

fuel_labels <- c(
  "All households",
  "VD5008 households",
  "VD5008 households, no BPC",
  "VD5008 + Bolsa households, no BPC"
)

fuel_body <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  " & Wood primary & Gas primary & Observations \\\\",
  "\\midrule"
)

for (i in seq_along(fuel_samples)) {
  wood_row <- pick_row(
    fuel_domcare,
    list(
      panel = "Primary fuel",
      sample = fuel_samples[[i]],
      outcome = "wood_primary",
      design = "pre_families_post",
      spec_label = "HH controls"
    )
  )
  gas_row <- pick_row(
    fuel_domcare,
    list(
      panel = "Primary fuel",
      sample = fuel_samples[[i]],
      outcome = "gas_primary",
      design = "pre_families_post",
      spec_label = "HH controls"
    )
  )
  fuel_body <- c(
    fuel_body,
    paste0(
      fuel_labels[[i]], " & ",
      fmt_coef(wood_row$coef, wood_row$p), " & ",
      fmt_coef(gas_row$coef, gas_row$p), " & ",
      fmt_int(wood_row$nobs), " \\\\"
    ),
    paste0(
      " & ",
      fmt_se(wood_row$se), " & ",
      fmt_se(gas_row$se), " & \\\\"
    )
  )
}

fuel_body <- c(fuel_body, "\\bottomrule", "\\end{tabular}")

fuel_note <- paste0(
  "Entries report coefficients and clustered standard errors from household-level regressions of primary cooking fuel outcomes ",
  "on $\\texttt{pre\\_families\\_post}_{st}=\\bar A^{pre}_s\\times 1[t\\geq 2022Q3]$, where $\\bar A^{pre}_s$ is pre-top-up Auxilio Gas beneficiary families per weighted household in estrato $s$. ",
  "All columns include estrato4 and year-quarter fixed effects, household survey weights, and household controls for rooms, bedrooms, and full-time grid electricity access. ",
  "Standard errors are clustered by estrato4. One unit of treatment corresponds to one additional beneficiary family per weighted household; multiplying a coefficient by 0.1 gives the implied effect of ten additional beneficiary families per 100 weighted households."
)

writeLines(
  tex_wrap(
    "Table 5: Auxilio Gas and Primary Cooking Fuel",
    "tab:table5_auxilio_gas_primary_fuel",
    fuel_body,
    fuel_note
  ),
  table5_out
)

domcare_rf_samples <- c(
  "all_women",
  "eligible_vd5008_only",
  "eligible_vd5008_only_no_bpc",
  "eligible_vd5008_bolsa_no_bpc"
)

domcare_rf_labels <- c(
  "All women",
  "VD5008 women",
  "VD5008 women, no BPC",
  "VD5008 + Bolsa women, no BPC"
)

domcare_td_samples <- c(
  "eligible_vd5008_only",
  "eligible_vd5008_only_no_bpc",
  "eligible_vd5008_bolsa_no_bpc"
)

domcare_td_labels <- c(
  "VD5008 women",
  "VD5008 women, no BPC",
  "VD5008 + Bolsa women, no BPC"
)

domcare_body <- c(
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & Domestic care reason & Observations \\\\",
  "\\midrule",
  "\\multicolumn{3}{l}{\\textit{Panel A: Reduced form (realized growth x post)}} \\\\"
)

for (i in seq_along(domcare_rf_samples)) {
  row <- pick_row(
    fuel_domcare,
    list(
      panel = "Domestic care",
      sample = domcare_rf_samples[[i]],
      outcome = "domestic_care_reason",
      design = "delta_families_post",
      spec_label = "Women controls"
    )
  )
  domcare_body <- c(
    domcare_body,
    paste0(domcare_rf_labels[[i]], " & ", fmt_coef(row$coef, row$p), " & ", fmt_int(row$nobs), " \\\\"),
    paste0(" & ", fmt_se(row$se), " & \\\\")
  )
}

domcare_body <- c(
  domcare_body,
  "\\addlinespace",
  "\\multicolumn{3}{l}{\\textit{Panel B: Triple difference (eligible x pre exposure x post)}} \\\\"
)

for (i in seq_along(domcare_td_samples)) {
  row <- pick_row(
    domcare_triple_diff,
    list(
      sample = domcare_td_samples[[i]],
      outcome = "female_domestic_care_reason",
      design = "eligible_pre_post_triple_diff",
      spec_label = "Women controls"
    )
  )
  domcare_body <- c(
    domcare_body,
    paste0(domcare_td_labels[[i]], " & ", fmt_coef(row$coef, row$p), " & ", fmt_int(row$nobs), " \\\\"),
    paste0(" & ", fmt_se(row$se), " & \\\\")
  )
}

domcare_body <- c(domcare_body, "\\bottomrule", "\\end{tabular}")

domcare_note <- paste0(
  "Panel A reports reduced-form woman-level estimates using $\\texttt{delta\\_families\\_post}_{st}=(\\bar A^{post}_s-\\bar A^{pre}_s)\\times 1[t\\geq 2022Q3]$. ",
  "Panel B reports the triple-difference coefficient from regressions of the outcome on eligible $\\times$ pre-exposure $\\times$ post, along with all lower-order terms. ",
  "All regressions include estrato4 and year-quarter fixed effects, person weights, and woman controls for age, age squared, secondary-plus education, tertiary education, household size, number of children aged 0--14, linked Visit 1 rooms, and linked Visit 1 bedrooms. ",
  "Standard errors are clustered by estrato4. In the current linked women file, \\texttt{female\\_domestic\\_care\\_reason} and \\texttt{outlf\\_domestic\\_care} are numerically identical, so we report the former throughout."
)

writeLines(
  tex_wrap(
    "Table 6: Auxilio Gas and Domestic-Care Constraints",
    "tab:table6_auxilio_gas_domestic_care",
    domcare_body,
    domcare_note
  ),
  table6_out
)

labor_rows <- list(
  list(
    sample = "eligible_05mw_labor",
    sample_label = "Low-income women",
    outcome = "effective_hours_population",
    outcome_label = "Effective weekly hours",
    design = "pre_families_post",
    design_label = "Pre exposure x post",
    spec = "M1"
  ),
  list(
    sample = "eligible_05mw_labor_kids",
    sample_label = "Low-income mothers",
    outcome = "effective_hours_population",
    outcome_label = "Effective weekly hours",
    design = "pre_families_post",
    design_label = "Pre exposure x post",
    spec = "M1"
  ),
  list(
    sample = "eligible_05mw_labor_wood",
    sample_label = "Low-income wood users",
    outcome = "wants_more_hours",
    outcome_label = "Wants more hours",
    design = "pre_families_post",
    design_label = "Pre exposure x post",
    spec = "M2"
  )
)

labor_body <- c(
  "\\begin{tabular}{lccc}",
  "\\toprule",
  "Sample and margin & Estimate & Design & Observations \\\\",
  "\\midrule"
)

for (entry in labor_rows) {
  row <- pick_row(
    labor,
    list(
      sample = entry$sample,
      outcome = entry$outcome,
      design = entry$design,
      spec = entry$spec
    )
  )
  labor_body <- c(
    labor_body,
    paste0(
      entry$sample_label, ": ", entry$outcome_label, " & ",
      fmt_coef(row$coef, row$p), " & ",
      entry$design_label, " & ",
      fmt_int(row$nobs), " \\\\"
    ),
    paste0(" & ", fmt_se(row$se), " & & \\\\")
  )
}

labor_body <- c(labor_body, "\\bottomrule", "\\end{tabular}")

labor_note <- paste0(
  "This table reports selected broader labor-market estimates and is intended as tentative supporting evidence rather than as the core mechanism result. ",
  "The first two rows use a broader labor-income eligibility proxy and regress effective weekly hours in the population on $\\texttt{pre\\_families\\_post}_{st}$; the third row reports unmet labor supply among low-income wood-using women. ",
  "All regressions include estrato4 and year-quarter fixed effects, person weights, and the control set indicated in the underlying regression files. ",
  "Because these outcomes are farther downstream from the policy and rely on looser eligibility definitions than the primary-fuel and domestic-care tables, they should be interpreted cautiously."
)

writeLines(
  tex_wrap(
    "Table 7: Broader Labor Outcomes from Auxilio Gas",
    "tab:table7_auxilio_gas_broader_labor",
    labor_body,
    labor_note
  ),
  table7_out
)

section_lines <- c(
  "\\subsection{Auxilio Gas: Setting and Empirical Design}",
  "Auxilio Gas subsidizes LPG purchases for low-income Brazilian households. The program existed before our main empirical shock, but exposure rose sharply beginning in 2022Q3, when the federal top-up expanded the effective reach and intensity of the subsidy. Because PNAD-C does not identify actual recipient households, our treatment is not beneficiary receipt. Instead, we measure local program exposure at the estrato4-by-quarter level using the number of beneficiary families divided by the sum of calibrated household weights. Let",
  "\\[",
  "A_{st}=\\frac{\\text{Auxilio Gas beneficiary families}_{st}}{\\text{weighted households}_{st}},",
  "\\]",
  "so that $A_{st}$ is beneficiary families per weighted household in place $s$ and quarter $t$.",
  "",
  "For household fuel outcomes, our preferred treatment is a predetermined exposure measure interacted with the post-top-up period:",
  "\\[",
  "\\texttt{pre\\_families\\_post}_{st}=\\bar A^{pre}_s\\times 1[t\\geq 2022Q3],",
  "\\]",
  "where $\\bar A^{pre}_s$ is average exposure in 2021Q4--2022Q2. We estimate household-level regressions of the form",
  "\\[",
  "y_{hst}=\\beta\\,\\texttt{pre\\_families\\_post}_{st}+X'_{hst}\\gamma+\\alpha_s+\\lambda_t+\\varepsilon_{hst},",
  "\\]",
  "where $y_{hst}$ is a household fuel outcome, $X_{hst}$ includes household controls, $\\alpha_s$ are estrato4 fixed effects, and $\\lambda_t$ are year-quarter fixed effects. This is our cleanest quasi-experimental design for the fuel margin because treatment intensity is defined entirely using pre-top-up exposure.",
  "",
  "For downstream domestic-care outcomes, we first use a reduced-form specification based on realized program expansion:",
  "\\[",
  "\\texttt{delta\\_families\\_post}_{st}=(\\bar A^{post}_s-\\bar A^{pre}_s)\\times 1[t\\geq 2022Q3].",
  "\\]",
  "This specification asks whether women living in places with larger realized top-up expansion experienced larger post-top-up declines in domestic-care constraints. We estimate",
  "\\[",
  "y_{ist}=\\beta\\,\\texttt{delta\\_families\\_post}_{st}+X'_{ist}\\gamma+\\alpha_s+\\lambda_t+\\varepsilon_{ist},",
  "\\]",
  "where $y_{ist}$ is a woman-level outcome and $X_{ist}$ includes age, education, household size, children, rooms, and bedrooms. Because this treatment uses post-period realized expansion, we interpret it as our main reduced-form downstream specification rather than as our strongest causal design.",
  "",
  "To strengthen causal interpretation, we also estimate a triple-difference specification that uses only predetermined exposure and pre-treatment eligibility:",
  "\\[",
  "y_{ist}=\\beta\\left(\\text{Eligible}_i\\times \\bar A^{pre}_s\\times 1[t\\geq 2022Q3]\\right)+\\text{lower-order terms}+X'_{ist}\\gamma+\\alpha_s+\\lambda_t+\\varepsilon_{ist}.",
  "\\]",
  "Here, $\\text{Eligible}_i$ is a fixed visit-1 eligibility proxy, primarily households with $VD5008\\leq 0.5$ minimum wage, with variants excluding BPC and requiring Bolsa Familia. This design compares eligible and ineligible women within the same place-time cells and is therefore our stronger causal downstream specification. All regressions cluster standard errors at the estrato4 level; household regressions use household weights and woman-level regressions use person weights.",
  "",
  "\\subsection{Effects on Primary Cooking Fuel}",
  "We first test whether Auxilio Gas shifted the main cooking technology used by households. This is the sharpest mechanism test in the paper. Using \\texttt{pre\\_families\\_post}, we find that higher pre-top-up exposure predicts a post-top-up shift away from wood and toward gas as the primary cooking fuel. In the full household sample, the coefficient on wood as the primary cooking fuel is $-0.0445$ ($p<0.001$), while the coefficient on gas as the primary cooking fuel is $0.0461$ ($p<0.001$). The same pattern holds in more targeted low-income samples. Among households with $VD5008\\leq 0.5$ minimum wage and excluding BPC, the estimates are $-0.0310$ ($p=0.008$) for wood primary and $0.0264$ ($p=0.032$) for gas primary. These results indicate that places with greater pre-top-up program exposure experienced a meaningful reallocation of primary cooking fuel away from wood and toward LPG once the top-up began.",
  "",
  "This distinction between primary fuel and any reported use matters. Earlier any-use indicators allowed fuel stacking, so gas could rise without an observable decline in wood. Once we move to primary fuel, the mechanism becomes much clearer: the top-up is associated with substitution rather than with additional gas use alongside continued wood cooking.",
  "",
  "\\subsection{Domestic-Care Constraints}",
  "We next ask whether this fuel transition translated into lower domestic-care constraints among women. In the reduced-form downstream specification using \\texttt{delta\\_families\\_post}, the estimates are negative across all main samples and strongest for women in households with $VD5008\\leq 0.5$ minimum wage. For this group, the coefficient on domestic-care constraints is $-0.0530$ ($p=0.049$). The corresponding estimate is $-0.0486$ ($p=0.068$) when BPC households are excluded. We interpret these estimates as reduced-form evidence that places with larger realized expansion in Auxilio Gas experienced larger declines in domestic-care barriers after the top-up.",
  "",
  "Our stronger causal specification replaces realized expansion with a triple difference based on pre-top-up exposure and fixed eligibility. This design yields smaller but still negative coefficients. For women with $VD5008\\leq 0.5$ minimum wage and excluding BPC, the triple-interaction coefficient is $-0.0158$ ($p=0.044$). For the narrower group defined by $VD5008\\leq 0.5$, Bolsa Familia, and no BPC, the corresponding estimate is $-0.0189$ ($p=0.037$). We therefore view the domestic-care evidence as consistent with the fuel mechanism: the top-up first shifts households toward gas as their primary cooking fuel, and then, more tentatively, relaxes domestic-care constraints among low-income women.",
  "",
  "\\subsection{Broader Labor Outcomes}",
  "Finally, we examine broader labor-market outcomes. These estimates are farther downstream from the policy and should be interpreted more cautiously. The clearest patterns appear on intensive margins and on unmet labor supply rather than on broad employment status. In a broader low-income sample defined by household labor income per capita below one-half of the minimum wage, effective weekly hours in the population rise by $0.578$ ($p<0.001$) for low-income women and by $0.764$ ($p<0.001$) for low-income mothers. Among low-income wood-using women, the probability of reporting that they want more hours falls by $0.0389$ ($p=0.015$). These estimates are directionally consistent with the domestic-burden mechanism, but they are less tightly tied to the statutory eligibility rule and less stable across alternative samples and specifications than the primary-fuel and domestic-care tables. We therefore treat them as suggestive supporting evidence rather than as the central contribution of this section."
)

writeLines(section_lines, section_out)
