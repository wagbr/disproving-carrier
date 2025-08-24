
# validate_anchors.R
# Validates anchors CSV: structure, classes, QID format, missing sources.
#
# Usage:
#   Rscript validate_anchors.R anchors_hm_long_resolved.csv
suppressPackageStartupMessages({
  required <- c("readr","dplyr","stringr")
  to_install <- setdiff(required, rownames(installed.packages()))
  if (length(to_install)) install.packages(to_install, repos="https://cloud.r-project.org")
  lapply(required, library, character.only = TRUE)
})
args <- commandArgs(trailingOnly = TRUE)
in_csv  <- ifelse(length(args)>=1, args[1], "data/anchors/anchors_hm_long_resolved.csv")
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
A <- readr::read_csv2(in_csv, show_col_types = FALSE)

errs <- list()

need_cols <- c("qid","name","class","primary_source","notes")
miss <- setdiff(need_cols, names(A))
if (length(miss)) errs <- c(errs, paste0("Missing columns: ", paste(miss, collapse=", ")))

if ("class" %in% names(A)) {
  badc <- A %>% dplyr::filter(!class %in% c("H","M"))
  if (nrow(badc)) errs <- c(errs, paste0("Rows with invalid class (not H/M): ", nrow(badc)))
}

if ("qid" %in% names(A)) {
  badq <- A %>% dplyr::filter(qid!="" & !grepl("^Q[0-9]+$", qid))
  if (nrow(badq)) errs <- c(errs, paste0("Rows with malformed QID: ", nrow(badq)))
}

if ("primary_source" %in% names(A)) {
  empt <- A %>% dplyr::filter(is.na(primary_source) | primary_source=="")
  if (nrow(empt)) errs <- c(errs, paste0("Rows with empty primary_source: ", nrow(empt)))
}

if (length(errs)) {
  cat("VALIDATION ISSUES:\n")
  for (e in errs) cat("- ", e, "\n", sep="")
} else {
  cat("OK: anchors file looks consistent.\n")
}

