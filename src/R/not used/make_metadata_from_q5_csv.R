
# make_metadata_from_q5_csv.R
# -----------------------------------------------------------------------------
# Build metadata.csv (qid, n_statements, n_sitelinks) from a semicolon-delimited
# CSV like 'all_properties_long.csv' that includes a 'qid' column.
#
# - n_statements is computed LOCALLY by counting rows per qid (after simple dedupe)
# - n_sitelinks is fetched from Wikidata's SPARQL endpoint in batches
#   with retries and throttling. If the fetch fails, it's left as NA.
#
# Usage:
#   Rscript make_metadata_from_q5_csv.R input_csv output_metadata.csv
# Examples:
#   Rscript make_metadata_from_q5_csv.R all_properties_long.csv metadata.csv
#
# Optional env vars:
#   WD_UA='YourAppName/1.0 (contact@example.com)'  # polite User-Agent
# -----------------------------------------------------------------------------
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
suppressPackageStartupMessages({
  req <- c("readr","dplyr","stringr","httr","jsonlite","purrr","tidyr")
  inst <- rownames(installed.packages())
  miss <- setdiff(req, inst)
  if (length(miss)) install.packages(miss, repos = "https://cloud.r-project.org")
  lapply(req, library, character.only = TRUE)
})

args <- commandArgs(trailingOnly = TRUE)
in_csv  <- ifelse(length(args)>=1, args[1], "data/all_properties_long.csv")
out_csv <- ifelse(length(args)>=2, args[2], "data/metadata.csv")

# ---- Load CSV (only qid + minimal columns) ----------------------------------
dat <- readr::read_delim(in_csv, show_col_types = FALSE, guess_max = 100000)

if (!"qid" %in% names(dat)) stop("Input CSV must contain a 'qid' column.")

# ---- Basic cleaning ----------------------------------------------------------
dat <- dat %>% dplyr::filter(!is.na(qid), qid != "")

# Make sure it's uppercase Q
dat$qid <- stringr::str_replace(toupper(dat$qid), "^WD:", "")
dat <- dat %>% dplyr::filter(stringr::str_detect(dat$qid, "^Q[0-9]+$"))

# ---- Compute n_statements locally -------------------------------------------
# We dedupe by (qid, property, value_raw) if columns exist to avoid counting the
# same statement twice. Otherwise, count rows per qid.
if (all(c("property","value_raw") %in% names(dat))) {
  ns <- dat %>%
    dplyr::distinct(qid, property, value_raw) %>%
    dplyr::count(qid, name = "n_statements")
} else if ("property" %in% names(dat)) {
  ns <- dat %>%
    dplyr::distinct(qid, property) %>%
    dplyr::count(qid, name = "n_statements")
} else {
  ns <- dat %>%
    dplyr::count(qid, name = "n_statements")
}
message("Computed n_statements for ", nrow(ns), " items (from local CSV).")

# ---- Fetch n_sitelinks from Wikidata SPARQL ---------------------------------
endpoint <- "https://query.wikidata.org/sparql"
ua <- Sys.getenv("WD_UA", unset = "MakeMetadataFromQ5/1.0 (https://www.wikidata.org/)")

safe_get <- function(url, query, times = 6, pause_base = 0.8, pause_cap = 8) {
  for (i in seq_len(times)) {
    res <- try(httr::GET(url, query = query, httr::add_headers("User-Agent" = ua), httr::timeout(60)), silent = TRUE)
    if (!inherits(res, "try-error") && httr::status_code(res) == 200) return(res)
    pause <- min(pause_cap, pause_base * (2^(i-1)))
    Sys.sleep(pause)
  }
  NULL
}

fetch_sitelinks_batch <- function(qids) {
  qids <- unique(qids)
  if (!length(qids)) return(tibble::tibble(qid = character(), n_sitelinks = integer()))
  values <- paste0("wd:", qids, collapse = " ")
  q <- sprintf('PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT ?qid ?n_sitelinks WHERE {
      VALUES ?item { %s }
      BIND(STRAFTER(STR(?item), "entity/") AS ?qid)
      ?item wikibase:sitelinks ?n_sitelinks .
    }', values)
  res <- safe_get(endpoint, list(query = q, format = "json"))
  if (is.null(res)) return(tibble::tibble(qid = qids, n_sitelinks = NA_integer_))
  js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8")), silent = TRUE)
  if (inherits(js, "try-error") || is.null(js$results$bindings)) return(tibble::tibble(qid = qids, n_sitelinks = NA_integer_))
  tibble::as_tibble(js$results$bindings) %>%
    dplyr::transmute(
      qid = qid$value,
      n_sitelinks = as.integer(n_sitelinks$value)
    )
}

# batch over qids
all_qids <- ns$qid
chunks <- split(all_qids, ceiling(seq_along(all_qids)/300))
sl_all <- purrr::map_dfr(seq_along(chunks), function(i) {
  Sys.sleep(0.5)  # throttle
  batch <- chunks[[i]]
  message(sprintf("Fetching sitelinks: batch %d/%d (size %d)", i, length(chunks), length(batch)))
  fetch_sitelinks_batch(batch)
})

# If some qids missing from SPARQL result (rare), fill NA
sl_all <- dplyr::right_join(sl_all, ns %>% dplyr::select(qid), by = "qid")

# ---- Merge and write ---------------------------------------------------------
meta <- ns %>% dplyr::left_join(sl_all, by = "qid") %>%
  dplyr::mutate(
    n_statements = as.integer(n_statements),
    n_sitelinks  = as.integer(n_sitelinks)
  ) %>%
  dplyr::arrange(qid)

readr::write_csv(meta, out_csv)
message("Done. Wrote: ", out_csv, " (", nrow(meta), " rows).")
