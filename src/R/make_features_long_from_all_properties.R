
# make_features_long_from_all_properties.R
# -----------------------------------------------------------------------------
# Create features_long.csv (qid, property, present) from a LONG properties CSV.
#
# INPUT (example: all_properties_long.csv or all_long_properties.csv)
#   - Must contain at least 'qid' and 'property' columns (or synonyms).
#   - Accepts property in forms like: 'P569', 'wd:P569', full URIs, etc.
#   - Accepts qid as 'Q123', 'wd:Q123', or full URI.
#
# OUTPUT
#   - features_long.csv with columns: qid, property, present (always 1)
#
# OPTIONS
#   - 3rd arg (exclude_external_ids): 1 (default) or 0
#     When 1, fetches property types via SPARQL and drops wikibase:ExternalId.
#   - Env var WD_UA to set a polite User-Agent for SPARQL requests.
#
# USAGE
#   Rscript make_features_long_from_all_properties.R all_properties_long.csv features_long.csv 1
#   Rscript make_features_long_from_all_properties.R all_long_properties.csv features_long.csv 0
# -----------------------------------------------------------------------------
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
suppressPackageStartupMessages({
  req <- c("readr","dplyr","stringr","tidyr","purrr","httr","jsonlite","tibble")
  inst <- rownames(installed.packages())
  miss <- setdiff(req, inst)
  if (length(miss)) install.packages(miss, repos = "https://cloud.r-project.org")
  lapply(req, library, character.only = TRUE)
})

args <- commandArgs(trailingOnly = TRUE)
in_csv  <- ifelse(length(args)>=1, args[1], "data/processed/all_properties_long.csv")
out_csv <- ifelse(length(args)>=2, args[2], "data/processed/features_long.csv")
exclude_external_ids <- ifelse(length(args)>=3, as.integer(args[3])!=0, TRUE)

# ---- Load CSV ----------------------------------------------------------------
dat <- readr::read_delim(in_csv, show_col_types = FALSE)

# ---- Identify/normalize columns ---------------------------------------------
# qid column candidates
qid_cols <- intersect(names(dat), c("qid","item","subject","entity"))
if (!length(qid_cols)) stop("Could not find a qid column (tried: qid, item, subject, entity).")
qid_col <- qid_cols[1]

# property column candidates
prop_cols <- intersect(names(dat), c("property","pid","prop","p","predicate"))
if (!length(prop_cols)) stop("Could not find a property column (tried: property, pid, prop, p, predicate).")
prop_col <- prop_cols[1]

D <- dat %>% dplyr::select(qid = !!dplyr::sym(qid_col), property = !!dplyr::sym(prop_col))

# Normalize qid: keep only 'Q[0-9]+'
D$qid <- D$qid %>% as.character() %>% stringr::str_to_upper() %>%
  stringr::str_replace("^WD:", "") %>%
  stringr::str_replace(".*/(Q[0-9]+)$", "\\1") %>%
  stringr::str_extract("Q[0-9]+")
D <- D %>% dplyr::filter(!is.na(qid))

# Normalize property: keep only 'P[0-9]+'
D$property <- D$property %>% as.character() %>% stringr::str_to_upper() %>%
  stringr::str_replace("^WD:", "") %>%
  stringr::str_replace(".*/(P[0-9]+)$", "\\1") %>%
  stringr::str_extract("P[0-9]+")
D <- D %>% dplyr::filter(!is.na(property))

# ---- (Optional) Exclude ExternalId properties via SPARQL --------------------
if (exclude_external_ids) {
  ua <- Sys.getenv("WD_UA", unset = "MakeFeaturesLong/1.0 (https://www.wikidata.org/)")
  endpoint <- "https://query.wikidata.org/sparql"

  safe_get <- function(url, query, times = 6, pause_base = 0.8, pause_cap = 8) {
    for (i in seq_len(times)) {
      res <- try(httr::GET(url, query = query, httr::add_headers("User-Agent" = ua), httr::timeout(60)), silent = TRUE)
      if (!inherits(res, "try-error") && httr::status_code(res) == 200) return(res)
      pause <- min(pause_cap, pause_base * (2^(i-1)))
      Sys.sleep(pause)
    }
    NULL
  }

  props <- unique(D$property)
  chunks <- split(props, ceiling(seq_along(props)/400))

  keep_props <- purrr::map_dfr(seq_along(chunks), function(i) {
    Sys.sleep(0.4)
    batch <- chunks[[i]]
    values <- paste0("wd:", batch, collapse = " ")
    q <- sprintf('PREFIX wikibase: <http://wikiba.se/ontology#>
      SELECT ?pid ?ptype WHERE {
        VALUES ?prop { %s }
        BIND(STRAFTER(STR(?prop), "entity/") AS ?pid)
        ?prop wikibase:propertyType ?ptype .
      }', values)
    res <- safe_get(endpoint, list(query = q, format = "json"))
    if (is.null(res)) return(tibble::tibble(pid = batch, ptype = NA_character_))
    js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8")), silent = TRUE)
    if (inherits(js, "try-error") || is.null(js$results$bindings)) return(tibble::tibble(pid = batch, ptype = NA_character_))
    tibble::as_tibble(js$results$bindings) %>%
      dplyr::transmute(pid = pid$value, ptype = ptype$value)
  })

  # Keep everything that is NOT ExternalId; if ptype missing, keep (conservative)
  external_id_uri <- "http://wikiba.se/ontology#ExternalId"
  props_to_keep <- keep_props %>%
    dplyr::mutate(is_ext = (ptype == external_id_uri)) %>%
    dplyr::filter(is.na(is_ext) | !is_ext) %>%
    dplyr::pull(pid) %>% unique()

  dropped <- setdiff(props, props_to_keep)
  message("ExternalId properties dropped: ", length(dropped))
  if (length(dropped)) message("  e.g.: ", paste(head(dropped, 10), collapse=", "), if (length(dropped)>10) " …")

  D <- D %>% dplyr::semi_join(tibble::tibble(property = props_to_keep), by = "property")
}

# ---- Collapse to presence (qid, property, present=1) ------------------------
features_long <- D %>% dplyr::distinct(qid, property) %>%
  dplyr::mutate(present = 1L) %>%
  dplyr::arrange(qid, property)

readr::write_csv(features_long, out_csv)
message("Done. Wrote: ", out_csv, " (", nrow(features_long), " rows; ", length(unique(features_long$qid)), " items; ", length(unique(features_long$property)), " properties).")
