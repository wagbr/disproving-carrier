
# resolve_qids.R
# Fills missing QIDs in anchors CSV by querying Wikidata labels/aliases.
#
# Usage:
#   Rscript resolve_qids.R anchors_hm_long.csv anchors_hm_long_resolved.csv
#
suppressPackageStartupMessages({
  required <- c("readr","dplyr","stringr","purrr","httr","jsonlite","utils")
  to_install <- setdiff(required, rownames(installed.packages()))
  if (length(to_install)) install.packages(to_install, repos="https://cloud.r-project.org")
  lapply(required, library, character.only = TRUE)
})

args <- commandArgs(trailingOnly = TRUE)
in_csv  <- ifelse(length(args)>=1, args[1], "anchors_hm.csv")
out_csv <- ifelse(length(args)>=2, args[2], "anchors_hm_long_resolved.csv")

A <- readr::read_csv(in_csv, show_col_types = FALSE)
stopifnot(all(c("qid","name","class") %in% names(A)))

endpoint <- "https://query.wikidata.org/sparql"

sparql <- function(q) {
  res <- httr::GET(endpoint, httr::add_headers(Accept="application/sparql-results+json"),
                   query = list(query = q))
  if (httr::status_code(res) != 200) return(NULL)
  js <- jsonlite::fromJSON(httr::content(res, "text", encoding="UTF-8"))
  if (!"results" %in% names(js)) return(NULL)
  tibble::as_tibble(js$results$bindings)
}

build_query <- function(name, cls) {
  type_filter <- if (cls == "H") {
    'VALUES ?prefType { wd:Q5 }'
  } else {
    'VALUES ?prefType { wd:Q22989102 wd:Q22988604 wd:Q15397819 }'
  }
  sprintf('SELECT ?item ?itemLabel WHERE {
    %s
    ?item wdt:P31 ?type .
    ?item rdfs:label ?lab .
    FILTER(LANG(?lab) IN ("en","pt"))
    FILTER(STRLEN("%s")>0 && (lcase(str(?lab)) = lcase("%s")))
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,pt". }
  }
  LIMIT 5', type_filter, name, name)
}

build_query_fallback <- function(name) {
  sprintf('SELECT ?item ?itemLabel WHERE {
    ?item rdfs:label ?lab .
    FILTER(LANG(?lab) IN ("en","pt"))
    FILTER(CONTAINS(lcase(?lab), lcase("%s")))
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,pt". }
  }
  LIMIT 10', name)
}

resolve_one <- function(nm, cls) {
  q1 <- build_query(nm, cls)
  r1 <- sparql(q1)
  if (!is.null(r1) && nrow(r1)) return(r1$item.value[1])
  q2 <- build_query_fallback(nm)
  r2 <- sparql(q2)
  if (!is.null(r2) && nrow(r2)) return(r2$item.value[1])
  NA_character_
}

need <- which(is.na(A$qid) | A$qid=="")
if (length(need)) {
  message("Resolving ", length(need), " missing QIDs via Wikidata…")
  A$qid[need] <- purrr::map_chr(need, function(i) {
    nm <- A$name[i]
    cl <- A$class[i]
    uri <- resolve_one(nm, cl)
    if (is.na(uri)) return(NA_character_)
    sub(".*/", "", uri)
  })
} else {
  message("No missing QIDs.")
}

readr::write_csv(A, out_csv)
message("Wrote: ", out_csv)
