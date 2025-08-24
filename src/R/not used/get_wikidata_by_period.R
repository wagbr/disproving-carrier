
# make_qids_by_period.R
# -----------------------------------------------------------------------------
# Fetch human QIDs active in a given [start,end] window WITHOUT relying on
# birth (P569) / death (P570). Uses one or more of:
#   - Position held (P39) with start/end qualifiers (P580/P582)
#   - Floruit (P1317)
#   - Significant event (P793) with point in time (P585)
#   - Participation (P710) in an event dated by P585
#
# Usage:
#   Rscript make_qids_by_period.R START_Z END_Z OUT_CSV [LIMIT] [MAX_PAGES]
# Example:
#   Rscript make_qids_by_period.R -0100-01-01T00:00:00Z 0100-12-31T23:59:59Z qids_period.csv 10000 50
#
# Notes:
#   - START_Z / END_Z must be xsd:dateTime strings (UTC 'Z'), BCE uses leading '-'.
#   - You can tweak the unions below if you want to include/exclude any source.
# -----------------------------------------------------------------------------
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")

suppressPackageStartupMessages({
  req <- c("httr","jsonlite","readr","dplyr","stringr","purrr","tibble")
  inst <- rownames(installed.packages())
  miss <- setdiff(req, inst)
  if (length(miss)) install.packages(miss, repos="https://cloud.r-project.org")
  lapply(req, library, character.only=TRUE)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: Rscript get_wikidata_by_period.R START_Z END_Z OUT_CSV [LIMIT] [MAX_PAGES]")
}
START <- "-0100-01-01T00:00:00Z"
END   <- "0100-12-31T23:59:59Z"
OUT   <- "data/qids_period.csv"
LIMIT <- 10000
MAXPG <- 50

endpoint <- "https://query.wikidata.org/sparql"
ua <- Sys.getenv("WD_UA", unset = "MakeQIDsByPeriod/1.0 (https://www.wikidata.org/)")

# One page query builder (with placeholders for LIMIT/OFFSET)
build_query <- function(start, end, limit, offset) {
  sprintf('PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?human WHERE {
  ?human wdt:P31 wd:Q5 .
  {
    # Position held with start/end qualifiers
    ?human p:P39 ?st .
    ?st ps:P39 ?office .
    OPTIONAL { ?st pq:P580 ?start . }
    OPTIONAL { ?st pq:P582 ?endt . }
    BIND(COALESCE(?start, ?endt) AS ?t39)
    FILTER(BOUND(?t39) && ?t39 >= "%s"^^xsd:dateTime && ?t39 <= "%s"^^xsd:dateTime)
  }
  UNION
  {
    # Floruit
    ?human wdt:P1317 ?floruit .
    FILTER(?floruit >= "%s"^^xsd:dateTime && ?floruit <= "%s"^^xsd:dateTime)
  }
  UNION
  {
    # Significant event with point in time
    ?human p:P793 ?sev .
    ?sev ps:P793 ?event .
    ?sev pq:P585 ?when .
    FILTER(?when >= "%s"^^xsd:dateTime && ?when <= "%s"^^xsd:dateTime)
  }
  UNION
  {
    # Participation in an event dated by P585
    ?event wdt:P585 ?edate ;
           wdt:P710 ?human .
    FILTER(?edate >= "%s"^^xsd:dateTime && ?edate <= "%s"^^xsd:dateTime)
  }
}
LIMIT %d OFFSET %d', start, end, start, end, start, end, start, end, limit, offset)
}

safe_get <- function(url, query, tries=6, base=0.8, cap=8) {
  for (i in seq_len(tries)) {
    res <- try(httr::GET(url, query=list(query=query, format="json"),
                         httr::add_headers("User-Agent"=ua), httr::timeout(60)),
               silent=TRUE)
    if (!inherits(res, "try-error") && httr::status_code(res)==200) return(res)
    Sys.sleep(min(cap, base * (2^(i-1))))
  }
  NULL
}

# paginate
out <- list(); page <- 0L
repeat {
  q <- build_query(START, END, LIMIT, page*LIMIT)
  res <- safe_get(endpoint, q)
  if (is.null(res)) break
  js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding="UTF-8")), silent=TRUE)
  if (inherits(js,"try-error") || is.null(js$results$bindings)) break
  tb <- tibble::as_tibble(js$results$bindings)
  if (!nrow(tb)) break
  out[[length(out)+1]] <- tb
  message(sprintf("Fetched page %d: %d rows", page+1, nrow(tb)))
  if (nrow(tb) < LIMIT) break
  page <- page + 1L
  if (page >= MAXPG) { message("Reached MAX_PAGES."); break }
  Sys.sleep(0.5)
}

if (!length(out)) {
  message("No results. Writing empty CSV.")
  readr::write_csv(tibble::tibble(qid=character()), OUT)
  quit(save="no")
}

D <- dplyr::bind_rows(out)

# Clean to QIDs
Q <- D %>% dplyr::transmute(qid = stringr::str_replace(human.value, ".*/", "")) %>%
  dplyr::filter(stringr::str_detect(qid, "^Q[0-9]+$")) %>%
  dplyr::distinct() %>%
  dplyr::arrange(qid)

readr::write_csv(Q, OUT)
message("Done. Wrote: ", OUT, " (", nrow(Q), " qids).")
