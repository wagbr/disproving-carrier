
# make_qids_by_period_v2.R
# -----------------------------------------------------------------------------
# Fetch human QIDs active in a date window WITHOUT relying on P569/P570.
# Signals used (each with its own UNION branch and filter):
#   (A1) P39 start (pq:P580) within [START, END]
#   (A2) P39 end   (pq:P582) within [START, END]
#   (B)  P1317 (floruit) within [START, END]
#   (C)  P793 (significant event) with P585 within [START, END]
#   (D)  Participation (P710) in an event dated by P585 within [START, END]
#
# Adds diagnostics: counts per branch, optional fallback to ±N years if empty.
#
# Usage:
#   Rscript make_qids_by_period_v2.R START_Z END_Z OUT_CSV [LIMIT] [MAX_PAGES] [FALLBACK_YEARS]
# Example (100 BCE to 100 CE; note ISO year 0000 = 1 BCE, so 100 BCE = -0099):
#   Rscript make_qids_by_period_v2.R -0099-01-01T00:00:00Z 0100-12-31T23:59:59Z qids_period.csv 5000 60 0
# -----------------------------------------------------------------------------
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
suppressPackageStartupMessages({
  req <- c("httr","jsonlite","readr","dplyr","stringr","purrr","tibble")
  inst <- rownames(installed.packages())
  miss <- setdiff(req, inst)
  if (length(miss)) install.packages(miss, repos="https://cloud.r-project.org")
  lapply(req, library, character.only=TRUE)
})

START <- "-0100-01-01T00:00:00Z"
END   <- "0100-12-31T23:59:59Z"
OUT   <- "data/qids_misc.csv"
LIMIT <- 50
MAXPG <- 100
BRANCHES <- c("A1","A2","B","C"); 
PAUSE <- 0.4

# START <- args$start; END <- args$end; OUT <- args$out
# BRANCHES <- unlist(strsplit(args$branches, ","))
# LIMIT <- args$limit; MAXPG <- args$pages; PAUSE <- args$sleep
REQUIRE_Q5 <- FALSE

endpoint <- "https://query.wikidata.org/sparql"
ua <- Sys.getenv("WD_UA", unset = "MakeQIDsByPeriod-Keyset/1.0 (https://www.wikidata.org/)")

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

q5_clause <- function() if (REQUIRE_Q5) "?human wdt:P31 wd:Q5 ." else ""

# Builder genérico com cursor (?t como tempo e ?human como chave secundária)
build_query_keyset <- function(core, start, end, after_t = NULL, after_human_iri = NULL, limit = 50) {
  # core() deve produzir o bloco SPARQL que define ?human e ?t (xsd:dateTime)
  add_after <- ""
  if (!is.null(after_t) && !is.null(after_human_iri)) {
    add_after <- sprintf('FILTER( ?t > "%s"^^xsd:dateTime || ( ?t = "%s"^^xsd:dateTime && STR(?human) > "%s" ) )',
                         after_t, after_t, after_human_iri)
  }
  sprintf('
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT DISTINCT ?human ?t WHERE {
  hint:Query hint:optimizer "None" .
  %s
  %s
  FILTER(?t >= "%s"^^xsd:dateTime && ?t <= "%s"^^xsd:dateTime)
  %s
}
ORDER BY ?t ?human
LIMIT %d', q5_clause(), core, start, end, add_after, limit)
}

core_A1 <- '
?human p:P39 ?st .
?st ps:P39 [] .
?st pqv:P580 ?node . ?node wikibase:timeValue ?t .
'
core_A2 <- '
?human p:P39 ?st .
?st ps:P39 [] .
?st pqv:P582 ?node . ?node wikibase:timeValue ?t .
'
core_B <- '
?human wdt:P1317 ?t .
'
core_C <- '
?human p:P793 ?st .
?st ps:P793 [] .
?st pqv:P585 ?node . ?node wikibase:timeValue ?t .
'
core_D <- '
?event wdt:P585 ?t ; wdt:P710 ?human .
'

branch_cores <- list(A1=core_A1, A2=core_A2, B=core_B, C=core_C, D=core_D)

Q_SEEN <- character(0)
RESULTS <- list()

run_branch_keyset <- function(tag, core) {
  cat(sprintf("\n== Branch %s (keyset; require_q5=%s) ==\n", tag, ifelse(REQUIRE_Q5,"1","0")))
  after_t <- NULL; after_h <- NULL; page <- 0L
  repeat {
    q <- build_query_keyset(core, START, END, after_t, after_h, LIMIT)
    res <- safe_get(endpoint, q)
    if (is.null(res)) { cat("  (sem resposta)\n"); break }
    js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding="UTF-8")), silent=TRUE)
    if (inherits(js, "try-error") || is.null(js$results$bindings)) { cat("  (erro ao parsear)\n"); break }
    tb <- tibble::as_tibble(js$results$bindings)
    if (!nrow(tb)) { cat("  (sem linhas)\n"); break }
    qids <- unique(sub(".*/","", tb$human$value))
    times <- tb$t$value
    n_batch <- length(qids)
    new_mask <- !(qids %in% Q_SEEN)
    n_new <- sum(new_mask)
    Q_SEEN <<- unique(c(Q_SEEN, qids))
    RESULTS[[length(RESULTS)+1]] <<- tibble::tibble(src=tag, qid=qids, t=times)
    page <- page + 1L
    cat(sprintf("[%-2s] page=%3d  batch=%3d  new=%3d  total=%5d  last_t=%s\n",
                tag, page, n_batch, n_new, length(Q_SEEN), tail(times,1)))
    # prepara próximo cursor
    after_t <- tail(times, 1)
    after_h <- tail(tb$human.value, 1)
    if (n_batch < LIMIT) break
    if (page >= MAXPG) { cat(sprintf("[%-2s] atingiu --pages=%d\n", tag, MAXPG)); break }
    Sys.sleep(PAUSE)
  }
}

to_run <- intersect(BRANCHES, names(branch_cores))
if (!length(to_run)) stop("Nenhum ramo válido em --branches.")

for (b in to_run) run_branch_keyset(b, branch_cores[[b]])

if (!length(RESULTS)) {
  cat("\nNenhum resultado. Verifique janela/ramos.\n")
  readr::write_csv(tibble::tibble(qid=character(), src=character()), OUT)
  quit(save="no")
}

ALL <- dplyr::bind_rows(RESULTS)
cat("\n--- Resumo por ramo (linhas recuperadas, com duplicatas) ---\n")
print(ALL %>% dplyr::count(src, name="n"))
FINAL <- ALL %>% dplyr::distinct(qid) %>% dplyr::arrange(qid)
cat(sprintf("\nTOTAL ÚNICO: %d QIDs\n", nrow(FINAL)))
readr::write_csv(FINAL %>% dplyr::select(qid), OUT)
cat(sprintf("Gravei: %s\n", OUT))