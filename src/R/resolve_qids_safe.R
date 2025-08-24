
# resolve_qids_safe.R
# Robust resolver for missing QIDs in anchors CSV.
# - Uses Wikidata API (wbsearchentities) for candidate IDs
# - Filters candidates by P31 via SPARQL (batched)
# - Retries with exponential backoff + timeouts
# - Throttles requests and periodically writes partial progress
# - Resumes from previous partial output if present
#
# Usage:
#   Rscript resolve_qids_safe.R anchors_hm_long.csv anchors_hm_long_resolved.csv
# Optional env vars:
#   WD_UA='YourAppName/1.0 (contact@example.com)'
#
suppressPackageStartupMessages({
  req <- c("readr","dplyr","stringr","purrr","httr","jsonlite","tidyr")
  inst <- rownames(installed.packages())
  miss <- setdiff(req, inst)
  if (length(miss)) install.packages(miss, repos = "https://cloud.r-project.org")
  lapply(req, library, character.only = TRUE)
})

setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
args <- commandArgs(trailingOnly = TRUE)
in_csv  <- ifelse(length(args)>=1, args[1], "data/anchors/anchors_hm.csv")
out_csv <- ifelse(length(args)>=2, args[2], "data/anchors/anchors_hm_long_resolved.csv")
partial_csv <- sub("\\.csv$", "_partial.csv", out_csv)

options(timeout = max(120, getOption("timeout")))

ua <- Sys.getenv("WD_UA", unset = "AnchorsResolver/1.0 (https://www.wikidata.org/)")
wd_api <- "https://www.wikidata.org/w/api.php"
wd_sparql <- "https://query.wikidata.org/sparql"

langs <- c("en","pt","la","el")  # search languages (can add "it","fr","de" if helpful)

allowed_types <- list(
  H = c("Q5"),  # human
  M = c("Q22989102","Q22988604","Q15397819","Q16334295") # deity, greek myth char, mythological figure, roman myth char
)

safe_get <- function(url, query, times=6, pause_base=0.8, pause_cap=8) {
  for (i in seq_len(times)) {
    res <- try(httr::GET(url, query = query, httr::timeout(60), httr::add_headers("User-Agent"=ua)), silent=TRUE)
    if (!inherits(res, "try-error") && httr::status_code(res) == 200) return(res)
    pause <- min(pause_cap, pause_base * (2^(i-1)))
    Sys.sleep(pause)
  }
  return(NULL)
}

safe_post <- function(url, body, times=6, pause_base=0.8, pause_cap=8) {
  for (i in seq_len(times)) {
    res <- try(httr::POST(url, body = body, encode="form", httr::timeout(60), httr::add_headers("User-Agent"=ua)), silent=TRUE)
    if (!inherits(res, "try-error") && httr::status_code(res) == 200) return(res)
    pause <- min(pause_cap, pause_base * (2^(i-1)))
    Sys.sleep(pause)
  }
  return(NULL)
}

wb_search <- function(name, limit=8) {

  out <- list()
  for (lg in langs) {
    res <- safe_get(wd_api, list(
      action="wbsearchentities", format="json", language=lg, uselang=lg,
      type="item", search=name, limit=limit, strictlanguage="false"
    ))
    if (is.null(res)) next
    js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding="UTF-8")), silent=TRUE)
    if (inherits(js, "try-error") || is.null(js$search)) next
    if (length(js$search)) {
      if (is.null(js$search) || length(js$search) == 0) next
      if (is.data.frame(js$search)) {
        df <- tibble::as_tibble(js$search)
        if (!"id" %in% names(df)) next
        if (!"label" %in% names(df)) df$label <- NA_character_
        if (!"description" %in% names(df)) df$description <- NA_character_
        out[[lg]] <- tibble::tibble(
          id          = df$id,
          label       = dplyr::coalesce(df$label, ""),
          description = dplyr::coalesce(df$description, ""),
          lang        = lg
        )
      } else if (is.list(js$search)) {
        or_empty <- function(x) if (is.null(x)) "" else as.character(x)
        out[[lg]] <- tibble::tibble(
          id          = purrr::map_chr(js$search, ~ or_empty(.x$id)),
          label       = purrr::map_chr(js$search, ~ or_empty(.x$label)),
          description = purrr::map_chr(js$search, ~ or_empty(.x$description)),
          lang        = lg
        )
      } else {
        next
      }
      
    }
    Sys.sleep(0.5)
  }
  if (!length(out)) return(tibble::tibble())
  dplyr::bind_rows(out) %>% dplyr::distinct(id, .keep_all=TRUE)
}

get_types_batch <- function(qids) {
  qids <- unique(qids)
  if (!length(qids)) return(tibble::tibble(item=character(), type=character()))
  chunks <- split(qids, ceiling(seq_along(qids)/40))
  res_all <- purrr::map_dfr(chunks, function(qq) {
    values <- paste0("wd:", qq, collapse=" ")
    q <- sprintf('SELECT ?item ?type WHERE { VALUES ?item { %s } ?item wdt:P31 ?type . }', values)
    res <- safe_get(wd_sparql, list(query=q, format="json"))
    if (is.null(res)) return(tibble::tibble(item=character(), type=character()))
    js <- try(jsonlite::fromJSON(httr::content(res, "text", encoding="UTF-8")), silent=TRUE)
    if (inherits(js, "try-error")) return(tibble::tibble(item=character(), type=character()))
    b <- js$results$bindings
    if (is.null(b)) return(tibble::tibble(item=character(), type=character()))
    tibble::tibble(
      item = sub(".*/", "", vapply(b$item$value, identity, character(1))),
      type = sub(".*/", "", vapply(b$type$value, identity, character(1)))
    )
  })
  res_all
}

choose_candidate <- function(cands, cls) {
  if (!nrow(cands)) return(NA_character_)
  types <- get_types_batch(cands$id)
  if (nrow(types)) {
    by_id <- types %>% dplyr::group_by(item) %>% dplyr::summarise(types = list(unique(type)), .groups="drop")
    cands <- cands %>% dplyr::left_join(by_id, by=c("id"="item"))
    cands$types <- lapply(cands$types, function(x) if (is.null(x)) character(0) else x)
    allow <- allowed_types[[cls]]
    cands$score <- vapply(cands$types, function(v) sum(v %in% allow), numeric(1))
  } else {
    cands$types <- list()
    cands$score <- 0
  }

  hint <- if (cls=="H") c("roman","emperor","governor","prefect","tetrarch","consul","priest") else
          c("god","goddess","myth","deity","hero","heroine")
  cands$hint <- vapply(tolower(cands$description), function(d) sum(stringr::str_detect(d, paste(hint, collapse="|"))), numeric(1))
  lang_order <- setNames(seq_along(langs), langs)
  cands$lang_rank <- lang_order[ match(cands$lang, names(lang_order)) ]
  cands$lang_rank[is.na(cands$lang_rank)] <- max(lang_order)+1
  cands <- cands %>% dplyr::arrange(dplyr::desc(score), dplyr::desc(hint), lang_rank)
  cands$id[1]
}

# ---------------- Main ----------------
A <- readr::read_csv(in_csv, show_col_types = FALSE)
stopifnot(all(c("qid","name","class") %in% names(A)))

if (file.exists(partial_csv)) {
  message("Found partial output: ", partial_csv, " — resuming from it.")
  P <- readr::read_csv(partial_csv, show_col_types = FALSE)
  if (all(c("qid","name","class") %in% names(P))) {
    A <- A %>% dplyr::select(names(P)) %>% dplyr::rows_update(P, by=c("name","class"))
  }
}

need <- which(is.na(A$qid) | A$qid=="")
if (!length(need)) {
  message("No missing QIDs. Writing output.")
  readr::write_csv(A, out_csv)
  quit(save="no")
}

resolved <- 0L
for (i in seq_along(need)) {
  idx <- need[i]
  nm  <- A$name[idx]
  cl  <- A$class[idx]
  cat(sprintf("[%d/%d] Resolving '%s' (class %s) ...\n", i, length(need), nm, cl))
  cands <- wb_search(nm, limit=10)
  if (!nrow(cands)) {
    cat("  - No candidates found.\n")
  } else {
    pick <- choose_candidate(cands, cl)
    if (!is.na(pick)) {
      A$qid[idx] <- pick
      cat("  + Picked: ", pick, "\n", sep="")
    } else {
      cat("  - No suitable candidate after filtering.\n")
    }
  }
  resolved <- resolved + !is.na(A$qid[idx]) && A$qid[idx] != ""
  if (i %% 10 == 0) {
    message("Saving partial at step ", i)
    readr::write_csv(A, partial_csv)
  }
  Sys.sleep(0.6) # global throttle
}

# Final write
readr::write_csv(A, out_csv)
U <- A %>% dplyr::filter(is.na(qid) | qid=="")
if (nrow(U)) readr::write_csv(U, sub("\\.csv$", "_unresolved.csv", out_csv))
message("Done. Wrote: ", out_csv, if (nrow(U)) paste0(" (", nrow(U), " unresolved)"))
