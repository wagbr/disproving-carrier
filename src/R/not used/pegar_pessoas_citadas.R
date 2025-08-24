library(httr)
library(jsonlite)
library(glue)
library(dplyr)
library(purrr)
library(rlang)
library(arules)

`%||%` <- rlang::`%||%`
wdqs    <- "https://query.wikidata.org/sparql"
long_path  <- "data/all_properties_long.csv"

classify_sources <- function(qid) {
  sparql <- glue("
      PREFIX wd:  <http://www.wikidata.org/entity/>
      PREFIX wdt: <http://www.wikidata.org/prop/direct/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      SELECT ?bucket WHERE {{
        VALUES ?src {{ wd:{qid} }}
        ?src wdt:P31 ?class .
        OPTIONAL {{ ?src wdt:P577 ?date. }}
        BIND(
          IF(
            (BOUND(?date) && ?date <= '0600-01-01T00:00:00Z'^^xsd:dateTime) ||
            EXISTS {{ ?class (wdt:P279*) wd:Q7725634 }}
          , 'primary_ancient',
            IF(
              EXISTS {{ ?class (wdt:P279*) wd:Q11424  }} ||
              EXISTS {{ ?class (wdt:P279*) wd:Q5398426 }}
            , 'fiction_media',
              'scholarly_modern'
            )
          ) AS ?bucket
        )
      }}")
  
  url  <- paste0(wdqs, "?format=json&query=", URLencode(sparql, reserved = TRUE))
  res  <- httr::GET(url, ua = "WDQS-classify/1.2")
  if (httr::http_error(res)) return(NA_character_)      # timeout etc.
  
  dat <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  bindings <- dat$results$bindings
  if (!length(bindings)) return(NA_character_)          # lista vazia
  
  # transforma em data-frame e pega a coluna value
  bucket_val <- as.data.frame(bindings)$bucket$value[1]
  bucket_val
}


# ────────────────────────── 1) extrai relatos  ─────────────────────────
src_props   <- c("P1343", "P5323", "P1441")

relatos_long <- read_delim(long_path, show_col_types = FALSE) %>% 
  dplyr::filter(property %in% src_props, snaktype == "value") %>% 
  select(qid, value_raw)

relatos_ids <- relatos_long %>%             # lista de Q-IDs por personagem
  mutate(src_qid = str_extract(value_raw, "Q\\d{1,9}")) %>% 
  dplyr::filter(!is.na(src_qid)) %>% 
  distinct(qid, src_qid) %>% 
  group_by(qid) %>% 
  summarise(src_vec = list(src_qid), .groups = "drop")

# Se quiser focar em Q302 e Q82732, descomente:
# relatos_ids <- filter(relatos_ids, qid %in% c("Q302","Q82732"))

# ────────────────────────── 2) classifica obras ────────────────────────
conflicted::conflicts_prefer(dplyr::filter)
pb <- progress::progress_bar$new(
  total  = sum(lengths(relatos_ids$src_vec)),
  format = "[:bar] :current/:total"
)

source_stats <- relatos_ids %>% 
  mutate(
    bucket_vec = map(src_vec, ~ {
      map_chr(.x, \(id) { pb$tick(); classify_sources(id) })
    }),
    n_src_primaryAnc = map_int(bucket_vec, ~ sum(. == "primary_ancient",  na.rm = TRUE)),
    n_src_scholarly  = map_int(bucket_vec, ~ sum(. == "scholarly_modern", na.rm = TRUE)),
    n_src_mediaFict  = map_int(bucket_vec, ~ sum(. == "fiction_media",    na.rm = TRUE))
  ) %>% 
  select(-bucket_vec)


# ────────────────────────── 3) junta à base  ───────────────────────────
data <- data %>%
  left_join(source_stats, by = "qid") %>% 
  mutate(
    n_src_primaryAnc = replace_na(n_src_primaryAnc, 0),
    n_src_scholarly  = replace_na(n_src_scholarly,  0),
    n_src_mediaFict  = replace_na(n_src_mediaFict,  0),
    
    n_src_total  = n_src_primaryAnc + n_src_scholarly,
    n_src_log    = log1p(n_src_total),
    n_src_bucket = cut(
      n_src_total,
      breaks = c(-1, 0, 1, 3, 6, Inf),
      labels = c("0", "1", "2-3", "4-6", "7+")
    )
  )

data$src_vec <- NULL

write_csv(data, "data/data_final.csv")
