library(httr)      # requisições HTTP
library(jsonlite)  # parse do JSON
library(glue)      # interpolar strings
library(dplyr)     # bind_rows
library(data.table)  # optional: rbindlist rápido

endpoint <- "https://query.wikidata.org/sparql"

fetch_page <- function(offset = 0, limit = 5000) {
  sparql <- glue::glue(
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT DISTINCT ?human WHERE {{
          {{
            SELECT DISTINCT ?human WHERE {{
              {{ ?human wdt:P569 ?birth .
                 FILTER (?birth >= \"-0100-01-01T00:00:00Z\"^^xsd:dateTime &&
                         ?birth <=  \"0100-12-31T00:00:00Z\"^^xsd:dateTime) }}
              UNION
              {{ ?human wdt:P570 ?death .
                 FILTER (?death >= \"-0100-01-01T00:00:00Z\"^^xsd:dateTime &&
                         ?death <=  \"0100-12-31T00:00:00Z\"^^xsd:dateTime) }}
              ?human wdt:P31 wd:Q5 .
            }} LIMIT {limit} OFFSET {offset}
          }}
        }}"
  )
  res <- POST(endpoint,
              body = list(query = sparql,
                          format = "json",
                          timeout = 120000),
              encode = "form",
              user_agent("WDQS-R/0.1"))
  stop_for_status(res)
  fromJSON(content(res, "text"))$results$bindings
}

offset <- 0; passo <- 10000; tudo <- list()
repeat {
  parte <- fetch_page(offset, passo)
  if (length(parte) == 0) break
  tudo[[length(tudo)+1]] <- parte
  offset <- offset + passo
  Sys.sleep(1)
}

people <- dplyr::bind_rows(tudo)

fetch_mythic_page <- function(offset = 0, limit = 5000) {
  
  sparql <- glue(
    'SELECT DISTINCT ?item WHERE {{
  {{
    SELECT DISTINCT ?item WHERE {{
      VALUES ?instance {{
        wd:Q22988604   # mythological Greek character
        wd:Q4271324    # mythical character
        wd:Q178885     # deity
        wd:Q23015925   # demigod of Greek mythology
      }}
      ?item wdt:P31 ?instance .
    }} LIMIT {limit} OFFSET {offset}
  }}
}}'
  )
  
  res <- POST(
    endpoint,
    body = list(
      query   = sparql,
      format  = "json",
      timeout = 120000
    ),
    encode      = "form",
    user_agent  = "WDQS-R mythic/0.1"
  )
  
  stop_for_status(res)
  fromJSON(content(res, "text", encoding = "UTF-8"))$results$bindings
}

step   <- 5000
offset <- 0
all_pages <- list()

repeat {
  page <- fetch_mythic_page(offset, step)
  if (length(page) == 0) break
  all_pages[[length(all_pages) + 1]] <- page
  offset <- offset + step
  Sys.sleep(1)
}

mythic_ids <- bind_rows(all_pages) 

qids <- c(paste0("Q", gsub(".*Q(\\d+)$", "\\1", people$human$value)),
          paste0("Q", gsub(".*Q(\\d+)$", "\\1", mythic_ids$item$value)))

setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")

write.csv(data.frame(qid = qids), "data/qid_selected.csv", row.names = FALSE)
