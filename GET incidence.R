rm(list=ls(all=TRUE))
gc()

library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(duckdb)

base_url <- "https://onemocneni-aktualne.mzcr.cz/api/v3"
token <- "SET YOUR OWN TOKEN"


res <- GET(
  url = paste0(base_url, "/incidence-7-14-cr"),
  query = list(
    page = 1,
    itemsPerPage = 1000,
    `datum[before]` = "2022-12-31",
    `datum[after]` = "2020-01-01",
    apiToken = token
  )
)

stop_for_status(res)
raw_json <- content(res, as = "text", encoding = "UTF-8")
data_list <- fromJSON(raw_json, flatten = TRUE)

incidence <- data_list$`hydra:member` %>%
  select(datum, incidence_7_100000) %>%
  transmute(date = as.Date(datum), incidence_7 = incidence_7_100000)

con <- dbConnect(duckdb::duckdb(), dbdir = "data/datacon25.duckdb")
dbWriteTable(con, "covid_incidence", incidence, overwrite = TRUE)