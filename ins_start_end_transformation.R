# Clean WS
rm(list = ls(all = TRUE))
gc()

# Load dependencies
library(duckdb)
library(dplyr)
library(DBI)

# routes
dtb_name <- "data/datacon25.duckdb"
ozp_file_name <- "../OZP_preskladane.csv"
cpzp_file_name <- "../CPZP_preskladane.csv"

# OZP
ozp_norm_path <- normalizePath("../OZP_preskladane.csv")
con <- dbConnect(duckdb::duckdb(), dbdir = dtb_name)

dbExecute(
  con,
  sprintf(
    "
  CREATE TEMP TABLE ozp AS
  SELECT * FROM read_csv_auto('%s', nullstr = 'NA')
",
    ozp_norm_path
  )
)

query_ozp <- "
  SELECT DISTINCT
    Id_pojistence,

    FIRST_VALUE(Posledni_zahajeni_pojisteni) OVER (
      PARTITION BY Id_pojistence
      ORDER BY Posledni_zahajeni_pojisteni ASC
    ) AS ins_start_date,

    CASE
      WHEN
        FIRST_VALUE(Datum_umrti) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Datum_umrti DESC
        ) IS NULL
        OR FIRST_VALUE(Datum_umrti) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Datum_umrti DESC
        ) > FIRST_VALUE(Posledni_ukonceni_pojisteni) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Posledni_ukonceni_pojisteni DESC
        )
      THEN FIRST_VALUE(Posledni_ukonceni_pojisteni) OVER (
             PARTITION BY Id_pojistence
             ORDER BY Posledni_ukonceni_pojisteni DESC
           )
      ELSE FIRST_VALUE(Datum_umrti) OVER (
             PARTITION BY Id_pojistence
             ORDER BY Datum_umrti DESC
           )
    END AS ins_end_date

  FROM ozp
"

ins_start_end_ozp <- dbGetQuery(con, query_ozp)
dbWriteTable(con, "ozp_ins_start_end", ins_start_end_ozp, overwrite = TRUE)
dbDisconnect(con, shutdown = TRUE)
rm(ins_start_end_ozp)

# CPZP
cpzp_norm_path <- normalizePath("../CPZP_preskladane.csv")
con <- dbConnect(duckdb::duckdb(), dbdir = dtb_name)

dbExecute(
  con,
  sprintf(
    "
  CREATE TEMP TABLE cpzp AS
  SELECT * FROM read_csv_auto('%s', nullstr = 'NA')
",
    cpzp_norm_path
  )
)

query_cpzp <- "
  SELECT DISTINCT
    Id_pojistence,

    FIRST_VALUE(Posledni_zahajeni_pojisteni) OVER (
      PARTITION BY Id_pojistence
      ORDER BY Posledni_zahajeni_pojisteni ASC
    ) AS ins_start_date,

    CASE
      WHEN
        FIRST_VALUE(Datum_umrti) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Datum_umrti DESC
        ) IS NULL
        OR FIRST_VALUE(Datum_umrti) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Datum_umrti DESC
        ) > FIRST_VALUE(Posledni_ukonceni_pojisteni) OVER (
          PARTITION BY Id_pojistence
          ORDER BY Posledni_ukonceni_pojisteni DESC
        )
      THEN FIRST_VALUE(Posledni_ukonceni_pojisteni) OVER (
             PARTITION BY Id_pojistence
             ORDER BY Posledni_ukonceni_pojisteni DESC
           )
      ELSE FIRST_VALUE(Datum_umrti) OVER (
             PARTITION BY Id_pojistence
             ORDER BY Datum_umrti DESC
           )
    END AS ins_end_date

  FROM cpzp
"

ins_start_end_cpzp <- dbGetQuery(con, query_cpzp)
dbWriteTable(con, "cpzp_ins_start_end", ins_start_end_cpzp, overwrite = TRUE)
dbDisconnect(con, shutdown = TRUE)
