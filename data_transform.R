rm(list=ls(all=TRUE))
gc()

library(duckdb)
library(dplyr)

new_dtb_name <- "data/datacon25b.duckdb"
ozp_file_name <- "../OZP_preskladane.csv"
cpzp_file_name <- "../CPZP_preskladane.csv"
kortizon_file_name <- "../kortizonove_ekvivalenty.csv"


#### OZP ####
ozp <- read.table(ozp_file_name, sep = ",", header = TRUE)

###### clients ######
ozp_clients <- ozp %>% 
  filter(is.na(Datum_umrti)) %>%
  distinct(Id_pojistence, .keep_all = TRUE) %>% 
  transmute(client_id = Id_pojistence, sex = as.factor(Pohlavi), age = 2025-Rok_narozeni, n_vacc = pocet_vakcinaci, n_presc = pocet_predpisu)

###### prescriptions ######
ozp_drugs_catalog <- ozp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "předpis") %>% 
  distinct(
    Detail_udalosti, Nazev, léková_forma_zkr, ATC_skupina, síla, doplněk_názvu, léková_forma, léčivé_látky, Equiv_sloucenina, Prednison_equiv, Pocet_v_baleni
  ) %>% 
  transmute(
    detail_id = Detail_udalosti, drug_name = Nazev, drug_form_short = léková_forma_zkr, ATC_group = ATC_skupina, strength = síla, 
    subname = doplněk_názvu, drug_form = léková_forma, medicinal_substances = léčivé_látky, equiv_substance = Equiv_sloucenina, Prednison_equiv,
    count_in_box = Pocet_v_baleni
  ) %>% mutate(presc_id = 1:n())

ozp_prescriptions <- ozp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "předpis") %>% 
  transmute(client_id = Id_pojistence, detail_id = Detail_udalosti, drug_name = Nazev, n_boxes = Pocet_baleni, date = as.Date(Datum_udalosti)) %>% 
  left_join(
    ozp_drugs_catalog %>% transmute(detail_id, drug_name, presc_id), by = c("detail_id", "drug_name")
  ) %>% 
  transmute(client_id, presc_id, n_boxes, date)


###### vaccination ######
ozp_vaccinations <- ozp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "vakcinace") %>% 
  transmute(client_id = Id_pojistence, detail_id = Detail_udalosti, date = as.Date(Datum_udalosti)) %>% 
  arrange(client_id, date) %>%  
  group_by(client_id) %>% mutate(event_order = row_number()) %>%
  ungroup()

kortiz_eq <- read.csv(kortizon_file_name)
#### saving to dtb ####
con <- dbConnect(duckdb::duckdb(), dbdir = new_dtb_name)
dbWriteTable(con, "kortizon_eq", kortiz_eq, overwrite = TRUE)
dbWriteTable(con, "ozp_vaccinations", ozp_vaccinations, overwrite = TRUE)
dbWriteTable(con, "ozp_drugs_catalog", ozp_drugs_catalog, overwrite = TRUE)
dbWriteTable(con, "ozp_prescriptions", ozp_prescriptions, overwrite = TRUE)
dbWriteTable(con, "ozp_clients", ozp_clients, overwrite = TRUE)
dbDisconnect(con, shutdown = TRUE)

rm(ozp, ozp_vaccinations, ozp_prescriptions, ozp_drugs_catalog, ozp_clients)

#### CPZP ####
cpzp <- read.table(cpzp_file_name, sep = ",", header = TRUE)

###### clients ######
cpzp_clients <- cpzp %>% 
  filter(is.na(Datum_umrti)) %>%
  distinct(Id_pojistence, .keep_all = TRUE) %>% 
  transmute(client_id = Id_pojistence, sex = as.factor(Pohlavi), age = 2025-Rok_narozeni, n_vacc = pocet_vakcinaci, n_presc = pocet_predpisu)

###### prescriptions ######
cpzp_drugs_catalog <- cpzp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "předpis") %>% 
  distinct(Detail_udalosti, Kod_udalosti, léková_forma_zkr, ATC_skupina, síla, doplněk_názvu, léková_forma, 
           léčivé_látky, Equiv_sloucenina, Prednison_equiv, Pocet_v_baleni) %>% 
  transmute(
    detail_id = Detail_udalosti, detail_code = Kod_udalosti, drug_form_short = léková_forma_zkr, ATC_group = ATC_skupina, force = síla, 
    subname = doplněk_názvu, drug_form = léková_forma, medicinal_substances = léčivé_látky, equiv_substance = Equiv_sloucenina, Prednison_equiv,
    count_in_box = Pocet_v_baleni
  ) %>% mutate(presc_id = (1:n()) + 1000)

cpzp_prescriptions <- cpzp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "předpis") %>% 
  transmute(client_id = Id_pojistence, detail_id = Detail_udalosti, detail_code = Kod_udalosti, n_boxes = Pocet_baleni, 
            date = as.Date(Datum_udalosti), specialization = Specializace) %>% 
  left_join(
    cpzp_drugs_catalog  %>% select(detail_id, detail_code, presc_id), by = c("detail_id", "detail_code")
  ) %>% 
  transmute(client_id, presc_id, n_boxes, date, specialization)

###### vaccination ######
cpzp_vaccinations <- cpzp %>% 
  filter(is.na(Datum_umrti), Typ_udalosti == "vakcinace") %>% 
  transmute(client_id = Id_pojistence, detail_id = Detail_udalosti, detail_code = Kod_udalosti, event_order = poradi, date = as.Date(Datum_udalosti))

#### saving to dtb ####
con <- dbConnect(duckdb::duckdb(), dbdir = new_dtb_name)
dbWriteTable(con, "cpzp_clients", cpzp_clients, overwrite = TRUE)
dbWriteTable(con, "cpzp_prescriptions", cpzp_prescriptions, overwrite = TRUE)
dbWriteTable(con, "cpzp_drugs_catalog", cpzp_drugs_catalog, overwrite = TRUE)
dbWriteTable(con, "cpzp_vaccinations", cpzp_vaccinations, overwrite = TRUE)
dbDisconnect(con, shutdown = TRUE)
