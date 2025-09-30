library(data.table)
library(lubridate)
library(dplyr)
library(ggplot2)

#### data loadings ####

ozp_csv_path = "../OZP_preskladane.csv"
cpzp_csv_path = "../CPZP_preskladane.csv"

## Function to load and filter a single file
load_prescriptions <- function(file) {
  dt <- fread(
    file,
    select = c("Typ_udalosti", "Datum_udalosti", "Equiv_sloucenina"),
    colClasses = c(Typ_udalosti = "character", Datum_udalosti = "IDate", Equiv_sloucenina = "character")
  )
  # filter rows and select relevant columns
  dt <- dt[
    Typ_udalosti == "předpis" & !is.na(Equiv_sloucenina),
    .(date = Datum_udalosti, equiv_substance = Equiv_sloucenina)
  ]
  return(dt)
}

## Load both files
ozp <- load_prescriptions(ozp_csv_path)
cpzp <- load_prescriptions(cpzp_csv_path)

##Combine both data.tables and aggregate counts by month and substance
df <- rbindlist(list(ozp, cpzp)) %>% 
  count(date = make_date(year(date), month(date), 1), equiv_substance)

## Free memory
rm(ozp, cpzp)

#### plots ####
# --- Relative proportions of prescriptions by year and substance ---
df %>% 
  group_by(YR = year(date), equiv_substance) %>% summarise(n = sum(n), .groups = "drop") %>% 
  ggplot(aes(YR, n, fill = equiv_substance)) +
  geom_col(position = "fill") +
  labs(x = "rok", y = "poměr počtu předpisů", fill = "")  + 
  theme_minimal()

# --- Absolute counts of prescriptions by year and substance ---
df %>% 
  group_by(YR = year(date), equiv_substance) %>% summarise(n = sum(n), .groups = "drop") %>% 
  ggplot(aes(YR, n, col = equiv_substance)) +
  geom_line() + geom_point() +
  labs(x = "rok", y = "počty předpisů", col = "") +
  theme_minimal()

# --- Time series of monthly counts by substance ---
df %>% 
  ggplot(aes(date, n, col = equiv_substance)) +
  geom_line() + 
  labs(x = "", y = "počty předpisů", col = "") +
  theme_minimal()

# --- Time series split into three periods with different line types ---
df %>% 
  mutate(
    gr = case_when(
      date <= ymd("2018-01-01") ~ "do 2018-01",
      date <= ymd("2021-06-01") ~ "2018-01 až 2021-06",
      TRUE ~ "po 2021-06"
    ),
    gr = factor(gr, levels = c("do 2018-01", "2018-01 až 2021-06", "po 2021-06"))
  ) %>%
  ggplot(aes(x = date, y = n, color = equiv_substance, linetype = gr)) +
  geom_point(alpha = 0.2) +
  geom_smooth(method = "lm", se = FALSE)  +
  labs(x = "", y = "počty předpisů", col = "", linetype = "") +
  theme_minimal()