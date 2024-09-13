rm(list = ls())

library(data.table)
library(stringr)
library(urltools)
library(lubridate)
library(readxl)
library(readr)
library(dplyr)
library(ggmap)
library(duckdb)
library(dplyr)
library(DBI)
library(arrow)
library(jsonlite)
library(tidyverse)
library(censusapi)
library(rscopus)

# system_info <- Sys.info()
# if (system_info["sysname"] == "Windows") {
#   # Settings for Windows
#   # DATA <- "//research.hbs.edu/projects3/cfarronato_webmunk_project/data"
#   # SAVE_PATH <- "P://GitHub//ra-amazon-project//output//"
#   # CODE <- "P://GitHub//ra-amazon-project//code//"
# } else {
#   # Settings for Linux (on the Grid)
#   # DATA <- "/export/projects3/cfarronato_webmunk_project/data"
#   # SAVE_PATH <- "/export/home/dor/hschrauff/GitHub/ra-amazon-project/output/"
#   # CODE <- "/export/home/dor/hschrauff/GitHub/ra-amazon-project/code/"
# }

this_date <- "2024-07-16"

# Goal: Get Census API to work

########################################################################################
## Get Census data 
########################################################################################

Sys.setenv(CENSUS_KEY="8b3eb3ee8ea4af9df0dbb7fdde36bba2a12136af")

cbp <- getCensus(
  name = "2022/cbp",
  vars = c("CBSA", "NAICS2017", "EMP", "ESTAB", "EMPSZES", "YEAR", "ZIPCODE", "PAYANN", "SECTOR", "CD", "INDLEVEL", "INDGROUP", "SUBSECTOR", "SUMLEVEL"), 
  region = "metropolitan statistical area/micropolitan statistical area:*")

table(cbp$EMPSZES)

cbp1 <- cbp

cbp2 <- cbp1 %>%
  pivot_wider(
    id_cols = c(metropolitan_statistical_area_micropolitan_statistical_area, CBSA,  NAICS2017),
    names_from = EMPSZES,
    values_from = ESTAB,
    names_prefix = "EMPSZES_"
  )

cbp_35620_6221 <- subset(cbp2, CBSA == "10100" & NAICS2017 == "42")

# Rename EMPSZES_001 to all and EMPSZES_210 to N<5
cbp2 <- cbp2 %>%
  rename(
    all = EMPSZES_001,
    N0_5 = EMPSZES_210,
    N5_9 = EMPSZES_220, 
    N10_19 = EMPSZES_230,
    N20_49 = EMPSZES_241,
    N50_99 = EMPSZES_242,
    N100_249 = EMPSZES_251,
    N250_499 = EMPSZES_252, 
    N500_999 = EMPSZES_254,
    N1000_plus = EMPSZES_260, 
    N1000_1499 = EMPSZES_262, 
    N1500_2499 = EMPSZES_263,
    N2500_4999 = EMPSZES_271, 
    N5000_plus = EMPSZES_273
  )

# Define the NAICS codes for each year
naics_codes <- list(
  "2022" = "NAICS2017",
  "2021" = "NAICS2017",
  "2020" = "NAICS2017",
  "2019" = "NAICS2017",
  "2018" = "NAICS2017",
  "2017" = "NAICS2017",
  "2016" = "NAICS2012",
  "2015" = "NAICS2012",
  "2014" = "NAICS2012",
  "2013" = "NAICS2012",
  "2012" = "NAICS2007",
  "2011" = "NAICS2007",
  "2010" = "NAICS2007",
  "2009" = "NAICS2007",
  "2008" = "NAICS2007",
  "2007" = "NAICS2002",
  "2006" = "NAICS2002",
  "2005" = "NAICS2002",
  "2004" = "NAICS2002",
  "2003" = "NAICS2002",
  "2002" = "NAICS1997",
  "2001" = "NAICS1997",
  "2000" = "NAICS1997",
  "1999" = "NAICS1997",
  "1998" = "NAICS1997",
  "1997" = "SIC",
  "1996" = "SIC",
  "1995" = "SIC",
  "1994" = "SIC",
  "1993" = "SIC",
  "1992" = "SIC",
  "1991" = "SIC",
  "1990" = "SIC",
  "1989" = "SIC",
  "1988" = "SIC",
  "1987" = "SIC",
  "1986" = "SIC"
)

# Function to get Census data for a range of years
getCensusData <- function(start_year, end_year) {
  census_data <- list()
  
  for (year in seq(start_year, end_year)) {
    Sys.setenv(CENSUS_KEY = "8b3eb3ee8ea4af9df0dbb7fdde36bba2a12136af")
    
    # Get Census data for the current year
    cbp <- getCensus(
      name = paste0(year, "/cbp"),
      vars = c("CBSA", naics_codes[[as.character(year)]], "EMP", "ESTAB", "EMPSZES", "YEAR", "PAYANN", "SECTOR", "CD", "INDLEVEL", "INDGROUP", "SUBSECTOR", "SUMLEVEL"),
      region = "metropolitan statistical area/micropolitan statistical area:*"
    )
    
    # Store the data in the list
    census_data[[as.character(year)]] <- cbp
  }
  
  return(census_data)
}

# Call the function to get Census data from 2022 to 1986
census_data <- getCensusData(2020, 2016)

# Make a dataframe
census_data_df <- bind_rows(census_data)


census_data_df1 <- census_data_df %>%
  pivot_wider(
    id_cols = c(metropolitan_statistical_area_micropolitan_statistical_area, CBSA,  NAICS2017),
    names_from = EMPSZES,
    values_from = ESTAB,
    names_prefix = "EMPSZES_"
  )

census_data_35620_6221 <- subset(cbp2, CBSA == "10100" & NAICS2017 == "42")

# Rename EMPSZES_001 to all and EMPSZES_210 to N<5
census_data_df1 <- census_data_df1 %>%
  rename(
    all = EMPSZES_001,
    N0_5 = EMPSZES_210,
    N5_9 = EMPSZES_220, 
    N10_19 = EMPSZES_230,
    N20_49 = EMPSZES_241,
    N50_99 = EMPSZES_242,
    N100_249 = EMPSZES_251,
    N250_499 = EMPSZES_252, 
    N500_999 = EMPSZES_254,
    N1000_plus = EMPSZES_260, 
    N1000_1499 = EMPSZES_262, 
    N1500_2499 = EMPSZES_263,
    N2500_4999 = EMPSZES_271, 
    N5000_plus = EMPSZES_273
  )