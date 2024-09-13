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

# Goal: Clean the data for the wages and concentration project

########################################################################################
## Load in and clean the data
########################################################################################

# Field           Data
# Name            Type    Description
# MSA             C       Metropolitan or Micropolitan Area Code
# NAICS           C       Industry Code - 6-digit NAICS code.
# EMP_NF          C       Total Mid-March Employees Noise Flag (See all Noise Flag definitions at the end of this record layout)
# EMP             N       Total Mid-March Employees with Noise
# QP1_NF          C       Total First Quarter Payroll Noise Flag
# QP1             N       Total First Quarter Payroll ($1,000) with Noise
# AP_NF           C       Total Annual Payroll Noise Flag
# AP              N       Total Annual Payroll ($1,000) with Noise
# EST             N       Total Number of Establishments
# N<5             N       Number of Establishments: Less than 5 Employee Size Class
# N5_9            N       Number of Establishments: 5-9 Employee Size Class
# N10_19          N       Number of Establishments: 10-19 Employee Size Class
# N20_49          N       Number of Establishments: 20-49 Employee Size Class
# N50_99          N       Number of Establishments: 50-99 Employee Size Class
# N100_249        N       Number of Establishments: 100-249 Employee Size Class
# N250_499        N       Number of Establishments: 250-499 Employee Size Class
# N500_999        N       Number of Establishments: 500-999 Employee Size Class
# N1000           N       Number of Establishments: 1,000 or More Employee Size Class
# N1000_1         N       Number of Establishments: Employment Size Class: 1,000-1,499 Employees
# N1000_2         N       Number of Establishments: Employment Size Class: 1,500-2,499 Employees
# N1000_3         N       Number of Establishments: Employment Size Class: 2,500-4,999 Employees
# N1000_4         N       Number of Establishments: Employment Size Class: 5,000 or More Employees
# NOTE: Noise Flag definitions (fields ending in _NF) are:
# G       0 to < 2% noise (low noise)
# H       2 to < 5% noise (medium noise)
# J       >= 5% noise (high noise)
# Flag definition for Establishment by Employment Size Class fields (N<5, N5_9, etc.):
# N       Not available or not comparable

df <- read.csv("P://GitHub/Papers/wages_and_concentration/data/cbp22msa.txt")

msa <- read.csv("P://GitHub/Papers/wages_and_concentration/data/Core_based_statistical_area_for_the_US_July_2023_-5413359380187677741.csv")

df1 <- merge(df, msa, by.x = "msa", by.y = "CBSA.Code", all.x = TRUE)

df1$naics <- str_replace_all(df1$naics, "/", "")
df1$naics <- ifelse(df1$naics == "------", "ALL", df1$naics)
df1$naics <- str_replace_all(df1$naics, "-", "")

df1 <- df1 %>%
    select(-(c("OBJECTID", "Land.area", "Water.area", "Shape__Area", "Shape__Length")))

head(df1$naics)

str(df1)

df1$emp <- as.numeric(df1$emp)
df1$qp1 <- as.numeric(df1$qp1)
df1$ap <- as.numeric(df1$ap)
df1$est <- as.numeric(df1$est)
df1$n5 <- as.numeric(df1$n5)
df1$n5_9 <- as.numeric(df1$n5_9)
df1$n10_19 <- as.numeric(df1$n10_19)
df1$n20_49 <- as.numeric(df1$n20_49)
df1$n50_99 <- as.numeric(df1$n50_99)
df1$n100_249 <- as.numeric(df1$n100_249)
df1$n250_499 <- as.numeric(df1$n250_499)
df1$n500_999 <- as.numeric(df1$n500_999)
df1$n1000 <- as.numeric(df1$n1000)
df1$n1000_1 <- as.numeric(df1$n1000_1)
df1$n1000_2 <- as.numeric(df1$n1000_2)
df1$n1000_3 <- as.numeric(df1$n1000_3)
df1$n1000_4 <- as.numeric(df1$n1000_4)

summary(df1$emp)

df1_35620_6221 <- subset(df1, msa == "10100" & naics == "42")
