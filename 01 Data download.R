# Packages
library(tidyverse)
library(duckdb)
library(microdatasus)
library(cli)
library(tictoc)

# Database
con <- dbConnect(duckdb(), dbdir = "data.duckdb", read_only = FALSE)

# Download data
ufs <- c("RS")
years <- 2022:2024
months <- 1:12
table_name <- "sia_pa"

if(dbExistsTable(con, table_name)) dbRemoveTable(con, table_name)

tic()
for(uf in ufs){
  for(y in years){
    for(m in months){
      cli_inform("UF {uf}, year {y}, month {m}")
      tmp <- fetch_datasus(
        year_start = y, year_end = y, 
        month_start = m, month_end = m, 
        uf = uf, 
        information_system = "SIA-PA", 
        timeout = 600
      )
      dbWriteTable(con, table_name, tmp, append = TRUE)
      rm(tmp)
    }
  }
}
toc() # 1761.339 sec elapsed

# Disconnect database
dbDisconnect(con)


