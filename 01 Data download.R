# Packages
library(tidyverse)
library(stringr)
library(fs)
library(curl)
library(duckdb)
library(read.dbc)
library(cli)
library(tictoc)

# Database
con <- dbConnect(duckdb(), dbdir = "data.duckdb", read_only = FALSE)

# Download data
s3_base <- "https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com"
temp_file <- file_temp()
temp_dir <- path_temp()

ufs <- c("RS")
years <- substr(2023:2024, 3, 4) 
months <- str_pad(1:12, 2, pad = "0") 

grid <- expand.grid(ufs, years, months)
files_to_download <- sort(paste0("PA", grid[,1], grid[,2], grid[,3], ".dbc"))

res_sia <- multi_download(
  urls = path(s3_base, "/SIASUS/200801_/Dados/", files_to_download), 
  destfiles = path(temp_dir, files_to_download), 
  progress = TRUE
)


# Read and store data
table_name <- "sia_pa"
if(dbExistsTable(con, table_name)) dbRemoveTable(con, table_name)

tic()
for(f in res_sia$destfile[which(res_sia$status_code==200)]){
  message(f)
  tmp <- read.dbc(f, as.is = TRUE) |> 
    mutate(across(where(is.character), stringi::stri_enc_tonative))
  dbWriteTable(con, table_name, tmp, append = TRUE)
  rm(tmp)
}
rm(f)
toc()

# Disconnect database
dbDisconnect(con)


