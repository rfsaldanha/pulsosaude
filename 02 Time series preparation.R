# Packages
library(tidyverse)
library(duckdb)
library(cli)

# Database
con <- dbConnect(duckdb(), dbdir = "data.duckdb", read_only = TRUE)

# Table connection
table_name <- "sia_pa"
sia_tb <- tbl(con, table_name)

# Count events
## Records
sia_tb |> tally()
## Health units
sia_tb |> distinct(PA_CODUNI) |> pull(PA_CODUNI) |> length()
## Procedures
sia_tb |> distinct(PA_PROC_ID) |> pull(PA_PROC_ID) |> length()

# Time series
res_ts <- sia_tb |>
  group_by(PA_CMP, PA_CODUNI, PA_PROC_ID) |>
  summarise(freq = n()) |>
  ungroup() |>
  arrange(PA_CMP, PA_CODUNI, PA_PROC_ID) |>
  collect()

# Disconnect database
dbDisconnect(con)

# Dates complete
res_ts_comp <- tibble()
cli_progress_bar("Completing dates", total = length(unique(res_ts$PA_CODUNI)))
for(cod_uni in unique(res_ts$PA_CODUNI)){
  tmp <- res_ts |>
    filter(PA_CODUNI == cod_uni) |>
    mutate(
      year = as.numeric(substr(PA_CMP, 0, 4)),
      month = as.numeric(substr(PA_CMP, 5, 6))
    )

  tmp_dates <- expand_grid(
    year = min(tmp$year):max(tmp$year), 
    month = min(tmp$month):max(tmp$month), 
  ) |>
  mutate(
    month = str_pad(month, 2, pad = 0),
    date = paste0(year, month)
  ) |>
  pull(date)
  
  tmp <- tmp |>
    select(-year, -month) |>
    complete(PA_CMP = tmp_dates, fill = list(freq = 0))

  res_ts_comp <- bind_rows(res_ts_comp, tmp)

  rm(tmp, tmp_dates)
  cli_progress_update()
}
cli_progress_done()

# Save
write_rds(res_ts_comp, "sia_ts.rds")
