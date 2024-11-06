# Packages
library(tidyverse)
library(cli)
library(harbinger)

# Load data
sia_ts <- read_rds("sia_ts.rds") |>
  drop_na()

# Anomaly model definition
model <- hanr_arima()
sia_ts_anom <- tibble()
for(coduni in unique(sia_ts$PA_CODUNI)){
  for(proc in unique(sia_ts$PA_PROC_ID)){
    cli_inform("Unidade {coduni}, procedimento {proc}")

    tmp <- sia_ts |>
      mutate(PA_CMP = as_date(PA_CMP, format = "%Y%m")) |>
      filter(PA_CODUNI == coduni & PA_PROC_ID == proc) |>
      mutate(idx = row_number())

    if(nrow(tmp) < 12) next

    model_fit <- daltoolbox::fit(model, tmp$freq)
    detection <- detect(model_fit, tmp$freq)

    tmp_anom <- left_join(tmp, detection, by = "idx")
    sia_ts_anom <- bind_rows(sia_ts_anom, tmp_anom)

    rm(tmp, model_fit, detection, tmp_anom)
  }
}





