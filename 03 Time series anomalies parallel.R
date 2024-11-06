# Packages
library(tidyverse)
library(furrr)
library(cli)
library(harbinger)

# Load data
sia_ts <- read_rds("sia_ts.rds") |>
  mutate(PA_CMP = as_date(PA_CMP, format = "%Y%m")) |>
  drop_na()

# Processing function
anom_process <- function(df, model = hanr_arima()){
  if(nrow(df) < 12) return(NULL)

  tmp <-  mutate(df, idx = row_number())

  model_fit <- daltoolbox::fit(model, tmp$freq)
  detection <- detect(model_fit, tmp$freq)

  tmp_anom <- left_join(tmp, detection, by = "idx")
  return(tmp_anom)
}

# Split data to list
sia_gp <- sia_ts |>
  group_by(PA_CODUNI, PA_PROC_ID) |>
    group_split(.keep = TRUE)


# Plan parallel 
plan(multisession, workers = 18)

# Execute
res <- future_map(.x = sia_gp, .f = anom_process, .progress = TRUE)



ggplot(res[[1159]], aes(x = PA_CMP, y = freq, stat = "identity")) +
  geom_line() +
  geom_point(aes(color = event))
