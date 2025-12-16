#!/usr/bin/env Rscript
library(plumber)

# Load consolidated API first
pr1 <- plumb("/srv/shiny-server/EnvizAPI/consolidated_api.R")
pr2 <- plumb("/srv/shiny-server/EnvizAPI/plumber.R")

# Mount plumber.R into consolidated API
pr1$mount("", pr2)

# Run on port 8002
pr1$run(host = "127.0.0.1", port = 8002)
