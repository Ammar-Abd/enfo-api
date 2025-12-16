# library(plumber)
# 
# 
# r <- plumb(file = "plumber.R")
# 
# r$run(host = "203.135.63.47", port = 8000)
# api.R
#!/usr/bin/env Rscript
#!/usr/bin/env Rscript
library(plumber)

cat("Starting Combined API Server...\n")
cat("================================\n\n")

# Load consolidated API first
pr1 <- plumb("consolidated_api.R")
pr2 <- plumb("plumber.R")

cat("Loaded consolidated_api.R with", length(pr1$endpoints), "endpoint(s)\n")
cat("Loaded plumber.R with", length(pr2$endpoints), "endpoint(s)\n\n")

# Mount plumber.R into consolidated API (at root level)
pr1$mount("", pr2)

# Run server
cat("Starting Combined API Server at http://203.135.63.47:8000\n")
cat("API Docs: http://203.135.63.47:8000/__docs__/\n\n")
pr1$run(host = "203.135.63.47", port = 8000)