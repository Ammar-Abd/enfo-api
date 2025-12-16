#!/usr/bin/env Rscript

# Consolidated API Server Startup Script
# ======================================
# Unified startup script for Daily Analysis, Electricity Bill, and Historic File Load APIs

cat("Starting Consolidated API Server...\n")
cat("===================================\n\n")

# Default configuration
DEFAULT_HOST <- "203.135.63.47"
DEFAULT_PORT <- 8000

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
host <- DEFAULT_HOST
port <- DEFAULT_PORT

if (length(args) >= 1) {
  port <- as.numeric(args[1])
  if (is.na(port)) {
    cat("Error: Invalid port number '", args[1], "'\n")
    quit(status = 1)
  }
}

if (length(args) >= 2) {
  host <- args[2]
}

# Print configuration
cat("Server URL: http://", host, ":", port, "\n", sep = "")

# Check required packages for all services
required_packages <- c('plumber', 'jsonlite', 'httr', 'dplyr', 'magrittr', 
                      'plyr', 'lubridate', 'stringr', 'plotly', 'zoo')
missing_packages <- required_packages[!sapply(required_packages, requireNamespace, quietly = TRUE)]

if (length(missing_packages) > 0) {
  cat("Missing packages:", paste(missing_packages, collapse = ", "), "\n")
  cat("Install with: install.packages(c(", paste0("'", missing_packages, "'", collapse = ", "), "))\n")
  quit(status = 1)
}

# Load consolidated API server
api_file <- "consolidated_api.R"
if (!file.exists(api_file)) {
  cat("Error: API file '", api_file, "' not found\n")
  quit(status = 1)
}

cat("Loading Consolidated API...\n")
source(api_file)

cat("Starting server...\n")
cat("\n=== API Information ===\n")
cat("Health Check: http://", host, ":", port, "/health\n", sep = "")
cat("API Docs: http://", host, ":", port, "/__docs__/\n", sep = "")

cat("\n=== Service Endpoints ===\n")
cat("Daily Analysis API:\n")
cat("  Health: http://", host, ":", port, "/daily-analysis/health\n", sep = "")
cat("  Comparative Analysis: POST /daily-analysis/comparative-analysis\n")
cat("  Log Download: POST /daily-analysis/log-download\n")

cat("\nElectricity Bill API:\n")
cat("  Health: http://", host, ":", port, "/electricity-bill/health\n", sep = "")
cat("  Load Bill Data: POST /electricity-bill/load-bill-data\n") 
cat("  Set Run Mode: POST /electricity-bill/set-run-mode\n")

cat("\nHistoric File Load API:\n")
cat("  Health: http://", host, ":", port, "/historic-file/health\n", sep = "")
cat("  Load Data: POST /historic-file/load-historic-data\n")
cat("  Column Info: GET /historic-file/column-info\n")
cat("  Date Range: GET /historic-file/date-range\n")

cat("\n=== Dependencies ===\n")
cat("Historic File Load: Direct file system access\n")
cat("Daily Analysis: Uses Historic File Load API internally\n")
cat("Electricity Bill: External API at 203.135.63.47:8000 (for non-DEP mode)\n")

cat("\n=== Configuration ===\n")
cat("Default RUN_MODE: DEP\n")
cat("Press Ctrl+C to stop\n\n")

# Start server
tryCatch({
  start_consolidated_server(port = port, host = host)
}, error = function(e) {
  cat("\nServer error:", conditionMessage(e), "\n")
  if (grepl("address already in use|bind", conditionMessage(e), ignore.case = TRUE)) {
    cat("Port", port, "is in use. Try: Rscript start_consolidated_api.R", port + 1, "\n")
    cat("Alternative ports to try:\n")
    cat("  8081, 8082, 8083, 8090, 8100\n")
  }
  quit(status = 1)
})