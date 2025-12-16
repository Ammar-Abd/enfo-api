# Consolidated API Server
# =======================
# Combines Daily Analysis API, Electricity Bill API, and Historic File Load API
# Each API maintains its own endpoint namespace for clear separation

# Load all required libraries
library(plumber)
library(jsonlite)
library(httr)
library(dplyr)
library(magrittr)
library(plyr)
library(lubridate)
library(data.table)
library(stringr)
library(plotly)
library(zoo)
library(xts)
library(forecast)
library(base64enc) 



# Global configuration
RUN_MODE <- 'DEP'  # Can be 'DEV' or 'DEP'

# ============================================================================
# SHARED UTILITY FUNCTIONS
# ============================================================================

# Standard API response formatter (shared across all APIs)
format_api_response <- function(success = TRUE, data = NULL, message = "", status_code = 200) {
  response <- list(
    success = success,
    timestamp = as.character(Sys.time()),
    message = as.character(message)
  )
  
  if (!is.null(data)) {
    response$data <- data
  }
  
  return(response)
}

# ============================================================================
# DAILY ANALYSIS API FUNCTIONS
# ============================================================================

# spike_check function from daily analysis
spike_check <- function(df, type) {
  if (exists('RUN_MODE') && RUN_MODE == 'DEP') df[, -1] <- lapply(df[, -1], as.numeric)
  powerColumns = c("Usage", grep('[kW]', colnames(df), value = T, fixed = T))
  voltageColumns = grep('[V]', colnames(df), value = T, fixed = T)
  currentColumns = grep('[A]', colnames(df), value = T, fixed = T)
  powerfactorColumns = grep('[PF]', colnames(df), value = T, fixed = T)
  
  if (type == "minute") {
    # Power
    for (col in powerColumns) {
      if (col %in% colnames(df)){
        spikes <- which(df[, col] > 4000)
        if (length(spikes) > 0) df[spikes, col] <- NA
      }
    }
    # Voltage
    for (col in voltageColumns) {
      spikes <- which(df[, col] > 1000)
      if (length(spikes) > 0) df[spikes, col] <- NA
    }
  } else {
    # Power
    for (col in powerColumns) {
      if (col %in% colnames(df)) {
        spikes <- which(df[, col] > 4000)
        if (length(spikes) > 0) df[spikes, col] <- NA
      }
    }
    # Voltage
    for (col in voltageColumns) {
      spikes <- which(df[, col] > 1000)
      if (length(spikes) > 0) df[spikes, col] <- NA
    }
  }
  
  df
}

# Configure historic_file_load API endpoint for daily analysis
HISTORIC_API_URL <- "http://203.135.63.47:8000/historic-file"

# Function to call historic_file_load API from daily analysis
call_historic_api <- function(start_date, end_date, dir, meterType, buildingMap) {
  body_data <- list(
    start_date = as.character(start_date),
    end_date = as.character(end_date),
    dir = as.character(dir),
    meterType = as.character(meterType),
    buildingMap = if(is.list(buildingMap)) jsonlite::toJSON(buildingMap, auto_unbox = TRUE) else buildingMap
  )
  
  response <- POST(
    paste0(HISTORIC_API_URL, "/load-historic-data"),
    body = body_data,
    encode = "json",
    timeout(300)
  )
  
  if (status_code(response) != 200) {
    stop(paste("Historic API request failed with status:", status_code(response)))
  }
  
  raw_content <- content(response, "text", encoding = "UTF-8")
  result <- jsonlite::fromJSON(raw_content, simplifyDataFrame = TRUE)
  
  success_val <- if(is.list(result$success) || length(result$success) > 1) result$success[[1]] else result$success
  
  if (!success_val) {
    message_val <- if(is.list(result$message) || length(result$message) > 1) result$message[[1]] else result$message
    stop(paste("Historic API error:", message_val))
  }
  
  # Fix DateTime format if needed
  if ("summaryData" %in% names(result$data)) {
    if (!is.data.frame(result$data$summaryData)) {
      result$data$summaryData <- data.frame(result$data$summaryData, stringsAsFactors = FALSE)
    }
    if ("DateTime" %in% colnames(result$data$summaryData) && is.character(result$data$summaryData$DateTime)) {
      result$data$summaryData$DateTime <- as.POSIXct(result$data$summaryData$DateTime)
    }
  }
  
  return(result$data)
}

# Input validation helper for daily analysis
validate_comparative_inputs <- function(dates, start_time, end_time, dir_hourwise, dir_minutewise, meterType, buildingMap) {
  errors <- c()
  
  # Validate dates
  if (is.null(dates) || length(dates) == 0) {
    errors <- c(errors, "dates parameter is required")
  } else {
    tryCatch({
      as.Date(dates)
    }, error = function(e) {
      errors <<- c(errors, "Invalid date format in dates parameter")
    })
  }
  
  # Validate time range
  if (is.null(start_time) || is.null(end_time)) {
    errors <- c(errors, "start_time and end_time parameters are required")
  } else if (!is.numeric(start_time) || !is.numeric(end_time) || start_time < 0 || start_time > 23 || end_time < 0 || end_time > 23) {
    errors <- c(errors, "start_time and end_time must be integers between 0-23")
  }
  
  # Validate directories
  if (is.null(dir_hourwise) || is.null(dir_minutewise)) {
    errors <- c(errors, "dir_hourwise and dir_minutewise parameters are required")
  }
  
  # Validate meter type
  if (is.null(meterType) || !is.character(meterType)) {
    errors <- c(errors, "meterType parameter is required and must be a string")
  }
  
  # Validate building map
  if (is.null(buildingMap)) {
    errors <- c(errors, "buildingMap parameter is required")
  }
  
  return(errors)
}

# ============================================================================
# ELECTRICITY BILL API FUNCTIONS
# ============================================================================

# Helper function to convert API list to data frame
api_list2df <- function(api_list) {
  if (is.null(api_list) || length(api_list) == 0) {
    return(data.frame())
  }
  
  # Convert list structure to data frame
  if (is.list(api_list) && !is.data.frame(api_list)) {
    # If it's a list of lists (rows), convert to data frame
    if (all(sapply(api_list, is.list))) {
      return(do.call(rbind, lapply(api_list, function(x) as.data.frame(x, stringsAsFactors = FALSE))))
    } else {
      # If it's a simple list, make it a single-row data frame
      return(as.data.frame(api_list, stringsAsFactors = FALSE))
    }
  }
  
  return(as.data.frame(api_list, stringsAsFactors = FALSE))
}

# Input validation helper for electricity bill
validate_bill_inputs <- function(selectedBranch, billSelectionInput = NULL) {
  errors <- c()
  
  if (is.null(selectedBranch) || !is.character(selectedBranch) || selectedBranch == "") {
    errors <- c(errors, "selectedBranch parameter is required and must be a non-empty string")
  }
  
  if (!is.null(billSelectionInput) && !is.character(billSelectionInput)) {
    errors <- c(errors, "billSelectionInput must be a string if provided")
  }
  
  return(errors)
}

# Core bill data loading function
load_bill_data <- function(selectedBranch, billSelectionInput = NULL) {
  # Initialize variables
  billDir <- paste0('../Clients Usage Data/Electricity Bills/', selectedBranch)
  billDate <- Sys.Date()
  billMonth <- paste(str_to_upper(lubridate::month(billDate, label = TRUE)), 
                     substr(year(billDate), 3, 4), sep = '_')
  
  # Get bill files list
  if (RUN_MODE == 'DEP') {
    billFiles <- list.files(billDir)
  } else {
    # For non-DEP mode, we'll handle this in the API call
    billFiles <- c()  # Will be populated by external API
  }
  
  # Determine filenames based on selection
  billSummaryFilename <- ""
  billHistoryFilename <- ""
  
  if (RUN_MODE == 'DEP') {
    billSummaryFilename_pattern <- grep(paste0(selectedBranch, '_', billMonth, '_billing_summary'), 
                                       billFiles, fixed = TRUE)
    
    if (length(billSummaryFilename_pattern) > 1 && !is.null(billSelectionInput)) {
      # Multiple files found, use selection input
      digits_at_end <- gsub(".*?(\\d+)$", "\\1", billSelectionInput)
      billSummaryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, 
                                   '_billing_summary_', digits_at_end, '.csv')
      billHistoryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, 
                                   '_billing_history_', digits_at_end, '.csv')
    } else {
      # Single file or no selection
      billSummaryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, '_billing_summary.csv')
      billHistoryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, '_billing_history.csv')
    }
  } else {
    # Non-DEP mode uses different filename logic
    billSummaryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, '_billing_summary.csv')
    billHistoryFilename <- paste0(billDir, '/', selectedBranch, '_', billMonth, '_billing_history.csv')
  }
  
  # Check if bill summary file exists
  billSummaryCheck <- FALSE
  
  if (RUN_MODE == 'DEP') {
    billSummaryCheck <- file.exists(billSummaryFilename)
  } else {
    # External API call to check file existence
    tryCatch({
      encoded_billDir <- URLencode(billDir)
      response <- httr::GET(paste0('203.135.63.47:8000/listFiles?folderPath=', encoded_billDir))
      if (status_code(response) == 200) {
        file_list <- content(response)
        billSummaryCheck <- basename(billSummaryFilename) %in% file_list[[1]]
      }
    }, error = function(e) {
      billSummaryCheck <- FALSE
    })
  }
  
  # Load data based on file existence
  billSummary <- NULL
  billHistory <- NULL
  
  if (billSummaryCheck) {
    # Current month files exist
    if (RUN_MODE == 'DEP') {
      billSummary <- read.csv(billSummaryFilename, check.names = FALSE)
      billHistory <- read.csv(billHistoryFilename, check.names = FALSE)
    } else {
      # External API call to get bill data
      tryCatch({
        response <- httr::GET(paste0('203.135.63.47:8000/bill?username=', selectedBranch))
        if (status_code(response) == 200) {
          billData <- content(response)
          billHistory <- api_list2df(billData[[2]])
          billSummary <- api_list2df(billData[[3]])
        }
      }, error = function(e) {
        # Fall back to sample data
        billSummary <- read.csv('sample_billing_summary.csv', check.names = FALSE)
        billHistory <- read.csv('sample_billing_history.csv', check.names = FALSE)
      })
    }
  } else {
    # Current month files don't exist, try previous months
    months_to_try <- c(1, 2)  # Try 1 month back, then 2 months back
    
    for (months_back in months_to_try) {
      billDate_back <- Sys.Date() - months(months_back)
      billMonth_back <- paste(str_to_upper(lubridate::month(billDate_back, label = TRUE)), 
                             substr(year(billDate_back), 3, 4), sep = '_')
      
      billSummaryFilename_back <- paste0(billDir, '/', selectedBranch, '_', billMonth_back, '_billing_summary.csv')
      billHistoryFilename_back <- paste0(billDir, '/', selectedBranch, '_', billMonth_back, '_billing_history.csv')
      
      if (RUN_MODE == 'DEP' && file.exists(billSummaryFilename_back)) {
        billSummary <- read.csv(billSummaryFilename_back, check.names = FALSE)
        billHistory <- read.csv(billHistoryFilename_back, check.names = FALSE)
        break
      }
    }
    
    # If still no data found, use sample data
    if (is.null(billSummary)) {
      if (RUN_MODE == 'DEP') {
        billSummary <- read.csv('../Clients Usage Data/Electricity Bills/sample_billing_summary.csv', check.names = FALSE)
        billHistory <- read.csv('../Clients Usage Data/Electricity Bills/sample_billing_history.csv', check.names = FALSE)
      } else {
        billSummary <- read.csv('sample_billing_summary.csv', check.names = FALSE)
        billHistory <- read.csv('sample_billing_history.csv', check.names = FALSE)
      }
    }
  }
  
  # Process bill history data
  if (!is.null(billHistory)) {
    colnames(billHistory) <- str_to_upper(colnames(billHistory))
    billHistory$MONTH <- as.factor(billHistory$MONTH)
  }
  
  return(list(
    billSummary = billSummary,
    billHistory = billHistory,
    success = TRUE
  ))
}

# Function to create sparkline plots (as JSON for frontend rendering)
create_sparkline_data <- function(billHistory, metric = "BILL") {
  if (is.null(billHistory) || nrow(billHistory) == 0) {
    return(list(x = c(), y = c()))
  }
  
  # Return data for frontend plotting
  return(list(
    x = as.character(billHistory$MONTH),
    y = as.numeric(billHistory[[metric]]),
    type = "line",
    fill = "tozeroy",
    opacity = 0.2,
    line = list(color = "white")
  ))
}

# ============================================================================
# HISTORIC FILE LOAD API FUNCTIONS
# ============================================================================

# Helper Functions (preserved from original)
getNodesAtLevel <- function(tree, level) {
  if (level == 0) {
    if (!is.null(tree$Name)) return(tree$Name)
    else return(NA)
  } else {
    nodes <- list()
    for (node in tree) {
      if (is.list(node)) {
        nodes <- c(nodes, getNodesAtLevel(node, level - 1))
      }
    }
    return(unlist(nodes))
  }
}

col_postfix <- function(colname, sub){
  colname <- paste0(colname, sub)
}

# Original historic_file_load function (preserved exactly)
historic_file_load <- function(start_date, end_date, dir, meterType, buildingMap) {
  if (RUN_MODE == 'DEP') historic_files = list.files(path = dir)
  else {
    encodedPath <- URLencode(dir)
    historic_files <- httr::GET(paste0('203.135.63.47:8000/listFiles?folderPath=', encodedPath)) %>% content()
    historic_files <- unlist(historic_files[[1]])
    # DONE
  }
  # Append the directory path to each file name to create full file paths
  historic_files = paste0(dir, historic_files)
  
  # Function to extract the date from a file name for sorting purposes
  extract_date <- function(file_path) {
    base_name <- basename(file_path)
    parts <- strsplit(base_name, "_")[[1]]
    date_string <- parts[2]
    as.Date(date_string, format="%Y-%m-%d")
  }
  
  # Extract dates from file names and sort files chronologically
  dates <- sapply(historic_files, extract_date)
  historic_files <- historic_files[order(dates)]
  
  # Extract dates from file names for comparison with input dates
  historic_dates = sapply(strsplit(basename(historic_files), '_', fixed = T), `[`, 2)
  
  # Convert input dates to Date objects for comparison
  start <- as.Date(start_date)
  end <- as.Date(end_date)
  # Calculate the difference in days between start and end dates
  day_diff <- abs(difftime(start, end, units = "days"))
  
  # Initialize an empty data frame to store the combined data
  retData <- data.frame()
  # Initialize empty strings for column categories
  powerColumns <- ''
  voltageColumns <- ''
  currentColumns <- ''
  powerfactorColumns <- ''
  frequencyColumns <- ''
  energyPColumns <- ''   
  energyNColumns <- ''
  # Wrap the main logic in a tryCatch block to handle errors gracefully
  tryCatch({
    # Ensure end_date does not exceed the latest available date in the files
    end_date = min(end_date, historic_dates[length(historic_dates)])
    # If start_date is after end_date, adjust start_date to maintain the date range
    if (start_date > end_date) {
      start_date <- as.character(as.Date(end_date) - day_diff)
    }
    # Ensure start_date is within the range of available dates
    start_date = min(start_date, historic_dates[length(historic_dates)])
    start_date = max(start_date, historic_dates[1])
    
    # If start_date is not exactly in historic_dates, find the closest available date after it
    if (!start_date %in% historic_dates) {
      start_date_temp <- as.Date(start_date)
      historic_dates_temp <- as.Date(historic_dates)
      closest_date <- historic_dates_temp[which(historic_dates_temp > start_date_temp)[1]] %>% as.character()
      start_date <- closest_date %>% as.character()
    }
    # If end_date is not exactly in historic_dates, find the closest available date
    if (!end_date %in% historic_dates) {
      end_date_temp <- as.Date(end_date)
      historic_dates_temp <- as.Date(historic_dates)
      closest_date <- historic_dates_temp[which.min(abs(historic_dates_temp - end_date_temp))]
      end_date <- closest_date %>% as.character()
    }
    
    # Recompute start_date if it exceeds end_date after adjustments
    if (start_date > end_date) {
      start_date <- as.character(as.Date(end_date) - day_diff)
    }
    
    # Load data based on the run mode (DEP for local files, else API)
    if (RUN_MODE == 'DEP') {
      # Read and combine CSV files within the date range
      retData <- do.call(
        rbind.fill,
        lapply(
          historic_files[match(start_date, historic_dates) : match(end_date, historic_dates)],
          function(x) fread(x, check.names = FALSE)
        )
      )
    } else {
      retData <- httr::GET(paste0('203.135.63.47:8000/serverData?fileDir=', encodedPath, '&start_date=', start_date, '&end_date=', end_date)) %>% content()
      retData <- api_list2df(retData)
      
      for (temp_col in 2:ncol(retData)) {
        retData[[temp_col]] <- as.numeric(retData[[temp_col]])
      }
      ## DONE
    }
    
    # Specific adjustments for the 'nauman' directory
    if (basename(dir) == 'nauman') {
      # Calculate import as the absolute value of negative Usage
      retData$import <- ifelse(retData$`Usage [kW]` < 0, abs(retData$`Usage [kW]`), 0)
      # Calculate export as the absolute value of positive Usage
      retData$export <- ifelse(retData$`Usage [kW]` > 0, abs(retData$`Usage [kW]`), 0)
      # Recalculate Usage as import + Solar Generation - export
      retData$`Usage [kW]` <- retData$import + retData$`Solar Generation [kW]` - retData$export
      # Remove the temporary import and export columns
      retData <- retData[, 1:(ncol(retData)-2)]
    }
    
    # Convert the Date & Time column to POSIXct format for proper time handling
    retData$`Date & Time` = as.POSIXct(retData$`Date & Time`)
    # retData$`Date & Time` = as.POSIXct(retData$`Date & Time`, tz = "Asia/Karachi")
    # Sort the data by DateTime to ensure chronological order
    retData <<- retData[order(retData$`Date & Time`),]
    
    # Rename the first column to 'DateTime' for consistency
    colnames(retData)[1] = 'DateTime'
    
    # Determine the level in the buildingMap where usage columns are defined
    curr_level = 0
    usage_columns <- NULL
    usage_columns_kWh <- NULL
    while(T) {
      usage_columns <- getNodesAtLevel(buildingMap, curr_level)
      if (any(is.na(usage_columns))) curr_level = curr_level + 1
      else break
    }
    
    # Define a column name for filtering
    colname <- "Inverter Output_[kW]"
    # Specific filtering for the 'kamranniazi' directory
    if (basename(dir) == 'kamranniazi') {
      # Set Inverter Output values below 50 to 0
      retData <- retData %>%
        mutate_at(vars(colname), ~ifelse(. < 50, 0, .))
    }
    
    # Handle Neubolt meter type data
    if (meterType == 'neubolt') {
      # Append '_[kW]' to usage column names for Neubolt meters
      usage_columns_kWh <- unlist(lapply(usage_columns, col_postfix, '_[EP]'))
      
      usage_columns <- unlist(lapply(usage_columns, col_postfix, '_[kW]'))
      
      # Identify columns containing power, voltage, current, and power factor data
      powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
      voltageColumns = grep('[V]', colnames(retData), value = T, fixed = T)
      currentColumns = grep('[A]', colnames(retData), value = T, fixed = T)
      powerfactorColumns = grep('[PF]', colnames(retData), value = T, fixed = T)
      frequencyColumns = grep('[F]', colnames(retData), value = T, fixed = T)
      energyPColumns = grep('[EP]', colnames(retData), value = T, fixed = T)
      energyNColumns = grep('[EN]', colnames(retData), value = T, fixed = T)
      
      # Convert power columns from watts to kilowatts by dividing by 1000
      retData[, powerColumns] = retData[, powerColumns]/1000
    } else {
      # For non-Neubolt meters, ensure all numeric columns are positive
      retData[,2:ncol(retData)] = abs(retData[,2:ncol(retData)])
      # Extract meterID from the latest file name for use in file paths
      meterID = strsplit(historic_files[length(historic_files)], '/', fixed = T)[[1]]
      meterID = meterID[length(meterID)]
      meterID = strsplit(meterID, '_', fixed = T)[[1]][1]
      file_present = F
      # Check if a column names mapping file exists for the meter
      if (RUN_MODE == 'DEP') file_present <- file.exists(paste0('../EnergyMonitor2.0/Egauge Column Names/', meterID, "_New_column_names.csv"))
      else {
        ## New API for egauge 
      }
      # If the mapping file exists, apply the new column names
      if (file_present) {
        updateColNames <- read.csv(paste0('../EnergyMonitor2.0/Egauge Column Names/', meterID, "_New_column_names.csv"))
        # Loop through each row in the mapping file
        for (row in 1:nrow(updateColNames)) {
          # If the old name exists in retData, rename it to the new name
          if (updateColNames$Old_name[row] %in% colnames(retData)) {
            colnames(retData)[which(colnames(retData) == updateColNames$Old_name[row])] <- updateColNames$New_name[row]
            next
          }
          # If the old name contains a '+', evaluate it as an expression to create a new column
          if (grepl('\\+', updateColNames$Old_name[row])) {
            expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", updateColNames$Old_name[row])
            expression_str_fixed <- paste0('`', expression_str_fixed, '`')
            retData <- retData %>%
              mutate(!!updateColNames$New_name[row] := eval(parse(text = expression_str_fixed)))
          }
        }
        # Remove columns that are neither in Old_name nor New_name from the mapping
        retData_cols <- colnames(retData)
        for (cols in 2:length(retData_cols)) 
          if (!retData_cols[cols] %in% updateColNames$Old_name & !retData_cols[cols] %in% updateColNames$New_name) retData <- retData[, -c(which(colnames(retData) == retData_cols[cols]))]
      } 
      # Append ' [kW]' to usage columns for non-Neubolt meters
      usage_columns <- unlist(lapply(usage_columns, col_postfix, ' [kW]'))
      
      # Ensure a Usage column exists, initialize with NA if not present
      if (!"Usage" %in% colnames(retData)) retData$Usage <- NA
      # Move the Usage column to be the second column (after DateTime)
      retData = retData %>% relocate('Usage', .after = 'DateTime')
      
      # Remove 'Usage [kW]' column if it exists
      if ("Usage [kW]" %in% colnames(retData)) retData <- retData[, -c(which(colnames(retData) == "Usage [kW]"))]
      
      # Update column lists for non-Neubolt meters
      powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
      voltageColumns = grep('[V]', colnames(retData), value = T, fixed = T)
      currentColumns = grep('[A]', colnames(retData), value = T, fixed = T)
      powerfactorColumns = grep('[PF]', colnames(retData), value = T, fixed = T)
      frequencyColumns = grep('[F]', colnames(retData), value = T, fixed = T)
      
      # Adjust non-phase voltage columns by multiplying by sqrt(3) if corresponding phase columns exist
      nonPhaseVolts <- voltageColumns[!grepl('phase', voltageColumns)]
      if(length(nonPhaseVolts) > 0) {
        for (volt_col in 1:length(nonPhaseVolts)) {
          temp <- nonPhaseVolts[volt_col]
          temp_short <- strsplit(temp, ' \\[V\\]')[[1]][1]
          if (length(grep(paste0(temp_short, ' phase'), colnames(retData))) > 0)
            retData[[temp]] <- sqrt(3) * retData[[temp]]
        }
      }
    }
    
    # Ensure a Usage column exists for all meter types, initialize with NA if not present
    if (!"Usage" %in% colnames(retData)) retData$Usage <- NA
    if (!"Usage_kWh" %in% colnames(retData)) retData$Usage_kWh <- NA
    # Move the Usage column to be the second column (after DateTime)
    retData = retData %>% relocate('Usage', .after = 'DateTime')
    
    # Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
    for (col in 2:ncol(retData)) {
      retData[[col]] <- na.locf(retData[[col]], na.rm = F, maxgap = 5)
    }
    if (length(energyPColumns) > 0) {
      for (col in energyPColumns) {
        power_col <- gsub("\\[EP\\]", "[kW]", col)   # match the kW column
        if (power_col %in% colnames(retData)) {
          retData[[col]][is.na(retData[[col]])] <- retData[[power_col]][is.na(retData[[col]])]
        }
      }
    }

    if (length(energyNColumns) > 0) {
      for (col in energyNColumns) {
        power_col <- gsub("\\[EN\\]", "[kW]", col)
        if (power_col %in% colnames(retData)) {
          retData[[col]][is.na(retData[[col]])] <- retData[[power_col]][is.na(retData[[col]])]
        }
      }
    }
    client_profiles <- read.csv('../Clients Usage Data/client_profiles.csv', check.names = F)
    meterID <- client_profiles$Meter_id[client_profiles$user == basename(dir)]
    
    # New logic for Virtual Register calculations with fix for operator handling
    # Construct the file path for the Virtual Register CSV using the meterID
    vr_file <- paste0('/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/', meterID, '_VirtualRegister.csv')
    # Check if the Virtual Register file exists on the server
    vr_present <- file.exists(vr_file)
    
    # Proceed only if the Virtual Register file exists
    if (vr_present) {
      # Load the Virtual Register file into a data frame, ensuring strings are not converted to factors
      vr_data <- read.csv(vr_file, stringsAsFactors = FALSE)
      
      # Check if the Virtual Register file has any rows to process
      if (nrow(vr_data) > 0) {
        # Loop through each row in the Virtual Register file
        for (i in 1:nrow(vr_data)) {
          # Extract the Virtual Register name (e.g., "Mani") from the current row
          vr_name <- vr_data$Name[i]
          # Extract the formula (e.g., "-Generator phase 1 + Main") and remove leading/trailing whitespace
          original_formula <- trimws(vr_data$Formula[i])
          
          # Create a list to store meter names with hyphens and their replacements
          meter_names_with_hyphens <- list()
          
          # Identify potential meter names with hyphens
          # This pattern looks for words with hyphens that aren't isolated +/- operators
          meter_pattern <- "([A-Za-z0-9]+(-[A-Za-z0-9 ]+)+)"
          meter_matches <- gregexpr(meter_pattern, original_formula, perl = TRUE)
          meter_names <- unlist(regmatches(original_formula, meter_matches))
          
          # Create a working copy of the formula
          formula <- original_formula
          
          # Replace hyphens in meter names with a special character (|)
          for (meter_name in meter_names) {
            if (grepl("-", meter_name)) {
              replacement <- gsub("-", "|", meter_name)
              meter_names_with_hyphens[[meter_name]] <- replacement
              formula <- gsub(meter_name, replacement, formula, fixed = TRUE)
            }
          }
          
          # Split the formula into components (terms and operators) using a regex
          # The pattern splits around + or - operators, preserving them with their terms
          components <- unlist(strsplit(formula, "(?<=\\S)\\s*(?=[+-])", perl = TRUE))
          
          # Process Power [kW]: Compute the new column by applying the formula directly
          # Start with an empty formula, which we'll rebuild
          power_formula <- ""
          # Loop through each component of the formula
          for (j in seq_along(components)) {
            comp <- trimws(components[j])
            # Skip if the component is just an operator (+ or -), but add it to the formula
            if (comp %in% c("+", "-")) {
              # Add the operator to the formula with a space
              power_formula <- paste0(power_formula, " ", comp, " ")
              next
            }
            # Extract the operator (if any) and the term
            operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
            # Remove any leading + or - and all following whitespace from the component
            comp_clean <- sub("^[+-]\\s*", "", comp)
            
            # CHANGE #1: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            # Construct the column name by adding the [kW] suffix (e.g., "Generator phase 1_[kW]")
            power_col <- paste0(comp_clean, "_[kW]")
            # If the column doesn't exist in retData, add it with NA values to avoid errors
            if (!power_col %in% colnames(retData)) {
              retData[[power_col]] <- NA
            }
            # Add the operator (if any) and the column name (wrapped in backticks) to the formula
            power_formula <- paste0(power_formula, operator, "`", power_col, "`")
          }
          # Trim any extra spaces from the final formula
          power_formula <- trimws(power_formula)
          # Evaluate the modified formula (e.g., +`Building 1_[kW]` + `Main_[kW]`) row-wise
          # Store the result in a temporary column named "temp"
          retData <- retData %>%
            mutate(temp = eval(parse(text = power_formula)))
          # Rename the "temp" column to the desired Virtual Register name (e.g., "Mani_[kW]")
          colnames(retData)[colnames(retData) == "temp"] <- paste0(vr_name, "_[kW]")
          
          # Process Current [A]: Similar to Power, compute the new column by applying the formula
          current_formula <- ""
          for (j in seq_along(components)) {
            comp <- trimws(components[j])
            if (comp %in% c("+", "-")) {
              current_formula <- paste0(current_formula, " ", comp, " ")
              next
            }
            operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
            comp_clean <- sub("^[+-]\\s*", "", comp)  # Remove leading + or - and whitespace
            
            # CHANGE #2: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            current_col <- paste0(comp_clean, "_[A]")  # Construct column name with [A] suffix
            if (!current_col %in% colnames(retData)) {
              retData[[current_col]] <- NA  # Add column with NA if it doesn't exist
            }
            current_formula <- paste0(current_formula, operator, "`", current_col, "`")
          }
          current_formula <- trimws(current_formula)
          retData <- retData %>%
            mutate(temp = eval(parse(text = current_formula)))  # Evaluate formula for Current
          colnames(retData)[colnames(retData) == "temp"] <- paste0(vr_name, "_[A]")  # Rename to "Mani_[A]"
          
          # Process Voltage [V]: Compute as the average of the components' voltage values
          voltage_formula <- formula
          # Initialize a vector to store the voltage column names
          voltage_cols <- c()
          for (comp in components) {
            comp <- trimws(comp)
            if (comp %in% c("+", "-")) next
            comp_clean <- sub("^[+-]\\s*", "", comp)  # Remove leading + or - and whitespace
            
            # CHANGE #3: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            voltage_col <- paste0(comp_clean, "_[V]")  # Construct column name with [V] suffix
            if (!voltage_col %in% colnames(retData)) {
              retData[[voltage_col]] <- NA  # Add column with NA if it doesn't exist
            }
            # Collect the voltage column name for averaging
            voltage_cols <- c(voltage_cols, voltage_col)
          }
          # If there are voltage columns to average, compute the row-wise mean
          if (length(voltage_cols) > 0) {
            retData[[paste0(vr_name, "_[V]")]] <- rowMeans(retData[, voltage_cols, drop = FALSE], na.rm = TRUE)
          } else {
            # If no columns to average, set the new column to NA
            retData[[paste0(vr_name, "_[V]")]] <- NA
          }
          
          # Process Power Factor [PF]: Compute as the average of the components' power factor values
          pf_formula <- formula
          # Initialize a vector to store the power factor column names
          pf_cols <- c()
          for (comp in components) {
            comp <- trimws(comp)
            if (comp %in% c("+", "-")) next
            comp_clean <- sub("^[+-]\\s*", "", comp)  # Remove leading + or - and whitespace
            
            # CHANGE #4: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            pf_col <- paste0(comp_clean, "_[PF]")  # Construct column name with [PF] suffix
            if (!pf_col %in% colnames(retData)) {
              retData[[pf_col]] <- NA  # Add column with NA if it doesn't exist
            }
            # Collect the power factor column name for averaging
            pf_cols <- c(pf_cols, pf_col)
          }
          # If there are power factor columns to average, compute the row-wise mean
          if (length(pf_cols) > 0) {
            retData[[paste0(vr_name, "_[PF]")]] <- rowMeans(retData[, pf_cols, drop = FALSE], na.rm = TRUE)
          } else {
            # If no columns to average, set the new column to NA
            retData[[paste0(vr_name, "_[PF]")]] <- NA
          }
          
          # Process Frequency [F]: Compute as the average of the components' frequency values
          frequency_formula <- formula
          # Initialize a vector to store the frequency column names
          frequency_cols <- c()
          for (comp in components) {
            comp <- trimws(comp)
            if (comp %in% c("+", "-")) next
            comp_clean <- sub("^[+-]\\s*", "", comp)  # Remove leading + or - and whitespace
            
            # Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            frequency_col <- paste0(comp_clean, "_[F]")  # Construct column name with [F] suffix
            if (!frequency_col %in% colnames(retData)) {
              retData[[frequency_col]] <- NA  # Add column with NA if it doesn't exist
            }
            # Collect the frequency column name for averaging
            frequency_cols <- c(frequency_cols, frequency_col)
          }
          # If there are frequency columns to average, compute the row-wise mean
          if (length(frequency_cols) > 0) {
            retData[[paste0(vr_name, "_[F]")]] <- rowMeans(retData[, frequency_cols, drop = FALSE], na.rm = TRUE)
          } else {
            # If no columns to average, set the new column to NA
            retData[[paste0(vr_name, "_[F]")]] <- NA
          }
          
          # Process Energy [EP]: Compute the new column by applying the formula directly 
          energyP_formula <- ""
          # Loop through each component of the formula
          for (j in seq_along(components)) {
            comp <- trimws(components[j])
            # Skip if the component is just an operator (+ or -), but add it to the formula
            if (comp %in% c("+", "-")) {
              # Add the operator to the formula with a space
              energyP_formula <- paste0(energyP_formula, " ", comp, " ")
              next
            }
            # Extract the operator (if any) and the term
            operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
            # Remove any leading + or - and all following whitespace from the component
            comp_clean <- sub("^[+-]\\s*", "", comp)
            
            # CHANGE #1: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            # Construct the column name by adding the [kW] suffix (e.g., "Generator phase 1_[kW]")
            energyP_col <- paste0(comp_clean, "_[EP]")
            # If the column doesn't exist in retData, add it with NA values to avoid errors
            if (!energyP_col %in% colnames(retData)) {
              retData[[energyP_col]] <- NA
            }
            # Add the operator (if any) and the column name (wrapped in backticks) to the formula
            energyP_formula <- paste0(energyP_formula, operator, "`", energyP_col, "`")
          }
          # Trim any extra spaces from the final formula
          energyP_formula <- trimws(energyP_formula)
          # Evaluate the modified formula (e.g., +`Building 1_[EP]` + `Main_[EP]`) row-wise
          retData <- retData %>%
            mutate(temp = eval(parse(text = energyP_formula)))
          # Rename the "temp" column to the desired Virtual Register name (e.g., "Mani_[EP]")
          colnames(retData)[colnames(retData) == "temp"] <- paste0(vr_name, "_[EP]")
          
          
          # Process Energy [EN]: Compute the new column by applying the formula directly 
          energyN_formula <- ""
          # Loop through each component of the formula
          for (j in seq_along(components)) {
            comp <- trimws(components[j])
            # Skip if the component is just an operator (+ or -), but add it to the formula
            if (comp %in% c("+", "-")) {
              # Add the operator to the formula with a space
              energyN_formula <- paste0(energyN_formula, " ", comp, " ")
              next
            }
            # Extract the operator (if any) and the term
            operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
            # Remove any leading + or - and all following whitespace from the component
            comp_clean <- sub("^[+-]\\s*", "", comp)
            
            # CHANGE #1: Directly replace all pipe characters with hyphens
            # This ensures all meter names are restored correctly
            comp_clean <- gsub("\\|", "-", comp_clean)
            
            # Construct the column name by adding the [kW] suffix (e.g., "Generator phase 1_[kW]")
            energyN_col <- paste0(comp_clean, "_[EN]")
            # If the column doesn't exist in retData, add it with NA values to avoid errors
            if (!energyN_col %in% colnames(retData)) {
              retData[[energyN_col]] <- NA
            }
            # Add the operator (if any) and the column name (wrapped in backticks) to the formula
            energyN_formula <- paste0(energyN_formula, operator, "`", energyN_col, "`")
          }
          # Trim any extra spaces from the final formula
          energyN_formula <- trimws(energyN_formula)
          # Evaluate the modified formula (e.g., +`Building 1_[EN]` + `Main_[EN]`) row-wise
          retData <- retData %>%
            mutate(temp = eval(parse(text = energyN_formula)))
          # Rename the "temp" column to the desired Virtual Register name (e.g., "Mani_[EP]")
          colnames(retData)[colnames(retData) == "temp"] <- paste0(vr_name, "_[EN]")
          
          
        }
      }
    }
    # Filter usage columns to only those present in retData
    usage_columns <- intersect(usage_columns, colnames(retData))
    usage_columns_kWh <- intersect(usage_columns_kWh, colnames(retData))
    
    # Compute Usage as the sum of usage columns if multiple exist, else use the single column
    if (meterType == "neubolt") 
    {
      if (length(usage_columns_kWh) > 1) {
        retData$Usage_kWh = rowSums(retData[, usage_columns_kWh], na.rm = T)
      } else retData$Usage_kWh = retData[, usage_columns_kWh]
    }
    if (length(usage_columns) > 1) {
      retData$Usage = rowSums(retData[, usage_columns], na.rm = T)
    } else retData$Usage = retData[, usage_columns]
    
    # Remove 'Usage [kW]' column if it exists (redundant after computing Usage)
    if ("Usage [kW]" %in% colnames(retData)) retData <- retData[, -c(which(colnames(retData) == "Usage [kW]"))]
    
    # Specific column renaming for the 'demo1' directory
    # if (basename(dir) == 'demo1') {
    #   # Vehicle Chiller Charger 1 to Motor 1 (16 columns: V, A, kW, PF, and phase-specific)
    #   curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 1_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 1_[V]', 'Motor 1_[A]', 'Motor 1_[kW]', 'Motor 1_[PF]',
    #                                                  'Motor 1 phase 1_[V]', 'Motor 1 phase 1_[A]', 'Motor 1 phase 1_[kW]', 'Motor 1 phase 1_[PF]',
    #                                                  'Motor 1 phase 2_[V]', 'Motor 1 phase 2_[A]', 'Motor 1 phase 2_[kW]', 'Motor 1 phase 2_[PF]',
    #                                                  'Motor 1 phase 3_[V]', 'Motor 1 phase 3_[A]', 'Motor 1 phase 3_[kW]', 'Motor 1 phase 3_[PF]')
    #   # Vehicle Chiller Charger 2 to Motor 2
    #   curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 2_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 2_[V]', 'Motor 2_[A]', 'Motor 2_[kW]', 'Motor 2_[PF]',
    #                                                  'Motor 2 phase 1_[V]', 'Motor 2 phase 1_[A]', 'Motor 2 phase 1_[kW]', 'Motor 2 phase 1_[PF]',
    #                                                  'Motor 2 phase 2_[V]', 'Motor 2 phase 2_[A]', 'Motor 2 phase 2_[kW]', 'Motor 2 phase 2_[PF]',
    #                                                  'Motor 2 phase 3_[V]', 'Motor 2 phase 3_[A]', 'Motor 2 phase 3_[kW]', 'Motor 2 phase 3_[PF]')
    #   # Vehicle Chiller Charger 3 to Motor 3
    #   curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 3_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 3_[V]', 'Motor 3_[A]', 'Motor 3_[kW]', 'Motor 3_[PF]',
    #                                                  'Motor 3 phase 1_[V]', 'Motor 3 phase 1_[A]', 'Motor 3 phase 1_[kW]', 'Motor 3 phase 1_[PF]',
    #                                                  'Motor 3 phase 2_[V]', 'Motor 3 phase 2_[A]', 'Motor 3 phase 2_[kW]', 'Motor 3 phase 2_[PF]',
    #                                                  'Motor 3 phase 3_[V]', 'Motor 3 phase 3_[A]', 'Motor 3 phase 3_[kW]', 'Motor 3 phase 3_[PF]')
    #   # Vehicle Chiller Charger 4 to Motor 4
    #   curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 4_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 4_[V]', 'Motor 4_[A]', 'Motor 4_[kW]', 'Motor 4_[PF]',
    #                                                  'Motor 4 phase 1_[V]', 'Motor 4 phase 1_[A]', 'Motor 4 phase 1_[kW]', 'Motor 4 phase 1_[PF]',
    #                                                  'Motor 4 phase 2_[V]', 'Motor 4 phase 2_[A]', 'Motor 4 phase 2_[kW]', 'Motor 4 phase 2_[PF]',
    #                                                  'Motor 4 phase 3_[V]', 'Motor 4 phase 3_[A]', 'Motor 4 phase 3_[kW]', 'Motor 4 phase 3_[PF]')
    #   # Vehicle Chiller Charger 5 to Motor 5 (Note: typo in original code 'Motor 52 phase 2_[V]')
    #   curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 5_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 5_[V]', 'Motor 5_[A]', 'Motor 5_[kW]', 'Motor 5_[PF]',
    #                                                  'Motor 5 phase 1_[V]', 'Motor 5 phase 1_[A]', 'Motor 5 phase 1_[kW]', 'Motor 5 phase 1_[PF]',
    #                                                  'Motor 52 phase 2_[V]', 'Motor 5 phase 2_[A]', 'Motor 5 phase 2_[kW]', 'Motor 5 phase 2_[PF]',
    #                                                  'Motor 5 phase 3_[V]', 'Motor 5 phase 3_[A]', 'Motor 5 phase 3_[kW]', 'Motor 5 phase 3_[PF]')
    #   # Crusher Machine 1 to CNC Machine 1
    #   curr_i <- which(colnames(retData) == 'Crusher Machine 1_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('CNC Machine 1_[V]', 'CNC Machine 1_[A]', 'CNC Machine 1_[kW]', 'CNC Machine 1_[PF]',
    #                                                  'CNC Machine 1 phase 1_[V]', 'CNC Machine 1 phase 1_[A]', 'CNC Machine 1 phase 1_[kW]', 'CNC Machine 1 phase 1_[PF]',
    #                                                  'CNC Machine 1 phase 2_[V]', 'CNC Machine 1 phase 2_[A]', 'CNC Machine 1 phase 2_[kW]', 'CNC Machine 1 phase 2_[PF]',
    #                                                  'CNC Machine 1 phase 3_[V]', 'CNC Machine 1 phase 3_[A]', 'CNC Machine 1 phase 3_[kW]', 'CNC Machine 1 phase 3_[PF]')
    #   # Crusher Machine 2 to CNC Machine 2
    #   curr_i <- which(colnames(retData) == 'Crusher Machine 2_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('CNC Machine 2_[V]', 'CNC Machine 2_[A]', 'CNC Machine 2_[kW]', 'CNC Machine 2_[PF]',
    #                                                  'CNC Machine 2 phase 1_[V]', 'CNC Machine 2 phase 1_[A]', 'CNC Machine 2 phase 1_[kW]', 'CNC Machine 2 phase 1_[PF]',
    #                                                  'CNC Machine 2 phase 2_[V]', 'CNC Machine 2 phase 2_[A]', 'CNC Machine 2 phase 2_[kW]', 'CNC Machine 2 phase 2_[PF]',
    #                                                  'CNC Machine 2 phase 3_[V]', 'CNC Machine 2 phase 3_[A]', 'CNC Machine 2 phase 3_[kW]', 'CNC Machine 2 phase 3_[PF]')
    #   # Crusher Machine 3 to CNC Machine 3
    #   curr_i <- which(colnames(retData) == 'Crusher Machine 3_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('CNC Machine 3_[V]', 'CNC Machine 3_[A]', 'CNC Machine 3_[kW]', 'CNC Machine 3_[PF]',
    #                                                  'CNC Machine 3 phase 1_[V]', 'CNC Machine 3 phase 1_[A]', 'CNC Machine 3 phase 1_[kW]', 'CNC Machine 3 phase 1_[PF]',
    #                                                  'CNC Machine 3 phase 2_[V]', 'CNC Machine 3 phase 2_[A]', 'CNC Machine 3 phase 2_[kW]', 'CNC Machine 3 phase 2_[PF]',
    #                                                  'CNC Machine 3 phase 3_[V]', 'CNC Machine 3 phase 3_[A]', 'CNC Machine 3 phase 3_[kW]', 'CNC Machine 3 phase 3_[PF]')
    #   # Icing Building Generator to Main Building Generator
    #   curr_i <- which(colnames(retData) == 'Icing Building Generator_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Main Building Generator_[V]', 'Main Building Generator_[A]', 'Main Building Generator_[kW]', 'Main Building Generator_[PF]',
    #                                                  'Main Building Generator phase 1_[V]', 'Main Building Generator phase 1_[A]', 'Main Building Generator phase 1_[kW]', 'Main Building Generator phase 1_[PF]',
    #                                                  'Main Building Generator phase 2_[V]', 'Main Building Generator phase 2_[A]', 'Main Building Generator phase 2_[kW]', 'Main Building Generator phase 2_[PF]',
    #                                                  'Main Building Generator phase 3_[V]', 'Main Building Generator phase 3_[A]', 'Main Building Generator phase 3_[kW]', 'Main Building Generator phase 3_[PF]')
    #   # Icing Building Main to Main Building
    #   curr_i <- which(colnames(retData) == 'Icing Building Main_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 3)] <- c('Main Building_[V]', 'Main Building_[A]', 'Main Building_[kW]', 'Main Building_[PF]')
    #   # Cross Section Mixing Machine to Motor 6
    #   curr_i <- which(colnames(retData) == 'Cross Section Mixing Machine_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 15)] <- c('Motor 6_[V]', 'Motor 6_[A]', 'Motor 6_[kW]', 'Motor 6_[PF]',
    #                                                  'Motor 6 phase 1_[V]', 'Motor 6 phase 1_[A]', 'Motor 6 phase 1_[kW]', 'Motor 6 phase 1_[PF]',
    #                                                  'Motor 6 phase 2_[V]', 'Motor 6 phase 2_[A]', 'Motor 6 phase 2_[kW]', 'Motor 6 phase 2_[PF]',
    #                                                  'Motor 6 phase 3_[V]', 'Motor 6 phase 3_[A]', 'Motor 6 phase 3_[kW]', 'Motor 6 phase 3_[PF]')
    #   # AC Raw Storage to Motor 7
    #   curr_i <- which(colnames(retData) == 'AC Raw Storage_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 3)] <- c('Motor 7_[V]', 'Motor 7_[A]', 'Motor 7_[kW]', 'Motor 7_[PF]')
    #   
    #   # Baking Area Lights to Motor 8
    #   curr_i <- which(colnames(retData) == 'Baking Area Lights_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 3)] <- c('Motor 8_[V]', 'Motor 8_[A]', 'Motor 8_[kW]', 'Motor 8_[PF]')
    #   
    #   # Cold Store AC 2 to Motor 9
    #   curr_i <- which(colnames(retData) == 'Cold Store AC 2_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 3)] <- c('Motor 9_[V]', 'Motor 9_[A]', 'Motor 9_[kW]', 'Motor 9_[PF]')
    #   
    #   # Cold Store AC 1 to Motor 10
    #   curr_i <- which(colnames(retData) == 'Cold Store AC 1_[V]')
    #   if (length(curr_i) > 0)
    #     colnames(retData)[curr_i:(curr_i + 3)] <- c('Motor 10_[V]', 'Motor 10_[A]', 'Motor 10_[kW]', 'Motor 10_[PF]')
    # }
    # Vehicle Chiller Charger 1 to Motor 1 (20 columns: V, A, kW, PF, F, and phase-specific)
    if (basename(dir) == 'demo1') {
      
      curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 1_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 1_[V]', 'Motor 1_[A]', 'Motor 1_[kW]', 'Motor 1_[PF]', 'Motor 1_[F]',
                                                     'Motor 1 phase 1_[V]', 'Motor 1 phase 1_[A]', 'Motor 1 phase 1_[kW]', 'Motor 1 phase 1_[PF]', 'Motor 1 phase 1_[F]',
                                                     'Motor 1 phase 2_[V]', 'Motor 1 phase 2_[A]', 'Motor 1 phase 2_[kW]', 'Motor 1 phase 2_[PF]', 'Motor 1 phase 2_[F]',
                                                     'Motor 1 phase 3_[V]', 'Motor 1 phase 3_[A]', 'Motor 1 phase 3_[kW]', 'Motor 1 phase 3_[PF]', 'Motor 1 phase 3_[F]')
      # Vehicle Chiller Charger 2 to Motor 2
      curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 2_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 2_[V]', 'Motor 2_[A]', 'Motor 2_[kW]', 'Motor 2_[PF]', 'Motor 2_[F]',
                                                     'Motor 2 phase 1_[V]', 'Motor 2 phase 1_[A]', 'Motor 2 phase 1_[kW]', 'Motor 2 phase 1_[PF]', 'Motor 2 phase 1_[F]',
                                                     'Motor 2 phase 2_[V]', 'Motor 2 phase 2_[A]', 'Motor 2 phase 2_[kW]', 'Motor 2 phase 2_[PF]', 'Motor 2 phase 2_[F]',
                                                     'Motor 2 phase 3_[V]', 'Motor 2 phase 3_[A]', 'Motor 2 phase 3_[kW]', 'Motor 2 phase 3_[PF]', 'Motor 2 phase 3_[F]')
      # Vehicle Chiller Charger 3 to Motor 3
      curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 3_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 3_[V]', 'Motor 3_[A]', 'Motor 3_[kW]', 'Motor 3_[PF]', 'Motor 3_[F]',
                                                     'Motor 3 phase 1_[V]', 'Motor 3 phase 1_[A]', 'Motor 3 phase 1_[kW]', 'Motor 3 phase 1_[PF]', 'Motor 3 phase 1_[F]',
                                                     'Motor 3 phase 2_[V]', 'Motor 3 phase 2_[A]', 'Motor 3 phase 2_[kW]', 'Motor 3 phase 2_[PF]', 'Motor 3 phase 2_[F]',
                                                     'Motor 3 phase 3_[V]', 'Motor 3 phase 3_[A]', 'Motor 3 phase 3_[kW]', 'Motor 3 phase 3_[PF]', 'Motor 3 phase 3_[F]')
      # Vehicle Chiller Charger 4 to Motor 4
      curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 4_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 4_[V]', 'Motor 4_[A]', 'Motor 4_[kW]', 'Motor 4_[PF]', 'Motor 4_[F]',
                                                     'Motor 4 phase 1_[V]', 'Motor 4 phase 1_[A]', 'Motor 4 phase 1_[kW]', 'Motor 4 phase 1_[PF]', 'Motor 4 phase 1_[F]',
                                                     'Motor 4 phase 2_[V]', 'Motor 4 phase 2_[A]', 'Motor 4 phase 2_[kW]', 'Motor 4 phase 2_[PF]', 'Motor 4 phase 2_[F]',
                                                     'Motor 4 phase 3_[V]', 'Motor 4 phase 3_[A]', 'Motor 4 phase 3_[kW]', 'Motor 4 phase 3_[PF]', 'Motor 4 phase 3_[F]')
      # Vehicle Chiller Charger 5 to Motor 5 (Note: typo in original code 'Motor 52 phase 2_[V]')
      curr_i <- which(colnames(retData) == 'Vehicle Chiller Charger 5_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 5_[V]', 'Motor 5_[A]', 'Motor 5_[kW]', 'Motor 5_[PF]', 'Motor 5_[F]',
                                                     'Motor 5 phase 1_[V]', 'Motor 5 phase 1_[A]', 'Motor 5 phase 1_[kW]', 'Motor 5 phase 1_[PF]', 'Motor 5 phase 1_[F]',
                                                     'Motor 52 phase 2_[V]', 'Motor 5 phase 2_[A]', 'Motor 5 phase 2_[kW]', 'Motor 5 phase 2_[PF]', 'Motor 5 phase 2_[F]',
                                                     'Motor 5 phase 3_[V]', 'Motor 5 phase 3_[A]', 'Motor 5 phase 3_[kW]', 'Motor 5 phase 3_[PF]', 'Motor 5 phase 3_[F]')
      # Crusher Machine 1 to CNC Machine 1
      curr_i <- which(colnames(retData) == 'Crusher Machine 1_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('CNC Machine 1_[V]', 'CNC Machine 1_[A]', 'CNC Machine 1_[kW]', 'CNC Machine 1_[PF]', 'CNC Machine 1_[F]',
                                                     'CNC Machine 1 phase 1_[V]', 'CNC Machine 1 phase 1_[A]', 'CNC Machine 1 phase 1_[kW]', 'CNC Machine 1 phase 1_[PF]', 'CNC Machine 1 phase 1_[F]',
                                                     'CNC Machine 1 phase 2_[V]', 'CNC Machine 1 phase 2_[A]', 'CNC Machine 1 phase 2_[kW]', 'CNC Machine 1 phase 2_[PF]', 'CNC Machine 1 phase 2_[F]',
                                                     'CNC Machine 1 phase 3_[V]', 'CNC Machine 1 phase 3_[A]', 'CNC Machine 1 phase 3_[kW]', 'CNC Machine 1 phase 3_[PF]', 'CNC Machine 1 phase 3_[F]')
      # Crusher Machine 2 to CNC Machine 2
      curr_i <- which(colnames(retData) == 'Crusher Machine 2_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('CNC Machine 2_[V]', 'CNC Machine 2_[A]', 'CNC Machine 2_[kW]', 'CNC Machine 2_[PF]', 'CNC Machine 2_[F]',
                                                     'CNC Machine 2 phase 1_[V]', 'CNC Machine 2 phase 1_[A]', 'CNC Machine 2 phase 1_[kW]', 'CNC Machine 2 phase 1_[PF]', 'CNC Machine 2 phase 1_[F]',
                                                     'CNC Machine 2 phase 2_[V]', 'CNC Machine 2 phase 2_[A]', 'CNC Machine 2 phase 2_[kW]', 'CNC Machine 2 phase 2_[PF]', 'CNC Machine 2 phase 2_[F]',
                                                     'CNC Machine 2 phase 3_[V]', 'CNC Machine 2 phase 3_[A]', 'CNC Machine 2 phase 3_[kW]', 'CNC Machine 2 phase 3_[PF]', 'CNC Machine 2 phase 3_[F]')
      # Crusher Machine 3 to CNC Machine 3
      curr_i <- which(colnames(retData) == 'Crusher Machine 3_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('CNC Machine 3_[V]', 'CNC Machine 3_[A]', 'CNC Machine 3_[kW]', 'CNC Machine 3_[PF]', 'CNC Machine 3_[F]',
                                                     'CNC Machine 3 phase 1_[V]', 'CNC Machine 3 phase 1_[A]', 'CNC Machine 3 phase 1_[kW]', 'CNC Machine 3 phase 1_[PF]', 'CNC Machine 3 phase 1_[F]',
                                                     'CNC Machine 3 phase 2_[V]', 'CNC Machine 3 phase 2_[A]', 'CNC Machine 3 phase 2_[kW]', 'CNC Machine 3 phase 2_[PF]', 'CNC Machine 3 phase 2_[F]',
                                                     'CNC Machine 3 phase 3_[V]', 'CNC Machine 3 phase 3_[A]', 'CNC Machine 3 phase 3_[kW]', 'CNC Machine 3 phase 3_[PF]', 'CNC Machine 3 phase 3_[F]')
      # Icing Building Generator to Main Building Generator
      curr_i <- which(colnames(retData) == 'Icing Building Generator_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Main Building Generator_[V]', 'Main Building Generator_[A]', 'Main Building Generator_[kW]', 'Main Building Generator_[PF]', 'Main Building Generator_[F]',
                                                     'Main Building Generator phase 1_[V]', 'Main Building Generator phase 1_[A]', 'Main Building Generator phase 1_[kW]', 'Main Building Generator phase 1_[PF]', 'Main Building Generator phase 1_[F]',
                                                     'Main Building Generator phase 2_[V]', 'Main Building Generator phase 2_[A]', 'Main Building Generator phase 2_[kW]', 'Main Building Generator phase 2_[PF]', 'Main Building Generator phase 2_[F]',
                                                     'Main Building Generator phase 3_[V]', 'Main Building Generator phase 3_[A]', 'Main Building Generator phase 3_[kW]', 'Main Building Generator phase 3_[PF]', 'Main Building Generator phase 3_[F]')
      # Icing Building Main to Main Building (5 columns: V, A, kW, PF, F)
      curr_i <- which(colnames(retData) == 'Icing Building Main_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 4)] <- c('Main Building_[V]', 'Main Building_[A]', 'Main Building_[kW]', 'Main Building_[PF]', 'Main Building_[F]')
      # Cross Section Mixing Machine to Motor 6
      curr_i <- which(colnames(retData) == 'Cross Section Mixing Machine_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 19)] <- c('Motor 6_[V]', 'Motor 6_[A]', 'Motor 6_[kW]', 'Motor 6_[PF]', 'Motor 6_[F]',
                                                     'Motor 6 phase 1_[V]', 'Motor 6 phase 1_[A]', 'Motor 6 phase 1_[kW]', 'Motor 6 phase 1_[PF]', 'Motor 6 phase 1_[F]',
                                                     'Motor 6 phase 2_[V]', 'Motor 6 phase 2_[A]', 'Motor 6 phase 2_[kW]', 'Motor 6 phase 2_[PF]', 'Motor 6 phase 2_[F]',
                                                     'Motor 6 phase 3_[V]', 'Motor 6 phase 3_[A]', 'Motor 6 phase 3_[kW]', 'Motor 6 phase 3_[PF]', 'Motor 6 phase 3_[F]')
      # AC Raw Storage to Motor 7 (5 columns: V, A, kW, PF, F)
      curr_i <- which(colnames(retData) == 'AC Raw Storage_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 4)] <- c('Motor 7_[V]', 'Motor 7_[A]', 'Motor 7_[kW]', 'Motor 7_[PF]', 'Motor 7_[F]')
      
      # Baking Area Lights to Motor 8 (5 columns: V, A, kW, PF, F)
      curr_i <- which(colnames(retData) == 'Baking Area Lights_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 4)] <- c('Motor 8_[V]', 'Motor 8_[A]', 'Motor 8_[kW]', 'Motor 8_[PF]', 'Motor 8_[F]')
      
      # Cold Store AC 2 to Motor 9 (5 columns: V, A, kW, PF, F)
      curr_i <- which(colnames(retData) == 'Cold Store AC 2_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 4)] <- c('Motor 9_[V]', 'Motor 9_[A]', 'Motor 9_[kW]', 'Motor 9_[PF]', 'Motor 9_[F]')
      
      # Cold Store AC 1 to Motor 10 (5 columns: V, A, kW, PF, F)
      curr_i <- which(colnames(retData) == 'Cold Store AC 1_[V]')
      if (length(curr_i) > 0)
        colnames(retData)[curr_i:(curr_i + 4)] <- c('Motor 10_[V]', 'Motor 10_[A]', 'Motor 10_[kW]', 'Motor 10_[PF]', 'Motor 10_[F]')
      
      
    }
    # New logic to replace new_name with Display_name from Config file
    # Extract meterID from the latest file name to construct the config file path
    meterID <- strsplit(historic_files[length(historic_files)], '/', fixed = TRUE)[[1]]
    meterID <- strsplit(meterID[length(meterID)], '_', fixed = TRUE)[[1]][1]
    # Construct the file path for the Config CSV using the meterID
    config_file <- paste0('/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/', meterID, '_Config.csv')
    # Check if the Config file exists on the server
    file_present <- file.exists(config_file)
    
    # Proceed only if the Config file exists
    if (file_present) {
      # Load the Config file into a data frame, ensuring strings are not converted to factors
      config_data <- read.csv(config_file, stringsAsFactors = FALSE)
      
      # Create a mapping from new_name to Display_name
      name_mapping <- setNames(config_data$Display_name, config_data$new_name)
      
      # Process each column in retData
      new_colnames <- colnames(retData)
      # Loop through columns, starting from the second (skip DateTime)
      for (i in 2:length(new_colnames)) {
        # Extract base name and unit (e.g., "Building 1_[kW]" -> base: "Building 1", unit: "_[kW]")
        if (grepl('_\\[[A-Za-z]+\\]', new_colnames[i])) {
          col_parts <- strsplit(new_colnames[i], '_\\[')[[1]]
          base_name <- col_parts[1]
          unit <- paste0('_[', strsplit(col_parts[2], '\\]')[[1]][1], ']')
        } else {
          base_name <- new_colnames[i]
          unit <- ''
        }
        
        # Replace base_name with Display_name if it exists, is valid, and differs from new_name
        if (base_name %in% names(name_mapping) && !is.na(name_mapping[base_name]) && name_mapping[base_name] != '' && name_mapping[base_name] != base_name) {
          new_colnames[i] <- paste0(name_mapping[base_name], unit)
        }
      }
      
      # Apply new column names, ensuring the length matches the number of columns
      if (length(new_colnames) == ncol(retData)) colnames(retData) <- new_colnames
    }
    
    
    # Update column lists after all renaming and Virtual Register processing to reflect final column names
    powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
    voltageColumns = grep('[V]', colnames(retData), value = T, fixed = T)
    currentColumns = grep('[A]', colnames(retData), value = T, fixed = T)
    powerfactorColumns = grep('[PF]', colnames(retData), value = T, fixed = T)
    frequencyColumns = grep('[F]', colnames(retData), value = T, fixed = T)
    if (meterType == "neubolt") 
    {
      energyPColumns = grep('[EP]', colnames(retData), value = T, fixed = T)
      energyNColumns = grep('[EN]', colnames(retData), value = T, fixed = T)
      
    }
  }, warning = function(w) {},
  # Log any errors with a timestamp for debugging
  error = function(e) {cat("Error ", conditionMessage(e), " at", Sys.time(), '\n\n') })
  
  # Return a list containing the processed data and column lists
  list("summaryData" = retData, "power" = powerColumns, "volt" = voltageColumns, "current" = currentColumns, "pf" = powerfactorColumns)}

# Input validation helper function for historic file load
validate_historic_inputs <- function(start_date, end_date, dir, meterType, buildingMap) {
  errors <- c()
  
  # Validate dates
  tryCatch({
    as.Date(start_date)
  }, error = function(e) {
    errors <<- c(errors, "Invalid start_date format. Use YYYY-MM-DD.")
  })
  
  tryCatch({
    as.Date(end_date)
  }, error = function(e) {
    errors <<- c(errors, "Invalid end_date format. Use YYYY-MM-DD.")
  })
  
  # Validate directory
  if (is.null(dir) || !is.character(dir) || length(dir) == 0) {
    errors <- c(errors, "Directory path is required and must be a string.")
  } else if (!dir.exists(dir)) {
    errors <- c(errors, paste("Directory does not exist:", dir))
  }
  
  # Validate meterType
  if (is.null(meterType) || !is.character(meterType) || length(meterType) == 0) {
    errors <- c(errors, "meterType is required and must be a string.")
  }
  
  # Validate buildingMap
  if (is.null(buildingMap) || (!is.list(buildingMap) && !is.character(buildingMap))) {
    errors <- c(errors, "buildingMap is required and must be a list or JSON string.")
  }
  
  return(errors)
}

# ============================================================================
# API ENDPOINTS
# ============================================================================

#* @apiTitle Consolidated API Server
#* @apiDescription Combined Daily Analysis, Electricity Bill, and Historic File Load APIs
#* @apiVersion 1.0.0

# ============================================================================
# MAIN HEALTH ENDPOINT
# ============================================================================

#* Get overall API health status
#* @get /health
#* @serializer json
function() {
  format_api_response(
    success = TRUE,
    message = "Consolidated API Server is running",
    data = list(
      status = "healthy",
      version = "1.0.0",
      server_time = Sys.time(),
      services = list(
        daily_analysis = "Available at /daily-analysis/*",
        electricity_bill = "Available at /electricity-bill/*", 
        historic_file_load = "Available at /historic-file/*"
      ),
      run_mode = RUN_MODE
    )
  )
}

# ============================================================================
# DAILY ANALYSIS ENDPOINTS (/daily-analysis/*)
# ============================================================================

#* Get Daily Analysis API health status
#* @get /daily-analysis/health
#* @serializer json
function() {
  format_api_response(
    success = TRUE,
    message = "Daily Analysis API is running",
    data = list(
      status = "healthy",
      version = "1.0.0",
      server_time = Sys.time(),
      dependencies = list(
        historic_api = HISTORIC_API_URL
      )
    )
  )
}

#* Process comparative energy data analysis
#* @post /daily-analysis/comparative-analysis
#* @serializer json
function(req) {
  # Parse JSON request body
  body_data <- tryCatch({
    if (is.raw(req$postBody)) {
      json_string <- rawToChar(req$postBody)
    } else if (is.character(req$postBody)) {
      json_string <- req$postBody
    } else {
      json_string <- as.character(req$postBody)
    }
    jsonlite::fromJSON(json_string)
  }, error = function(e) {
    return(format_api_response(
      success = FALSE,
      message = paste("Invalid JSON in request body:", e$message),
      status_code = 400
    ))
  })

  saveRDS(body_data, '/srv/shiny-server/EnvizFleet/hour_bug_check.rds')

  # Extract parameters
  dates <- body_data$dates
  start_time <- body_data$start_time
  end_time <- body_data$end_time
  dir_hourwise <- body_data$dir_hourwise
  dir_minutewise <- body_data$dir_minutewise
  meterType <- body_data$meterType
  buildingMap <- body_data$buildingMap

  # Parse buildingMap if it's a JSON string
  if (is.character(buildingMap)) {
    tryCatch({
      buildingMap <- fromJSON(buildingMap)
    }, error = function(e) {
      return(format_api_response(
        success = FALSE,
        message = paste("Invalid buildingMap JSON:", e$message),
        status_code = 400
      ))
    })
  }

  # Validate inputs
  validation_errors <- validate_comparative_inputs(dates, start_time, end_time, dir_hourwise, dir_minutewise, meterType, buildingMap)
  if (length(validation_errors) > 0) {
    return(format_api_response(
      success = FALSE,
      message = "Input validation failed",
      data = list(errors = validation_errors),
      status_code = 400
    ))
  }

  # Process comparative data analysis
  tryCatch({
    # Convert dates to proper format
    Date_selector_comparative <- as.Date(dates, tz = "Asia/Karachi")

    data_hourly <- NULL
    data_minutely <- NULL

    for (date in Date_selector_comparative) {
      date <- as.Date(date, tz = "Asia/Karachi")

      # Get data from historic API
      today_data_hourly <<- historic_file_load(
        as.character(date),
        as.character(date+1),
        dir_hourwise,
        meterType,
        buildingMap
      )$summaryData %>% spike_check("hour")





      today_data_minutely <- historic_file_load(
        as.character(date),
        as.character(date+1),
        dir_minutewise,
        meterType,
        buildingMap
      )$summaryData %>% spike_check("minute")

      start_time <- as.numeric(start_time)
      end_time <- as.numeric(end_time)
      # Apply time filtering
      today_data_hourly <<- today_data_hourly[hour(today_data_hourly$DateTime) >= start_time | as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == (date+1), ]
      today_data_minutely <<- today_data_minutely[hour(today_data_minutely$DateTime) >= start_time | as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == (date+1), ]

      if (end_time > start_time) {
        today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time & as.Date(today_data_hourly$DateTime) == date, ]
        today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time & as.Date(today_data_minutely$DateTime) == date, ]
      } else {
        today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time | as.Date(today_data_hourly$DateTime) == date, ]
        today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time | as.Date(today_data_minutely$DateTime) == date, ]
      }

      # Create time sequences and merge
      seq_df <- data.frame()
      if (end_time > start_time) {
        seq_df <- seq.POSIXt((date+hours(start_time)) %>% as.POSIXct(), (date)+hours(end_time), by='hour')
      } else {
        seq_df <- seq(date+hours(start_time), (date+1)+hours(end_time), by='hour')
      }
      seq_df <- data.frame("DateTime" = seq_df)
      tz(today_data_hourly[[1]]) <- tz(seq_df$DateTime)
      today_data_hourly <- merge(seq_df, today_data_hourly, all.x = TRUE)

      seq_df_min <- data.frame()
      if (end_time > start_time) {
        seq_df_min <- seq.POSIXt((date+hours(start_time)) %>% as.POSIXct(), (date+hours(end_time+1)), by='min')[-1441]
      } else {
        seq_df_min <- seq(date+hours(start_time), (date+1)+hours(start_time), by='min')[-1441]
      }
      seq_df_min <- data.frame("DateTime" = seq_df_min)
      tz(today_data_minutely[[1]]) <- tz(seq_df_min$DateTime)
      today_data_minutely <- merge(seq_df_min, today_data_minutely, all.x = TRUE)

      # Combine data
      if (length(data_hourly) == 0) data_hourly = today_data_hourly
      else data_hourly = rbind.fill(data_hourly, today_data_hourly)

      if (length(data_minutely) == 0) data_minutely = today_data_minutely
      else data_minutely = rbind.fill(data_minutely, today_data_minutely)
    }

    # Remove duplicates and add Day column
    data_hourly <- data_hourly[!duplicated(data_hourly), ]
    data_minutely <- data_minutely[!duplicated(data_minutely), ]

    data_hourly$Day <- as.Date(data_hourly$DateTime)
    data_minutely$Day <- as.Date(data_minutely$DateTime)
#
    # Adjust Day column for times before start_time
    data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] <- data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] - 1
    data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] <- data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] - 1

    # Return processed data
    format_api_response(
      success = TRUE,
      message = "Comparative analysis completed successfully",
      data = list(
        hourly_data = data_hourly,
        minutely_data = data_minutely
      )
    )

  }, error = function(e) {
    format_api_response(
      success = FALSE,
      message = paste("Error processing comparative analysis:", e$message),
      status_code = 500
    )
  })
}
# #* Process comparative energy data analysis
# #* @post /daily-analysis/comparative-analysis
# #* @serializer json
# function(req) {
#   # Parse JSON request body
#   body_data <- tryCatch({
#     if (is.raw(req$postBody)) {
#       json_string <- rawToChar(req$postBody)
#     } else if (is.character(req$postBody)) {
#       json_string <- req$postBody
#     } else {
#       json_string <- as.character(req$postBody)
#     }
#     jsonlite::fromJSON(json_string)
#   }, error = function(e) {
#     return(format_api_response(
#       success = FALSE,
#       message = paste("Invalid JSON in request body:", e$message),
#       status_code = 400
#     ))
#   })
#   
#   # Extract parameters
#   dates <- body_data$dates
#   start_time <- body_data$start_time
#   end_time <- body_data$end_time
#   dir_hourwise <- body_data$dir_hourwise
#   dir_minutewise <- body_data$dir_minutewise
#   meterType <- body_data$meterType
#   buildingMap <- body_data$buildingMap
#   
#   # Parse buildingMap if it's a JSON string
#   if (is.character(buildingMap)) {
#     tryCatch({
#       buildingMap <- fromJSON(buildingMap)
#     }, error = function(e) {
#       return(format_api_response(
#         success = FALSE,
#         message = paste("Invalid buildingMap JSON:", e$message),
#         status_code = 400
#       ))
#     })
#   }
#   
#   # Validate inputs
#   validation_errors <- validate_comparative_inputs(dates, start_time, end_time, dir_hourwise, dir_minutewise, meterType, buildingMap)
#   if (length(validation_errors) > 0) {
#     return(format_api_response(
#       success = FALSE,
#       message = "Input validation failed",
#       data = list(errors = validation_errors),
#       status_code = 400
#     ))
#   }
#   
#   # Process comparative data analysis
#   tryCatch({
#     # Convert dates to proper format
#     Date_selector_comparative <- as.Date(dates, tz = "Asia/Karachi")
#     
#     data_hourly <- NULL
#     data_minutely <- NULL
#     
#     for (date in Date_selector_comparative) {
#       date <- as.Date(date, tz = "Asia/Karachi")
#       
#       # Get data from historic API
#       today_data_hourly <<- historic_file_load(
#         as.character(date),
#         as.character(date+1),
#         dir_hourwise,
#         meterType,
#         buildingMap
#       )$summaryData %>% spike_check("hour")
#       
#       
#       
#       
#       
#       today_data_minutely <- historic_file_load(
#         as.character(date),
#         as.character(date+1),
#         dir_minutewise,
#         meterType,
#         buildingMap
#       )$summaryData %>% spike_check("minute")
#       today_data_hourly$DateTime <- lubridate::force_tz(today_data_hourly$DateTime, "Asia/Karachi")
#       today_data_minutely$DateTime <- lubridate::force_tz(today_data_minutely$DateTime, "Asia/Karachi")
#       print("====== DEBUG: BEFORE ANY FILTER ======")
#       print(paste("start_time:", start_time, "| type:", typeof(start_time)))
#       print(paste("end_time:", end_time, "| type:", typeof(end_time)))
#       print(paste("Rows:", nrow(today_data_hourly)))
#       print(paste("Hours:", paste(sort(unique(hour(today_data_hourly$DateTime))), collapse=",")))
#       
#       
#       # Apply time filtering
#       today_data_hourly <<- today_data_hourly[hour(today_data_hourly$DateTime) >= start_time | as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == (date+1), ]
#       
#       today_data_minutely <<- today_data_minutely[hour(today_data_minutely$DateTime) >= start_time | as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == (date+1), ]
#       print("====== DEBUG: AFTER FIRST FILTER (>= start_time) ======")
#       print(paste("Rows:", nrow(today_data_hourly)))
#       print(paste("Hours:", paste(sort(unique(hour(today_data_hourly$DateTime))), collapse=",")))
#       
#       if (end_time > start_time) {
#         print("====== USING end_time > start_time BRANCH ======")
#         today_data_hourly <<- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time & as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == date, ]
#         today_data_minutely <<- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time & as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == date, ]
#       } else {
#         print("====== USING else BRANCH ======")
#         today_data_hourly <<- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time | as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == date, ]
#         today_data_minutely <<- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time | as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == date, ]
#       }
#       print("====== DEBUG: AFTER SECOND FILTER (<= end_time) ======")
#       print(paste("Rows:", nrow(today_data_hourly)))
#       print(paste("Hours:", paste(sort(unique(hour(today_data_hourly$DateTime))), collapse=",")))
#       # Create time sequences and merge
#       # seq_df <- data.frame()
#       # if (end_time > start_time) {
#       #   seq_df <- seq.POSIXt((date+hours(start_time)) %>% as.POSIXct(), (date)+hours(end_time), by='hour')
#       # } else {
#       #   seq_df <- seq(date+hours(start_time), (date+1)+hours(end_time), by='hour')
#       # }
#       # seq_df <- data.frame("DateTime" = seq_df)
#       # tz(today_data_hourly[[1]]) <- tz(seq_df$DateTime)
#       # today_data_hourly <- merge(seq_df, today_data_hourly, all.x = TRUE)
#       # Create time sequences and merge - USE PKT
#       if (end_time > start_time) {
#         seq_df <- seq.POSIXt(
#           as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#           as.POSIXct(paste0(date, " ", end_time, ":00:00"), tz = "Asia/Karachi"),
#           by='hour'
#         )
#       } else {
#         seq_df <- seq.POSIXt(
#           as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#           as.POSIXct(paste0(date+1, " ", end_time, ":00:00"), tz = "Asia/Karachi"),
#           by='hour'
#         )
#       }
#       seq_df <- data.frame("DateTime" = seq_df)
#       today_data_hourly <- merge(seq_df, today_data_hourly, by = "DateTime", all.x = TRUE)
#       
#       # seq_df_min <- data.frame()
#       # if (end_time > start_time) {
#       #   seq_df_min <- seq.POSIXt((date+hours(start_time)) %>% as.POSIXct(), (date+hours(end_time+1)), by='min')[-1441]
#       # } else {
#       #   seq_df_min <- seq(date+hours(start_time), (date+1)+hours(start_time), by='min')[-1441]
#       # }
#       # seq_df_min <- data.frame("DateTime" = seq_df_min)
#       # tz(today_data_minutely[[1]]) <- tz(seq_df_min$DateTime)
#       # today_data_minutely <- merge(seq_df_min, today_data_minutely, all.x = TRUE)
#       if (end_time > start_time) {
#         seq_df_min <- seq.POSIXt(
#           as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#           as.POSIXct(paste0(date, " ", end_time, ":59:00"), tz = "Asia/Karachi"),
#           by='min'
#         )
#       } else {
#         seq_df_min <- seq.POSIXt(
#           as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#           as.POSIXct(paste0(date+1, " ", end_time, ":59:00"), tz = "Asia/Karachi"),
#           by='min'
#         )
#       }
#       seq_df_min <- data.frame("DateTime" = seq_df_min)
#       today_data_minutely <- merge(seq_df_min, today_data_minutely, by = "DateTime", all.x = TRUE)
#       # Combine data
#       if (length(data_hourly) == 0) data_hourly = today_data_hourly
#       else data_hourly = rbind.fill(data_hourly, today_data_hourly)
#       
#       if (length(data_minutely) == 0) data_minutely = today_data_minutely
#       else data_minutely = rbind.fill(data_minutely, today_data_minutely)
#     }
#     
#     # Remove duplicates and add Day column
#     data_hourly <- data_hourly[!duplicated(data_hourly), ]
#     data_minutely <- data_minutely[!duplicated(data_minutely), ]
#     
#     data_hourly$Day <- as.Date(data_hourly$DateTime, tz = tz(data_hourly$DateTime))
#     data_minutely$Day <- as.Date(data_minutely$DateTime, tz(data_minutely$DateTime))
#     #     
#     # Adjust Day column for times before start_time
#     data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] <- data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] - 1
#     data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] <- data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] - 1
#     print("====== DEBUG: FINAL API OUTPUT ======")
#     print(paste("Total hourly rows:", nrow(data_hourly)))
#     print(paste("Hourly DateTime range:", min(data_hourly$DateTime), "to", max(data_hourly$DateTime)))
#     print(paste("Hourly unique hours:", paste(sort(unique(hour(data_hourly$DateTime))), collapse=",")))
#     print(paste("NA count in Usage:", sum(is.na(data_hourly$Usage))))
#     print("Last 30 rows of hourly data:")
#     print(colnames(data_hourly)[1:5])
#     print(tail(data_hourly[, 1:5], 30))
#     # Return processed data
#     format_api_response(
#       success = TRUE,
#       message = "Comparative analysis completed successfully",
#       data = list(
#         hourly_data = data_hourly,
#         minutely_data = data_minutely
#       )
#     )
#     
#   }, error = function(e) {
#     format_api_response(
#       success = FALSE,
#       message = paste("Error processing comparative analysis:", e$message),
#       status_code = 500
#     )
#   })
# }
#* Process comparative energy data analysis
# #* @post /daily-analysis/comparative-analysis
# #* @serializer json
# function(req) {
#   # Parse JSON request body
#   body_data <- tryCatch({
#     if (is.raw(req$postBody)) {
#       json_string <- rawToChar(req$postBody)
#     } else if (is.character(req$postBody)) {
#       json_string <- req$postBody
#     } else {
#       json_string <- as.character(req$postBody)
#     }
#     jsonlite::fromJSON(json_string)
#   }, error = function(e) {
#     return(format_api_response(
#       success = FALSE,
#       message = paste("Invalid JSON in request body:", e$message),
#       status_code = 400
#     ))
#   })
# 
#   # Extract parameters
#   dates <- body_data$dates
#   start_time <- as.numeric(body_data$start_time)
#   end_time <- as.numeric(body_data$end_time)
#   dir_hourwise <- body_data$dir_hourwise
#   dir_minutewise <- body_data$dir_minutewise
#   meterType <- body_data$meterType
#   buildingMap <- body_data$buildingMap
# 
#   # Parse buildingMap if it's a JSON string
#   if (is.character(buildingMap)) {
#     tryCatch({
#       buildingMap <- fromJSON(buildingMap)
#     }, error = function(e) {
#       return(format_api_response(
#         success = FALSE,
#         message = paste("Invalid buildingMap JSON:", e$message),
#         status_code = 400
#       ))
#     })
#   }
# 
#   # Validate inputs
#   validation_errors <- validate_comparative_inputs(dates, start_time, end_time, dir_hourwise, dir_minutewise, meterType, buildingMap)
#   if (length(validation_errors) > 0) {
#     return(format_api_response(
#       success = FALSE,
#       message = "Input validation failed",
#       data = list(errors = validation_errors),
#       status_code = 400
#     ))
#   }
# 
#   # Process comparative data analysis
#   tryCatch({
#     # Convert dates to proper format
#     Date_selector_comparative <- as.Date(dates, tz = "Asia/Karachi")
# 
#     data_hourly <- NULL
#     data_minutely <- NULL
# 
#     for (date in Date_selector_comparative) {
#       date <- as.Date(date, origin = "1970-01-01", tz = "Asia/Karachi")
# 
#       # Get data from historic API
# 
#        today_data_hourly <- historic_file_load(
#         as.character(date),
#         as.character(date+1),
#         dir_hourwise,
#         meterType,
#         buildingMap
#       )$summaryData %>% spike_check("hour")
# 
#       today_data_minutely <- historic_file_load(
#         as.character(date),
#         as.character(date+1),
#         dir_minutewise,
#         meterType,
#         buildingMap
#       )$summaryData %>% spike_check("minute")
# 
#       # FIX: force_tz to label as PKT without converting
#       today_data_hourly$DateTime <- force_tz(today_data_hourly$DateTime, "Asia/Karachi")
#       today_data_minutely$DateTime <- force_tz(today_data_minutely$DateTime, "Asia/Karachi")
# 
#       # Apply time filtering
#       # today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) >= start_time | as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == (date+1), ]
#       # today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) >= start_time | as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == (date+1), ]
#       #
#       # if (end_time > start_time) {
#       #   today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time & as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == date, ]
#       #   today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time & as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == date, ]
#       # } else {
#       #   today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time | as.Date(today_data_hourly$DateTime, tz = "Asia/Karachi") == date, ]
#       #   today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time | as.Date(today_data_minutely$DateTime, tz = "Asia/Karachi") == date, ]
#       # }
#       # Apply time filtering - NO tz parameter since data is already PKT after force_tz
#       today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) >= start_time | as.Date(today_data_hourly$DateTime) == (date+1), ]
#       today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) >= start_time | as.Date(today_data_minutely$DateTime) == (date+1), ]
#       # Extract date from displayed time (not epoch) using format
#       data_dates_h <- as.Date(format(today_data_hourly$DateTime, "%Y-%m-%d"))
#       data_dates_m <- as.Date(format(today_data_minutely$DateTime, "%Y-%m-%d"))
# 
#       if (end_time > start_time) {
#         today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time & data_dates_h == date, ]
#         today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time & data_dates_m == date, ]
#       } else {
#         today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time | data_dates_h == date, ]
#         today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time | data_dates_m == date, ]
#       }
#       # if (end_time > start_time) {
#       #   today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time & as.Date(today_data_hourly$DateTime) == date, ]
#       #   today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time & as.Date(today_data_minutely$DateTime) == date, ]
#       # } else {
#       #   today_data_hourly <- today_data_hourly[hour(today_data_hourly$DateTime) <= end_time | as.Date(today_data_hourly$DateTime) == date, ]
#       #   today_data_minutely <- today_data_minutely[hour(today_data_minutely$DateTime) <= end_time | as.Date(today_data_minutely$DateTime) == date, ]
#       # }
# 
#       # Create time sequences and merge
#       # if (end_time > start_time) {
#       #   seq_df <- seq.POSIXt(
#       #     as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#       #     as.POSIXct(paste0(date, " ", end_time, ":00:00"), tz = "Asia/Karachi"),
#       #     by='hour'
#       #   )
#       # } else {
#       #   seq_df <- seq.POSIXt(
#       #     as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#       #     as.POSIXct(paste0(date+1, " ", end_time, ":00:00"), tz = "Asia/Karachi"),
#       #     by='hour'
#       #   )
#       # }
#       # seq_df <- data.frame("DateTime" = seq_df)
#       # today_data_hourly <- merge(seq_df, today_data_hourly, by = "DateTime", all.x = TRUE)
#       #
#       # if (end_time > start_time) {
#       #   seq_df_min <- seq.POSIXt(
#       #     as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#       #     as.POSIXct(paste0(date, " ", end_time, ":59:00"), tz = "Asia/Karachi"),
#       #     by='min'
#       #   )
#       # } else {
#       #   seq_df_min <- seq.POSIXt(
#       #     as.POSIXct(paste0(date, " ", start_time, ":00:00"), tz = "Asia/Karachi"),
#       #     as.POSIXct(paste0(date+1, " ", end_time, ":59:00"), tz = "Asia/Karachi"),
#       #     by='min'
#       #   )
#       # }
#       # seq_df_min <- data.frame("DateTime" = seq_df_min)
#       # today_data_minutely <- merge(seq_df_min, today_data_minutely, by = "DateTime", all.x = TRUE)
# 
#       # Combine data
#       if (length(data_hourly) == 0) data_hourly = today_data_hourly
#       else data_hourly = rbind.fill(data_hourly, today_data_hourly)
# 
#       if (length(data_minutely) == 0) data_minutely = today_data_minutely
#       else data_minutely = rbind.fill(data_minutely, today_data_minutely)
#     }
# 
#     # Remove duplicates and add Day column
#     data_hourly <- data_hourly[!duplicated(data_hourly), ]
#     data_minutely <- data_minutely[!duplicated(data_minutely), ]
# 
#     # data_hourly$Day <- as.Date(data_hourly$DateTime, tz = "Asia/Karachi")
#     data_hourly$Day <- as.Date(format(data_hourly$DateTime, "%Y-%m-%d"))
#     print(colnames(data_hourly))
#     View(data_hourly)
#     data_minutely$Day <- as.Date(data_minutely$DateTime, tz = "Asia/Karachi")
# 
#     # Adjust Day column for times before start_time
#     data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] <- data_hourly[hour(data_hourly$DateTime) < start_time, "Day"] - 1
#     data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] <- data_minutely[hour(data_minutely$DateTime) < start_time, "Day"] - 1
# 
#     # DEBUG
#     print("====== FINAL OUTPUT ======")
#     print(paste("Rows:", nrow(data_hourly)))
#     print(paste("Hours:", paste(sort(unique(hour(data_hourly$DateTime))), collapse=",")))
#     print(paste("NA count:", sum(is.na(data_hourly$Usage))))
# 
#     # Return processed data
#     format_api_response(
#       success = TRUE,
#       message = "Comparative analysis completed successfully",
#       data = list(
#         hourly_data = data_hourly,
#         minutely_data = data_minutely
#       )
#     )
# 
#   }, error = function(e) {
#     format_api_response(
#       success = FALSE,
#       message = paste("Error processing comparative analysis:", e$message),
#       status_code = 500
#     )
#   })
# }






#* Log download request
#* @post /daily-analysis/log-download
#* @serializer json
function(req) {
  # Parse JSON request body
  body_data <- tryCatch({
    if (is.raw(req$postBody)) {
      json_string <- rawToChar(req$postBody)
    } else if (is.character(req$postBody)) {
      json_string <- req$postBody
    } else {
      json_string <- as.character(req$postBody)
    }
    jsonlite::fromJSON(json_string)
  }, error = function(e) {
    return(format_api_response(
      success = FALSE,
      message = paste("Invalid JSON in request body:", e$message),
      status_code = 400
    ))
  })
  
  # Extract dates and format log entry
  dates <- body_data$dates
  
  tryCatch({
    selected_dates <- as.Date(dates, tz = "Asia/Karachi") %>%
      format("%d-%m-%Y")
    
    log_entry <- paste("Daily Analysis Tab| ", paste(selected_dates, collapse = "| "))
    
    # Note: You'll need to implement actual logging here
    # For now, we'll just return the log entry
    
    format_api_response(
      success = TRUE,
      message = "Download logged successfully",
      data = list(
        log_entry = log_entry,
        notification = list(
          title = "Done!",
          text = "Your Comparative energy data has been downloaded!",
          type = "success"
        )
      )
    )
    
  }, error = function(e) {
    format_api_response(
      success = FALSE,
      message = paste("Error logging download:", e$message),
      status_code = 500
    )
  })
}

# ============================================================================
# ELECTRICITY BILL ENDPOINTS (/electricity-bill/*)
# ============================================================================

#* Get Electricity Bill API health status
#* @get /electricity-bill/health
#* @serializer json
function() {
  format_api_response(
    success = TRUE,
    message = "Electricity Bill API is running",
    data = list(
      status = "healthy",
      version = "1.0.0",
      server_time = Sys.time(),
      run_mode = RUN_MODE
    )
  )
}

#* Load electricity bill data
#* @post /electricity-bill/load-bill-data
#* @serializer json
function(req) {
  # Parse JSON request body
  body_data <- tryCatch({
    if (is.raw(req$postBody)) {
      json_string <- rawToChar(req$postBody)
    } else if (is.character(req$postBody)) {
      json_string <- req$postBody
    } else {
      json_string <- as.character(req$postBody)
    }
    jsonlite::fromJSON(json_string)
  }, error = function(e) {
    return(format_api_response(
      success = FALSE,
      message = paste("Invalid JSON in request body:", e$message),
      status_code = 400
    ))
  })
  
  # Extract parameters
  selectedBranch <- body_data$selectedBranch
  billSelectionInput <- body_data$billSelectionInput  # Optional
  
  # Validate inputs
  validation_errors <- validate_bill_inputs(selectedBranch, billSelectionInput)
  if (length(validation_errors) > 0) {
    return(format_api_response(
      success = FALSE,
      message = "Input validation failed",
      data = list(errors = validation_errors),
      status_code = 400
    ))
  }
  
  # Load bill data
  tryCatch({
    result <- load_bill_data(selectedBranch, billSelectionInput)
    
    if (!result$success) {
      return(format_api_response(
        success = FALSE,
        message = "Failed to load bill data",
        status_code = 500
      ))
    }
    
    # Create sparkline data
    sparklineBill_data <- create_sparkline_data(result$billHistory, "BILL")
    sparklineUnits_data <- create_sparkline_data(result$billHistory, "UNITS")
    
    # Return processed data
    format_api_response(
      success = TRUE,
      message = "Bill data loaded successfully",
      data = list(
        billSummary = result$billSummary,
        billHistory = result$billHistory,
        sparklineBill = sparklineBill_data,
        sparklineUnits = sparklineUnits_data
      )
    )
    
  }, error = function(e) {
    format_api_response(
      success = FALSE,
      message = paste("Error loading bill data:", e$message),
      status_code = 500
    )
  })
}

#* Update RUN_MODE configuration
#* @post /electricity-bill/set-run-mode
#* @serializer json
function(req) {
  # Parse JSON request body
  body_data <- tryCatch({
    if (is.raw(req$postBody)) {
      json_string <- rawToChar(req$postBody)
    } else if (is.character(req$postBody)) {
      json_string <- req$postBody
    } else {
      json_string <- as.character(req$postBody)
    }
    jsonlite::fromJSON(json_string)
  }, error = function(e) {
    return(format_api_response(
      success = FALSE,
      message = paste("Invalid JSON in request body:", e$message),
      status_code = 400
    ))
  })
  
  # Extract and validate run mode
  new_run_mode <- body_data$run_mode
  
  if (is.null(new_run_mode) || !new_run_mode %in% c("DEV", "DEP")) {
    return(format_api_response(
      success = FALSE,
      message = "Invalid run_mode. Must be 'DEV' or 'DEP'",
      status_code = 400
    ))
  }
  
  # Update global RUN_MODE
  RUN_MODE <<- new_run_mode
  
  format_api_response(
    success = TRUE,
    message = paste("RUN_MODE updated to:", new_run_mode),
    data = list(run_mode = RUN_MODE)
  )
}

# ============================================================================
# HISTORIC FILE LOAD ENDPOINTS (/historic-file/*)
# ============================================================================

#* Get Historic File Load API health status
#* @get /historic-file/health
#* @serializer json
function() {
  format_api_response(
    success = TRUE,
    message = "Historic File Load API is running",
    data = list(
      status = "healthy",
      version = "1.0.0",
      server_time = Sys.time()
    )
  )
}

#* Load historic file data
#* @post /historic-file/load-historic-data
#* @serializer json
function(req) {
  
  # Parse JSON body instead of individual parameters
  body_data <- tryCatch({
    # Handle different ways the JSON body might come in
    if (is.raw(req$postBody)) {
      # If postBody is raw, convert to character first
      json_string <- rawToChar(req$postBody)
    } else if (is.character(req$postBody)) {
      # If postBody is already character, use directly
      json_string <- req$postBody
    } else {
      # Try to convert to character
      json_string <- as.character(req$postBody)
    }
    
    # Parse the JSON string
    jsonlite::fromJSON(json_string)
  }, error = function(e) {
    return(format_api_response(
      success = FALSE,
      message = paste("Invalid JSON in request body:", e$message),
      status_code = 400
    ))
  })
  
  # Check if we got an error response instead of parsed data
  if (is.list(body_data) && !is.null(body_data$success) && !body_data$success) {
    return(body_data)
  }
  
  # Extract parameters from JSON body
  start_date <- body_data$start_date
  end_date <- body_data$end_date
  dir <- body_data$dir
  meterType <- body_data$meterType
  buildingMap <- body_data$buildingMap
  
  # Parse buildingMap if it's a JSON string
  if (is.character(buildingMap)) {
    tryCatch({
      buildingMap <- fromJSON(buildingMap)
    }, error = function(e) {
      return(format_api_response(
        success = FALSE,
        message = paste("Invalid buildingMap JSON:", e$message),
        status_code = 400
      ))
    })
  }
  
  # Validate inputs
  validation_errors <- validate_historic_inputs(start_date, end_date, dir, meterType, buildingMap)
  if (length(validation_errors) > 0) {
    return(format_api_response(
      success = FALSE,
      message = "Input validation failed",
      data = list(errors = validation_errors),
      status_code = 400
    ))
  }
  
  # Call the original function
  tryCatch({
    result <- historic_file_load(start_date, end_date, dir, meterType, buildingMap)
    
    # Format the successful response
    format_api_response(
      success = TRUE,
      message = "Historic data loaded successfully",
      data = result
    )
    
  }, error = function(e) {
    # Handle errors and return appropriate response
    format_api_response(
      success = FALSE,
      message = paste("Error processing request:", e$message),
      status_code = 500
    )
  })
}

#* Get column information for a specific meter type
#* @get /historic-file/column-info
#* @param meterType:str Type of meter to get column information for
#* @serializer json
function(meterType) {
  if (is.null(meterType) || !is.character(meterType)) {
    return(format_api_response(
      success = FALSE,
      message = "meterType parameter is required",
      status_code = 400
    ))
  }
  
  tryCatch({
    # Return column type information based on meter type
    column_info <- list()
    
    if (meterType == 'neubolt') {
      column_info <- list(
        power_suffix = "_[kW]",
        voltage_suffix = "_[V]",
        current_suffix = "_[A]",
        power_factor_suffix = "_[PF]",
        frequency_suffix = "_[F]",
        power_conversion = "Watts to kW (divide by 1000)"
      )
    } else {
      column_info <- list(
        power_suffix = " [kW]",
        voltage_suffix = " [V]",
        current_suffix = " [A]",
        power_factor_suffix = " [PF]",
        frequency_suffix = " [F]",
        power_conversion = "None (values made positive)"
      )
    }
    
    format_api_response(
      success = TRUE,
      message = paste("Column information for", meterType, "meter type"),
      data = column_info
    )
    
  }, error = function(e) {
    format_api_response(
      success = FALSE,
      message = paste("Error retrieving column information:", e$message),
      status_code = 500
    )
  })
}

#* Get available date range for files in a directory
#* @get /historic-file/date-range
#* @param dir:str Directory path to check for available files
#* @serializer json
function(dir) {
  if (is.null(dir) || !is.character(dir)) {
    return(format_api_response(
      success = FALSE,
      message = "Directory path is required",
      status_code = 400
    ))
  }
  
  if (!dir.exists(dir)) {
    return(format_api_response(
      success = FALSE,
      message = paste("Directory does not exist:", dir),
      status_code = 404
    ))
  }
  
  tryCatch({
    historic_files <- list.files(path = dir)
    
    if (length(historic_files) == 0) {
      return(format_api_response(
        success = FALSE,
        message = "No files found in the specified directory",
        status_code = 404
      ))
    }
    
    # Extract dates from filenames
    historic_dates <- sapply(strsplit(basename(historic_files), '_', fixed = TRUE), `[`, 2)
    valid_dates <- historic_dates[!is.na(historic_dates)]
    
    if (length(valid_dates) == 0) {
      return(format_api_response(
        success = FALSE,
        message = "No valid date patterns found in filenames",
        status_code = 404
      ))
    }
    
    # Convert to Date objects and get range
    date_objects <- as.Date(valid_dates)
    valid_date_objects <- date_objects[!is.na(date_objects)]
    
    if (length(valid_date_objects) == 0) {
      return(format_api_response(
        success = FALSE,
        message = "No valid dates found in filenames",
        status_code = 404
      ))
    }
    
    date_range <- list(
      earliest_date = as.character(min(valid_date_objects)),
      latest_date = as.character(max(valid_date_objects)),
      total_files = length(historic_files),
      files_with_valid_dates = length(valid_date_objects)
    )
    
    format_api_response(
      success = TRUE,
      message = "Date range retrieved successfully",
      data = date_range
    )
    
  }, error = function(e) {
    format_api_response(
      success = FALSE,
      message = paste("Error retrieving date range:", e$message),
      status_code = 500
    )
  })
}

# Historical Tab
#* Process historic ON/OFF data
#* @param historic_usage JSON string of historic usage data
#* @param leafNodes JSON array of leaf node names
#* @post /process_on_off
function(historic_usage, leafNodes) {

  # Parse inputs
  historic_usage <- fromJSON(historic_usage)
  leafNodes <- fromJSON(leafNodes)

  for (col in 2:ncol(historic_usage)) {
    historic_usage[, col] <- zoo::na.locf(historic_usage[, col], na.rm = FALSE, maxgap = 15)
  }

  # Percentile function

  percentile_75th <- function(power) {
    vals <- na.omit(power)
    vals <- vals[is.finite(vals) & vals > 0]
    if (length(vals) == 0) return(NA)
    unname(quantile(vals, 0.50))
  }
  # K-means threshold
  kmeans_threshold <- function(power) {
    vals <- na.omit(power)
    vals <- vals[is.finite(vals) & vals > 0]
    if (length(vals) < 2) return(NA)
    km <- kmeans(vals, centers = 2)
    mean(sort(km$centers))
  }

  thresholds <- sapply(leafNodes, function(col) kmeans_threshold(historic_usage[[col]]))
  percentile_75 <- sapply(leafNodes, function(col) percentile_75th(historic_usage[[col]]))


  # Compute ON/OFF state
  for (col in leafNodes) {
    thr <- thresholds[[col]]
    p75 <- percentile_75[[col]]
    state_col <- paste0(col, "_State")
    historic_usage[[state_col]] <- ifelse(
      is.na(historic_usage[[col]]) | !is.finite(historic_usage[[col]]) | is.na(thr) | !is.finite(thr),
      0,
      ifelse(
        historic_usage[[col]] > p75 & thr < p75, 1,
        ifelse(historic_usage[[col]] > thr, 1, 0)
      )
    )
  }

  mins_on  <- colSums(historic_usage[, paste0(leafNodes, "_State"), drop = FALSE] == 1, na.rm = TRUE)
  mins_off <- colSums(historic_usage[, paste0(leafNodes, "_State"), drop = FALSE] == 0, na.rm = TRUE)

  appliance_df <- data.frame(
    Appliance = leafNodes,
    Hours_ON  = round(mins_on / 60, 2),
    Hours_OFF = round(mins_off / 60, 2)
  )

  appliance_df <- appliance_df %>%
    mutate(
      total_hours = Hours_ON + Hours_OFF,
      Missing_OFF = ifelse(total_hours < 24, 24 - total_hours, 0),
      Hours_OFF = Hours_OFF + Missing_OFF
    ) %>%
    select(-total_hours, -Missing_OFF)

  appliance_df <- appliance_df[order(appliance_df$Hours_ON, appliance_df$Hours_OFF, decreasing = TRUE), ]

  return(appliance_df)  # plumber auto-converts to JSON
}

####  Live Tab Api  ####


# Helper function: Convert to numeric with NA handling
convert_to_num <- function(x) {
  x_locf <- zoo::na.locf(x, na.rm = FALSE)  # Last observation carried forward
  x_num <- suppressWarnings(as.numeric(as.character(x_locf)))
  if(all(is.na(x_num))) rep(0, length(x_num)) else x_num
}


getLiveData <- function(username) {
  tryCatch({

    # STEP 1: Load client profile and identify meter

    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    client_profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)

    # Find user's meter details
    user_row <- client_profiles[client_profiles$user == username, ]
    if (nrow(user_row) == 0) return(list(error = "Username not found"))

    meter_id <- user_row$Meter_id[1]
    meter_type <- user_row$Meter_type[1]

    # Handle multiple meter IDs (take first one)
    meter_id <- strsplit(meter_id, '; ')[[1]][1]


    # STEP 2: Get usage columns from buildingMap
    # BuildingMap tells us which columns to sum for total usage

    rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
    if (!file.exists(rds_path)) return(list(error = "BuildingMap not found"))

    buildingMap <- readRDS(rds_path)

    # Recursive function to extract leaf nodes (appliances) from tree
    retrieveNodes <- function(tree, level) {
      if (level == 0) {
        if (is.atomic(tree)) {
          return(tree)
        } else if (is.list(tree) && !is.null(tree$Name)) {
          return(tree$Name)
        } else {
          return(NA)
        }
      } else {
        if (is.list(tree)) {
          unlist(lapply(tree, retrieveNodes, level = level - 1))
        } else {
          return(NA)
        }
      }
    }

    # Find the deepest level with valid nodes
    lvl <- 0
    repeat {
      usage_cols <- retrieveNodes(buildingMap, lvl)
      if (anyNA(usage_cols)) lvl <- lvl + 1 else break
    }
    usageColumns <- as.character(usage_cols)


    # STEP 3: Process based on meter type (Neubolt or Egauge)

    if (meter_type == 'neubolt') {
      # Load configuration file that maps meter IDs to appliance names
      config_dir <- paste0('/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/', meter_id, '_Config.csv')
      config <- read.csv(config_dir, check.names = F)

      # Get unique meter IDs and extract slot numbers
      smids <- unique(config$old_name)
      smids <- smids[order(smids)]

      # Extract slot number from meter ID (SM1-xxx = slot 1, SM3-xxx = slot 3, etc.)
      # extracted_digits <- str_extract(smids, "(?<=SM)(\\d+)?(?=[-D])")
      extracted_digits <- str_extract(smids, "(?<=SM|SMD|LM|TM)(\\d+)?(?=[-D])")

      extracted_digits[extracted_digits == ''] <- "1"


      # STEP 4: Load data for each slot type (1, 3, 6, 8, 9 slot meters)
      # Each slot type has different number of measurements

      ## 1-SLOT METERS (7 columns: TimeStamp, SensorID, V, I, P, PF, F)
      db_dir <- '/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/live1/'
      slot1_meters <- smids[which(extracted_digits == '1')]
      slot1_df <- list()
      closeAllConnections()

      if (length(slot1_meters) > 0) {
        for (meter_i in 1:length(slot1_meters)) {
          csv_dir <- paste0(db_dir, slot1_meters[meter_i], '.csv')

          # Load from local CSV file
          if (file.exists(csv_dir)) {
            if (file.info(csv_dir)$size > 0) {
              db_data <- read.csv(csv_dir, check.names = F)  # Read CSV with headers
            } else next
          } else next

          # Set column names based on first row content
          if (db_data[1, 1] == 'TimeStamp') {
            colnames(db_data) = db_data[1, ]  # Use first row as column names
            db_data <- db_data[-c(1), ]  # Remove header row
          } else {
            # Assign default column names if no header row
            colnames(db_data)[1:7] <- c('TimeStamp', 'SensorID', 'Voltage', 'Current', 'Power', 'PowerFactor', 'Frequency')
          }

          # Data integrity checks
          file_replace = F
          if (ncol(db_data) > 7) {
            db_data <- db_data[, 1:7]  # Keep only first 7 columns
            file_replace = T
          }
          db_data[[1]] <- as.POSIXct(db_data[[1]], format = '%Y-%m-%d %H:%M:%S')
          if (sum(is.na(db_data[[1]])) > 0) {
            db_data <- db_data[!is.na(db_data[[1]]), ]  # Remove rows with NA timestamps
            file_replace = T
          }
          # Write back cleaned data if needed
          if (file_replace == T) write.csv(db_data, csv_dir, row.names = F)

          slot1_df[[slot1_meters[meter_i]]] <- db_data
        }
      }

      ## 3-SLOT METERS (17 columns: TimeStamp, SensorID, then V0-V2, I0-I2, P0-P2, PF0-PF2, F0-F2)
      db_dir <- '/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/live3/'
      slot3_meters <- smids[which(extracted_digits == '3')]
      slot3_df <- list()
      closeAllConnections()

      if (length(slot3_meters) > 0) {
        for (meter_i in 1:length(slot3_meters)) {
          csv_dir <- paste0(db_dir, slot3_meters[meter_i], '.csv')

          # Load from local CSV file
          if (file.exists(csv_dir)) {
            if (file.info(csv_dir)$size > 0) {
              db_data <- read.csv(csv_dir, check.names = F)  # Read CSV with headers
            } else next
          } else next

          # Set column names based on first row content
          if (db_data[1, 1] == 'TimeStamp') {
            colnames(db_data) = db_data[1, ]  # Use first row as column names
            db_data <- db_data[-c(1), ]  # Remove header row
          } else {
            # Assign default column names for 3-slot meter
            colnames(db_data) <- c('TimeStamp', 'SensorID', 'V0', 'I0', 'P0', 'PF0', 'F0',
                                   'V1', 'I1', 'P1', 'PF1', 'F1', 'V2', 'I2', 'P2', 'PF2', 'F2')
          }

          # Data integrity checks
          file_replace = F
          if (ncol(db_data) > 17) {
            db_data <- db_data[, 1:17]  # Keep only first 17 columns
            file_replace = T
          }
          db_data[[1]] <- as.POSIXct(db_data[[1]], format = '%Y-%m-%d %H:%M:%S')
          if (sum(is.na(db_data[[1]])) > 0) {
            db_data <- db_data[!is.na(db_data[[1]]), ]  # Remove rows with NA timestamps
            file_replace = T
          }
          # Write back cleaned data if needed
          if (file_replace == T) write.csv(db_data, csv_dir, row.names = F)

          slot3_df[[slot3_meters[meter_i]]] <- db_data
        }
      }

      ## 6-SLOT METERS (32 columns)
      db_dir <- '/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/live6/'
      slot6_meters <- smids[which(extracted_digits == '6')]
      slot6_df <- list()
      closeAllConnections()

      if (length(slot6_meters) > 0) {
        for (meter_i in 1:length(slot6_meters)) {
          csv_dir <- paste0(db_dir, slot6_meters[meter_i], '.csv')

          # Load from local CSV file
          if (file.exists(csv_dir)) {
            if (file.info(csv_dir)$size > 0) {
              db_data <- read.csv(csv_dir, check.names = F)  # Read CSV with headers
            } else next
          } else next

          # Set column names based on first row content
          if (db_data[1, 1] == 'TimeStamp') {
            colnames(db_data) = db_data[1, ]  # Use first row as column names
            db_data <- db_data[-c(1), ]  # Remove header row
          } else {
            # Assign default column names for 6-slot meter
            colnames(db_data) <- c('TimeStamp', 'SensorID', 'V0', 'I0', 'P0', 'PF0', 'F0',
                                   'V1', 'I1', 'P1', 'PF1', 'F1', 'V2', 'I2', 'P2', 'PF2', 'F2',
                                   'V3', 'I3', 'P3', 'PF3', 'F3', 'V4', 'I4', 'P4', 'PF4', 'F4',
                                   'V5', 'I5', 'P5', 'PF5', 'F5')
          }

          # Data integrity checks
          file_replace = F
          if (ncol(db_data) > 32) {
            db_data <- db_data[, 1:32]  # Keep only first 32 columns
            file_replace = T
          }
          db_data[[1]] <- as.POSIXct(db_data[[1]], format = '%Y-%m-%d %H:%M:%S')
          if (sum(is.na(db_data[[1]])) > 0) {
            db_data <- db_data[!is.na(db_data[[1]]), ]  # Remove rows with NA timestamps
            file_replace = T
          }
          # Write back cleaned data if needed
          if (file_replace == T) write.csv(db_data, csv_dir, row.names = F)

          slot6_df[[slot6_meters[meter_i]]] <- db_data
        }
      }

      ## 8-SLOT METERS (42 columns)
      db_dir <- '/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/live8/'
      slot8_meters <- smids[which(extracted_digits == '8')]
      slot8_df <- list()
      closeAllConnections()

      if (length(slot8_meters) > 0) {
        for (meter_i in 1:length(slot8_meters)) {
          csv_dir <- paste0(db_dir, slot8_meters[meter_i], '.csv')

          # Load from local CSV file
          if (file.exists(csv_dir)) {
            if (file.info(csv_dir)$size > 0) {
              db_data <- read.csv(csv_dir, check.names = F)  # Read CSV with headers
            } else next
          } else next

          # Set column names based on first row content
          if (db_data[1, 1] == 'TimeStamp') {
            colnames(db_data) = db_data[1, ]  # Use first row as column names
            db_data <- db_data[-c(1), ]  # Remove header row
          } else {
            # Assign default column names for 8-slot meter
            colnames(db_data) <- c('TimeStamp', 'SensorID', 'V0', 'I0', 'P0', 'PF0', 'F0',
                                   'V1', 'I1', 'P1', 'PF1', 'F1', 'V2', 'I2', 'P2', 'PF2', 'F2',
                                   'V3', 'I3', 'P3', 'PF3', 'F3', 'V4', 'I4', 'P4', 'PF4', 'F4',
                                   'V5', 'I5', 'P5', 'PF5', 'F5', 'V6', 'I6', 'P6', 'PF6', 'F6',
                                   'V7', 'I7', 'P7', 'PF7', 'F7')
          }

          # Data integrity checks
          file_replace = F
          if (ncol(db_data) > 42) {
            db_data <- db_data[, 1:42]  # Keep only first 42 columns
            file_replace = T
          }
          db_data[[1]] <- as.POSIXct(db_data[[1]], format = '%Y-%m-%d %H:%M:%S')
          if (sum(is.na(db_data[[1]])) > 0) {
            db_data <- db_data[!is.na(db_data[[1]]), ]  # Remove rows with NA timestamps
            file_replace = T
          }
          # Write back cleaned data if needed
          if (file_replace == T) write.csv(db_data, csv_dir, row.names = F)

          slot8_df[[slot8_meters[meter_i]]] <- db_data
        }
      }

      ## 9-SLOT METERS (47 columns)
      db_dir <- '/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/live9/'
      slot9_meters <- smids[which(extracted_digits == '9')]
      slot9_df <- list()
      closeAllConnections()

      if (length(slot9_meters) > 0) {
        for (meter_i in 1:length(slot9_meters)) {
          csv_dir <- paste0(db_dir, slot9_meters[meter_i], '.csv')

          # Load from local CSV file
          if (file.exists(csv_dir)) {
            if (file.info(csv_dir)$size > 0) {
              db_data <- read.csv(csv_dir, check.names = F)  # Read CSV with headers
            } else next
          } else next

          # Set column names based on first row content
          if (db_data[1, 1] == 'TimeStamp') {
            colnames(db_data) = db_data[1, ]  # Use first row as column names
            db_data <- db_data[-c(1), ]  # Remove header row
          } else {
            # Assign default column names for 9-slot meter
            colnames(db_data) <- c('TimeStamp', 'SensorID', 'V0', 'I0', 'P0', 'PF0', 'F0',
                                   'V1', 'I1', 'P1', 'PF1', 'F1', 'V2', 'I2', 'P2', 'PF2', 'F2',
                                   'V3', 'I3', 'P3', 'PF3', 'F3', 'V4', 'I4', 'P4', 'PF4', 'F4',
                                   'V5', 'I5', 'P5', 'PF5', 'F5', 'V6', 'I6', 'P6', 'PF6', 'F6',
                                   'V7', 'I7', 'P7', 'PF7', 'F7', 'V8', 'I8', 'P8', 'PF8', 'F8')
          }

          # Data integrity checks
          file_replace = F
          if (ncol(db_data) > 47) {
            db_data <- db_data[, 1:47]  # Keep only first 47 columns
            file_replace = T
          }
          db_data[[1]] <- as.POSIXct(db_data[[1]], format = '%Y-%m-%d %H:%M:%S')
          if (sum(is.na(db_data[[1]])) > 0) {
            db_data <- db_data[!is.na(db_data[[1]]), ]  # Remove rows with NA timestamps
            file_replace = T
          }
          # Write back cleaned data if needed
          if (file_replace == T) write.csv(db_data, csv_dir, row.names = F)

          slot9_df[[slot9_meters[meter_i]]] <- db_data
        }
      }


      # STEP 5: Create final data frame with 30 minutes of data at 3-second intervals

      end_time <- round_date(Sys.time(), unit = '3 seconds')
      start_time <- round_date(Sys.time() - minutes(30), unit = '3 seconds')
      time_seq <- seq.POSIXt(from = start_time, to = end_time, by = 3)

      # Prepare column names based on configured appliances
      cols_list <- unique(config$new_name)
      column_list <- paste0(cols_list, '_[V]')   # Voltage columns
      column_list <- c(column_list, paste0(cols_list, '_[A]'))  # Current columns
      column_list <- c(column_list, paste0(cols_list, '_[kW]')) # Power columns
      column_list <- c(column_list, paste0(cols_list, '_[PF]')) # PowerFactor columns
      column_list <- c(column_list, paste0(cols_list, '_[F]'))  # Frequency columns
      column_list <- column_list[order(column_list)]

      # Initialize empty dataframe with 601 rows (30 minutes * 60 seconds/minute / 3 seconds + 1)
      liveData <- as.data.frame(matrix(data = NA, nrow = 601, ncol = length(column_list)+1))
      colnames(liveData) <- c('TimeStamp', column_list)
      liveData$TimeStamp <- time_seq


      # STEP 6: Process each meter and populate data

      for (i in 1:nrow(config)) {
        current_appliance <- config$new_name[i]
        current_meter <- config$old_name[i]

        # Check if column already exists (for duplicate appliances)
        postfix <- '.1'
        if (sum(is.na(liveData[[paste0(current_appliance, '_[V]')]]), na.rm = T) == nrow(liveData)) postfix <- ''

        # Get meter data from appropriate slot dataframe
        meter_df <- data.frame()
        if (!is.null(slot1_df[[current_meter]])) meter_df <- slot1_df[[current_meter]]
        else if (!is.null(slot3_df[[current_meter]])) meter_df <- slot3_df[[current_meter]]
        else if (!is.null(slot6_df[[current_meter]])) meter_df <- slot6_df[[current_meter]]
        else if (!is.null(slot8_df[[current_meter]])) meter_df <- slot8_df[[current_meter]]
        else if (!is.null(slot9_df[[current_meter]])) meter_df <- slot9_df[[current_meter]]
        else next

        # Round timestamps to nearest 3 seconds for alignment
        meter_df$TimeStamp <- as.POSIXct(meter_df$TimeStamp)
        meter_df$TimeStamp <- round_date(meter_df$TimeStamp, unit = "3 seconds")
        meter_df <- meter_df %>%
          distinct_at(.vars = 'TimeStamp', .keep_all = T)
        tz(meter_df$TimeStamp) <- tz(liveData$TimeStamp)

        # Keep only matching timestamps
        meter_df <- meter_df[meter_df$TimeStamp %in% liveData$TimeStamp, ]
        if (nrow(meter_df) == 0) next

        # Convert measurement columns to numeric
        meter_df[, 3:ncol(meter_df)] <- apply(meter_df[, 3:ncol(meter_df)], 2, FUN = as.numeric)

        # Calculate measurements using formulas from config

        ## Voltage calculation (average if multiple phases)
        meter_df <- meter_df %>%
          dplyr::mutate(temp = eval(parse(text = config$Voltage[i])))
        number_of_slots <- length(strsplit(config$Voltage[i], "\\+") %>% unlist())
        meter_df$temp <- meter_df$temp / number_of_slots
        if (number_of_slots > 2) meter_df$temp <- meter_df$temp * sqrt(3)  # Line-to-line conversion
        liveData <- merge(liveData, meter_df[, c(which(colnames(meter_df) == 'TimeStamp'), ncol(meter_df))], by = "TimeStamp", all.x = T)
        liveData[[paste0(current_appliance, '_[V]', postfix)]] <- liveData$temp
        liveData <- liveData[, -c(which(colnames(liveData) == 'temp'))]

        ## Current calculation (average if multiple phases)
        meter_df <- meter_df %>%
          mutate(temp = eval(parse(text = config$Current[i])))
        meter_df$temp <- (meter_df$temp / number_of_slots)
        liveData <- merge(liveData, meter_df[, c(which(colnames(meter_df) == 'TimeStamp'), ncol(meter_df))], by = "TimeStamp", all.x = T)
        liveData[[paste0(current_appliance, '_[A]', postfix)]] <- liveData$temp
        liveData <- liveData[, -c(which(colnames(liveData) == 'temp'))]

        ## Power calculation (sum if multiple phases)
        meter_df <- meter_df %>% mutate(temp = eval(parse(text = config$Power[i])))
        liveData <- merge(liveData, meter_df[, c(which(colnames(meter_df) == 'TimeStamp'), ncol(meter_df))], by = "TimeStamp", all.x = T)
        liveData[[paste0(current_appliance, '_[kW]', postfix)]] <- liveData$temp
        liveData <- liveData[, -c(which(colnames(liveData) == 'temp'))]

        ## PowerFactor calculation (average if multiple phases)
        meter_df <- meter_df %>%
          mutate(temp = eval(parse(text = config$PowerFactor[i])))
        meter_df$temp <- meter_df$temp / number_of_slots
        liveData <- merge(liveData, meter_df[, c(which(colnames(meter_df) == 'TimeStamp'), ncol(meter_df))], by = "TimeStamp", all.x = T)
        liveData[[paste0(current_appliance, '_[PF]', postfix)]] <- liveData$temp
        liveData <- liveData[, -c(which(colnames(liveData) == 'temp'))]

        ## Frequency calculation (average if multiple phases)
        meter_df <- meter_df %>%
          mutate(temp = eval(parse(text = config$Frequency[i])))
        meter_df$temp <- meter_df$temp / number_of_slots
        liveData <- merge(liveData, meter_df[, c(which(colnames(meter_df) == 'TimeStamp'), ncol(meter_df))], by = "TimeStamp", all.x = T)
        liveData[[paste0(current_appliance, '_[F]', postfix)]] <- liveData$temp
        liveData <- liveData[, -c(which(colnames(liveData) == 'temp'))]
      }


      # STEP 7: Handle duplicate columns (sum for power, average for others)
      # Rename TimeStamp to DateTime for consistency
      colnames(liveData)[1] <- 'DateTime'

      # Clean column names removing .1 suffix
      for (coln in 1:length(colnames(liveData))) {
        colname <- colnames(liveData)[coln]
        if (substr(colname, nchar(colname)-1, nchar(colname)) == ".1") colname <- substr(colname, 1, nchar(colname)-2)
        colnames(liveData)[coln] <- colname
      }

      # Combine duplicate columns
      same_name_columns <- unique(names(liveData[duplicated(names(liveData))]))
      for (col_name in same_name_columns) {
        if (substr(col_name, nchar(col_name)-3, nchar(col_name)) == "[kW]")
          # Sum power columns
          liveData[[paste0(".", col_name)]] <- rowSums(liveData[, startsWith(names(liveData), col_name)], na.rm = TRUE)
        else
          # Average other columns (V, A, PF, F)
          liveData[[paste0(".", col_name)]] <- rowMeans(liveData[, startsWith(names(liveData), col_name)], na.rm = TRUE)

        liveData <- liveData[, !startsWith(names(liveData), col_name), drop = FALSE]
        column_index <- which(colnames(liveData) == paste0(".", col_name))
        colnames(liveData)[column_index] <- gsub('\\.', '', colnames(liveData)[column_index])
      }


      # STEP 8: Apply client-specific adjustments
      # Some clients have special calculation requirements
      if (username == 'dunya') {
        # Multiply Studio 5.6 light readings by 5
        if ("Studio 5.6 (Light)_[kW]" %in% colnames(liveData)) {
          liveData$`Studio 5.6 (Light)_[kW]` <- liveData$`Studio 5.6 (Light)_[kW]`*5
          liveData$`Studio 5.6 (Light)_[A]` <- liveData$`Studio 5.6 (Light)_[A]`*5
        }
      } else if (username == 'progressive') {
        # Multiply compressor readings by 5
        liveData$`Low Pressure Compressor 1_[kW]` <- liveData$`Low Pressure Compressor 1_[kW]` * 5
        liveData$`Low Pressure Compressor 1_[A]` <- liveData$`Low Pressure Compressor 1_[A]` * 5
        liveData$`Low Pressure Compressor 2_[kW]` <- liveData$`Low Pressure Compressor 2_[kW]` * 5
        liveData$`Low Pressure Compressor 2_[A]` <- liveData$`Low Pressure Compressor 2_[A]` * 5
      } else if (username == 'razakhantb1') {
        # Solar meter adjustments
        liveData$`Solar_[V]` <- (liveData$`Main_[V]` + liveData$`Motor_[V]`) / 2
        liveData$`Solar_[A]`[liveData$`Solar_[A]` < 0] <- 0
        liveData$`Solar_[kW]`[liveData$`Solar_[kW]` < 0] <- 0
        liveData$`Main_[PF]` <- (liveData$`Main_[PF]` + liveData$`Motor_[PF]`) / 2
      } else if (username == 'cobija') {
        # Building power calculations with subtraction
        liveData <- liveData %>%
          mutate(`Textile Building_[kW]` = rowSums(cbind(`Textile Building_[kW]`, -1 * `Ceiling Hall + Compressor Room_[kW]`), na.rm = TRUE))
        liveData <- liveData %>%
          mutate(`Textile Building_[A]` = rowSums(cbind(`Textile Building_[A]`, -1 * `Ceiling Hall + Compressor Room_[A]`), na.rm = TRUE))
        liveData <- liveData %>%
          mutate(`IT Hall + Stitching Area_[kW]` = rowSums(cbind(`IT Hall + Stitching Area_[kW]`, -1 * `Offices Area_[kW]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`IT Hall + Stitching Area_[A]` = rowSums(cbind(`IT Hall + Stitching Area_[A]`, -1 * `Offices Area_[A]`), na.rm = T))
      } else if (username == 'nixor') {
        # Complex building 2 calculations from multiple sources
        # Single phase calculations
        liveData <- liveData %>%
          mutate(`Building 2_[V]` = rowMeans(cbind(`Main_[V]`, `Generator_[V]`, `Solar Consumption_[V]`, `Building 1_[V]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2_[A]` = rowSums(cbind(`Main_[A]`, `Generator_[A]`, `Solar Consumption_[A]`, -1 * `Building 1_[A]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2_[kW]` = rowSums(cbind(`Main_[kW]`, `Generator_[kW]`, `Solar Consumption_[kW]`, -1 * `Building 1_[kW]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2_[PF]` = rowMeans(cbind(`Main_[PF]`, `Generator_[PF]`, `Solar Consumption_[PF]`, `Building 1_[PF]`), na.rm = T))

        # Phase 1 calculations
        liveData <- liveData %>%
          mutate(`Building 2 phase 1_[V]` = rowMeans(cbind(`Main phase 1_[V]`, `Generator phase 1_[V]`, `Solar Consumption phase 1_[V]`, `Building 1 phase 1_[V]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 1_[A]` = rowSums(cbind(`Main phase 1_[A]`, `Generator phase 1_[A]`, `Solar Consumption phase 1_[A]`, -1 * `Building 1 phase 1_[A]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 1_[kW]` = rowSums(cbind(`Main phase 1_[kW]`, `Generator phase 1_[kW]`, `Solar Consumption phase 1_[kW]`, -1 * `Building 1 phase 1_[kW]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 1_[PF]` = rowMeans(cbind(`Main phase 1_[PF]`, `Generator phase 1_[PF]`, `Solar Consumption phase 1_[PF]`, `Building 1 phase 1_[PF]`), na.rm = T))

        # Phase 2 calculations
        liveData <- liveData %>%
          mutate(`Building 2 phase 2_[V]` = rowMeans(cbind(`Main phase 2_[V]`, `Generator phase 2_[V]`, `Solar Consumption phase 2_[V]`, `Building 1 phase 2_[V]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 2_[A]` = rowSums(cbind(`Main phase 2_[A]`, `Generator phase 2_[A]`, `Solar Consumption phase 2_[A]`, -1 * `Building 1 phase 2_[A]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 2_[kW]` = rowSums(cbind(`Main phase 2_[kW]`, `Generator phase 2_[kW]`, `Solar Consumption phase 2_[kW]`, -1 * `Building 1 phase 2_[kW]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 2_[PF]` = rowMeans(cbind(`Main phase 2_[PF]`, `Generator phase 2_[PF]`, `Solar Consumption phase 2_[PF]`, `Building 1 phase 2_[PF]`), na.rm = T))

        # Phase 3 calculations
        liveData <- liveData %>%
          mutate(`Building 2 phase 3_[V]` = rowMeans(cbind(`Main phase 3_[V]`, `Generator phase 3_[V]`, `Solar Consumption phase 3_[V]`, `Building 1 phase 3_[V]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 3_[A]` = rowSums(cbind(`Main phase 3_[A]`, `Generator phase 3_[A]`, `Solar Consumption phase 3_[A]`, -1 * `Building 1 phase 3_[A]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 3_[kW]` = rowSums(cbind(`Main phase 3_[kW]`, `Generator phase 3_[kW]`, `Solar Consumption phase 3_[kW]`, -1 * `Building 1 phase 3_[kW]`), na.rm = T))
        liveData <- liveData %>%
          mutate(`Building 2 phase 3_[PF]` = rowMeans(cbind(`Main phase 3_[PF]`, `Generator phase 3_[PF]`, `Solar Consumption phase 3_[PF]`, `Building 1 phase 3_[PF]`), na.rm = T))
      }

      # Forward fill NA values (carry last observation forward) with max gap of 60 rows
      liveData <- liveData %>%
        mutate(across(where(is.numeric), ~ na.locf(., na.rm = FALSE, maxgap = 60)))


      # STEP 9: Process Virtual Registers (calculated/derived appliances)
      # Virtual registers allow creating new appliances from formulas

      vr_file <- paste0('/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/', meter_id, '_VirtualRegister.csv')
      vr_present <- file.exists(vr_file)

      if (vr_present) {
        vr_data <- read.csv(vr_file, stringsAsFactors = FALSE)

        if (nrow(vr_data) > 0) {
          for (i in 1:nrow(vr_data)) {
            vr_name <- vr_data$Name[i]
            original_formula <- trimws(vr_data$Formula[i])

            # Handle meter names with hyphens by temporarily replacing with pipes
            meter_names_with_hyphens <- list()
            meter_pattern <- "([A-Za-z0-9]+(-[A-Za-z0-9 ]+)+)"
            meter_matches <- gregexpr(meter_pattern, original_formula, perl = TRUE)
            meter_names <- unlist(regmatches(original_formula, meter_matches))

            formula <- original_formula

            for (meter_name in meter_names) {
              if (grepl("-", meter_name)) {
                replacement <- gsub("-", "|", meter_name)
                meter_names_with_hyphens[[meter_name]] <- replacement
                formula <- gsub(meter_name, replacement, formula, fixed = TRUE)
              }
            }

            components <- unlist(strsplit(formula, "(?<=\\S)\\s*(?=[+-])", perl = TRUE))

            # Process Power [kW] - sum with signs
            power_formula <- ""
            for (j in seq_along(components)) {
              comp <- trimws(components[j])
              if (comp %in% c("+", "-")) {
                power_formula <- paste0(power_formula, " ", comp, " ")
                next
              }
              operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
              comp_clean <- sub("^[+-]\\s*", "", comp)
              comp_with_hyphens <- gsub("\\|", "-", comp_clean)
              if (comp_with_hyphens != comp_clean) {
                comp_clean <- comp_with_hyphens
              }
              power_col <- paste0(comp_clean, "_[kW]")
              if (!power_col %in% colnames(liveData)) {
                liveData[[power_col]] <- NA
              }
              power_formula <- paste0(power_formula, operator, "`", power_col, "`")
            }
            power_formula <- trimws(power_formula)

            columns_in_formula <- unlist(regmatches(power_formula, gregexpr("`[^`]+`", power_formula)))
            columns_in_formula <- gsub("`", "", columns_in_formula)

            has_all_columns <- all(columns_in_formula %in% colnames(liveData))
            if (has_all_columns) {
              tryCatch({
                # Safe evaluation handling NAs
                na_safe_eval <- function(expr, data) {
                  result <- rep(NA, nrow(data))
                  for (row in 1:nrow(data)) {
                    row_vals <- sapply(columns_in_formula, function(col) data[row, col])
                    if (all(!is.na(row_vals))) {
                      result[row] <- eval(parse(text = expr), envir = as.list(data[row,]))
                    } else {
                      expr_to_eval <- gsub("`([^`]+)`", "ifelse(is.na(data[row, '\\1']), 0, data[row, '\\1'])", expr)
                      result[row] <- eval(parse(text = expr_to_eval))
                    }
                  }
                  return(result)
                }
                liveData$temp <- na_safe_eval(power_formula, liveData)
                colnames(liveData)[colnames(liveData) == "temp"] <- paste0(vr_name, "_[kW]")
              }, error = function(e) {})
            } else {
              liveData[[paste0(vr_name, "_[kW]")]] <- NA
            }

            # Process Current [A] - similar to power
            current_formula <- ""
            for (j in seq_along(components)) {
              comp <- trimws(components[j])
              if (comp %in% c("+", "-")) {
                current_formula <- paste0(current_formula, " ", comp, " ")
                next
              }
              operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else ""
              comp_clean <- sub("^[+-]\\s*", "", comp)
              comp_with_hyphens <- gsub("\\|", "-", comp_clean)
              if (comp_with_hyphens != comp_clean) {
                comp_clean <- comp_with_hyphens
              }
              current_col <- paste0(comp_clean, "_[A]")
              if (!current_col %in% colnames(liveData)) {
                liveData[[current_col]] <- NA
              }
              current_formula <- paste0(current_formula, operator, "`", current_col, "`")
            }
            current_formula <- trimws(current_formula)

            columns_in_formula <- unlist(regmatches(current_formula, gregexpr("`[^`]+`", current_formula)))
            columns_in_formula <- gsub("`", "", columns_in_formula)
            has_all_columns <- all(columns_in_formula %in% colnames(liveData))

            if (has_all_columns) {
              tryCatch({
                na_safe_eval <- function(expr, data) {
                  result <- rep(NA, nrow(data))
                  for (row in 1:nrow(data)) {
                    row_vals <- sapply(columns_in_formula, function(col) data[row, col])
                    if (all(!is.na(row_vals))) {
                      result[row] <- eval(parse(text = expr), envir = as.list(data[row,]))
                    } else {
                      expr_to_eval <- gsub("`([^`]+)`", "ifelse(is.na(data[row, '\\1']), 0, data[row, '\\1'])", expr)
                      result[row] <- eval(parse(text = expr_to_eval))
                    }
                  }
                  return(result)
                }
                liveData$temp <- na_safe_eval(current_formula, liveData)
                colnames(liveData)[colnames(liveData) == "temp"] <- paste0(vr_name, "_[A]")
              }, error = function(e) {})
            } else {
              liveData[[paste0(vr_name, "_[A]")]] <- NA
            }

            # Process Voltage [V] - average of all components
            voltage_cols <- c()
            for (j in seq_along(components)) {
              comp <- trimws(components[j])
              if (comp %in% c("+", "-")) next
              comp_clean <- sub("^[+-]\\s*", "", comp)
              comp_with_hyphens <- gsub("\\|", "-", comp_clean)
              if (comp_with_hyphens != comp_clean) {
                comp_clean <- comp_with_hyphens
              }
              voltage_col <- paste0(comp_clean, "_[V]")
              if (!voltage_col %in% colnames(liveData)) {
                liveData[[voltage_col]] <- NA
              } else if (!all(is.na(liveData[[voltage_col]]))) {
                voltage_cols <- c(voltage_cols, voltage_col)
              }
            }
            if (length(voltage_cols) > 0) {
              tryCatch({
                liveData[[paste0(vr_name, "_[V]")]] <- rowMeans(liveData[, voltage_cols, drop = FALSE], na.rm = TRUE)
              }, error = function(e) {
                liveData[[paste0(vr_name, "_[V]")]] <- NA
              })
            } else {
              liveData[[paste0(vr_name, "_[V]")]] <- NA
            }

            # Process PowerFactor [PF] - average of all components
            pf_cols <- c()
            for (j in seq_along(components)) {
              comp <- trimws(components[j])
              if (comp %in% c("+", "-")) next
              comp_clean <- sub("^[+-]\\s*", "", comp)
              comp_with_hyphens <- gsub("\\|", "-", comp_clean)
              if (comp_with_hyphens != comp_clean) {
                comp_clean <- comp_with_hyphens
              }
              pf_col <- paste0(comp_clean, "_[PF]")
              if (!pf_col %in% colnames(liveData)) {
                liveData[[pf_col]] <- NA
              } else if (!all(is.na(liveData[[pf_col]]))) {
                pf_cols <- c(pf_cols, pf_col)
              }
            }
            if (length(pf_cols) > 0) {
              tryCatch({
                liveData[[paste0(vr_name, "_[PF]")]] <- rowMeans(liveData[, pf_cols, drop = FALSE], na.rm = TRUE)
              }, error = function(e) {
                liveData[[paste0(vr_name, "_[PF]")]] <- NA
              })
            } else {
              liveData[[paste0(vr_name, "_[PF]")]] <- NA
            }

            # Process Frequency [F] - average of all components
            frequency_cols <- c()
            for (j in seq_along(components)) {
              comp <- trimws(components[j])
              if (comp %in% c("+", "-")) next
              comp_clean <- sub("^[+-]\\s*", "", comp)
              comp_with_hyphens <- gsub("\\|", "-", comp_clean)
              if (comp_with_hyphens != comp_clean) {
                comp_clean <- comp_with_hyphens
              }
              frequency_col <- paste0(comp_clean, "_[F]")
              if (!frequency_col %in% colnames(liveData)) {
                liveData[[frequency_col]] <- NA
              } else if (!all(is.na(liveData[[frequency_col]]))) {
                frequency_cols <- c(frequency_cols, frequency_col)
              }
            }
            if (length(frequency_cols) > 0) {
              tryCatch({
                liveData[[paste0(vr_name, "_[F]")]] <- rowMeans(liveData[, frequency_cols, drop = FALSE], na.rm = TRUE)
              }, error = function(e) {
                liveData[[paste0(vr_name, "_[F]")]] <- NA
              })
            } else {
              liveData[[paste0(vr_name, "_[F]")]] <- NA
            }
          }

          # Remove temporary columns with pipes
          columns_with_pipes <- grep("\\|", colnames(liveData), value = TRUE)
          if (length(columns_with_pipes) > 0) {
            liveData <- liveData[, !colnames(liveData) %in% columns_with_pipes]
          }
        }
      }


      # STEP 10: Create column lists for UI filtering
      powerColumns <- grep('[kW]', colnames(liveData), value = TRUE, fixed = TRUE)
      powerColumns <- powerColumns[!grepl("\\|", powerColumns)]  # Remove columns with pipes

      voltageColumns <- grep('[V]', colnames(liveData), value = TRUE, fixed = TRUE)
      voltageColumns <- voltageColumns[!grepl("\\|", voltageColumns)]  # Remove columns with pipes

      currentColumns <- grep('[A]', colnames(liveData), value = TRUE, fixed = TRUE)
      currentColumns <- currentColumns[!grepl("\\|", currentColumns)]  # Remove columns with pipes

      powerfactorColumns <- grep('[PF]', colnames(liveData), value = TRUE, fixed = TRUE)
      powerfactorColumns <- powerfactorColumns[!grepl("\\|", powerfactorColumns)]  # Remove columns with pipes

      frequencyColumns <- grep('[F]', colnames(liveData), value = TRUE, fixed = TRUE)
      frequencyColumns <- frequencyColumns[!grepl("\\|", frequencyColumns)]  # Remove columns with pipes


      # STEP 11: Calculate total usage and convert units

      # Create usage column names with _[kW] suffix
      usageColumns_kw <- paste0(usageColumns, '_[kW]')
      usageColumns_kw <- intersect(usageColumns_kw, colnames(liveData))

      # Calculate total usage by summing usage columns
      if (length(usageColumns_kw) > 1) {
        liveData$Usage = rowSums(liveData[, usageColumns_kw], na.rm = T)
      } else if (length(usageColumns_kw) == 1) {
        liveData$Usage = liveData[, usageColumns_kw]
      } else {
        liveData$Usage = NA
      }

      # Move Usage column to front (after DateTime)
      liveData <- liveData %>%
        relocate('Usage', .after = 'DateTime')

      # Convert from watts to kilowatts
      liveData <- liveData %>%
        mutate(across(contains("[kW]"), ~ round(. / 1000, 2)))
      if ('Usage' %in% colnames(liveData)) liveData$Usage <- round(liveData$Usage / 1000, 2)

      # Remove last 5 rows (incomplete data)
      liveData <- liveData[1:(nrow(liveData)-5), ]

    }

    # Return the processed data (common for both neubolt and egauge)
    return(list(
      liveData = liveData,
      powerColumns = powerColumns,
      voltageColumns = voltageColumns,
      currentColumns = currentColumns,
      powerfactorColumns = powerfactorColumns,
      frequencyColumns = frequencyColumns,
      usageColumns = usageColumns,
      meterType = meter_type
    ))

  }, error = function(e) {
    return(list(error = as.character(e$message)))
  })
}


# API ENDPOINTS - These just call getLiveData and format responses

#* Main endpoint: Get all live data
#* @param username User's name
#* @get /liveData
#* @serializer json list(na='null')
function(username) {
  result <- getLiveData(username)
  return(result)
}


# TILE 4: VIRTUAL REGISTER API


#* Get virtual registers for a user
#* @param username User's username
#* @get /virtual-registers
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    # Get meter ID
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    meter_type <- user_row$Meter_type
    
    # Load VR file
    vr_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/",
                      meter_id, "_VirtualRegister.csv")
    
    if(!file.exists(vr_path)) {
      vr_data <- data.frame(Name = character(), Formula = character(), stringsAsFactors = FALSE)
    } else {
      vr_data <- read.csv(vr_path, stringsAsFactors = FALSE)
      if (nrow(vr_data) > 0 && !all(c("Name", "Formula") %in% colnames(vr_data))) {
        vr_data <- data.frame(Name = character(), Formula = character(), stringsAsFactors = FALSE)
      }
    }
    
    # Get available components from config
    available_components <- character()
    
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                            meter_id, "_Config.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        available_components <- unique(config$Display_name)
      }
    } else {
      first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/",
                            first_meter_id, "_New_column_names.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        available_components <- unique(config$New_name)
      }
    }
    
    return(list(
      virtualRegisters = vr_data,
      availableComponents = available_components,
      meterType = meter_type
    ))
    
  }, error = function(e) {
    return(list(error = paste("Error fetching virtual registers:", e$message)))
  })
}

#### Start Of INFO Tab API  ####
#* Get daily reports setting for a user
#* @param username User's username
#* @get /daily-reports
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
    
    if(!file.exists(settings_path)) {
      return(list(dailyReports = FALSE))
    }
    
    settings <- read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- settings[settings$user == username, ]
    
    if(nrow(user_row) > 0 && "Daily_Reports" %in% names(settings)) {
      return(list(dailyReports = isTRUE(as.logical(user_row$Daily_Reports))))
    } else {
      return(list(dailyReports = FALSE))
    }
    
  }, error = function(e) {
    return(list(dailyReports = FALSE))
  })
}

####  PERSONAL DETAILS API  ####

#* Get personal details for a user
#* @param username User's username
#* @get /personal-details
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    # Read settings data
    settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
    settings <- if(file.exists(settings_path)) {
      read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
    } else {
      data.frame()
    }
    
    # Read profiles data
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- if(file.exists(profiles_path)) {
      read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    } else {
      data.frame()
    }
    
    # Initialize response
    response <- list(
      userName = "",
      email = "",
      phoneNumbers = list(),
      sanctionedLoad = NULL,
      unitPrice = NULL,
      customerID = ""
    )
    
    # Get settings data
    if(nrow(settings) > 0 && username %in% settings$user) {
      user_row <- settings[settings$user == username, ]
      response$userName <- user_row$`User's_Name` %||% ""
      response$email <- user_row$email %||% ""
      
      # Parse phone numbers
      phone_str <- user_row$Phone_Number %||% ""
      if(nzchar(phone_str)) {
        response$phoneNumbers <- trimws(strsplit(phone_str, ",")[[1]])
      }
      
      response$sanctionedLoad <- as.numeric(user_row$sanctionedLoad) %||% NULL
      response$unitPrice <- as.numeric(user_row$unitPrice) %||% NULL
    }
    
    # Get customer ID from profiles
    if(nrow(profiles) > 0 && username %in% profiles$user) {
      user_profile <- profiles[profiles$user == username, ]
      response$customerID <- user_profile$Customer_ID %||% ""
    }
    
    return(response)
    
  }, error = function(e) {
    return(list(error = paste("Error fetching personal details:", e$message)))
  })
}

#* Upload logo for a user
#* @param req The request object
#* @post /upload-logo
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    logoData <- body$logoData  # Base64 encoded image
    
    if(is.null(username) || is.null(logoData)) {
      return(list(error = "Username and logo data are required"))
    }
    
    # Define www directory path
    www_dir <- "/srv/shiny-server/EnvizFleet/www/"
    
    
    # Create filename with pattern: logo_clientname.png
    filename <- paste0("logo_", username, ".png")
    filepath <- file.path(www_dir, filename)
    
    # Decode base64 and save file
    logoData <- sub("^data:image/png;base64,", "", logoData)
    writeBin(jsonlite::base64_dec(logoData), filepath)
    
    return(list(
      success = TRUE, 
      message = "Logo uploaded successfully!",
      filename = filename
    ))
    
  }, error = function(e) {
    return(list(error = paste("Error uploading logo:", e$message)))
  })
}

#* Update personal details for a user
#* @param req The request object
#* @post /personal-details
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    userName <- body$userName %||% ""
    email <- body$email %||% ""
    phoneNumbers <- body$phoneNumbers %||% ""
    sanctionedLoad <- body$sanctionedLoad %||% NA
    unitPrice <- body$unitPrice %||% NA
    customerID <- body$customerID %||% ""
    
    # Ensure data directory exists
    data_dir <- "/srv/shiny-server/Clients Usage Data"
    
    # Read existing settings
    settings_path <- file.path(data_dir, "client_settings.csv")
    settings <- if(file.exists(settings_path)) {
      read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
    } else {
      data.frame(user = character(), `User's_Name` = character(), 
                 email = character(), Phone_Number = character(),
                 sanctionedLoad = numeric(), unitPrice = numeric(),
                 stringsAsFactors = FALSE, check.names = FALSE)
    }
    
    # Ensure all columns exist
    req_cols <- c("user", "User's_Name", "email", "Phone_Number", "sanctionedLoad", "unitPrice")
    for(col in req_cols) {
      if(!col %in% names(settings)) {
        settings[[col]] <- if(col %in% c("sanctionedLoad", "unitPrice")) 0 else NA
      }
    }
    
    # Prepare values
    sanctionedLoadValue <- if(is.na(sanctionedLoad)) 0 else sanctionedLoad
    unitPriceValue <- if(is.na(unitPrice)) 0 else unitPrice
    
    # Update or append
    if(username %in% settings$user) {
      idx <- which(settings$user == username)
      settings[idx, "User's_Name"] <- userName
      settings[idx, "email"] <- email
      settings[idx, "Phone_Number"] <- phoneNumbers
      settings[idx, "sanctionedLoad"] <- sanctionedLoadValue
      settings[idx, "unitPrice"] <- unitPriceValue
    } else {
      new_row <- data.frame(
        user = username,
        `User's_Name` = userName,
        email = email,
        Phone_Number = phoneNumbers,
        sanctionedLoad = sanctionedLoadValue,
        unitPrice = unitPriceValue,
        stringsAsFactors = FALSE,
        check.names = FALSE
      )
      settings <- rbind(settings, new_row)
    }
    
    # Write settings back to CSV
    write.csv(settings, settings_path, row.names = FALSE, quote = TRUE)
    
    # Update profiles for customer ID
    profiles_path <- file.path(data_dir, "client_profiles.csv")
    profiles <- if(file.exists(profiles_path)) {
      read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    } else {
      data.frame(user = character(), Customer_ID = character(),
                 stringsAsFactors = FALSE, check.names = FALSE)
    }
    
    if(username %in% profiles$user) {
      profiles$Customer_ID[profiles$user == username] <- customerID
    } else {
      profiles <- rbind(profiles, data.frame(
        user = username,
        Customer_ID = customerID,
        stringsAsFactors = FALSE,
        check.names = FALSE
      ))
    }
    
    write.csv(profiles, profiles_path, row.names = FALSE, quote = TRUE)
    
    return(list(success = TRUE, message = "Info updated successfully!"))
    
  }, error = function(e) {
    return(list(error = paste("Error updating personal details:", e$message)))
  })
}

#### Machine Settings ####

#* Get machine settings for a user
#* @param username User's username
#* @get /machine-settings
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    # Get meter info
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    meter_type <- user_row$Meter_type
    
    # Load config based on meter type
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                            meter_id, "_Config.csv")
      
      if(!file.exists(config_path)) {
        return(list(error = "Configuration file not found"))
      }
      
      config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Add missing columns if needed
      required_cols <- c("Display_name", "Load_type", "Rated_Power", 
                         "Working_Hour_Start", "Working_Hour_END", "ON_OFF_Alert", "Color")
      for(col in required_cols) {
        if(!col %in% names(config)) {
          config[[col]] <- NA
        }
      }
      
      # Filter out phase data
      machines <- config[!grepl("(?i)phase\\s*[123]", config$Display_name), ]
      
      return(list(
        meterType = meter_type,
        machines = machines
      ))
      
    } else {
    #   # Egauge handling
    #   first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
    #   config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", 
    #                         first_meter_id, "_New_column_names.csv")
    #   
    #   if(!file.exists(config_path)) {
    #     return(list(error = "Configuration file not found"))
    #   }
    #   
    #   config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   
    #   # For egauge, ensure basic columns exist and add machine setting columns
    #   required_cols <- c("New_name", "Color", "Load_type", "Rated_Power", 
    #                      "Working_Hour_Start", "Working_Hour_END", "ON_OFF_Alert")
    #   for(col in required_cols) {
    #     if(!col %in% names(config)) {
    #       config[[col]] <- NA
    #     }
    #   }
    #   
    #   #Filter and process egauge data
    #   filtered <- config[!grepl("(?i)phase\\s*[123]", config$New_name), ]
    #   # Remove units like [kW], [V], [A], [PF] to get clean names
    #   filtered$clean_name <- gsub(" \\[[A-Za-z]+\\]", "", filtered$New_name)
    #   # Get unique machine names
    #   unique_names <- unique(filtered$clean_name)
    #   # Keep first occurrence of each unique name
    #   machines <- filtered[match(unique_names, filtered$clean_name), ]
    #   # Use clean name for display
    #   machines$Display_name <- machines$clean_name
    # 
    #   return(list(
    #     meterType = meter_type,
    #     machines = machines[, c("Display_name", required_cols[-1])]
    #   ))
    # }
    # 
      # Egauge handling
      first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", 
                            first_meter_id, "_New_column_names.csv")
      
      if(!file.exists(config_path)) {
        return(list(error = "Configuration file not found"))
      }
      
      config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Filter out NA/empty rows FIRST
      config <- config[!is.na(config$New_name) & nchar(trimws(config$New_name)) > 0, ]
      
      # Filter out phase data
      machines <- config[!grepl("(?i)phase\\s*[123]", config$New_name), ]
      
      # Ensure basic columns exist
      for(col in c("Old_name", "New_name", "Color", "blindColors")) {
        if(!col %in% names(machines)) {
          machines[[col]] <- NA
        }
      }
      
      return(list(
        meterType = meter_type,
        machines = machines  # Return as-is with New_name column
      ))
    }
  }, error = function(e) {
    return(list(error = paste("Error fetching machine settings:", e$message)))
  })
}

#* Update machine settings
#* @param req The request object
#* @post /machine-settings
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    machineName <- body$machineName
    loadType <- body$loadType %||% NA
    ratedPower <- body$ratedPower %||% NA
    startHour <- body$startHour %||% NA
    endHour <- body$endHour %||% NA
    onOffAlert <- body$onOffAlert %||% "OFF"
    color <- body$color %||% NA
    
    # Validation
    if(!is.null(startHour) && !is.na(startHour)) {
      if(startHour < 0 || startHour > 23) {
        return(list(error = "Start Hour must be between 0 and 23"))
      }
    }
    
    if(!is.null(endHour) && !is.na(endHour)) {
      if(endHour < 0 || endHour > 23) {
        return(list(error = "End Hour must be between 0 and 23"))
      }
    }
    
    if(!is.null(ratedPower) && !is.na(ratedPower)) {
      if(ratedPower < 0) {
        return(list(error = "Rated Power cannot be negative"))
      }
    }
    
    # Get meter info
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    meter_type <- user_row$Meter_type
    
    # Load and update config
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                            meter_id, "_Config.csv")
      config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Find machine rows
      machine_idx <- which(config$Display_name == machineName)
      
    } else {
      first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/",
                            first_meter_id, "_New_column_names.csv")
      config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Find all rows matching the machine name pattern
      base_name <- machineName
      pattern <- paste0("^", base_name, " \\[")
      machine_idx <- which(grepl(pattern, config$New_name))
    }
    
    if(length(machine_idx) == 0) {
      return(list(error = "Machine not found"))
    }
    
    # Update fields
    if("Load_type" %in% colnames(config)) {
      config[machine_idx, "Load_type"] <- if(!is.null(loadType) && loadType != "") loadType else NA
    } else {
      config$Load_type <- NA
      config[machine_idx, "Load_type"] <- if(!is.null(loadType) && loadType != "") loadType else NA
    }
    
    if("Rated_Power" %in% colnames(config)) {
      config[machine_idx, "Rated_Power"] <- if(!is.null(ratedPower) && !is.na(ratedPower)) {
        as.numeric(ratedPower)
      } else NA
    } else {
      config$Rated_Power <- NA
      config[machine_idx, "Rated_Power"] <- if(!is.null(ratedPower) && !is.na(ratedPower)) as.numeric(ratedPower) else NA
    }
    
    if("Working_Hour_Start" %in% colnames(config)) {
      config[machine_idx, "Working_Hour_Start"] <- if(!is.null(startHour) && !is.na(startHour)) {
        as.numeric(startHour) %% 24
      } else NA
    } else {
      config$Working_Hour_Start <- NA
      config[machine_idx, "Working_Hour_Start"] <- if(!is.null(startHour) && !is.na(startHour)) as.numeric(startHour) %% 24 else NA
    }
    
    if("Working_Hour_END" %in% colnames(config)) {
      config[machine_idx, "Working_Hour_END"] <- if(!is.null(endHour) && !is.na(endHour)) {
        as.numeric(endHour) %% 24
      } else NA
    } else {
      config$Working_Hour_END <- NA
      config[machine_idx, "Working_Hour_END"] <- if(!is.null(endHour) && !is.na(endHour)) as.numeric(endHour) %% 24 else NA
    }
    
    if("ON_OFF_Alert" %in% colnames(config)) {
      config[machine_idx, "ON_OFF_Alert"] <- if(!is.null(onOffAlert) && onOffAlert != "") {
        onOffAlert
      } else "OFF"
    } else {
      config$ON_OFF_Alert <- NA
      config[machine_idx, "ON_OFF_Alert"] <- if(!is.null(onOffAlert)) onOffAlert else "OFF"
    }
    
    if("Color" %in% colnames(config)) {
      current_color <- config[machine_idx[1], "Color"]
      input_color <- color
      
      if(!is.null(current_color) && !is.na(current_color) && current_color != "") {
        if(is.null(input_color) || input_color == current_color || input_color == "#000000") {
          config[machine_idx, "Color"] <- current_color
        } else {
          config[machine_idx, "Color"] <- input_color
        }
      } else {
        config[machine_idx, "Color"] <- if(!is.null(input_color) && input_color != "" && input_color != "#000000") {
          input_color
        } else NA
      }
    } else {
      config$Color <- NA
      config[machine_idx, "Color"] <- if(!is.null(color) && color != "" && color != "#000000") color else NA
    }
    
    # Create a temporary copy and ensure NO extra columns are added
    config_to_save <- config
    
    # Remove any unwanted columns
    unwanted_cols <- c("Meter_id", "Working_Hour_End")
    config_to_save <- config_to_save[, !colnames(config_to_save) %in% unwanted_cols, drop = FALSE]
    
    # Save config
    write.csv(config_to_save, config_path, row.names = FALSE)
    
    return(list(success = TRUE, message = paste("Configuration for", machineName, "saved!")))
    
  }, error = function(e) {
    return(list(error = paste("Error updating machine settings:", e$message)))
  })
}




#### Space for Other Functions 
####  Notifications ####
#* Get notification settings for a user
#* @param username User's username
#* @get /notifications
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
    
    if(!file.exists(settings_path)) {
      return(list(
        sanctionedLoad = NULL,
        sanctionedLoadAlert = FALSE,
        minimumThreshold = NULL,
        minimumThresholdAlert = FALSE,
        dailyUnitsLimit = NULL,
        dailyUnitsAlert = FALSE,
        mainGenDualityAlert = FALSE,
        voltageOutrangeAlert = FALSE,
        dailySummaryAlert = FALSE,
        powerCutoffAlert = FALSE
      ))
    }
    
    settings <- read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    usr <- trimws(tolower(username))
    
    if(!(usr %in% settings$user)) {
      return(list(
        sanctionedLoad = NULL,
        sanctionedLoadAlert = FALSE,
        minimumThreshold = NULL,
        minimumThresholdAlert = FALSE,
        dailyUnitsLimit = NULL,
        dailyUnitsAlert = FALSE,
        mainGenDualityAlert = FALSE,
        voltageOutrangeAlert = FALSE,
        dailySummaryAlert = FALSE,
        powerCutoffAlert = FALSE
      ))
    }
    
    user_row <- settings[settings$user == usr, ]
    
    return(list(
      sanctionedLoad = as.numeric(user_row$sanctionedLoad) %||% NULL,
      sanctionedLoadAlert = isTRUE(as.logical(user_row$Sanctioned_Load_Alert)),
      minimumThreshold = as.numeric(user_row$minimumThreshold) %||% NULL,
      minimumThresholdAlert = isTRUE(as.logical(user_row$Minimum_Threshold_Alert)),
      dailyUnitsLimit = as.numeric(user_row$daily_units_limit) %||% NULL,
      dailyUnitsAlert = isTRUE(as.logical(user_row$Daily_Units_Consumption_Alert)),
      mainGenDualityAlert = isTRUE(as.logical(user_row$Main_Gen_Duality_Alert)),
      voltageOutrangeAlert = isTRUE(as.logical(user_row$Main_Voltage_Outrange_Alert)),
      dailySummaryAlert = isTRUE(as.logical(user_row$Daily_Summary_Alert)),
      powerCutoffAlert = isTRUE(as.logical(user_row$Power_Cutoff_Alert))
    ))
    
  }, error = function(e) {
    return(list(error = paste("Error fetching notification settings:", e$message)))
  })
}

#* Update notification settings
#* @param req The request object
#* @post /notifications
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- trimws(tolower(body$username))
    
    settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
    
    # Expected columns
    expected_columns <- c(
      "user",
      "sanctionedLoad",
      "Sanctioned_Load_Alert",
      "minimumThreshold",
      "Minimum_Threshold_Alert",
      "daily_units_limit",
      "Daily_Units_Consumption_Alert",
      "Main_Gen_Duality_Alert",
      "Main_Voltage_Outrange_Alert",
      "Daily_Summary_Alert",
      "Power_Cutoff_Alert"
    )
    
    # Read or initialize settings
    settings <- if(file.exists(settings_path)) {
      df <- read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
      # Add missing columns
      for (col in setdiff(expected_columns, names(df))) {
        if (col == "user") {
          df[[col]] <- rep(NA_character_, nrow(df))
        } else if (grepl("Alert$", col)) {
          df[[col]] <- rep(NA, nrow(df))
        } else {
          df[[col]] <- rep(NA_real_, nrow(df))
        }
      }
      df
    } else {
      data.frame(user = character(), stringsAsFactors = FALSE)
    }
    
    # Find or create user row
    i <- which(settings$user == username)
    if (length(i) == 0) {
      new_row <- data.frame(user = username, stringsAsFactors = FALSE)
      for (col in setdiff(expected_columns, "user")) {
        new_row[[col]] <- if (grepl("Alert$", col)) NA else NA_real_
      }
      settings <- rbind(settings, new_row)
      i <- nrow(settings)
    }
    
    # Update all settings
    settings[i, "sanctionedLoad"] <- body$sanctionedLoad %||% NA
    settings[i, "Sanctioned_Load_Alert"] <- body$sanctionedLoadAlert %||% FALSE
    settings[i, "minimumThreshold"] <- body$minimumThreshold %||% NA
    settings[i, "Minimum_Threshold_Alert"] <- body$minimumThresholdAlert %||% FALSE
    settings[i, "daily_units_limit"] <- body$dailyUnitsLimit %||% NA
    settings[i, "Daily_Units_Consumption_Alert"] <- body$dailyUnitsAlert %||% FALSE
    settings[i, "Main_Gen_Duality_Alert"] <- body$mainGenDualityAlert %||% FALSE
    settings[i, "Main_Voltage_Outrange_Alert"] <- body$voltageOutrangeAlert %||% FALSE
    settings[i, "Daily_Summary_Alert"] <- body$dailySummaryAlert %||% FALSE
    settings[i, "Power_Cutoff_Alert"] <- body$powerCutoffAlert %||% FALSE
    
    # Save settings
    write.csv(settings, settings_path, row.names = FALSE, quote = TRUE)
    
    return(list(success = TRUE, message = "Settings saved successfully!"))
    
  }, error = function(e) {
    return(list(error = paste("Error updating notification settings:", e$message)))
  })
}

##### Virtual Register APIS ####
# Helper function for null coalescing
`%||%` <- function(x, y) if(is.null(x) || length(x) == 0 || is.na(x)) y else x


# HELPER FUNCTIONS FROM ORIGINAL CODE
# Helper function to check if formula contains any phase components
formulaHasPhaseComponents <- function(formula) {
  grepl("phase 1|phase 2|phase 3", formula, ignore.case = TRUE)
}

# Recursive function to extract leaf nodes (appliances) from tree
retrieveNodes <- function(tree, level) {
  if (level == 0) {
    if (is.atomic(tree)) {
      return(tree)
    } else if (is.list(tree) && !is.null(tree$Name)) {
      return(tree$Name)
    } else {
      return(NA)
    }
  } else {
    if (is.list(tree)) {
      unlist(lapply(tree, retrieveNodes, level = level - 1))
    } else {
      return(NA)
    }
  }
}

# Get nodes at specific level
getNodesAtLevel <- function(buildingMap, level) {
  retrieveNodes(buildingMap, level)
}

# Check if formula has all same operators
checkFormulaOperators <- function(formula) {
  formula <- trimws(formula)
  matches <- gregexpr("(^|\\s)[+-]\\s", formula)[[1]]
  
  if (matches[1] == -1) {
    return(list(allSame = FALSE, operator = NULL))
  }
  
  operators <- c()
  for (pos in matches) {
    if (pos == 1) {
      operators <- c(operators, substr(formula, pos, pos))
    } else {
      operators <- c(operators, substr(formula, pos + 1, pos + 1))
    }
  }
  
  if (length(operators) > 0 && length(unique(operators)) == 1) {
    return(list(allSame = TRUE, operator = operators[1]))
  } else {
    return(list(allSame = FALSE, operator = NULL))
  }
}

# Extract component names from formula
extractComponentsFromFormula <- function(formula) {
  formula <- trimws(formula)
  formula <- sub("^[+-]\\s*", "", formula)
  components <- strsplit(formula, "\\s+[+-]\\s+")[[1]]
  components <- trimws(components)
  components <- unique(components[components != ""])
  return(components)
}

# Find node location in building map
findNodeLocation <- function(buildingMap, nodeName, currentPath = c()) {
  for (key in names(buildingMap)) {
    if (key != "Name" && key == nodeName) {
      return(list(parentPath = currentPath, nodePath = c(currentPath, key)))
    }
    
    if (key != "Name" && is.list(buildingMap[[key]])) {
      if (!is.null(buildingMap[[key]]$Name) && buildingMap[[key]]$Name == nodeName) {
        return(list(parentPath = currentPath, nodePath = c(currentPath, key)))
      }
      
      result <- findNodeLocation(buildingMap[[key]], nodeName, c(currentPath, key))
      if (!is.null(result)) {
        return(result)
      }
    }
  }
  
  return(NULL)
}

# Extract node from anywhere in tree
extractNodeFromAnywhere <- function(buildingMap, nodeName) {
  if (nodeName %in% names(buildingMap)) {
    return(buildingMap[[nodeName]])
  }
  
  for (key in names(buildingMap)) {
    if (key != "Name" && is.list(buildingMap[[key]])) {
      if (!is.null(buildingMap[[key]]$Name) && buildingMap[[key]]$Name == nodeName) {
        return(buildingMap[[key]])
      }
      
      result <- extractNodeFromAnywhere(buildingMap[[key]], nodeName)
      if (!is.null(result)) {
        return(result)
      }
    }
  }
  
  return(NULL)
}

# Remove node from anywhere in tree
removeNodeFromAnywhere <- function(buildingMap, nodeName) {
  if (nodeName %in% names(buildingMap)) {
    buildingMap[[nodeName]] <- NULL
    return(buildingMap)
  }
  
  for (key in names(buildingMap)) {
    if (key != "Name" && is.list(buildingMap[[key]])) {
      if (!is.null(buildingMap[[key]]$Name) && buildingMap[[key]]$Name == nodeName) {
        buildingMap[[key]] <- NULL
      } else {
        buildingMap[[key]] <- removeNodeFromAnywhere(buildingMap[[key]], nodeName)
      }
    }
  }
  
  return(buildingMap)
}

# Move nodes under parent
# In settings_api.R, update this function:
# For move Nodes Under Parent 
moveNodesUnderParent <- function(buildingMap, parentPath, parentName, childNames) {
  # First check if we can find all components
  missingComponents <- c()
  for (childName in childNames) {
    if (is.null(findNodeLocation(buildingMap, childName))) {
      missingComponents <- c(missingComponents, childName)
    }
  }
  
  if (length(missingComponents) > 0) {
    # DON'T FAIL - JUST CREATE VR AS STANDALONE NODE
    parentNode <- structure(list(), Name = parentName)
    buildingMap[[parentName]] <- parentNode
    return(buildingMap)
  }
  
  # Check if all children have same parent
  parentPaths <- list()
  for (childName in childNames) {
    location <- findNodeLocation(buildingMap, childName)
    if (!is.null(location)) {
      parentPaths[[childName]] <- location$parentPath
    }
  }
  
  # Check if all have same parent
  uniquePaths <- unique(lapply(parentPaths, function(p) paste(p, collapse = "/")))
  sameParent <- length(uniquePaths) == 1
  
  # Determine where to place the new parent
  if (sameParent && length(parentPaths[[1]]) > 0) {
    targetPath <- parentPaths[[1]]
  } else {
    targetPath <- c() # Root level
  }
  
  # Collect all child nodes with their structure
  childNodes <- list()
  for (childName in childNames) {
    node <- extractNodeFromAnywhere(buildingMap, childName)
    if (!is.null(node)) {
      childNodes[[childName]] <- node
    }
  }
  
  # Remove nodes from their current locations
  for (childName in childNames) {
    buildingMap <- removeNodeFromAnywhere(buildingMap, childName)
  }
  
  # Create new parent node
  parentNode <- structure(childNodes, Name = parentName)
  
  # Add the new parent at target location
  if (length(targetPath) == 0) {
    buildingMap[[parentName]] <- parentNode
  } else {
    buildingMap <- addNodeAtPath(buildingMap, targetPath, parentName, parentNode)
  }
  
  return(buildingMap)
}

# Add node at path
addNodeAtPath <- function(buildingMap, path, nodeName, nodeContent) {
  if (length(path) == 0) {
    buildingMap[[nodeName]] <- nodeContent
    return(buildingMap)
  }
  
  if (length(path) == 1) {
    if (path[1] %in% names(buildingMap)) {
      buildingMap[[path[1]]][[nodeName]] <- nodeContent
    }
    return(buildingMap)
  }
  
  if (path[1] %in% names(buildingMap)) {
    buildingMap[[path[1]]] <- addNodeAtPath(buildingMap[[path[1]]], path[-1], nodeName, nodeContent)
  }
  
  return(buildingMap)
}

# Remove node from building map
removeNodeFromBuildingMap <- function(buildingMap, nodeName) {
  if (nodeName %in% names(buildingMap)) {
    nodeToDelete <- buildingMap[[nodeName]]
    
    if (is.list(nodeToDelete)) {
      for (childKey in names(nodeToDelete)) {
        if (childKey != "Name" && is.list(nodeToDelete[[childKey]])) {
          buildingMap[[childKey]] <- nodeToDelete[[childKey]]
        }
      }
    }
    
    buildingMap[[nodeName]] <- NULL
    return(buildingMap)
  }
  
  for (key in names(buildingMap)) {
    if (key != "Name" && is.list(buildingMap[[key]])) {
      buildingMap[[key]] <- removeNodeFromBuildingMap(buildingMap[[key]], nodeName)
    }
  }
  
  return(buildingMap)
}
#### END of VR Helper Fucntions #####
# TILE 4: VIRTUAL REGISTER API


#* Get virtual registers for a user
#* @param username User's username
#* @get /virtual-registers
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    # Get meter ID
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    meter_type <- user_row$Meter_type
    
    # Load VR file
    vr_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/",
                      meter_id, "_VirtualRegister.csv")
    
    if(!file.exists(vr_path)) {
      vr_data <- data.frame(Name = character(), Formula = character(), stringsAsFactors = FALSE)
    } else {
      vr_data <- read.csv(vr_path, stringsAsFactors = FALSE)
      if (nrow(vr_data) > 0 && !all(c("Name", "Formula") %in% colnames(vr_data))) {
        vr_data <- data.frame(Name = character(), Formula = character(), stringsAsFactors = FALSE)
      }
    }
    
    # Get available components from config
    available_components <- character()
    
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                            meter_id, "_Config.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        available_components <- unique(config$Display_name)
      }
    } else {
      first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/",
                            first_meter_id, "_New_column_names.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        available_components <- unique(config$New_name)
      }
    }
    
    return(list(
      virtualRegisters = vr_data,
      availableComponents = available_components,
      meterType = meter_type
    ))
    
  }, error = function(e) {
    return(list(error = paste("Error fetching virtual registers:", e$message)))
  })
}



#* Add a virtual register
#* @param req The request object
#* @post /virtual-registers
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    name <- body$name
    formula <- body$formula
    buildingMapLocation <- body$buildingMapLocation %||% ""
    
    # Validation
    if(trimws(name) == "") {
      return(list(error = "Cannot add Virtual Register: Name cannot be empty."))
    }
    
    if(trimws(formula) == "") {
      return(list(error = "Please add components to the formula."))
    }
    
    # Get meter info
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    meter_type <- user_row$Meter_type
    
    # Check for duplicate names in VR
    vr_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/",
                      meter_id, "_VirtualRegister.csv")
    
    existing_vr <- if(file.exists(vr_path)) {
      read.csv(vr_path, stringsAsFactors = FALSE)
    } else {
      data.frame(Name = character(), Formula = character(), stringsAsFactors = FALSE)
    }
    
    normalized_input <- tolower(gsub("\\s+", "", name))
    normalized_vr_names <- tolower(gsub("\\s+", "", existing_vr$Name))
    
    if(normalized_input %in% normalized_vr_names) {
      return(list(error = "Virtual Register name already exists."))
    }
    
    # Check against machine names
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                            meter_id, "_Config.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        normalized_display_names <- tolower(gsub("\\s+", "", config$Display_name))
        if(normalized_input %in% normalized_display_names) {
          return(list(error = "Machine Name Already Exist."))
        }
      }
    } else {
      first_meter_id <- strsplit(meter_id, "; ")[[1]][1]
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/",
                            first_meter_id, "_New_column_names.csv")
      if(file.exists(config_path)) {
        config <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
        normalized_new_names <- tolower(gsub("\\s+", "", config$New_name))
        if(normalized_input %in% normalized_new_names) {
          return(list(error = "Machine Name Already Exist."))
        }
      }
    }
    
    # Handle building map integration - BUT DON'T FAIL IF NO BUILDING MAP
    buildingMap_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
    
    building_map_updated <- FALSE
    building_map_error <- NULL
    
    #  Skip building map entirely if formula contains phase components
    has_phase_components <- formulaHasPhaseComponents(formula)
    
    if(file.exists(buildingMap_path) && !has_phase_components) {
      tryCatch({
        buildingMap <- readRDS(buildingMap_path)
        
        # CRITICAL CHECK: If this VR already exists in building map with correct structure, skip!
        # This prevents App.R-created VRs from being corrupted when API is called afterward
        vr_already_exists_correctly <- FALSE
        if(!is.null(buildingMap[[name]]) && is.list(buildingMap[[name]])) {
          # Check if it already contains the expected components
          formula_check_temp <- checkFormulaOperators(formula)
          if(formula_check_temp$allSame) {
            components_temp <- extractComponentsFromFormula(formula)
            # Check if all components are already children of this node
            existing_children <- names(buildingMap[[name]])
            existing_children <- existing_children[existing_children != "Name"]  # Exclude Name attribute
            if(length(components_temp) > 0 && all(components_temp %in% existing_children)) {
              message("VR '", name, "' already exists in building map with correct structure - skipping building map update")
              vr_already_exists_correctly <- TRUE
              building_map_error <- "VR already exists in building map (skipped duplicate update)"
            }
          }
        }
        
        # Only proceed with building map update if VR doesn't already exist in correct form
        if(!vr_already_exists_correctly) {
          # Check if formula has all same operators
          formula_check <- checkFormulaOperators(formula)
          
          if(formula_check$allSame) {
            # Extract components from formula
            components <- extractComponentsFromFormula(formula)
            
            # Check if all components exist before trying to move them
            missing_components <- c()
            for(comp in components) {
              if(is.null(findNodeLocation(buildingMap, comp))) {
                missing_components <- c(missing_components, comp)
              }
            }
            
            if(length(missing_components) == 0) {
              # All components exist, move them
              updated_map <- moveNodesUnderParent(buildingMap, c(), name, components)
              saveRDS(updated_map, buildingMap_path)
              building_map_updated <- TRUE
            } else {
              # Some components don't exist in building map - just add VR as standalone
              vr_node <- structure(list(), Name = name)
              buildingMap[[name]] <- vr_node
              saveRDS(buildingMap, buildingMap_path)
              building_map_updated <- TRUE
              building_map_error <- paste("Components not found in building map:", paste(missing_components, collapse = ", "))
            }
            
          } else {
            # Mixed operators - add as leaf node at specified location
            if(buildingMapLocation != "") {
              # Parse the display path to get the target location
              path_parts <- strsplit(buildingMapLocation, " → ")[[1]]
              
              # Find the actual path
              actual_path <- c()
              if(length(path_parts) > 0) {
                current_search_path <- c()
                for(part in path_parts) {
                  location <- findNodeLocation(buildingMap, part, current_search_path)
                  if(!is.null(location)) {
                    current_search_path <- location$nodePath
                  }
                }
                actual_path <- current_search_path
              }
              
              # Create VR node
              vr_node <- structure(list(), Name = name)
              
              # Add the node at the found location
              updated_map <- addNodeAtPath(buildingMap, actual_path, name, vr_node)
              saveRDS(updated_map, buildingMap_path)
              building_map_updated <- TRUE
            } else {
              # Add at root level if no location specified
              vr_node <- structure(list(), Name = name)
              buildingMap[[name]] <- vr_node
              saveRDS(buildingMap, buildingMap_path)
              building_map_updated <- TRUE
            }
          }
        }
      }, error = function(e) {
        # Building map operation failed, but continue with VR save
        building_map_error <- paste("Building map update failed:", e$message)
      })
    } else if(has_phase_components) {
      # Log that we're skipping building map for phase VR
      building_map_error <- "Skipped building map update (phase VR)"
    }
    
    # Add new VR to file regardless of building map status
    new_vr <- data.frame(Name = name, Formula = formula, stringsAsFactors = FALSE)
    updated_vr <- rbind(existing_vr, new_vr)
    
    # Save VR file
    write.csv(updated_vr, vr_path, row.names = FALSE)
    
    # Return success with any building map warnings
    response <- list(success = TRUE, message = paste(name, "added to Virtual Registers successfully!"))
    
    if(!is.null(building_map_error)) {
      response$warning <- building_map_error
    }
    
    return(response)
    
  }, error = function(e) {
    return(list(error = paste("Error adding virtual register:", e$message)))
  })
}


#* Delete a virtual register
#* @param username User's username
#* @param name Virtual register name to delete
#* @delete /virtual-registers
#* @serializer json list(na='null')
function(username, name) {
  tryCatch({
    # Get meter ID
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    
    # Load VR file
    vr_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/",
                      meter_id, "_VirtualRegister.csv")
    
    if(!file.exists(vr_path)) {
      return(list(error = "No virtual registers found"))
    }
    
    vr_data <- read.csv(vr_path, stringsAsFactors = FALSE)
    
    # Find and remove the VR
    idx <- which(vr_data$Name == name)
    if(length(idx) == 0) {
      return(list(error = "Virtual register not found"))
    }
    
    vr_data <- vr_data[-idx, , drop = FALSE]
    
    # Save updated VR file
    write.csv(vr_data, vr_path, row.names = FALSE)
    
    # Try to remove from building map if exists, but don't fail if it doesn't work
    buildingMap_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
    if(file.exists(buildingMap_path)) {
      tryCatch({
        buildingMap <- readRDS(buildingMap_path)
        updated_map <- removeNodeFromBuildingMap(buildingMap, name)
        saveRDS(updated_map, buildingMap_path)
      }, error = function(e) {
        # Building map update failed, but VR is already deleted
        # Just continue
      })
    }
    
    return(list(success = TRUE, message = "Virtual Register deleted."))
    
  }, error = function(e) {
    return(list(error = paste("Error deleting virtual register:", e$message)))
  })
}
# SINGLE BUILDING MAP API ENDPOINT  FOR vr code 

#* Handle all building map operations
#* @param req The request object
#* @post /building-map-operations
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    operation <- body$operation
    meter_id <- body$meter_id
    
    buildingMap_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
    
    if(operation == "check_exists") {
      return(list(exists = file.exists(buildingMap_path)))
      
    } else if(operation == "load") {
      if(file.exists(buildingMap_path)) {
        buildingMap <- readRDS(buildingMap_path)
        return(list(buildingMap = buildingMap))
      } else {
        return(list(error = "Building map not found", buildingMap = NULL))
      }
      
    } else if(operation == "save") {
      buildingMap <- body$buildingMap
      saveRDS(buildingMap, buildingMap_path)
      return(list(success = TRUE, message = "Building map saved"))
      
    } else {
      return(list(error = "Invalid operation"))
    }
    
  }, error = function(e) {
    return(list(error = paste("Error in building map operation:", e$message)))
  })
}





##### Distribution Map ####

#* Get distribution map (building hierarchy) for a user
#* @param username User's username
#* @get /distribution-map
#* @serializer json list(na='null')
function(username) {
  tryCatch({
    # Get meter ID
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- strsplit(user_row$Meter_id, "; ")[[1]][1]
    
    # Load building map
    buildingMap_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
    
    if(!file.exists(buildingMap_path)) {
      return(list(error = "Building map not found", hasDistributionMap = FALSE))
    }
    
    buildingMap <- readRDS(buildingMap_path)
    
    # Convert to JSON-friendly format (no specialUser check anymore)
    return(list(
      hasDistributionMap = TRUE,
      buildingMap = buildingMap
    ))
    
  }, error = function(e) {
    return(list(error = paste("Error fetching distribution map:", e$message)))
  })
}

#### ENF OF INFO TAB ####

#### Start of Other APIS #####

#### For Client Profiles and settings ####
#* @get /clientData
#* @serializer unboxedJSON
function() {
  # Define file paths for client_profiles.csv and client_settings.csv
  client_profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  client_settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
  
  
  # Check if the files exist
  if (!file.exists(client_profiles_path)) {
    return(list(error = "client_profiles.csv file not found"))
  }
  if (!file.exists(client_settings_path)) {
    return(list(error = "client_settings.csv file not found"))
  }
  
  # Read the CSV files
  client_profiles <- read.csv(client_profiles_path, check.names = FALSE)
  client_settings <- read.csv(client_settings_path, check.names = FALSE)
  
  # Return the data from both CSVs in a single JSON response
  return(list(
    client_profiles = as.list(client_profiles),
    client_settings = as.list(client_settings)
  ))
}

#### For Fleet Profiles ####
#* @get /fleetProfile
#* @serializer unboxedJSON

function() {
  # Define the file path for fleet_profiles.csv
  file_path <- "/srv/shiny-server/Clients Usage Data/fleet_profiles.csv"
  
  # Check if the file exists
  if (!file.exists(file_path)) {
    return(list(error = "File not found"))
  }
  
  # Read the CSV file
  fleet_data <- read.csv(file_path, check.names = FALSE)
  
  # Return the file content as JSON
  return(list(fleet_profiles = as.list(fleet_data)))
}
#### List all files in a folder  ####
#* @get /listFiles
#* @param folderPath The path to the folder whose files we want to list.
#* @serializer unboxedJSON
function(folderPath) {
  # Check if folderPath is provided
  if (is.null(folderPath) || nchar(folderPath) == 0) {
    return(list(error = "Missing folderPath parameter"))
  }
  
  # Check if the provided folder exists
  if (!dir.exists(folderPath)) {
    return(list(error = "Folder does not exist"))
  }
  
  # List all files in the folder (non-recursive, change recursive = TRUE if needed)
  files <- list.files(path = folderPath, full.names = FALSE)
  
  return(list(files = files))
}


# #### Building Map ####
# #* @get /buildingmap
# #* @serializer unboxedJSON
# 
# function(username) {
#   if (is.null(username)) return(list(error = "Username not specified"))
#   
#   clients <- read.csv("../Clients Usage Data/client_profiles.csv")
#   client <- clients[clients$user == username, ]
#   
#   meter_id <- strsplit(client$Meter_id, "; ")[[1]][1]
#   results <- readRDS(paste0("../EnergyMonitor2.0/", meter_id, "_buildingMap.rds"))
#   
#   return(results)
# }
#### Building Map ####
#* @get /buildingmap
#* @serializer unboxedJSON
function(username) {
  if (is.null(username)) return(list(error = "Username not specified"))
  
  clients <- read.csv("../Clients Usage Data/client_profiles.csv", stringsAsFactors = FALSE)
  client <- clients[clients$user == username, ]
  
  if (nrow(client) == 0) return(list(error = "User not found"))
  
  meter_id <- strsplit(client$Meter_id, "; ")[[1]][1]
  rds_file <- paste0("../EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
  
  if (!file.exists(rds_file)) return(list(error = "Building map file not found"))
  
  buildingMap <- readRDS(rds_file)
  
  # Return directly - no wrapper
  return(buildingMap)
}

#### Config ####
#* @get /config
#* @serializer unboxedJSON
function(username) {
  if (is.null(username)) return(list(error = "Username not specified"))
  
  clients <- read.csv("../Clients Usage Data/client_profiles.csv")
  client <- clients[clients$user == username, ]
  
  results <- data.frame()
  if (client$Meter_type == 'neubolt') {
    if (file.exists(paste0("../Clients Usage Data/Neubolt Meter Data/Column Correction/", client$Meter_id, "_Config.csv")))
      results <- read.csv(paste0("../Clients Usage Data/Neubolt Meter Data/Column Correction/", client$Meter_id, "_Config.csv"), check.names = F)
    else return(list(status = 'User data missing'))
  } else {
    meter_ids <- client$Meter_id
    meter_id <- strsplit(meter_ids, "; ")[[1]][1]
    # results <- read.csv(paste0("../EnergyMonitor2.0/Egauge Column Names/", meter_id, "_New_column"))
    results <- read.csv(paste0("../EnergyMonitor2.0/Egauge Column Names/", meter_id, "_New_column_names.csv"))
  }
  
  return(list(status = 'User data available', config = as.list(results)))
}

#* Get energy emissions data
#* @get /energy-emissions
#* @serializer json
function() {
  tryCatch({
    emissions_path <- "/srv/shiny-server/EnergyMonitor2.0/AnnualEnergyEmissions.csv"
    
    if(!file.exists(emissions_path)) {
      return(list(error = "Emissions file not found"))
    }
    
    emissions_data <- read.csv(emissions_path, stringsAsFactors = FALSE)
    
    return(list(emissions = emissions_data))
    
  }, error = function(e) {
    return(list(error = paste("Error loading emissions:", e$message)))
  })
}

#* Update client settings
#* @post /update-client-settings
#* @serializer json
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
    
    # Read current settings
    if(!file.exists(settings_path)) {
      return(list(error = "Settings file not found"))
    }
    
    settings <- read.csv(settings_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    # Update the settings with new data
    username <- body$username
    updates <- body$updates  # Should be a named list/object with column: value pairs
    
    # Find user row
    user_idx <- which(settings$user == username)
    
    if(length(user_idx) == 0) {
      # User doesn't exist, add new row
      new_row <- settings[1, ]  # Copy structure from first row
      new_row$user <- username
      for(col_name in names(updates)) {
        new_row[[col_name]] <- updates[[col_name]]
      }
      settings <- rbind(settings, new_row)
    } else {
      # Update existing user
      for(col_name in names(updates)) {
        settings[user_idx, col_name] <- updates[[col_name]]
      }
    }
    
    # Write back
    write.csv(settings, settings_path, row.names = FALSE, quote = FALSE)
    
    return(list(success = TRUE, message = "Settings updated successfully"))
    
  }, error = function(e) {
    return(list(error = paste("Error updating settings:", e$message)))
  })
}

#* Update fleet profile field
#* @post /update-fleet-profile
#* @serializer json
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    fleet_path <- "/srv/shiny-server/Clients Usage Data/fleet_profiles.csv"
    
    if(!file.exists(fleet_path)) {
      return(list(error = "Fleet profiles file not found"))
    }
    
    fleet_profile <- read.csv(fleet_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    username <- body$username
    field <- body$field  # e.g., "Tour_Status"
    value <- body$value  # e.g., "FALSE"
    
    # Find user
    user_idx <- which(fleet_profile$user == username)
    
    if(length(user_idx) == 0) {
      return(list(error = "User not found in fleet profiles"))
    }
    
    # Update field
    fleet_profile[user_idx, field] <- value
    
    # Write back
    write.csv(fleet_profile, fleet_path, row.names = FALSE)
    
    return(list(success = TRUE, message = paste(field, "updated successfully")))
    
  }, error = function(e) {
    return(list(error = paste("Error updating fleet profile:", e$message)))
  })
}

#* Save chat history
#* @post /save-chat-history
#* @serializer json
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    branch_name <- body$branch_name
    chat_buffer <- body$chat_buffer  # This is a list of message pairs
    
    dir_path <- "/srv/shiny-server/EnvizFleet/chat_Data"
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE)
    }
    
    file_path <- file.path(dir_path, paste0(gsub(" ", "_", branch_name), "_chat_history.txt"))
    con <- file(file_path, open = "at")  # append mode
    
    for (msg_pair in chat_buffer) {
      # Convert to character, handle NULLs as empty string
      user_msg <- if (is.null(msg_pair$user)) "" else as.character(msg_pair$user)
      bot_msg  <- if (is.null(msg_pair$bot)) "" else as.character(msg_pair$bot)
      if (trimws(user_msg) == "" && trimws(bot_msg) == "") next
      writeLines("User:", con)
      writeLines(user_msg, con)
      writeLines("Bot:", con)
      writeLines(bot_msg, con)
      writeLines("###END###", con)
    }
    
    close(con)
    
    return(list(success = TRUE, message = "Chat history saved"))
    
  }, error = function(e) {
    return(list(error = paste("Error saving chat history:", e$message)))
  })
}

#* Load chat history
#* @get /load-chat-history
#* @param branch_name Branch name
#* @serializer json
function(branch_name) {
  tryCatch({
    dir_path <- "/srv/shiny-server/EnvizFleet/chat_Data"
    file_path <- file.path(dir_path, paste0(gsub(" ", "_", branch_name), "_chat_history.txt"))
    
    if (!file.exists(file_path)) {
      return(list(chat_history = list()))
    }
    
    lines <- readLines(file_path)
    chat_history <- list()
    i <- 1
    
    while (i <= length(lines)) {
      if (startsWith(lines[i], "User:")) {
        i <- i + 1
        user_lines <- c()
        while (i <= length(lines) && !startsWith(lines[i], "Bot:")) {
          user_lines <- c(user_lines, lines[i])
          i <- i + 1
        }
        
        i <- i + 1  # Skip "Bot:"
        bot_lines <- c()
        while (i <= length(lines) && !grepl("^###END###", lines[i])) {
          bot_lines <- c(bot_lines, lines[i])
          i <- i + 1
        }
        
        chat_history <- append(chat_history, list(list(
          user = paste(user_lines, collapse = "\n"),
          bot  = paste(bot_lines, collapse = "\n")
        )))
      }
      i <- i + 1  
    }
    
    return(list(chat_history = tail(chat_history, 20)))
    
  }, error = function(e) {
    return(list(error = paste("Error loading chat history:", e$message)))
  })
}

#* Get alerts for user
#* @get /alerts
#* @param username Username
#* @serializer json
function(username) {
  tryCatch({
    # Get meter_id from client profiles
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- user_row$Meter_id
    file_path <- paste0("/srv/shiny-server/EnvizReports/alerts_notif/", meter_id, "_alerts.csv")
    
    if(!file.exists(file_path)) {
      return(list(alerts = data.frame()))
    }
    alerts$ReadStatus <- as.character(alerts$ReadStatus)
    alerts <- read.csv(file_path, stringsAsFactors = FALSE)
    
    return(list(alerts = alerts))
    
  }, error = function(e) {
    return(list(error = paste("Error loading alerts:", e$message)))
  })
}

#* Update alert read status
#* @post /update-alert-status
#* @serializer json
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    alert_index <- body$alert_index
    read_status <- body$read_status
    
    # Get meter_id
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    user_row <- profiles[profiles$user == username, ]
    if(nrow(user_row) == 0) {
      return(list(error = "User not found"))
    }
    
    meter_id <- user_row$Meter_id
    file_path <- paste0("/srv/shiny-server/EnvizReports/alerts_notif/", meter_id, "_alerts.csv")
    
    if(!file.exists(file_path)) {
      return(list(error = "Alerts file not found"))
    }
    
    alerts <- read.csv(file_path, stringsAsFactors = FALSE)
    
    # Update read status
    if(alert_index <= nrow(alerts)) {
      alerts$ReadStatus[alert_index] <- read_status
      write.csv(alerts, file_path, row.names = FALSE)
      return(list(success = TRUE, message = "Alert status updated"))
    } else {
      return(list(error = "Invalid alert index"))
    }
    
  }, error = function(e) {
    return(list(error = paste("Error updating alert status:", e$message)))
  })
}

#### test api ###
#### ============================================ ####
#### CUSTOMER SUPPORT TICKET APIs ####
#### ============================================ ####

#* Submit a new support ticket
#* @post /submit-ticket
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    # Extract values
    rv_user <- body$rv_user
    selected_branch <- body$selected_branch
    display_name <- body$display_name
    subject <- substr(as.character(body$subject), 1, 100)
    description <- substr(as.character(body$description), 1, 2000)
    attachment_filenames <- if(is.null(body$attachment_filenames) || body$attachment_filenames == "") "" else body$attachment_filenames
    
    # Define paths
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    user_dir <- file.path(base_dir, rv_user)
    tickets_path <- file.path(user_dir, paste0(rv_user, "_tickets.csv"))
    attachments_dir <- file.path(user_dir, "attachments")
    
    # Create directories if not exist
    if (!dir.exists(user_dir)) {
      dir.create(user_dir, recursive = TRUE)
    }
    if (!dir.exists(attachments_dir)) {
      dir.create(attachments_dir, recursive = TRUE)
    }
    
    # Read or create tickets file
    if (file.exists(tickets_path)) {
      tickets <- read.csv(tickets_path, stringsAsFactors = FALSE, check.names = FALSE)
    } else {
      tickets <- data.frame(
        ticket_id = character(),
        branch = character(),
        display_name = character(),
        subject = character(),
        description = character(),
        category = character(),
        status = character(),
        created_date = character(),
        created_time = character(),
        resolution_notes = character(),
        attachment_filenames = character(),
        stringsAsFactors = FALSE
      )
    }
    
    # Generate ticket ID
    prefix <- paste0(rv_user, "_", selected_branch, "_")
    existing <- grep(paste0("^", prefix), tickets$ticket_id, value = TRUE)
    
    if (length(existing) == 0) {
      next_num <- 1
    } else {
      nums <- as.numeric(gsub(prefix, "", existing))
      next_num <- max(nums, na.rm = TRUE) + 1
    }
    
    ticket_id <- paste0(prefix, next_num)
    
    # Create new ticket row - use "" not NA
    new_ticket <- data.frame(
      ticket_id = ticket_id,
      branch = selected_branch,
      display_name = display_name,
      subject = subject,
      description = description,
      category = "",
      status = "New",
      created_date = as.character(Sys.Date()),
      created_time = format(Sys.time(), "%H:%M:%S"),
      resolution_notes = "",
      attachment_filenames = "",
      stringsAsFactors = FALSE
    )
    
    # Append to CSV
    tickets <- rbind(tickets, new_ticket)
    write.csv(tickets, tickets_path, row.names = FALSE, na = "")
    
    return(list(
      success = TRUE,
      message = "Ticket submitted successfully",
      ticket_id = ticket_id
    ))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Get user's tickets
#* @get /get-user-tickets
#* @param rv_user The rv$user value
#* @serializer json
function(rv_user) {
  tryCatch({
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    user_dir <- file.path(base_dir, rv_user)
    tickets_path <- file.path(user_dir, paste0(rv_user, "_tickets.csv"))
    
    if (!file.exists(tickets_path)) {
      return(list(success = TRUE, tickets = list()))
    }
    
    tickets <- read.csv(tickets_path, stringsAsFactors = FALSE, check.names = FALSE)
    
    # Sort by date descending
    if (nrow(tickets) > 0) {
      tickets <- tickets[order(as.Date(tickets$created_date), decreasing = TRUE), ]
    }
    
    return(list(success = TRUE, tickets = as.list(tickets)))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Get single ticket details
#* @get /get-ticket-detail
#* @param rv_user The rv$user value
#* @param ticket_id The ticket ID
#* @serializer json
function(rv_user, ticket_id) {
  tryCatch({
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    user_dir <- file.path(base_dir, rv_user)
    tickets_path <- file.path(user_dir, paste0(rv_user, "_tickets.csv"))
    
    if (!file.exists(tickets_path)) {
      return(list(success = FALSE, error = "Ticket not found"))
    }
    
    tickets <- read.csv(tickets_path, stringsAsFactors = FALSE, check.names = FALSE)
    ticket <- tickets[tickets$ticket_id == ticket_id, ]
    
    if (nrow(ticket) == 0) {
      return(list(success = FALSE, error = "Ticket not found"))
    }
    
    return(list(success = TRUE, ticket = as.list(ticket[1, ])))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Upload ticket attachment
#* @post /upload-ticket-attachment
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    rv_user <- body$rv_user
    ticket_id <- body$ticket_id
    filename <- body$filename
    file_data <- body$file_data
    
    if(is.null(rv_user) || is.null(ticket_id) || is.null(filename) || is.null(file_data)) {
      return(list(success = FALSE, error = "Missing required fields"))
    }
    
    # Validate file type
    ext <- tolower(tools::file_ext(filename))
    if (!ext %in% c("png", "jpg", "jpeg")) {
      return(list(success = FALSE, error = "Only PNG, JPG, JPEG allowed"))
    }
    
    # Define path
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    attachments_dir <- file.path(base_dir, rv_user, "attachments")
    
    if (!dir.exists(attachments_dir)) {
      dir.create(attachments_dir, recursive = TRUE)
    }
    
    # Create filename
    new_filename <- paste0(ticket_id, "_", filename)
    file_path <- file.path(attachments_dir, new_filename)
    
    # Remove base64 prefix if present (like in logo upload)
    file_data <- sub("^data:image/[a-z]+;base64,", "", file_data)
    
    # Decode using jsonlite (same as logo upload)
    decoded <- jsonlite::base64_dec(file_data)
    
    # Check size (2MB)
    if (length(decoded) > 2 * 1024 * 1024) {
      return(list(success = FALSE, error = "File exceeds 2MB limit"))
    }
    
    writeBin(decoded, file_path)
    
    return(list(success = TRUE, filename = new_filename))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Update ticket attachment filenames
#* @post /update-ticket-attachments
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    rv_user <- as.character(body$rv_user)
    ticket_id <- as.character(body$ticket_id)
    attachment_filenames <- as.character(body$attachment_filenames)
    
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    tickets_path <- file.path(base_dir, rv_user, paste0(rv_user, "_tickets.csv"))
    
    if (!file.exists(tickets_path)) {
      return(list(success = FALSE, error = "Tickets file not found"))
    }
    
    tickets <- read.csv(tickets_path, stringsAsFactors = FALSE, check.names = FALSE)
    idx <- which(tickets$ticket_id == ticket_id)
    
    if (length(idx) == 0) {
      return(list(success = FALSE, error = "Ticket not found"))
    }
    
    # Update the attachment_filenames column
    tickets$attachment_filenames[idx] <- attachment_filenames
    
    # Write back with na="" to avoid NA values
    write.csv(tickets, tickets_path, row.names = FALSE, na = "")
    
    return(list(success = TRUE, message = "Attachments updated"))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}
#* Serve ticket image directly
#* @get /ticket-image
#* @serializer contentType list(type="image/png")
function(rv_user, filename) {
  file_path <- file.path("/srv/shiny-server/Clients Usage Data/support_tickets", rv_user, "attachments", filename)
  
  if (!file.exists(file_path)) {
    stop("Image not found")
  }
  
  readBin(file_path, "raw", file.info(file_path)$size)
}





#* Load bill history for user
#* @get /bill-history
#* @param username Username
#* @param months Number of months back (default 12)
#* @serializer json
function(username, months = 12) {
  tryCatch({
    billDir <- paste0('/srv/shiny-server/Clients Usage Data/Electricity Bills/', username)
    
    if(!dir.exists(billDir)) {
      return(list(bills = data.frame(), files_found = character()))
    }
    
    months <- as.integer(months)
    billDate <- Sys.Date() - months(1:months)
    billMonth <- paste(str_to_upper(lubridate::month(billDate, label = TRUE)), 
                       substr(year(billDate), 3, 4), sep = '_')
    
    billSummary <- data.frame()
    files_found <- character()
    
    for(month in billMonth) {
      billFile <- paste0(billDir, '/', username, '_', month, '_billing_summary.csv')
      
      if(file.exists(billFile)) {
        bill_data <- read.csv(billFile, check.names = FALSE, stringsAsFactors = FALSE)
        billSummary <- plyr::rbind.fill(billSummary, bill_data)
        files_found <- c(files_found, basename(billFile))
      }
    }
    
    return(list(bills = billSummary, files_found = files_found))
    
  }, error = function(e) {
    return(list(error = paste("Error loading bill history:", e$message)))
  })
}







# ============================================================================
# SERVER STARTUP FUNCTION
# ============================================================================

#* Start the Consolidated API server
#* @export
start_consolidated_server <- function(port = 8000, host = "203.135.63.47") {
  cat("Starting Consolidated API server...\\n")
  cat("Server will be available at: http://", host, ":", port, "\\n", sep = "")
  cat("\\nAPI endpoints:\\n")
  cat("  GET  /health - Overall API health\\n")
  cat("\\n  Daily Analysis API:\\n")
  cat("    GET  /daily-analysis/health\\n")
  cat("    POST /daily-analysis/comparative-analysis\\n") 
  cat("    POST /daily-analysis/log-download\\n")
  cat("\\n  Electricity Bill API:\\n")
  cat("    GET  /electricity-bill/health\\n")
  cat("    POST /electricity-bill/load-bill-data\\n")
  cat("    POST /electricity-bill/set-run-mode\\n")
  cat("\\n  Historic File Load API:\\n")
  cat("    GET  /historic-file/health\\n")
  cat("    POST /historic-file/load-historic-data\\n")
  cat("    GET  /historic-file/column-info\\n")
  cat("    GET  /historic-file/date-range\\n")
  cat("\\nCurrent RUN_MODE:", RUN_MODE, "\\n")
  
  # Create plumber router from this file
  pr <- plumber::plumb("consolidated_api.R")
  
  # Add CORS headers
  pr$filter("cors", function(req, res) {
    res$setHeader("Access-Control-Allow-Origin", "*")
    res$setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
    res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization")
    
    if (req$REQUEST_METHOD == "OPTIONS") {
      res$status <- 200
      return(list())
    } else {
      plumber::forward()
    }
  })
  
  # Run the server
  pr$run(port = port, host = host)
}

#* Log user action
#* @post /log-event
#* @serializer json
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    username <- body$username
    action <- body$action
    
    log_dir <- "/srv/shiny-server/EnvizFleet/ClientLogFiles"
    if(!dir.exists(log_dir)) {
      dir.create(log_dir, recursive = TRUE)
    }
    
    file_name <- file.path(log_dir, paste0(username, ".csv"))
    
    log_entry <- data.frame(
      Date = format(Sys.Date(), "%d-%m-%Y"),
      Timestamp = format(Sys.time(), "%H:%M:%S"),
      ActionPerformed = action,
      stringsAsFactors = FALSE
    )
    
    write.table(
      log_entry,
      file = file_name,
      sep = ",",
      row.names = FALSE,
      col.names = !file.exists(file_name),
      append = TRUE
    )
    
    return(list(success = TRUE, message = "Event logged"))
    
  }, error = function(e) {
    return(list(error = paste("Error logging event:", e$message)))
  })
}


# Example usage (uncomment to run):
# start_consolidated_server(port = 8080, host = "127.0.0.1")