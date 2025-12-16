library(plyr)
library(dplyr)
library(plumber)
library(openssl)
library(jsonlite)
library(digest)
library(base64enc)
library(lubridate)
library(RPostgreSQL)
library(tidyr)
library(rvest)
library(fs)
library(webshot)
library(stringr)
library(magick)
library(zoo)
library(data.tree)

#* @filter cors
function(req, res) {
  res$setHeader("Access-Control-Allow-Origin", "https://enviz.enfo.ai")
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DEL,AuthorizationONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization")

  plumber::forward()
}

#### Sign in ####
#* @post /signin
#* @serializer unboxedJSON
function(res, username, password) {
  res$setHeader("Access-Control-Allow-Origin", "https://enviz.enfo.ai")
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization")
  
  # Missing credentials
  if (is.null(username) || is.null(password)) return(list(error = "Missing Credentials"))
  
  clients <- read.csv("../Clients Usage Data/client_profiles.csv")
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv")
  clients <- merge(clients, client_settings, by = 'user')
  # client_settings <- client_settings[client_settings$user == username & client_settings$password == password, ]
  # if (nrow(client_settings) == 0) return(list(error = "Invalid Credentials"))
  client <- clients[clients$user == username & clients$password == password, ]
  #client <- clients[clients$user == username, ]
  
  if (nrow(client) == 0) return(list(error = "Invalid Credentials"))
  else {
    token <- create_jwt(list(user = username), "enviz123")
    client_setting <- client_settings[client_settings$user == username, ]
    #missing_cols <- !colnames(client_setting) %in% colnames(client)
    #client_setting <- client_setting[missing_cols]
    
    # if client is new
    if (nrow(client_setting) == 0) {
      # Add a new row
      client_settings[nrow(client_settings)+1, ] <- NA
      
      # Fill the new row
      client_settings$user[nrow(client_settings)] <- username
      client_settings$Meter_id[nrow(client_settings)] <- clients$Meter_id[clients$user == username]
      client_settings[nrow(client_settings)] <- client_settings[1, ]
      write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
      client_setting <- client_settings[nrow(client_settings), ]
    }

    return(list(token = token, client = as.list(client)))
  }
}

create_jwt <- function(claims, secret) {
  header <- '{"alg":"HS256", "typ":"JWT"}'
  payload <- toJSON(claims)
  
  encoded_header <- base64encode(charToRaw(header))
  encoded_payload <- base64encode(charToRaw(payload))
  
  signature <- base64encode(charToRaw(hmac(charToRaw(paste(encoded_header, encoded_payload, sep = '.')), key = charToRaw(secret), algo = "sha256")))
  
  paste(encoded_header, encoded_payload, signature, sep = '.')
}

#### Data ####
#* @get /data
#* @serializer unboxedJSON
function(username, mode = "hour", start, end = NULL) {
  # date format = "YYYY-MM-DD"
  if (is.null(start)) return(list(error = "Date not specified"))
  if (is.null(username)) return(list(error = "User not logged in"))
  if (!mode %in% c("minute", "hour", "day")) mode = "hour"
  temp_mode <- ""
  if (mode == "day") {
    temp_mode <- "day"
    mode <- "hour"  # Changed from "minute" to "hour"
  }
  
  client_profiles <- read.csv("../Clients Usage Data/client_profiles.csv", check.names = F)
  client <- client_profiles[client_profiles$user == username, ]
  
  dates_list <- c()
  if (is.null(end)) {
    dates_list = start
  } else {
    start = as.Date(start)
    end = as.Date(end)
    if (start > end) {
      temp = start
      start = end
      end = temp
    }
    
    dates_list <- seq.Date(from = start, to = end, by = "day")
  }
  
  retData <- list()
  if (is.null(end)){
    if (mode == "minute") {
      retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(start) + hours(24)-1, by = "min")
    } else {
      retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(start) + hours(23), by = "hour")
    }
  } else {
    if (mode == "minute") {
      retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(end) + hours(24)-1, by = "min")
    } else {
      retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(end) + hours(23), by = "hour")
    }
  }
  retData <- data.frame("Date & Time" = retData$`Date & Time`, check.names = F)
  
  dir <- ''
  if (mode == 'minute') { 
    if (client$Meter_type == 'neubolt') { dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/', client$user, "/")
    } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/minute-wise Data/', client$user, "/")
  } else {
    if (client$Meter_type == 'neubolt') { dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/', client$user, "/")
    } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/hour-wise Data/', client$user, "/")
  }
  
  available_data <- data.frame()
  files <- list.files(dir, full.names = T, pattern = ".*\\.csv$")
  file_dates <- sapply(basename(files), function(x) strsplit(x, "_")[[1]][2])
  filtered_file_names <- files[file_dates %in% dates_list]
  
  available_data <- do.call(rbind.fill, lapply(filtered_file_names, read.csv, check.names=F))
  
  # for (date in 1:length(dates_list)) {
  #   fname <- paste0(dir, meter_id_first, '_', dates_list[date], "_", mode, "-wise.csv")
  #   if (!file.exists(fname)) next
  #   todayData <- read.csv(fname, check.names = F)
  #   todayData$`Date & Time` <- as.POSIXct(todayData$`Date & Time`, tz = tz(retData$`Date & Time`))
  #   
  #   if (nrow(available_data) == 0) available_data <- todayData
  #   else available_data <- rbind.fill(available_data, todayData)
  # }
  available_data$`Date & Time` <- as.POSIXct(available_data$`Date & Time`, tz(retData$`Date & Time`))
  
  retData <- merge(retData, available_data, all.x = T)
  
  if (temp_mode == "day") {
    tz(retData$`Date & Time`) <- "UTC"
    retData$`Date & Time` <- as.Date(retData$`Date & Time`)
    postfix <- "_[kW]"
    if (client$Meter_type == "egauge") postfix <- " [kW]"
    
    retData <- retData %>%
      group_by(`Date & Time`) %>% 
      summarise(
        across(
          where(is.numeric), 
          ~ ifelse(endsWith(cur_column(), postfix), sum(.x, na.rm = TRUE), mean(.x, na.rm = TRUE))))
    
    
  }
  
  meter_id_first <- strsplit(client$Meter_id, "; ")[[1]][1]
  
  if (file.exists(paste0('../EnergyMonitor2.0/Egauge Column Names/', meter_id_first, "_New_column_names.csv"))){
    updateColNames <- read.csv(paste0('../EnergyMonitor2.0/Egauge Column Names/', meter_id_first, "_New_column_names.csv"))
    updateColNames$Old_name <- trimws(updateColNames$Old_name)
    colnames(retData) <- trimws(colnames(retData))
    retData <- retData[,unique(c(1,2,which(colnames(retData) %in% updateColNames$Old_name)))]
    for (row in 1:nrow(updateColNames)) {
      # If the old name exists in retData, rename it to the new name
      if (updateColNames$Old_name[row] %in% colnames(retData)) {
        colnames(retData)[which(colnames(retData) == updateColNames$Old_name[row])] <- updateColNames$New_name[row]
        next
      }
      
      # If the old name contains a '+', sum up those columns
      if (grepl("\\+", updateColNames$Old_name[row])) {
        cols <- strsplit(updateColNames$Old_name[row], "\\+")[[1]] %>% trimws()
        existing_cols <- cols[cols %in% colnames(retData)]
        
        if (length(existing_cols) > 0) {
          print("this exist")
          print(existing_cols)
          retData <- retData %>%
            mutate(!!updateColNames$New_name[row] := 
                     rowSums(across(all_of(existing_cols)), na.rm = TRUE))
        }
      }
    }
    
    
    
    matchedIndices <- match(updateColNames$Old_name, colnames(retData))
    toberemoved <- which(is.na(matchedIndices))
    if (length(toberemoved) > 0) updateColNames <- updateColNames[-c(toberemoved), ]
    matchedIndices <- matchedIndices[!is.na(matchedIndices)]
    colnames(retData)[matchedIndices] = updateColNames$New_name  
  }
  
  # Process meter-specific calculations
  if (client$Meter_type == 'neubolt') {
    # Identify columns containing power, voltage, current, and power factor data
    powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
    voltageColumns = grep('[V]', colnames(retData), value = T, fixed = T)
    currentColumns = grep('[A]', colnames(retData), value = T, fixed = T)
    powerfactorColumns = grep('[PF]', colnames(retData), value = T, fixed = T)
    
    # Convert power columns from watts to kilowatts by dividing by 1000
     retData[, powerColumns] = retData[, powerColumns]/1000
    
  } else {
    # For Egauge meters, ensure all numeric columns are positive
    retData[,2:ncol(retData)] = abs(retData[,2:ncol(retData)])
    
    colnames(retData) <- gsub(" \\[", "_\\[", colnames(retData))
    
    # Identify columns containing power, voltage, current, and power factor data
    powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
    voltageColumns = grep('[V]', colnames(retData), value = T, fixed = T)
    currentColumns = grep('[A]', colnames(retData), value = T, fixed = T)
    powerfactorColumns = grep('[PF]', colnames(retData), value = T, fixed = T)
    
    # Adjust non-phase voltage columns by multiplying by sqrt(3) if corresponding phase columns exist
    nonPhaseVolts <- voltageColumns[!grepl('phase', voltageColumns)]
    if(length(nonPhaseVolts) > 0) {
      for (volt_col in 1:length(nonPhaseVolts)) {
        temp <- nonPhaseVolts[volt_col]
        temp_short <- strsplit(temp, '_\\[V\\]')[[1]][1]
        if (length(grep(paste0(temp_short, ' phase'), colnames(retData))) > 0)
          retData[[temp]] <- sqrt(3) * retData[[temp]]
      }
    }
  } 
  
  # Usage column calculation
  # Helper function for getting usage columns from buildingMap
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
  
  # Load buildingMap and calculate Usage column
  rds_path <- paste0("../EnergyMonitor2.0/", meter_id_first, "_buildingMap.rds")
  buildingMap <- readRDS(rds_path)
  
  # Determine the level in the buildingMap where usage columns are defined
  curr_level <- 0
  usage_columns <- NULL
  while(TRUE) {
    usage_columns <- getNodesAtLevel(buildingMap, curr_level)
    if (any(is.na(usage_columns))) {
      curr_level <- curr_level + 1
    } else break
  }
  
  # Process usage columns based on meter type (after meter-specific processing above)
  if (client$Meter_type == 'neubolt') {
    # Append '_[kW]' to usage column names for Neubolt meters
    usage_columns <- paste0(usage_columns, "_[kW]")
  } else {
    # Append '_[kW]' to usage column names for Egauge meters (after gsub conversion)
    usage_columns <- paste0(usage_columns, "_[kW]")
  }
  
  # Filter usage columns to only those present in retData
  usage_columns <- intersect(usage_columns, colnames(retData))
  
  # Calculate Usage column
  if (length(usage_columns) > 1) {
    retData$Usage <- rowSums(retData[, usage_columns, drop = FALSE], na.rm = TRUE)
  } else if (length(usage_columns) == 1) {
    # retData$Usage <- retData[, usage_columns]
    retData$Usage <- retData[[usage_columns]]
  }
  
  # Move Usage column to be second (after Date & Time)
  retData <- retData %>% relocate(Usage, .after = 1)
  
  return(list(data = as.list(retData))) 
}
# #### Data ####
# #* @get /data
# #* @serializer unboxedJSON
# function(username, mode = "hour", start, end = NULL) {
#   # date format = "YYYY-MM-DD"
#   if (is.null(start)) return(list(error = "Date not specified"))
#   if (is.null(username)) return(list(error = "User not logged in"))
#   if (!mode %in% c("minute", "hour", "day")) mode = "hour"
#   temp_mode <- ""
#   if (mode == "day") {
#     temp_mode <- "day"
#     mode <- "minute"
#   }
#   
#   client_profiles <- read.csv("../Clients Usage Data/client_profiles.csv", check.names = F)
#   client <- client_profiles[client_profiles$user == username, ]
#   
#   dates_list <- c()
#   if (is.null(end)) {
#     dates_list = start
#   } else {
#     start = as.Date(start)
#     end = as.Date(end)
#     if (start > end) {
#       temp = start
#       start = end
#       end = temp
#     }
#     
#     dates_list <- seq.Date(from = start, to = end, by = "day")
#   }
#   
#   retData <- list()
#   if (is.null(end)){
#     if (mode == "minute") {
#       retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(start) + hours(24)-1, by = "min")
#     } else {
#       retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(start) + hours(23), by = "hour")
#     }
#   } else {
#     if (mode == "minute") {
#       retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(end) + hours(24)-1, by = "min")
#     } else {
#       retData[["Date & Time"]] <- seq.POSIXt(from = as.POSIXct(start), to = as.POSIXct(end) + hours(23), by = "hour")
#     }
#   }
#   retData <- data.frame("Date & Time" = retData$`Date & Time`, check.names = F)
#   
#   dir <- ''
#   if (mode == 'minute') { 
#     if (client$Meter_type == 'neubolt') { dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/minute-wise Data/', client$user, "/")
#     } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/minute-wise Data/', client$user, "/")
#   } else {
#     if (client$Meter_type == 'neubolt') { dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/', client$user, "/")
#     } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/hour-wise Data/', client$user, "/")
#   }
#   
#   available_data <- data.frame()
#   files <- list.files(dir, full.names = T, pattern = ".*\\.csv$")
#   file_dates <- sapply(basename(files), function(x) strsplit(x, "_")[[1]][2])
#   filtered_file_names <- files[file_dates %in% dates_list]
#   
#   available_data <- do.call(rbind.fill, lapply(filtered_file_names, read.csv, check.names=F))
#   
#   # for (date in 1:length(dates_list)) {
#   #   fname <- paste0(dir, meter_id_first, '_', dates_list[date], "_", mode, "-wise.csv")
#   #   if (!file.exists(fname)) next
#   #   todayData <- read.csv(fname, check.names = F)
#   #   todayData$`Date & Time` <- as.POSIXct(todayData$`Date & Time`, tz = tz(retData$`Date & Time`))
#   #   
#   #   if (nrow(available_data) == 0) available_data <- todayData
#   #   else available_data <- rbind.fill(available_data, todayData)
#   # }
#   available_data$`Date & Time` <- as.POSIXct(available_data$`Date & Time`, tz(retData$`Date & Time`))
# 
#   retData <- merge(retData, available_data, all.x = T)
#   
#   if (temp_mode == "day") {
#     tz(retData$`Date & Time`) <- "UTC"
#     retData$`Date & Time` <- as.Date(retData$`Date & Time`)
#     postfix <- "_[kW]"
#     if (client$Meter_type == "egauge") postfix <- " [kW]"
#     
#     retData <- retData %>%
#       group_by(`Date & Time`) %>% 
#       summarise(
#         across(
#           where(is.numeric), 
#           ~ ifelse(endsWith(cur_column(), postfix), sum(.x, na.rm = TRUE), mean(.x, na.rm = TRUE))))
#     
#     retData <- retData %>%
#       mutate_if(is.numeric, ~ . / 60)
#   }
#   
#   meter_id_first <- strsplit(client$Meter_id, "; ")[[1]][1]
#   
#   if (file.exists(paste0('../EnergyMonitor2.0/Egauge Column Names/', meter_id_first, "_New_column_names.csv"))){
#     updateColNames <- read.csv(paste0('../EnergyMonitor2.0/Egauge Column Names/', meter_id_first, "_New_column_names.csv"))
#     retData <- retData[,unique(c(1,2,which(colnames(retData) %in% updateColNames$Old_name)))]
#     matchedIndices <- match(updateColNames$Old_name, colnames(retData))
#     toberemoved <- which(is.na(matchedIndices))
#     if (length(toberemoved) > 0) updateColNames <- updateColNames[-c(toberemoved), ]
#     matchedIndices <- matchedIndices[!is.na(matchedIndices)]
#     colnames(retData)[matchedIndices] = updateColNames$New_name  
#   }
#   
#   if (client$Meter_type == 'egauge') {
#     colnames(retData) <- gsub(" \\[", "_\\[", colnames(retData))
#     
#     powerColumns <- grep("_\\[kW\\]", colnames(retData), value = T)
#     retData <- retData %>%
#       mutate_at(powerColumns, ~ . * 1000)
#   } 
#   
#   return(list(data = as.list(retData))) 
# }


# #### Config ####
# #* @get /config
# #* @serializer unboxedJSON
# function(username) {
#   if (is.null(username)) return(list(error = "Username not specified"))
#   
#   clients <- read.csv("../Clients Usage Data/client_profiles.csv")
#   client <- clients[clients$user == username, ]
#   
#   results <- data.frame()
#   if (client$Meter_type == 'neubolt') {
#     if (file.exists(paste0("../Clients Usage Data/Neubolt Meter Data/Column Correction/", client$Meter_id, "_Config.csv")))
#       results <- read.csv(paste0("../Clients Usage Data/Neubolt Meter Data/Column Correction/", client$Meter_id, "_Config.csv"), check.names = F)
#       else return(list(status = 'User data missing'))
#   } else {
#     meter_ids <- client$Meter_id
#     meter_id <- strsplit(meter_ids, "; ")[[1]][1]
#     # results <- read.csv(paste0("../EnergyMonitor2.0/Egauge Column Names/", meter_id, "_New_column"))
#     results <- read.csv(paste0("../EnergyMonitor2.0/Egauge Column Names/", meter_id, "_New_column_names.csv"))
#   }
#   
#   return(list(status = 'User data available', config = as.list(results)))
# }
# 
# 
# #### Building Map ####
# #* @get /buildingmap
# #* @serializer json
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


#### Client Profile Settings ####
#* @post /setNotif
function(username = "", notif = "") {
  if (username == "" | notif == "") return(list(error = "Username not specified"))
  
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv", check.names = F)
  if (notif == 'T') client_settings[client_settings$user == username, 'notifEnable'] <- "TRUE"
  else if (notif == 'F') client_settings[client_settings$user == username, 'notifEnable'] <- "FALSE"
  
  write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
  return(list(message = "Notifications Enabled", status = 200))
}


#* @post /setHour
function(username = "", hour = "") {
  if (username == "" | hour == "") return(list(error = "Username not specified"))
  
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv", check.names = F)
  client_settings[client_settings$user == username, 'notifHour'] <- hour
  
  write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
  return(list(message = "Notification Hour Set", status = 200))
}


#* @post /setLimit
function(username = "", limit = "") {
  if (username == "" | limit == "") return(list(error = "Username not specified"))
  
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv", check.names = F)
  client_settings[client_settings$user == username, 'notifLimit'] <- limit
  
  write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
  return(list(message = "Energy Limit Set", status = 200))
}



#### Client Device Token Setting ####
#* @post /setDeviceToken
function(username = "", token = "") {
  if (username == "" | token == "") return(list(error = "Username not specified"))
  
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv", check.names = F)
  client_settings <- client_settings[!is.na(client_settings$user), ]
  client_settings[client_settings$user == username, "deviceToken"] <- token
  
  write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
  return(list(message = "Device Token Saved", status = 200))
}


#### Bill(s) ####
#* @get /bill
#* @serializer unboxedJSON
function(username, curr_month = NULL) {
  if (is.null(username)) return(list(error = "Username not specified"))
  
  client_profiles <- read.csv("../Clients Usage Data/client_profiles.csv", check.names = F)
  client_profiles <- client_profiles[client_profiles$user == username, ]
  
  dir <- paste0("../Clients Usage Data/Electricity Bills/", username, '/')
  default_history <- read.csv("../Clients Usage Data/Electricity Bills/sample_billing_history.csv", check.names = F)
  default_summary <- read.csv("../Clients Usage Data/Electricity Bills/sample_billing_summary.csv", check.names = F)
  
  client_profiles <- read.csv("../Clients Usage Data/client_profiles.csv", check.names = F)
  client_profiles <- client_profiles[client_profiles$user == username, ]
  
  #if (client_profiles$Customer_ID == 'XYZ') return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  if (!dir.exists(dir)) return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  
  number_of_bills <- length(strsplit(client_profiles$Customer_ID, '; ')[[1]])
  
  if (is.null(curr_month)) {
    curr_month <- c('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')
    last_month <- month(Sys.Date())-1
    if (last_month == 0) last_month = 12
    curr_month <- curr_month[last_month]
  }
  
  files_list <- list.files(dir, pattern = '\\.csv$', full.names = T)
  files_list <- files_list[which(grepl(paste0('^', username, '_', curr_month), basename(files_list)))]
  
  if (length(files_list) == 0) {
    curr_month <- c('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')
    last_month <- month(Sys.Date())-2
    if (last_month == 0) last_month = 12
    if (last_month == -1) last_month = 11
    curr_month <- curr_month[last_month]
  }
  
  # If no files found for the selected month
  if (length(files_list) == 0) return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  
  bill_1_files <- files_list[grepl('1\\.csv$', files_list)]
  bill_2_files <- files_list[grepl('2\\.csv$', files_list)]
  
  # If theres 2 bills but only information for one of them:
  if (number_of_bills == 2 && length(files_list) == 2) bill_1_files = files_list
  
  # For single bill, if there are not exactly 2 files
  if (number_of_bills == 1 && !length(files_list) == 2) return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  # For single bill in case the above regex does not filters the files correctly
  if (number_of_bills == 1) bill_1_files <- files_list
  
  # check for assurity
  if (length(bill_1_files) == 0) return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  
  bill_1_history <- bill_1_files[grepl('history', basename(bill_1_files))]
  bill_1_summary <- bill_1_files[grepl('summary', basename(bill_1_files))]
  
  bill_1_history <- read.csv(bill_1_history, check.names = F)
  bill_1_summary <- read.csv(bill_1_summary, check.names = F)
  
  bill_2_history <- NA
  bill_2_summary <- NA
  
  # For double bills
  if (number_of_bills == 2 && !length(bill_2_files) == 2) {
    bill_2_history <- default_history
    bill_2_summary <- default_summary
  } else if (number_of_bills == 2) {
    bill_2_history <- bill_2_files[grepl('history', basename(bill_2_files))]
    bill_2_summary <- bill_2_files[grepl('summary', basename(bill_2_files))]
    
    bill_2_history <- read.csv(bill_2_history, check.names = F)
    bill_2_summary <- read.csv(bill_2_summary, check.names = F)
  }
  
  if (number_of_bills == 1 && length(bill_2_history) == 1) return(list(status = 'success', history = as.list(bill_1_history), summary = as.list(bill_1_summary)))
  else if (number_of_bills == 2 && length(bill_2_history) == 1) return(list(status = 'failed', history = as.list(default_history), summary = as.list(default_summary)))
  else {
    if (number_of_bills == 2 && !length(bill_2_files) == 2) return(list(status = 'incomplete', history = as.list(bill_1_history), summary = as.list(bill_1_summary)))
    
    final_history <- bill_1_history
    final_summary <- bill_1_summary
    
    final_history[, 2:ncol(final_history)] <- final_history[, 2:ncol(final_history)] + bill_2_history[, 2:ncol(bill_2_history)]
    final_summary[, 10:ncol(final_summary)] <- final_summary[, 10:ncol(final_summary)] + bill_2_summary[, 10:ncol(bill_2_summary)]
    
    return(list(status = 'success', history = as.list(final_history), summary = as.list(final_summary)))
    
    # return(list(status = 'success', 
    #           'Bill 1' = list(history = as.list(bill_1_history), summary = as.list(bill_1_summary)),
    #           'Bill 2' = list(history = as.list(bill_2_history), summary = as.list(bill_2_summary))
    #           ))
  }
}





















#### Step 1: BuildingData ####
#* @get /getbuildingdata
#* @serializer unboxedJSON
function() {
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  
  return(list(buildingData = as.list(buildingData)))
}

#* @post /setbuildingdata
#* @serializer json
function(req) {
  form_data <- req$body
  saveRDS(form_data, 'temp.rds')
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  if (length(form_data) == ncol(buildingData)) {

    my_list <- lapply(form_data, function(x) if (is.null(x)) NA else x)
    new_row <- as.data.frame(my_list, check.names = F)
    new_row <- new_row[, colnames(buildingData)]
    
    if (!my_list$`Building Name` %in% buildingData$`Building Name`) {
      buildingData <- rbind(buildingData, new_row)
      if (!dir.exists(paste0('../step1/metadata/', my_list$`Building Name`))) 
        dir.create(paste0('../step1/metadata/', my_list$`Building Name`))
      
      write.csv(buildingData, '../step1/BuildingData.csv', row.names = F)
      return(status = 'success')
    } else {
      buildingData[buildingData$`Building Name` == my_list$`Building Name`, ] <- new_row
      write.csv(buildingData, '../step1/BuildingData.csv', row.names = F)
      return(status = 'success')
    }
  }
  return(status = 'failure', message = 'Not all information was included. Empty fields should be sent as NULL')
}

#* @delete /delbuildingdata
#* @serializer unboxedJSON
function(buildingName) {
  if (is.na(buildingName)) return (status = 'failure', message = 'Empty buildingName')
  if (is.null(buildingName)) return (status = 'failure', message = 'Empty buildingName')
  if (nchar(buildingName) == 0) return (status = 'failure', message = 'Empty buildingName')
  
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  
  if (!buildingName %in% buildingData$`Building Name`) return (status = 'failure', message = paste0('buildingName ', buildingName, ' does not exist.'))
  else buildingData <- buildingData[-c(which(buildingData$`Building Name` == buildingName)), ]
  
  write.csv(buildingData, '../step1/BuildingData.csv', row.names = F)
  return(status = 'success')
}

#### Step 1: Building Sections ####
#* @get /getsections
#* @serializer unboxedJSON
function(username) {
  if (is.na(username) | is.null(username)) return (status = 'Fail')
  path_to_file <- paste0('../step1/metadata/', username, '/sections.csv')
  sections <- read.csv(path_to_file, check.names = F)
  
  return(list(sections = as.list(sections)))
}

#* @post /setsections
#* @serializer json
function(req) {
  form_data <- req$body
  username <- form_data$username
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  if (!username %in% buildingData$`Building Name`) return(list(status = 'Error: Username not found'))
  
  file_dir <- paste0('../step1/metadata/', username, '/sections.csv')
  if (!file.exists(file_dir)) {
    new_df <- as.data.frame(matrix(nrow = 0, ncol = length(form_data)-1))
    colnames(new_df) <- names(form_data)[2:length(form_data)]
    write.csv(new_df, file_dir, row.names = F)
  }
  
  sectionData <- read.csv(file_dir, check.names = F)
  if (length(form_data)-1 == ncol(sectionData)) {
    
    my_list <- lapply(form_data, function(x) if (is.null(x)) NA else x)
    new_row <- as.data.frame(my_list, check.names = F)
    new_row <- new_row[, colnames(sectionData)]
    
    if (!my_list$`Section Name` %in% sectionData$`Section Name`) {
      sectionData <- rbind(sectionData, new_row)
      write.csv(sectionData, paste0('../step1/metadata/', username, '/sections.csv'), row.names = F)
      return(status = 'success! New section added')
    } else {
      sectionData[sectionData$`Section Name` == my_list$`Section Name`, ] <- new_row
      write.csv(sectionData, paste0('../step1/metadata/', username, '/sections.csv'), row.names = F)
      return(status = 'success! Section information updated')
    }
  }
  return(status = 'failure', message = 'Not all information was included. Empty fields should be sent as NULL')
}

#* @delete /delsection
#* @serializer unboxedJSON
function(username, sectionName) {
  if (is.na(sectionName)) return (status = 'failure', message = 'Empty sectionName')
  if (is.null(sectionName)) return (status = 'failure', message = 'Empty sectionName')
  if (nchar(sectionName) == 0) return (status = 'failure', message = 'Empty sectionName')
  
  sectionData <- read.csv(paste0('../step1/metadata/', username, '/sections.csv'), check.names = F)
  
  if (!sectionName %in% sectionData$`Section Name`) return (status = 'failure', message = paste0('sectionName ', sectionName, ' does not exist.'))
  else sectionData <- sectionData[-c(which(sectionData$`Section Name` == sectionName)), ]
  
  write.csv(sectionData, paste0('../step1/metadata/', username, '/sections.csv'), row.names = F)
  return(status = 'success')
}

#### Step 1: Appliances ####
#* @get /getappliances
#* @serializer unboxedJSON
function(username) {
  if (is.na(username) | is.null(username)) return (status = 'Fail')
  path_to_file <- paste0('../step1/metadata/', username, '/appliances.csv')
  appliances <- read.csv(path_to_file, check.names = F)
  
  return(list(appliances = as.list(appliances)))
}

#* @post /setappliances
#* @serializer json
function(req) {
  form_data <- req$body
  username <- form_data$username
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  if (!username %in% buildingData$`Building Name`) return(list(status = 'Error: Username not found'))
  
  file_dir <- paste0('../step1/metadata/', username, '/appliances.csv')
  if (!file.exists(file_dir)) {
    new_df <- as.data.frame(matrix(nrow = 0, ncol = length(form_data)-1))
    colnames(new_df) <- names(form_data)[2:length(form_data)]
    write.csv(new_df, file_dir, row.names = F)
  }
  
  applianceData <- read.csv(paste0('../step1/metadata/', username, '/appliances.csv'), check.names = F)
  if (length(form_data)-1 == ncol(applianceData)) {
    
    my_list <- lapply(form_data, function(x) if (is.null(x)) NA else x)
    new_row <- as.data.frame(my_list, check.names = F)
    new_row <- new_row[, colnames(applianceData)]
    
    if (!my_list$`Appliances Name` %in% applianceData$`Appliances Name`) {
      applianceData <- rbind(applianceData, new_row)
      write.csv(applianceData, paste0('../step1/metadata/', username, '/appliances.csv'), row.names = F)
      return(status = 'success! New appliance added')
    } else {
      applianceData[applianceData$`Appliances Name` == my_list$`Appliances Name`, ] <- new_row
      write.csv(applianceData, paste0('../step1/metadata/', username, '/appliances.csv'), row.names = F)
      return(status = 'success! Appliance information updated')
    }
  }
  return(status = 'failure', message = 'Not all information was included. Empty fields should be sent as NULL')
}

#* @delete /delappliance
#* @serializer unboxedJSON
function(username, applianceName) {
  if (is.na(applianceName)) return (status = 'failure', message = 'Empty buildingName')
  if (is.null(applianceName)) return (status = 'failure', message = 'Empty buildingName')
  if (nchar(applianceName) == 0) return (status = 'failure', message = 'Empty buildingName')
  
  applianceData <- read.csv(paste0('../step1/metadata/', username, '/appliances.csv'), check.names = F)
  
  if (!applianceName %in% applianceData$`Appliances Name`) return (status = 'failure', message = paste0('applianceName ', applianceName, ' does not exist.'))
  else applianceData <- applianceData[-c(which(applianceData$`Appliances Name` == applianceName)), ]
  
  write.csv(applianceData, paste0('../step1/metadata/', username, '/appliances.csv'), row.names = F)
  return(status = 'success')
}



#### Step 1: Image Upload ####
#* @post /uploadimg
#* @serializer json
function(req, username, rename = NA) {
  if (is.na(username)) return(list(status = 'Error: Username cannot be empty'))
  buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
  if (!username %in% buildingData$`Building Name`) return(list(status = 'Error: Username does not exist in DB'))
  bodyfiles <- req$body$imgfile$value
  folderPath <- paste0('../step1/metadata/', username, '/images')
  if (!dir.exists(folderPath)) dir.create(folderPath)
  # Handle rename parameter
  if (!is.na(rename) && nchar(rename) > 0) {
    # Sanitize rename input to remove invalid filename characters
    rename <- gsub("[^[:alnum:]_-]", "", rename)
    if (nchar(rename) == 0) return(list(status = 'Error: Invalid rename value after sanitization'))
    fileName <- paste0(rename, '_', as.numeric(Sys.time()))
  } else {
    fileName <- paste0('image_', as.numeric(Sys.time()))
  }
  # Construct final file path
  finalPath <- paste0(folderPath, '/', fileName, '.png')
  # Write the file
  writeBin(bodyfiles, finalPath)
  
  return(list(status = 'Success'))
}
# function(req, username, mode, appliance = NA, section = NA, rename = NA) {
#   if (is.na(username)) return(list(status = 'Error: Username cannot be empty'))
#   if (is.na(mode)) return(list(status = 'Error: Mode name cannot be empty'))
#   if (mode == 'Appliance' & is.na(appliance)) return(list(status = 'Error: Appliance name cannot be empty'))
#   if (mode == 'Section' & is.na(section)) return(list(status = 'Error: Section name cannot be empty'))
#   
#   buildingData <- read.csv('../step1/BuildingData.csv', check.names = F)
#   if (!username %in% buildingData$`Building Name`) return(list(status = 'Error: Username does not exist in DB'))
#   
#   bodyfiles <- req$body$imgfile$value
#   
#   folderPath <- paste0('../step1/metadata/', username, '/images')
#   if (!dir.exists(folderPath)) dir.create(folderPath)
#   
#   # if (mode == 'Outside') folderPath <- paste0(folderPath, '/', 'building')
#   # else if (mode == 'Meter') folderPath <- paste0(folderPath, '/', 'meter')
#   # else if (mode == 'Bill') folderPath <- paste0(folderPath, '/', 'bill')
#   # else if (mode == 'Section') folderPath <- paste0(folderPath, '/', section, '_sketch')
#   # else if (mode == 'Appliance') paste0(folderPath, '/', appliance, '_appliance')
#   # 
#   # folderPath <- paste0(folderPath, '_', as.numeric(Sys.time()), '.png')
#   # writeBin(bodyfiles, folderPath)
#   # return(list(status = 'Success'))
# 
#   
#   baseName <- if (mode == 'Outside') 'building'
#   else if (mode == 'Meter') 'meter'
#   else if (mode == 'Bill') 'bill'
#   else if (mode == 'Section') paste0(section, '_sketch')
#   else if (mode == 'Appliance') paste0(appliance, '_appliance')
#   else return(list(status = 'Error: Invalid mode'))
# 
#   # Handle rename parameter
#   if (!is.na(rename) && nchar(rename) > 0) {
#     # Sanitize rename input to remove invalid filename characters
#     rename <- gsub("[^[:alnum:]_-]", "", rename)
#     if (nchar(rename) == 0) return(list(status = 'Error: Invalid rename value after sanitization'))
#     fileName <- paste0(rename, '_', as.numeric(Sys.time()))
#   } else {
#     fileName <- paste0(baseName, '_', as.numeric(Sys.time()))
#   }
# 
#   # # Construct final file path
#   finalPath <- paste0(folderPath, '/', fileName, '.png')
#   #
#   # # Write the file
#   writeBin(bodyfiles, finalPath)
# 
#   return(list(status = 'Success'))
# }

#### Step 1: Distribution Box ####



# ============================== Helper Functions ==============================

get_distribution_csv_path <- function(username) {
  folderPath <- file.path("/srv/shiny-server/step1/metadata", username)
  if (!dir.exists(folderPath)) dir.create(folderPath, recursive = TRUE)
  file.path(folderPath, "distribution_db.csv")
}

get_breaker_csv_path <- function(username) {
  folderPath <- file.path("/srv/shiny-server/step1/metadata", username)
  if (!dir.exists(folderPath)) dir.create(folderPath, recursive = TRUE)
  file.path(folderPath, "breakers_db.csv")
}

# ============================== Distribution Box Section ==============================

#* Create distribution box entry
#* @post /distribution_box
#* @serializer json
function(username, db_name, section, no_of_breakers, condition) {
  username <- URLdecode(username)
  csv_path <- get_distribution_csv_path(username)
  
  if (!file.exists(csv_path)) {
    write.csv(data.frame(
      username = character(),
      db_name = character(),
      section = character(),
      no_of_breakers = integer(),
      condition = numeric(),
      stringsAsFactors = FALSE
    ), csv_path, row.names = FALSE)
  }
  
  if (username == "") return(list(status = "Error", message = "username is required"))
  if (db_name == "") return(list(status = "Error", message = "db_name is required"))
  if (section == "") return(list(status = "Error", message = "section is required"))
  no_of_breakers <- as.numeric(no_of_breakers)
  condition <- as.numeric(condition)
  if (is.na(no_of_breakers)) return(list(status = "Error", message = "no_of_breakers must be numeric"))
  if (is.na(condition) || condition < 1 || condition > 5) {
    return(list(status = "Error", message = "condition must be between 1 and 5"))
  }
  
  data <- read.csv(csv_path, stringsAsFactors = FALSE)
  new_row <- data.frame(
    username = username,
    db_name = db_name,
    section = section,
    no_of_breakers = as.integer(no_of_breakers),
    condition = condition,
    stringsAsFactors = FALSE
  )
  data <- rbind(data, new_row)
  write.csv(data, csv_path, row.names = FALSE)
  list(status = "Success", message = "Entry created", data = new_row)
}

#* Get all distribution boxes for user
#* @get /distribution_box
#* @serializer json
function(username = "") {
  username <- URLdecode(username)
  csv_path <- get_distribution_csv_path(username)
  if (!file.exists(csv_path)) return(list(status = "Error", message = "No entries found for this username"))
  read.csv(csv_path, stringsAsFactors = FALSE)
}

#* Get distribution box by username and db_name
#* @get /distribution_box/entry
#* @serializer json
function(username = "", db_name = "") {
  username <- URLdecode(username)
  db_name <- URLdecode(db_name)
  csv_path <- get_distribution_csv_path(username)
  if (!file.exists(csv_path)) return(list(status = "Error", message = "No entries found for this username"))
  data <- read.csv(csv_path, stringsAsFactors = FALSE)
  result <- subset(data, username == username & db_name == db_name)
  if (nrow(result) == 0) return(list(status = "Error", message = "No entry found"))
  return(result)
}

#* Update distribution box
#* @put /distribution_box
#* @serializer json
function(req, username = "", db_name = "") {
  username <- URLdecode(username)
  db_name <- URLdecode(db_name)
  csv_path <- get_distribution_csv_path(username)
  if (!file.exists(csv_path)) return(list(status = "Error", message = "No entry found"))
  data <- read.csv(csv_path, stringsAsFactors = FALSE)
  index <- which(data$username == username & data$db_name == db_name)
  if (length(index) == 0) return(list(status = "Error", message = "Entry not found"))
  
  body <- fromJSON(req$postBody)
  valid_fields <- setdiff(names(data), c("username", "db_name"))
  
  for (field in intersect(valid_fields, names(body))) {
    value <- body[[field]]
    if (field == "no_of_breakers") value <- as.integer(value)
    if (field == "condition") {
      value <- as.numeric(value)
      if (is.na(value) || value < 1 || value > 5)
        return(list(status = "Error", message = "condition must be between 1 and 5"))
    }
    data[index, field] <- value
  }
  
  write.csv(data, csv_path, row.names = FALSE)
  list(status = "Success", message = "Entry updated", updated_data = data[index, ])
}

#* Delete distribution box
#* @delete /distribution_box
#* @serializer json
function(username = "", db_name = "") {
  username <- URLdecode(username)
  db_name <- URLdecode(db_name)
  csv_path <- get_distribution_csv_path(username)
  if (!file.exists(csv_path)) return(list(status = "Error", message = "No entry found"))
  data <- read.csv(csv_path, stringsAsFactors = FALSE)
  index <- which(data$username == username & data$db_name == db_name)
  if (length(index) == 0) return(list(status = "Error", message = "Entry not found"))
  deleted <- data[index, ]
  data <- data[-index, ]
  write.csv(data, csv_path, row.names = FALSE)
  list(status = "Success", message = "Entry deleted", deleted_data = deleted)
}

# ============================== Breaker Box Section ==============================

valid_types <- c("MCCB", "MCB", "ACB")

get_next_breaker_id <- function(existing_ids) {
  if (length(existing_ids) == 0) return("BR001")
  nums <- as.integer(sub("BR", "", existing_ids))
  next_num <- max(nums, na.rm = TRUE) + 1
  sprintf("BR%03d", next_num)
}

#* Add breaker
#* @post /breakers
#* @serializer json
function(username, DB, Main_Breaker, Spare_Breaker, Ampere, Voltage, Type, Machines = "", Connected_DB = "", Temperature = NA) {
  username <- URLdecode(username)
  csv_path_breakers <- get_breaker_csv_path(username)
  
  if (!file.exists(csv_path_breakers)) {
    write.csv(data.frame(
      Breaker_ID = character(),
      username = character(),
      DB = character(),
      Main_Breaker = logical(),
      Spare_Breaker = logical(),
      Ampere = numeric(),
      Voltage = numeric(),
      Type = character(),
      Machines = character(),
      Connected_DB = character(),
      Temperature = numeric(),
      stringsAsFactors = FALSE
    ), csv_path_breakers, row.names = FALSE)
  }
  
  Main_Breaker <- tolower(as.character(Main_Breaker)) %in% c("true", "t", "1", "yes")
  Spare_Breaker <- tolower(as.character(Spare_Breaker)) %in% c("true", "t", "1", "yes")
  Type <- toupper(Type)
  if (!(Type %in% valid_types)) return(list(status = "Error", message = "Invalid Type"))
  
  Ampere <- as.numeric(Ampere)
  Voltage <- as.numeric(Voltage)
  if (is.na(Ampere) || Ampere <= 0) return(list(status = "Error", message = "Ampere must be a positive number"))
  if (is.na(Voltage) || Voltage <= 0) return(list(status = "Error", message = "Voltage must be a positive number"))
  
  if (!is.na(Temperature)) {
    Temperature <- as.numeric(Temperature)
    if (is.na(Temperature)) return(list(status = "Error", message = "Temperature must be numeric"))
  }
  
  data <- read.csv(csv_path_breakers, stringsAsFactors = FALSE)
  new_id <- get_next_breaker_id(data$Breaker_ID)
  
  new_row <- data.frame(
    Breaker_ID = new_id,
    username = username,
    DB = DB,
    Main_Breaker = Main_Breaker,
    Spare_Breaker = Spare_Breaker,
    Ampere = Ampere,
    Voltage = Voltage,
    Type = Type,
    Machines = Machines,
    Connected_DB = Connected_DB,
    Temperature = Temperature,
    stringsAsFactors = FALSE
  )
  
  data <- rbind(data, new_row)
  write.csv(data, csv_path_breakers, row.names = FALSE)
  list(status = "Success", message = "Breaker added", data = new_row)
}

#* Get all breakers for a user
#* @get /breakers
#* @serializer json
function(username = "") {
  username <- URLdecode(username)
  csv_path_breakers <- get_breaker_csv_path(username)
  if (!file.exists(csv_path_breakers)) return(list(status = "Error", message = "No breakers found for this user"))
  read.csv(csv_path_breakers, stringsAsFactors = FALSE)
}

#* Get breaker by ID
#* @get /breakers/entry
#* @serializer json
function(username = "", Breaker_ID = "") {
  username <- URLdecode(username)
  Breaker_ID <- URLdecode(Breaker_ID)
  csv_path_breakers <- get_breaker_csv_path(username)
  data <- read.csv(csv_path_breakers, stringsAsFactors = FALSE)
  result <- subset(data, Breaker_ID == Breaker_ID)
  if (nrow(result) == 0) return(list(status = "Error", message = "Breaker_ID not found"))
  return(result)
}

#* Update breaker
#* @put /breakers
#* @serializer json
function(req, username = "", Breaker_ID = "") {
  username <- URLdecode(username)
  Breaker_ID <- URLdecode(Breaker_ID)
  csv_path_breakers <- get_breaker_csv_path(username)
  data <- read.csv(csv_path_breakers, stringsAsFactors = FALSE)
  index <- which(data$Breaker_ID == Breaker_ID)
  if (length(index) == 0) return(list(status = "Error", message = "Breaker_ID not found"))
  
  body <- fromJSON(req$postBody)
  valid_fields <- setdiff(names(data), c("Breaker_ID", "username"))
  
  for (field in intersect(valid_fields, names(body))) {
    val <- body[[field]]
    if (field == "Type" && !(toupper(val) %in% valid_types)) return(list(status = "Error", message = "Invalid Type"))
    if (field %in% c("Ampere", "Voltage", "Temperature")) val <- as.numeric(val)
    if (field %in% c("Main_Breaker", "Spare_Breaker")) val <- tolower(as.character(val)) %in% c("true", "t", "1", "yes")
    data[index, field] <- val
  }
  
  write.csv(data, csv_path_breakers, row.names = FALSE)
  list(status = "Success", message = paste("Breaker", Breaker_ID, "updated"), updated_data = data[index, ])
}

#* Delete breaker
#* @delete /breakers
#* @serializer json
function(username = "", Breaker_ID = "") {
  username <- URLdecode(username)
  Breaker_ID <- URLdecode(Breaker_ID)
  csv_path_breakers <- get_breaker_csv_path(username)
  data <- read.csv(csv_path_breakers, stringsAsFactors = FALSE)
  index <- which(data$Breaker_ID == Breaker_ID)
  if (length(index) == 0) return(list(status = "Error", message = "Breaker_ID not found"))
  deleted <- data[index, ]
  data <- data[-index, ]
  write.csv(data, csv_path_breakers, row.names = FALSE)
  list(status = "Success", message = paste("Breaker", Breaker_ID, "deleted"), deleted_data = deleted)
}



#### Bill Reconciliation ####
#* @get /reconciliation
#* @serializer unboxedJSON
function(customer_id, disco) {
  
  #### Customer ID Input, Defining URL and Extracting HTML Content ####
  
  # Set local time
  Sys.setlocale("LC_TIME", "en_US.UTF-8")
  
  # Enter customer ID
  customerID <- customer_id

  disco <- tolower(disco)
  
  # Define the  Industrial Online bill URL
  url <- paste0("https://bill.pitc.com.pk/",disco,"bill/industrial?appno=", customerID)
  
  # Read the HTML content of the webpage
  page <- read_html(url)
  
  # Extract table data using rvest
  table <- page %>%
    html_table(fill = TRUE)
  
  #### Check whether the bill is industrial or general ####
  if (length(table) == 0) {
    # If not then read the general bill html
    url <- paste0("https://bill.pitc.com.pk/",disco,"bill/general?appno=", customerID)
    
    # Read the HTML content of the webpage
    page <- read_html(url)
    
    # Extract table data using rvest
    table <- page %>%
      html_table(fill = TRUE)
    
    # If no table is retrieved from either bill sites
    if (length(table) == 0) {
      cat("Incorrect Customer ID entered:", customerID, "\n")
      
      # Extract data from general bill html
    } else {
      #### Historical Data (General) ####
      
      # Extract history table
      result <- table[[6]]
      
      # Create dataframe
      history_df <- data.frame(table[[6]])
      
      # Use row 1 as column names
      colnames(history_df) <- history_df[1, ]
      
      # Remove row 1
      history_df <- history_df[-1, ]
      
      # Change MONTH column to date format
      history_df$MONTH <- dmy(paste0("01-", history_df$MONTH))
      history_df$MONTH <- format(history_df$MONTH, "%B-%Y")
      
      # Replace all non-numeric characters with an empty string
      history_df$UNITS <- gsub("[^0-9]", "", history_df$UNITS)
      
      # Convert the result to numeric
      history_df$UNITS <- as.numeric(history_df$UNITS)
      
      #### Summary Data (General) ####
      
      # Extract tables containing summary info
      summary_1 <- data.frame(table[[5]])
      summary_2 <- data.frame(table[[1]])
      summary_3 <- data.frame(table[[7]])
      summary_4 <- data.frame(table[[11]])
      summary_5 <- data.frame(table[[15]])
      summary_6 <- data.frame(table[[2]])
      
      # Store all values in respective variables
      username <- paste(unlist(strsplit(gsub("\\s{2,}", " ", summary_1$X1[2]), " "))[4:5], collapse = " ")
      # Changing billingMonth to date format
      billingMonth <- dmy(paste0("01-", summary_2$X4[2]))
      billingMonth <- format(billingMonth, "%B-%Y")
      customerID <- sub(".*?(\\d+)\\s*/.*", "\\1", summary_6$X1[2])
      referenceNumber <- summary_6$X1[4]
      tariff <- summary_6$X2[2]
      sanctionedLoad <- summary_6$X3[2]
      # Changing reading, issue and due date to date format
      dateReading <- dmy(summary_2$X5[2])
      dateReading <- format(dateReading, "%Y-%m-%d")
      dateIssue <- dmy(summary_2$X6[2])
      dateIssue <- format(dateIssue, "%Y-%m-%d")
      dateDue <- dmy(summary_2$X7[2])
      dateDue <- format(dateDue, "%Y-%m-%d")
      billTotal <- ifelse(summary_5$X5[1] == "", 0, as.numeric(summary_5$X5[1]))
      billLate <- ifelse(summary_5$X5[2] == "", 0, as.numeric(str_extract(summary_5$X5[2], "\\d+$")))
      unitsPeak <- 0
      unitsOffpeak <- as.numeric(summary_3$X2[2])
      unitsTotal <- as.numeric(summary_3$X2[2])
      MDI <- 0
      costEnergy <- ifelse(summary_3$X2[3] == "", 0, as.numeric(summary_3$X2[3]))
      costFixed <- ifelse(summary_3$X2[4] == "", 0, as.numeric(str_extract(summary_3$X2[4], "\\d+$")))
      costLPFPenalty <- 0
      costSeasonalCharges <- 0
      costMeterRent <- 0
      costServiceRent <- ifelse(summary_3$X2[5] == "", 0, as.numeric(summary_3$X2[5]))
      costVARFPA <- 0
      costQTRADJ <- ifelse(summary_3$X2[9] == "", 0, as.numeric(summary_3$X2[9]))
      costTotal <- ifelse(summary_3$X2[10] == "", 0, as.numeric(summary_3$X2[10]))
      taxEDuty <- ifelse(summary_3$X4[2] == "", 0, as.numeric(summary_3$X4[2]))
      taxTVFee <- ifelse(summary_3$X4[3] == "", 0, as.numeric(summary_3$X4[3]))
      taxGST <- ifelse(summary_3$X4[4] == "", 0, as.numeric(summary_3$X4[4]))
      taxIncome <- as.numeric(sub(".*?(\\d+)\\s*\\r\\n.*", "\\1", summary_3$X4[5], perl = TRUE))
      taxIncome <- ifelse(is.na(taxIncome), 0, taxIncome)
      taxExtra <- ifelse(summary_3$X4[6] == "", 0, as.numeric(summary_3$X4[6]))
      taxFurther <- ifelse(summary_3$X4[7] == "", 0, as.numeric(summary_3$X4[7]))
      taxITS <- 0
      taxSTAX <- 0
      taxNJSurcharge <- 0
      taxSales <- 0
      taxFCSurcharge <- ifelse(summary_3$X2[7] == "", 0, as.numeric(summary_3$X2[7]))
      taxTRSurcharge <- 0
      FPA <- unlist(strsplit(gsub("\\s{2,}", " ", summary_3$X4[13]), " "))
      taxFPA <- as.numeric(FPA[length(FPA)])
      if (is.na(taxFPA)) {taxFPA = 0}
      GSTFPA <- unlist(strsplit(gsub("\\s{2,}", " ", summary_3$X4[13]), " "))
      taxGSTADJ <- as.numeric(GSTFPA[1])
      if (is.na(taxGSTADJ)) {taxGSTADJ = 0}
      taxTotal <- ifelse(summary_3$X28[18] == "", 0, as.numeric(summary_3$X28[18]))
      defferedAmount <- ifelse(summary_3$X2[27] == "", 0, as.numeric(summary_3$X2[27]))
      outstandingAmount <- ifelse(summary_3$X4[28] == "", 0, as.numeric(summary_3$X4[28]))
      arrear <- ifelse(summary_4$X2[2] == "", 0, as.numeric(sub("/.*", "", summary_4$X2[2])))
      currentBill <- ifelse(summary_4$X2[3] == "", 0, as.numeric(summary_4$X2[3]))
      billAdjustment <- ifelse(summary_4$X2[4] == "", 0, as.numeric(summary_4$X2[4]))
      installment <- ifelse(summary_4$X2[5] == "", 0, as.numeric(summary_4$X2[5]))
      get_totalTaxFPA <- unlist(strsplit(summary_4$X2[7], "\\s+"))
      totalTaxFPA <- as.numeric(get_totalTaxFPA[which(get_totalTaxFPA != "")[1]])
      LPSurcharge <- ifelse(summary_4$X2[9] == "", 0, as.numeric(str_extract(summary_4$X2[9], "^\\d+")))
      
      # Create data frame
      summary_df <- data.frame(
        username = username,
        billingMonth = billingMonth,
        customerID = customerID,
        referenceNumber = referenceNumber,
        tariff = tariff,
        sanctionedLoad = sanctionedLoad,
        dateReading = dateReading,
        dateIssue = dateIssue,
        dateDue = dateDue,
        billTotal = billTotal,
        billLate = billLate,
        unitsPeak = unitsPeak,
        unitsOffpeak = unitsOffpeak,
        unitsTotal = unitsTotal,
        MDI = MDI,
        `Energy Cost` = costEnergy,
        costFixed = costFixed,
        `LPF Penalty` = costLPFPenalty,
        `Seasonal Charges` = costSeasonalCharges,
        `Meter Rent` = costMeterRent,
        `Service Rent` = costServiceRent,
        `Variable FPA` = costVARFPA,
        costQTRADJ = costQTRADJ,
        `Total Cost` = costTotal,
        `E-Duty` = taxEDuty,
        `TV Fee` = taxTVFee,
        GST = taxGST,
        `Income Tax` = taxIncome,
        `Extra Tax` = taxExtra,
        `Further Tax` = taxFurther,
        `ITS Tax` = taxITS,
        `S. Tax` = taxSTAX,
        `NJ Surcharge` = taxNJSurcharge,
        `Sales Tax` = taxSales,
        `FC Surcharge` = taxFCSurcharge,
        `TR Surcharge` = taxTRSurcharge,
        FPA = taxFPA,
        `GST. Adj.` = taxGSTADJ,
        `Total Tax` = taxTotal,
        `Deffered Amount` = defferedAmount,
        `Outstanding Amount` = outstandingAmount,
        Arrear = arrear,
        `Current Bill` = currentBill,
        `Bill Adjustment` = billAdjustment,
        Installment = installment,
        `Total FPA` = totalTaxFPA,
        `LP Surcharge` = LPSurcharge,
        `Costs & Adj` = 0,
        amountPaid = 0,
        paymentDate = 0,
        check.names = FALSE
      )
    }
    # If tables exist in industrial bill html then extract data
  } else {
    #### Historical Data (Industrial) ####
    
    # Extract history table
    result <- table[[7]]
    
    # Create dataframe
    history_df <- data.frame(table[[7]])
    
    # Use row 1 as column names
    colnames(history_df) <- history_df[1, ]
    
    # Remove row 1
    history_df <- history_df[-1, ]
    
    # Rename the UNITS column
    colnames(history_df)[colnames(history_df) == "KWH UNITS"] <- "UNITS"
    
    # Replace all non-numeric characters with an empty string
    history_df$MDI <- gsub("[^0-9]", "", history_df$MDI)
    
    # Check the number of rows in the data frame
    if (nrow(history_df) > 12) {
      # Keep only the first 12 rows
      history_df <- history_df[1:12, ]
    }
    
    # Convert X2 to numeric, replace non-numeric values with NA
    history_df$MDI <- as.numeric(history_df$MDI)
    
    # Find the index of the first NA value in MDI
    index <- which(is.na(history_df$MDI))[1]
    
    # Remove rows with NA values in MDI and all rows after it
    if (!is.na(index)) {
      history_df <- history_df[seq_len(index - 1), ]
    }
    
    # Change MONTH column to date format
    history_df$MONTH <- dmy(paste0("01-", history_df$MONTH))
    history_df$MONTH <- format(history_df$MONTH, "%B-%Y")
    
    #### Summary Data (Industrial) ####
    
    # Extract tables containing summary info
    summary_1 <- data.frame(table[[4]])
    summary_2 <- data.frame(table[[1]])
    summary_3 <- data.frame(table[[8]])
    summary_4 <- data.frame(table[[9]])
    summary_5 <- data.frame(table[[13]])
    summary_6 <- data.frame(table[[2]])
    
    # Store all values in respective variables
    username <- sub("\\s{2,}.*", "", summary_1$X1[2])
    # Changing billingMonth to date format
    billingMonth <- dmy(paste0("01-", summary_2$X5[3]))
    billingMonth <- format(billingMonth, "%B-%Y")
    customerID <- sub(".*?(\\d+)\\s*/.*", "\\1", summary_6$X1[3])
    referenceNumber <- summary_6$X1[5]
    tariff <- summary_6$X2[3]
    sanctionedLoad <- summary_6$X3[3]
    # Changing reading, issue and due date to date format
    dateReading <- dmy(summary_2$X6[3])
    dateReading <- format(dateReading, "%Y-%m-%d")
    dateIssue <- dmy(summary_2$X7[3])
    dateIssue <- format(dateIssue, "%Y-%m-%d")
    dateDue <- dmy(summary_2$X8[3])
    dateDue <- format(dateDue, "%Y-%m-%d")
    billTotal <- ifelse(summary_5$X2[7] == "", 0, as.numeric(summary_5$X2[7]))
    billLate <- ifelse(summary_5$X2[9] == "", 0, as.numeric(str_extract(summary_5$X2[9], "\\d+$")))
    unitsPeak <- as.numeric(sub(".*\\(O\\)\\s*(\\d+)\\s*\\(P\\).*", "\\1", summary_3[, 1][4]))
    unitsOffpeak <- as.numeric(sub(".*\\b(\\d+)\\b.*", "\\1", summary_3[, 1][4]))
    unitsTotal <- unitsPeak + unitsOffpeak
    MDI <- as.numeric(sub(".*\\b(\\d+)\\b.*", "\\1", summary_3[, ncol(summary_3)][4]))
    costEnergy <- ifelse(summary_4$X2[3] == "", 0, as.numeric(summary_4$X2[3]))
    costFixed <- ifelse(summary_4$X2[4] == "", 0, as.numeric(summary_4$X2[4]))
    costLPFPenalty <- ifelse(summary_4$X2[5] == "", 0, as.numeric(summary_4$X2[5]))
    costSeasonalCharges <- ifelse(summary_4$X2[6] == "", 0, as.numeric(summary_4$X2[6]))
    costMeterRent <- ifelse(summary_4$X2[7] == "", 0, as.numeric(summary_4$X2[7]))
    costServiceRent <- ifelse(summary_4$X2[8] == "", 0, as.numeric(summary_4$X2[8]))
    costVARFPA <- ifelse(summary_4$X2[9] == "", 0, as.numeric(summary_4$X2[9]))
    costQTRADJ <- ifelse(summary_4$X2[10] == "", 0, as.numeric(summary_4$X2[10]))
    costTotal <- ifelse(summary_4$X2[11] == "", 0, as.numeric(summary_4$X2[11]))
    taxEDuty <- ifelse(summary_4$X4[2] == "", 0, as.numeric(summary_4$X4[2]))
    taxTVFee <- ifelse(summary_4$X4[3] == "", 0, as.numeric(summary_4$X4[3]))
    taxGST <- ifelse(summary_4$X4[4] == "", 0, as.numeric(summary_4$X4[4]))
    taxIncome <- as.numeric(sub(".*?(\\d+)\\s*\\r\\n.*", "\\1", summary_4$X4[5], perl = TRUE))
    taxIncome <- ifelse(is.na(taxIncome), 0, taxIncome)
    taxExtra <- ifelse(summary_4$X4[6] == "", 0, as.numeric(summary_4$X4[6]))
    taxFurther <- ifelse(summary_4$X4[7] == "", 0, as.numeric(summary_4$X4[7]))
    taxITS <- ifelse(summary_4$X4[8] == "", 0, as.numeric(summary_4$X4[8]))
    taxSTAX <- ifelse(summary_4$X4[9] == "", 0, as.numeric(summary_4$X4[9]))
    taxNJSurcharge <- ifelse(summary_4$X4[10] == "", 0, as.numeric(summary_4$X4[10]))
    taxSales <- ifelse(summary_4$X4[11] == "", 0, as.numeric(summary_4$X4[11]))
    taxFCSurcharge <- ifelse(summary_4$X4[12] == "", 0, as.numeric(summary_4$X4[12]))
    taxTRSurcharge <- ifelse(summary_4$X30[13] == "", 0, as.numeric(summary_4$X30[13]))
    taxFPA <- ifelse(summary_4$X2[18] == "", 0, as.numeric(summary_4$X2[18]))
    taxGSTADJ <- ifelse(summary_4$X36[20] == "", 0, as.numeric(summary_4$X36[20]))
    taxTotal <- ifelse(summary_4$X2[19] == "", 0, as.numeric(summary_4$X2[19]))
    defferedAmount <- ifelse(summary_4$X2[29] == "", 0, as.numeric(summary_4$X2[29]))
    outstandingAmount <- ifelse(summary_4$X2[30] == "", 0, as.numeric(summary_4$X2[30]))
    arrear <- ifelse(summary_5$X2[1] == "", 0, as.numeric(summary_5$X2[1]))
    currentBill <- ifelse(summary_5$X2[2] == "", 0, as.numeric(summary_5$X2[2]))
    billAdjustment <- ifelse(summary_5$X2[3] == "", 0, as.numeric(summary_5$X2[3]))
    installment <- ifelse(summary_5$X2[4] == "", 0, as.numeric(summary_5$X2[4]))
    get_totalTaxFPA <- unlist(strsplit(summary_5$X2[5], "\\s+"))
    totalTaxFPA <- as.numeric(get_totalTaxFPA[which(get_totalTaxFPA != "")[1]])
    LPSurcharge <- ifelse(summary_5$X2[9] == "", 0, as.numeric(str_extract(summary_5$X2[9], "^\\d+")))
    
    # Create data frame
    summary_df <- data.frame(
      username = username,
      billingMonth = billingMonth,
      customerID = customerID,
      referenceNumber = referenceNumber,
      tariff = tariff,
      sanctionedLoad = sanctionedLoad,
      dateReading = dateReading,
      dateIssue = dateIssue,
      dateDue = dateDue,
      billTotal = billTotal,
      billLate = billLate,
      unitsPeak = unitsPeak,
      unitsOffpeak = unitsOffpeak,
      unitsTotal = unitsTotal,
      MDI = MDI,
      `Energy Cost` = costEnergy,
      costFixed = costFixed,
      `LPF Penalty` = costLPFPenalty,
      `Seasonal Charges` = costSeasonalCharges,
      `Meter Rent` = costMeterRent,
      `Service Rent` = costServiceRent,
      `Variable FPA` = costVARFPA,
      costQTRADJ = costQTRADJ,
      `Total Cost` = costTotal,
      `E-Duty` = taxEDuty,
      `TV Fee` = taxTVFee,
      GST = taxGST,
      `Income Tax` = taxIncome,
      `Extra Tax` = taxExtra,
      `Further Tax` = taxFurther,
      `ITS Tax` = taxITS,
      `S. Tax` = taxSTAX,
      `NJ Surcharge` = taxNJSurcharge,
      `Sales Tax` = taxSales,
      `FC Surcharge` = taxFCSurcharge,
      `TR Surcharge` = taxTRSurcharge,
      FPA = taxFPA,
      `GST. Adj.` = taxGSTADJ,
      `Total Tax` = taxTotal,
      `Deffered Amount` = defferedAmount,
      `Outstanding Amount` = outstandingAmount,
      Arrear = arrear,
      `Current Bill` = currentBill,
      `Bill Adjustment` = billAdjustment,
      Installment = installment,
      `Total FPA` = totalTaxFPA,
      `LP Surcharge` = LPSurcharge,
      `Costs & Adj` = 0,
      amountPaid = 0,
      paymentDate = 0,
      check.names = FALSE
    )
  }
  return(list(history = as.list(history_df), summary = as.list(summary_df)))
}

#* @get /reconciliation_file
#* @serializer json
function(req) {
  req_body <- req$body
  saveRDS(req_body, 'temp_file_info.rds')
}

# #### List all files in a folder  ####
# #* @get /listFiles
# #* @param folderPath The path to the folder whose files we want to list.
# #* @serializer unboxedJSON
# function(folderPath) {
#   # Check if folderPath is provided
#   if (is.null(folderPath) || nchar(folderPath) == 0) {
#     return(list(error = "Missing folderPath parameter"))
#   }
#   
#   # Check if the provided folder exists
#   if (!dir.exists(folderPath)) {
#     return(list(error = "Folder does not exist"))
#   }
#   
#   # List all files in the folder (non-recursive, change recursive = TRUE if needed)
#   files <- list.files(path = folderPath, full.names = FALSE)
#   
#   return(list(files = files))
# }

#### Fetch Meter Data ####
#* @get /fetchMeterData
#* @param meterNumber The meter number (e.g., SM8-002 or SMT8).
#* @serializer unboxedJSON

function(meterNumber) {
  # Check if meterNumber is provided
  if (is.null(meterNumber) || nchar(meterNumber) == 0) {
    return(list(error = "Missing meterNumber parameter"))
  }
  
  # Base directory for meter data
  base_dir <- "/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/"
  
  # Extract the live folder number from meterNumber
  # For SM8-002 or SMT8, extract the number after SM/SMT (e.g., 8 → live8)
  # For SM-004, default to live1 since there's no number between SM and the hyphen
  folder_number <- gsub("^(SM|SMT)([0-9]*).*", "\\2", meterNumber)
  live_folder <- if (folder_number == "") "live1" else paste0("live", folder_number)
  
  # Construct the full path to the live folder
  full_folder_path <- paste0(base_dir, live_folder, "/")
  
  # Check if the live folder exists
  if (!dir.exists(full_folder_path)) {
    return(list(error = paste0("Live folder '", live_folder, "' does not exist")))
  }
  
  # Construct the expected CSV file name (e.g., SM-004.csv)
  csv_file <- paste0(meterNumber, ".csv")
  full_file_path <- paste0(full_folder_path, csv_file)
  
  # Check if the CSV file exists
  if (!file.exists(full_file_path)) {
    return(list(error = paste0("File '", csv_file, "' does not exist in folder '", live_folder, "'")))
  }
  
  # Read the CSV file
  meter_data <- read.csv(full_file_path, check.names = FALSE)
  
  # Return the contents of the CSV file as a list
  return(list(data = as.list(meter_data)))
}  

# #### For Fleet Profiles ####
# #* @get /fleetProfile
# #* @serializer unboxedJSON
# 
# function() {
#   # Define the file path for fleet_profiles.csv
#   file_path <- "/srv/shiny-server/EnvizFleet/fleet_profiles.csv"
#   
#   # Check if the file exists
#   if (!file.exists(file_path)) {
#     return(list(error = "File not found"))
#   }
#   
#   # Read the CSV file
#   fleet_data <- read.csv(file_path, check.names = FALSE)
#   
#   # Return the file content as JSON
#   return(list(fleet_profiles = as.list(fleet_data)))
# }
# 
# 
# 
# 
# #### For Client Profiles and settings ####
# #* @get /clientData
# #* @serializer unboxedJSON
# function() {
#   # Define file paths for client_profiles.csv and client_settings.csv
#   client_profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
#   client_settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
#   
#   
#   # Check if the files exist
#   if (!file.exists(client_profiles_path)) {
#     return(list(error = "client_profiles.csv file not found"))
#   }
#   if (!file.exists(client_settings_path)) {
#     return(list(error = "client_settings.csv file not found"))
#   }
#   
#   # Read the CSV files
#   client_profiles <- read.csv(client_profiles_path, check.names = FALSE)
#   client_settings <- read.csv(client_settings_path, check.names = FALSE)
#   
#   # Return the data from both CSVs in a single JSON response
#   return(list(
#     client_profiles = as.list(client_profiles),
#     client_settings = as.list(client_settings)
#   ))
# }

####  For Update Client Settings ####
#* @post /updateClientSettings
#* @param username The username whose settings need to be updated.
#* @param column_name The column name to be updated.
#* @param new_value The new value to set for the specified column.
#* @serializer unboxedJSON
function(username, column_name, new_value) {
  
  # Define the file path for client_settings.csv
  client_settings_path <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
  
  # Check if the file exists
  if (!file.exists(client_settings_path)) {
    return(list(error = "client_settings.csv file not found"))
  }
  
  # Read the CSV file
  client_settings <- read.csv(client_settings_path, check.names = FALSE)
  
  # Check if the username exists in the file
  if (!(username %in% client_settings$user)) {
    return(list(error = paste("Username", username, "not found")))
  }
  
  # Check if the column_name exists in the file
  if (!(column_name %in% colnames(client_settings))) {
    return(list(error = paste("Column", column_name, "not found in the file")))
  }
  
  # Update the specified column with the new value
  client_settings[client_settings$user == username, column_name] <- new_value
  
  # Save the updated CSV back to the file
  write.csv(client_settings, client_settings_path, row.names = FALSE, quote = FALSE)
  
  return(list(status = "success", message = paste("Successfully updated", column_name, "for", username)))
}

#### Sign Up ####
#* Register a new user
#* @post /register
#* @param username:string Username of the new user
#* @param email:string Email address of the new user
#* @param password:string Password for the new user
#* @param confirm_password:string Password confirmation
#* @serializer unboxedJSON
function(req) {
  # Extract body as a list
  body <- req$body
  
  # Check for required parameters
  required_fields <- c("username", "email", "password", "confirm_password")
  if (!all(required_fields %in% names(body))) {
    return(list(status = "error", message = "All fields are required"))
  }
  
  # Extract required parameters
  username <- body$username
  email <- body$email
  password <- body$password
  confirm_password <- body$confirm_password
  
  # Check for empty parameters
  if (is.null(username) || is.null(email) || is.null(password) || is.null(confirm_password) ||
      username == "" || email == "" || password == "" || confirm_password == "") {
    return(list(status = "error", message = "No field can be empty"))
  }
  
  # Check if password matches confirm_password
  if (password != confirm_password) {
    return(list(status = "error", message = "Passwords do not match"))
  }
  
  # Validate email format
  if (!grepl("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email)) {
    return(list(status = "error", message = "Invalid email format"))
  }
  
  # Define CSV file path
  csv_file <- "/srv/shiny-server/Clients Usage Data/client_settings.csv"
  
  # Read existing CSV data
  users <- read.csv(csv_file, stringsAsFactors = FALSE, check.names = FALSE)
  
  # Check for duplicate username
  if (nrow(users) > 0 && username %in% users$user) {
    return(list(status = "error", message = "Username already exists"))
  }
  
  # Check for duplicate email
  if (nrow(users) > 0 && email %in% users$email) {
    return(list(status = "error", message = "Email already exists"))
  }
  
  # Handle dynamic parameters (excluding required fields and confirm_password)
  exclude_fields <- c("username", "email", "password", "confirm_password")
  extra_fields <- setdiff(names(body), exclude_fields)
  
  # Update CSV with new columns for extra fields
  new_columns <- extra_fields[!(extra_fields %in% colnames(users))]
  if (length(new_columns) > 0) {
    if (nrow(users) == 0) {
      users <- data.frame(
        matrix(NA, nrow = 0, ncol = length(c(colnames(users), new_columns))),
        stringsAsFactors = FALSE
      )
      colnames(users) <- c(colnames(users), new_columns)
    } else {
      for (field in new_columns) {
        users[[field]] <- NA
      }
    }
    write.csv(users, csv_file, row.names = FALSE, quote = FALSE)
  }
  
  # Create user data frame with all CSV columns
  all_columns <- colnames(users)
  user <- data.frame(matrix(NA, nrow = 1, ncol = length(all_columns)))
  colnames(user) <- all_columns
  
  # Populate required fields
  user$user <- username
  user$email <- email
  user$password <- password
  
  # Populate extra fields
  for (field in extra_fields) {
    user[[field]] <- body[[field]]
  }
  
  # Append user to CSV
  write.table(user, csv_file, append = TRUE, col.names = FALSE, sep = ",", row.names = FALSE, quote = FALSE)
  
  # Prepare response (exclude confirm_password)
  response_user <- body
  response_user$confirm_password <- NULL
  
  # Return success response
  list(status = "success", message = "User registered successfully", data = response_user)
}
#### Display Users ####
#* Retrieve usernames associated with a given email
#* @post /display_users
#* @param email:string The email address to search for
#* @serializer unboxedJSON
function(email) {
  # Check for missing or empty email
  if (is.null(email) || email == "") {
    return(list(status = "error", message = "Email is required"))
  }
  
  # Validate email format
  if (!grepl("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email)) {
    return(list(status = "error", message = "Invalid email format"))
  }
  
  # Define CSV file path
  csv_file <- "../Clients Usage Data/client_settings.csv"
  
  # Check if CSV file exists
  if (!file.exists(csv_file)) {
    return(list(status = "error", message = "No user data found"))
  }
  
  # Read CSV data
  users <- read.csv(csv_file, stringsAsFactors = FALSE, check.names = FALSE)
  
  # Find rows where email matches
  matching_users <- users[users$email == email, "user", drop = TRUE]
  
  # Check if any users found
  if (length(matching_users) == 0) {
    return(list(status = "error", message = "No users found with this email"))
  }
  
  # Return list of usernames (filter out NA/null)
  usernames <- matching_users[!is.na(matching_users)]
  
  return(list(
    status = "success",
    message = "Users retrieved successfully",
    data = list(usernames = usernames)
  ))
}
#### User Sign in ####
#* Sign in after user selection using Google Account
#* @post /email_signin
#* @param username:string The username to sign in with
#* @serializer unboxedJSON
function(username) {
  # Missing username
  if (is.null(username) || username == "") return(list(error = "Username is required"))
  
  clients <- read.csv("../Clients Usage Data/client_profiles.csv")
  client_settings <- read.csv("../Clients Usage Data/client_settings.csv")
  clients <- merge(clients, client_settings)
  
  client <- clients[clients$user == username, ]
  
  if (nrow(client) == 0) return(list(error = "Invalid Username"))
  else {
    token <- create_jwt(list(user = username), "enviz123")
    client_setting <- client_settings[client_settings$user == username, ]
    
    # if client is new
    if (nrow(client_setting) == 0) {
      # Add a new row
      client_settings[nrow(client_settings)+1, ] <- NA
      
      # Fill the new row
      client_settings$user[nrow(client_settings)] <- username
      client_settings$Meter_id[nrow(client_settings)] <- clients$Meter_id[clients$user == username]
      client_settings[nrow(client_settings)] <- client_settings[1, ]
      write.csv(client_settings, "../Clients Usage Data/client_settings.csv", row.names = F, quote = F)
      client_setting <- client_settings[nrow(client_settings), ]
    }
    
    return(list(token = token, client = as.list(client)))
  }
}

create_jwt <- function(claims, secret) {
  header <- '{"alg":"HS256", "typ":"JWT"}'
  payload <- toJSON(claims)
  
  encoded_header <- base64encode(charToRaw(header))
  encoded_payload <- base64encode(charToRaw(payload))
  
  signature <- base64encode(charToRaw(hmac(charToRaw(paste(encoded_header, encoded_payload, sep = '.')), key = charToRaw(secret), algo = "sha256")))
  
  paste(encoded_header, encoded_payload, signature, sep = '.')
}





#### Notification & Alerts ####
getNodesAtLevel <- function(file_path, level) {
  buildingMap <- readRDS(file_path)
  
  retrieveNodes <- function(tree, level) {
    if (level == 0) {
      if (!is.null(tree$Name)) return(tree$Name)
      else return(NA)
    } else {
      nodes <- list()
      for (node in tree) {
        if (is.list(node)) {
          nodes <- c(nodes, retrieveNodes(node, level - 1))
        }
      }
      return(unlist(nodes))
    }
  }
  
  # Initialize level and usage_columns
  curr_level <- as.numeric(level)
  usage_columns <- NULL
  
  # Loop until no NA values
  while(TRUE) {
    usage_columns <- retrieveNodes(buildingMap, curr_level)
    if (any(is.na(usage_columns))) {
      curr_level <- curr_level + 1
    } else {
      break
    }
  }
  
  return(usage_columns)
}

#* @get /getUsageColumns
#* @serializer unboxedJSON
function(meter_id) {
  # Construct the file path based on the meter_id
  file_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
  
  # Assuming the level you're interested in is '0'
  level <- "0"
  result <- getNodesAtLevel(file_path, level)
  
  if (is.null(result)) {
    # Return an error message if no columns are found
    return(list(error = "No columns found or file does not exist"))
  }
  
  # Convert result to character for JSON compatibility
  return(list(usage_columns = as.character(result)))
}


#### Notification API's ####
#* @get /getAlerts
#* @param meter_id The meter ID of the user
#* @serializer unboxedJSON
function(meter_id) {
  
  # Validate input
  if (is.null(meter_id) || meter_id == "") {
    return(list(error = "Missing meter_id parameter"))
  }
  
  # Construct file path
  file_path <- paste0("/srv/shiny-server/EnvizReports/alerts_notif/", meter_id, "_alerts.csv")
  
  # Check if file exists
  if (!file.exists(file_path)) {
    return(list(error = paste0("Alert file not found for meter_id: ", meter_id)))
  }
  
  # Read the CSV file
  alert_data <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
  
  # Convert to list of rows
  alert_list <- unname(split(alert_data, seq(nrow(alert_data))))
  
  # Return the data
  return(alert_list)
}




# #* @get /machineContribution
# #* @serializer unboxedJSON
# machineContribution <- function(username, is_live = FALSE, start_date = NULL, end_date = NULL) {
#   
#   # Load dplyr for consistent calculations
#   library(dplyr)
#   
#   if (is_live) {
#     if (is.null(username)) {
#       return(list(error = "Missing username"))
#     }
#     
#     # Step 2: Load client profile and get Meter ID
#     profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
#     profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
#     user_row <- profiles[profiles$user == username, ]
#     if (nrow(user_row) == 0) return(list(error = "Username not found"))
#     meter_id <- as.character(user_row$Meter_id[1])
#     
#     # Step 3: Load config file
#     config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
#     if (!file.exists(config_path)) return(list(error = "Config file not found"))
#     config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
#     
#     # Step 4: Load buildingMap usage columns
#     getUsageColumns <- function(file_path) {
#       buildingMap <- readRDS(file_path)
#       retrieveNodes <- function(tree, level) {
#         if (level == 0) {
#           if (!is.null(tree$Name)) return(tree$Name) else return(NA)
#         } else {
#           nodes <- list()
#           for (node in tree) {
#             if (is.list(node)) {
#               nodes <- c(nodes, retrieveNodes(node, level - 1))
#             }
#           }
#           return(unlist(nodes))
#         }
#       }
#       
#       curr_level <- 0
#       while (TRUE) {
#         usage_columns <<- retrieveNodes(buildingMap, curr_level)
#         if (any(is.na(usage_columns))) {
#           curr_level <- curr_level + 1
#         } else break
#       }
#       return(as.character(usage_columns))
#     }
#     
#     rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
#     if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
#     usage_columns <- getUsageColumns(rds_path)
#     
#     # Step 5: Create machine_names
#     config_machines <- config_df$new_name
#     config_machines <- config_machines[!grepl("(?i)phase", config_machines)]
#     machine_names <- setdiff(config_machines, usage_columns)
#     if (length(machine_names) == 0) {
#       machine_names <- usage_columns
#     }
#     machine_names <- as.character(machine_names)
#     
#     # Get metadata with formulas for each machine
#     machine_metadata <- config_df[config_df$new_name %in% machine_names, 
#                                   c("new_name", "old_name", "Power", "Voltage")]
#     names(machine_metadata) <- c("machine", "old_name", "power_expr", "voltage_expr")
#     
#     machine_power_sum <- setNames(rep(0, length(machine_names)), machine_names)
#     total_power_sum <- 0
#     
#     for (i in 1:nrow(machine_metadata)) {
#       machine <- machine_metadata$machine[i]
#       old_name <- machine_metadata$old_name[i]
#       power_expr <- machine_metadata$power_expr[i]
#       voltage_expr <- machine_metadata$voltage_expr[i]
#       
#       # Resolve live folder
#       match <- regmatches(old_name, regexpr("(?<=SM)\\d*", old_name, perl = TRUE))
#       live_folder <- ifelse(match == "", "live1", paste0("live", match))
#       
#       file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", 
#                           live_folder, "/", old_name, ".csv")
#       if (!file.exists(file_path)) next
#       
#       # Read meter data
#       meter_df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
#       
#       # Check if first row is header
#       if (nrow(meter_df) > 0 && meter_df[1, 1] == 'TimeStamp') {
#         colnames(meter_df) = meter_df[1, ]
#         meter_df <- meter_df[-1, ]
#       }
#       
#       if (nrow(meter_df) == 0) next
#       
#       # Convert numeric columns
#       meter_df[, 3:ncol(meter_df)] <- apply(meter_df[, 3:ncol(meter_df)], 2, FUN = as.numeric)
#       
#       # Get last row as data frame
#       last_meter_df <- meter_df[nrow(meter_df), , drop = FALSE]
#       
#       # Calculate power using mutate with eval(parse()) - EXACTLY like dashboard
#       tryCatch({
#         power_calculation <- last_meter_df %>%
#           mutate(power_temp = eval(parse(text = power_expr)))
#         
#         val <- power_calculation$power_temp[1]
#         
#         # Power is in watts, NO conversion here (will convert once at the end)
#         if (!is.na(val)) {
#           machine_power_sum[machine] <- val
#           total_power_sum <- total_power_sum + val
#         }
#       }, error = function(e) {
#         # If evaluation fails, skip this machine
#         next
#       })
#     }
#     
#     if (total_power_sum == 0) {
#       return(list(error = "No data found for contribution calculation"))
#     }
#     
#     # Now convert to kW and calculate percentages
#     machine_power_vec <- as.numeric(unname(machine_power_sum))
#     contribution_df <- data.frame(
#       machine_name = machine_names,
#       contribution_kW = round(machine_power_vec / 1000, 2),  # Convert W to kW here
#       contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
#       stringsAsFactors = FALSE
#     )
#     
#     contribution_df$contribution_kW[is.na(contribution_df$contribution_kW)] <- 0
#     contribution_df$contribution_percent[is.na(contribution_df$contribution_percent)] <- 0
#     contribution_df$contribution_kW <- paste0(contribution_df$contribution_kW, " kW")
#     contribution_df$contribution_percent <- paste0(contribution_df$contribution_percent, "%")
#     
#     return(list(
#       contributions = unname(split(contribution_df, seq(nrow(contribution_df))))
#     ))
#   }
#   
#   # Historical data section (is_live = FALSE)
#   # Step 1: Validate input
#   if (is.null(username) || is.null(start_date)) {
#     return(list(error = "Missing username or start date"))
#   }
#   
#   # Step 2: Load client profile and get Meter ID
#   profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
#   profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
#   user_row <- profiles[profiles$user == username, ]
#   if (nrow(user_row) == 0) return(list(error = "Username not found"))
#   meter_id <- as.character(user_row$Meter_id[1])
#   
#   # Step 3: Load config file
#   config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
#   if (!file.exists(config_path)) return(list(error = "Config file not found"))
#   config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
#   
#   # Step 4: Load buildingMap usage columns
#   getUsageColumns <- function(file_path) {
#     buildingMap <- readRDS(file_path)
#     retrieveNodes <- function(tree, level) {
#       if (level == 0) {
#         if (!is.null(tree$Name)) return(tree$Name) else return(NA)
#       } else {
#         nodes <- list()
#         for (node in tree) {
#           if (is.list(node)) {
#             nodes <- c(nodes, retrieveNodes(node, level - 1))
#           }
#         }
#         return(unlist(nodes))
#       }
#     }
#     
#     curr_level <- 0
#     while (TRUE) {
#       usage_columns <<- retrieveNodes(buildingMap, curr_level)
#       if (any(is.na(usage_columns))) {
#         curr_level <- curr_level + 1
#       } else break
#     }
#     return(as.character(usage_columns))
#   }
#   
#   rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
#   if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
#   usage_columns <- getUsageColumns(rds_path)
#   
#   # Step 5: Create machine_names
#   config_machines <- config_df$new_name
#   config_machines <- config_machines[!grepl("(?i)phase", config_machines)]
#   
#   machine_names <- setdiff(config_machines, usage_columns)
#   if (length(machine_names) == 0) {
#     machine_names <- usage_columns
#   }
#   
#   machine_names <- as.character(machine_names)
#   
#   # Step 6: Define dates
#   if (is.null(end_date)) {
#     dates <- as.Date(start_date)
#   } else {
#     dates <- seq(as.Date(start_date), as.Date(end_date), by = "day")
#   }
#   
#   # Step 7: Initialize accumulators
#   total_power_sum <- 0
#   machine_power_sum <- setNames(rep(0, length(machine_names)), machine_names)
#   
#   # Step 8: Process hour-wise files
#   for (d in dates) {
#     d <- as.Date(d, origin = "1970-01-01")
#     date_str <- format(d, "%Y-%m-%d")
#     filepath <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/", 
#                        username, "/", meter_id, "_", date_str, "_hour-wise.csv")
#     if (!file.exists(filepath)) next
#     
#     df <- read.csv(filepath, check.names = FALSE, stringsAsFactors = FALSE)
#     
#     machine_cols <- paste0(machine_names, "_[kW]")
#     available_cols <- machine_cols[machine_cols %in% names(df)]
#     
#     if (length(available_cols) == 0) next
#     
#     # Total power
#     total_power <- sum(rowSums(df[, available_cols, drop = FALSE], na.rm = TRUE), na.rm = TRUE)
#     total_power_sum <- total_power_sum + total_power
#     
#     # Each machine's power
#     for (m in machine_names) {
#       col_name <- paste0(m, "_[kW]")
#       if (col_name %in% names(df)) {
#         machine_power_sum[m] <- machine_power_sum[m] + sum(df[[col_name]], na.rm = TRUE)
#       }
#     }
#   }
#   
#   if (total_power_sum == 0) {
#     return(list(error = "No data available for the given date range"))
#   }
#   
#   # Step 9: Calculate contribution
#   # NOTE: Hour-wise data columns say [kW] but values are actually in Watts
#   machine_power_vec <- as.numeric(unname(machine_power_sum))
#   
#   contribution_df <- data.frame(
#     machine_name = machine_names,
#     contribution_kW = round(machine_power_vec / 1000, 2),  # Convert W to kW
#     contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
#     stringsAsFactors = FALSE
#   )
#   
#   contribution_df$contribution_kW[is.na(contribution_df$contribution_kW)] <- 0
#   contribution_df$contribution_percent[is.na(contribution_df$contribution_percent)] <- 0
#   contribution_df$contribution_kW <- paste0(contribution_df$contribution_kW, " kW")
#   contribution_df$contribution_percent <- paste0(contribution_df$contribution_percent, "%")
#   
#   # Step 10: Return clean output
#   return(list(
#     contributions = unname(split(contribution_df, seq(nrow(contribution_df))))
#   ))
# }
#* @get /machineContribution
#* @serializer unboxedJSON
machineContribution <- function(username, is_live = FALSE, start_date = NULL, end_date = NULL) {
  
  # Load dplyr for consistent calculations
  library(dplyr)
  
  if (is_live) {
    if (is.null(username)) {
      return(list(error = "Missing username"))
    }
    
    # Step 2: Load client profile and get Meter ID & Meter Type
    profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
    profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
    user_row <- profiles[profiles$user == username, ]
    if (nrow(user_row) == 0) return(list(error = "Username not found"))
    
    meter_id <- as.character(user_row$Meter_id[1])
    meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
    
    # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
    first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
    
    # Step 3: Load config file - EGAUGE: Added conditional logic based on meter type
    if(meter_type == 'neubolt') {
      config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
      if (!file.exists(config_path)) return(list(error = "Config file not found"))
      config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
    } else {
      # EGAUGE: Use first meter ID and egauge config path
      config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", first_meter_id, "_New_column_names.csv")
      if (!file.exists(config_path)) return(list(error = "Egauge config file not found"))
      config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
    }
    
    # # Step 4: Load buildingMap usage columns
    # getUsageColumns <- function(file_path) {
    #   buildingMap <- readRDS(file_path)
    #   retrieveNodes <- function(tree, level) {
    #     if (level == 0) {
    #       if (!is.null(tree$Name)) return(tree$Name) else return(NA)
    #     } else {
    #       nodes <- list()
    #       for (node in tree) {
    #         if (is.list(node)) {
    #           nodes <- c(nodes, retrieveNodes(node, level - 1))
    #         }
    #       }
    #       return(unlist(nodes))
    #     }
    #   }
    #   
    #   curr_level <- 0
    #   while (TRUE) {
    #     usage_columns <<- retrieveNodes(buildingMap, curr_level)
    #     if (any(is.na(usage_columns))) {
    #       curr_level <- curr_level + 1
    #     } else break
    #   }
    #   return(as.character(usage_columns))
    # }
    getUsageColumns <- function(file_path) {
      buildingMap <- readRDS(file_path)
      
      retrieveNodes <- function(tree, level) {
        if (level == 0) {
          # At level 0, return the Name if it exists
          if (is.list(tree) && !is.null(tree$Name)) {
            return(tree$Name)
          } else if (is.list(tree)) {
            # If tree is a list of nodes, get their names
            names_list <- lapply(tree, function(node) {
              if (is.list(node) && !is.null(node$Name)) {
                return(node$Name)
              }
              return(NA)
            })
            return(unlist(names_list))
          }
          return(NA)
        } else {
          # For deeper levels, recurse
          nodes <- list()
          if (is.list(tree)) {
            for (node in tree) {
              if (is.list(node)) {
                nodes <- c(nodes, retrieveNodes(node, level - 1))
              }
            }
          }
          return(unlist(nodes))
        }
      }
      
      # ONLY get level 0 (parent nodes: Main Breaker, Other)
      usage_columns <- retrieveNodes(buildingMap, 0)
      usage_columns <- usage_columns[!is.na(usage_columns)]
      
      return(as.character(usage_columns))
    }
    # EGAUGE: Use first_meter_id for both neubolt and egauge
    rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", first_meter_id, "_buildingMap.rds")
    if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
    usage_columns <- getUsageColumns(rds_path)
    
    # Step 5: Create machine_names - EGAUGE: Handle different config column names
    if(meter_type == 'neubolt') {
      config_machines <- config_df$Display_name
    } else {
      # EGAUGE: Use New_name column and extract base names (remove units)
      config_machines <- gsub(" \\[[A-Za-z]+\\]", "", config_df$New_name)
      config_machines <- unique(config_machines)
    }
    
    config_machines <- config_machines[!grepl("(?i)phase", config_machines)]
    machine_names <- setdiff(config_machines, usage_columns)
    if (length(machine_names) == 0) {
      machine_names <- usage_columns
    }
    
    machine_names <- as.character(machine_names)
    machine_names <- trimws(machine_names)
    machine_power_sum <- setNames(rep(0, length(machine_names)), machine_names)
    total_power_sum <- 0
    
    # EGAUGE: Split live data processing based on meter type
    if (meter_type == 'neubolt') {
      
      # NEUBOLT LOGIC
      # Get metadata with formulas for each machine
      machine_metadata <- config_df[config_df$new_name %in% machine_names,
                                    c("new_name", "old_name", "Power", "Voltage")]
      names(machine_metadata) <- c("machine", "old_name", "power_expr", "voltage_expr")
      
      for (i in 1:nrow(machine_metadata)) {
        machine <- machine_metadata$machine[i]
        old_name <- machine_metadata$old_name[i]
        power_expr <- machine_metadata$power_expr[i]
        voltage_expr <- machine_metadata$voltage_expr[i]
        
        # Resolve live folder
        match <- regmatches(old_name, regexpr("(?<=SM)\\d*", old_name, perl = TRUE))
        live_folder <- ifelse(match == "", "live1", paste0("live", match))
        
        file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/",
                            live_folder, "/", old_name, ".csv")
        if (!file.exists(file_path)) next
        
        # Read meter data
        meter_df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
        
        # Check if first row is header
        if (nrow(meter_df) > 0 && meter_df[1, 1] == 'TimeStamp') {
          colnames(meter_df) = meter_df[1, ]
          meter_df <- meter_df[-1, ]
        }
        
        if (nrow(meter_df) == 0) next
        
        # Convert numeric columns
        meter_df[, 3:ncol(meter_df)] <- apply(meter_df[, 3:ncol(meter_df)], 2, FUN = as.numeric)
        
        # Get last row as data frame
        last_meter_df <- meter_df[nrow(meter_df), , drop = FALSE]
        
        if (nrow(last_meter_df) < 1) return(list(error = "No data found for contribution calculation"))
        
        # Calculate power using mutate with eval(parse()) - EXACTLY like dashboard
        tryCatch({
          power_calculation <- last_meter_df %>%
            mutate(power_temp = eval(parse(text = power_expr)))
          
          val <- power_calculation$power_temp[1]
          
          # Power is in watts, NO conversion here (will convert once at the end)
          if (!is.na(val)) {
            machine_power_sum[machine] <- val
            total_power_sum <- total_power_sum + val
          }
        }, error = function(e) {
          # If evaluation fails, skip this machine
          next
        })
      }
      
    } else {
      
      # EGAUGE LOGIC (NEW) - Use historic_file_load 
      
      # EGAUGE: Set directory for egauge minute-wise data
      dir_minutewise <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/minute-wise Data/", username, "/")
      
      # EGAUGE: Load recent files (last 2 days to ensure we have data)
      files <- list.files(path = dir_minutewise, full.names = TRUE)
      if (length(files) == 0) return(list(error = "No egauge data files found"))
      
      # EGAUGE: Sort files and get recent ones
      file_dates <- sapply(files, function(f) {
        base_name <- basename(f)
        date_part <- strsplit(base_name, "_")[[1]][2]
        as.Date(date_part, format="%Y-%m-%d")
      })
      
      # EGAUGE: Get last 2 days of files to ensure we have recent data
      end_date_live <- Sys.Date()
      start_date_live <- end_date_live - 1
      recent_files <- files[file_dates >= start_date_live & file_dates <= end_date_live]
      
      if (length(recent_files) == 0) {
        # EGAUGE: Fallback to most recent file if no recent files found
        recent_files <- files[order(file_dates)]
        recent_files <- tail(recent_files, 1)
      }
      
      # EGAUGE: Load and combine data from multiple files
      combined_data <- data.frame()
      for (file in recent_files) {
        if (file.exists(file)) {
          data <- read.csv(file, check.names = FALSE, stringsAsFactors = FALSE)
          combined_data <- rbind(combined_data, data)
        }
      }
      
      if (nrow(combined_data) == 0) return(list(error = "No data in egauge files"))
      
      # EGAUGE: Process like historic
      combined_data$`Date & Time` <- as.POSIXct(combined_data$`Date & Time`)
      combined_data <- combined_data[order(combined_data$`Date & Time`), ]
      colnames(combined_data)[1] <- "DateTime"
      
      # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
      combined_data[,2:ncol(combined_data)] = abs(combined_data[,2:ncol(combined_data)])
      
      # EGAUGE: Apply column name mapping from config
      for (row in 1:nrow(config_df)) {
        old_name <- config_df$Old_name[row]
        new_name <- config_df$New_name[row]
        
        if (old_name %in% colnames(combined_data) && old_name != new_name) {
          colnames(combined_data)[which(colnames(combined_data) == old_name)] <- new_name
          next
        }
        
        if (grepl('\\+', old_name)) {
          expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
          expression_str_fixed <- paste0('`', expression_str_fixed, '`')
          combined_data <- combined_data %>%
            mutate(!!new_name := eval(parse(text = expression_str_fixed)))
        }
      }
      
      # EGAUGE: Get the last row (most recent data)
      last_row <- combined_data[nrow(combined_data), ]
      colnames(last_row) <- trimws(colnames(last_row))
      machine_names <- unique(machine_names)
      # EGAUGE: Extract power values for each machine directly from [kW] columns
      for (machine in machine_names) {
        kw_col <- paste0(machine, " [kW]")  # Egauge exact pattern
        
        if (kw_col %in% colnames(last_row)) {
          val <- as.numeric(last_row[[kw_col]])
          if (!is.na(val) && val > 0) {
            machine_power_sum[machine] <- val  # Already in kW for egauge
            total_power_sum <- total_power_sum + val
          }
        }
      }
    }
    
    
    # Now convert to kW and calculate percentages - EGAUGE: Added conditional conversion
    machine_power_vec <- as.numeric(unname(machine_power_sum))
    
    if (meter_type == 'neubolt') {
      # NEUBOLT: Convert from watts to kW
      contribution_df <- data.frame(
        machine_name = machine_names,
        contribution_kW = round(machine_power_vec / 1000, 2),  # Convert W to kW here
        contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
        stringsAsFactors = FALSE
      )
    } else {
      # EGAUGE: Data already in kW, no conversion needed
      contribution_df <- data.frame(
        machine_name = machine_names,
        contribution_kW = round(machine_power_vec, 2),  # Already in kW
        contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
        stringsAsFactors = FALSE
      )
    }
    
    contribution_df$contribution_kW[is.na(contribution_df$contribution_kW)] <- 0
    contribution_df$contribution_percent[is.na(contribution_df$contribution_percent)] <- 0
    contribution_df$contribution_kW <- paste0(contribution_df$contribution_kW, " kW")
    contribution_df$contribution_percent <- paste0(contribution_df$contribution_percent, "%")
    
    return(list(
      contributions = unname(split(contribution_df, seq(nrow(contribution_df))))
    ))
  }
  
  # Historical data section (is_live = FALSE)
  # Step 1: Validate input
  if (is.null(username) || is.null(start_date)) {
    return(list(error = "Missing username or start date"))
  }
  
  # Step 2: Load client profile and get Meter ID & Meter Type
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # Step 3: Load config file 
  if(meter_type == 'neubolt') {
    config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
    if (!file.exists(config_path)) return(list(error = "Config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", first_meter_id, "_New_column_names.csv")
    if (!file.exists(config_path)) return(list(error = "Egauge config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  }
  
  # Step 4: Load buildingMap usage columns
  # getUsageColumns <- function(file_path) {
  #   buildingMap <- readRDS(file_path)
  #   retrieveNodes <- function(tree, level) {
  #     if (level == 0) {
  #       if (!is.null(tree$Name)) return(tree$Name) else return(NA)
  #     } else {
  #       nodes <- list()
  #       for (node in tree) {
  #         if (is.list(node)) {
  #           nodes <- c(nodes, retrieveNodes(node, level - 1))
  #         }
  #       }
  #       return(unlist(nodes))
  #     }
  #   }
  #   
  #   curr_level <- 0
  #   while (TRUE) {
  #     usage_columns <<- retrieveNodes(buildingMap, curr_level)
  #     if (any(is.na(usage_columns))) {
  #       curr_level <- curr_level + 1
  #     } else break
  #   }
  #   return(as.character(usage_columns))
  # }
  getUsageColumns <- function(file_path) {
    buildingMap <- readRDS(file_path)
    
    retrieveNodes <- function(tree, level) {
      if (level == 0) {
        # At level 0, return the Name if it exists
        if (is.list(tree) && !is.null(tree$Name)) {
          return(tree$Name)
        } else if (is.list(tree)) {
          # If tree is a list of nodes, get their names
          names_list <- lapply(tree, function(node) {
            if (is.list(node) && !is.null(node$Name)) {
              return(node$Name)
            }
            return(NA)
          })
          return(unlist(names_list))
        }
        return(NA)
      } else {
        # For deeper levels, recurse
        nodes <- list()
        if (is.list(tree)) {
          for (node in tree) {
            if (is.list(node)) {
              nodes <- c(nodes, retrieveNodes(node, level - 1))
            }
          }
        }
        return(unlist(nodes))
      }
    }
    
    # ONLY get level 0 (parent nodes: Main Breaker, Other)
    usage_columns <- retrieveNodes(buildingMap, 0)
    usage_columns <- usage_columns[!is.na(usage_columns)]
    
    return(as.character(usage_columns))
  }
  # EGAUGE: Use first_meter_id for both neubolt and egauge
  rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", first_meter_id, "_buildingMap.rds")
  if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
  usage_columns <- getUsageColumns(rds_path)
  
  # Step 5: Create machine_names - EGAUGE: Handle different config column names
  if(meter_type == 'neubolt') {
    config_machines <- config_df$new_name
  } else {
    # EGAUGE: Use New_name column and extract base names (remove units)
    config_machines <- gsub(" \\[[A-Za-z]+\\]", "", config_df$New_name)
    config_machines <- unique(config_machines)
  }
  
  config_machines <- config_machines[!grepl("(?i)phase", config_machines)]
  
  machine_names <- setdiff(config_machines, usage_columns)
  if (length(machine_names) == 0) {
    machine_names <- usage_columns
  }
  
  machine_names <- as.character(machine_names)
  
  # Step 6: Define dates
  if (is.null(end_date)) {
    dates <- as.Date(start_date)
  } else {
    dates <- seq(as.Date(start_date), as.Date(end_date), by = "day")
  }
  
  # Step 7: Initialize accumulators
  total_power_sum <- 0
  machine_power_sum <- setNames(rep(0, length(machine_names)), machine_names)
  
  # Step 8: Process hour-wise files 
  for (d in dates) {
    d <- as.Date(d, origin = "1970-01-01")
    date_str <- format(d, "%Y-%m-%d")
    
    if(meter_type == 'neubolt') {
      # NEUBOLT: Use original hour-wise path
      filepath <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/",
                         username, "/", meter_id, "_", date_str, "_hour-wise.csv")
    } else {
      # EGAUGE: Use egauge hour-wise path
      filepath <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/hour-wise Data/",
                         username, "/", first_meter_id, "_", date_str, "_hour-wise.csv")
    }
    
    if (!file.exists(filepath)) next
    
    df <- read.csv(filepath, check.names = FALSE, stringsAsFactors = FALSE)
    
    # EGAUGE: Apply full historic_file_load processing for egauge hour-wise files
    if(meter_type == 'egauge') {
      
      # EGAUGE: Convert DateTime column and rename
      if("Date & Time" %in% colnames(df)) {
        df$`Date & Time` <- as.POSIXct(df$`Date & Time`)
        df <- df[order(df$`Date & Time`), ]
        colnames(df)[1] <- "DateTime"
      }
      
      # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
      df[,2:ncol(df)] = abs(df[,2:ncol(df)])
      
      # EGAUGE: Apply column name mapping from config (same as historic_file_load)
      for (row in 1:nrow(config_df)) {
        old_name <- config_df$Old_name[row]
        new_name <- config_df$New_name[row]
        
        # EGAUGE: If the old name exists in df, rename it to the new name
        if (old_name %in% colnames(df) && old_name != new_name) {
          colnames(df)[which(colnames(df) == old_name)] <- new_name
          next
        }
        
        # EGAUGE: If the old name contains a '+', evaluate it as an expression to create a new column
        if (grepl('\\+', old_name)) {
          expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
          expression_str_fixed <- paste0('`', expression_str_fixed, '`')
          df <- df %>%
            mutate(!!new_name := eval(parse(text = expression_str_fixed)))
        }
      }
      
      # EGAUGE: Remove columns that are neither in Old_name nor New_name from the mapping
      retData_cols <- colnames(df)
      for (cols in 2:length(retData_cols)) {
        if (!retData_cols[cols] %in% config_df$Old_name & !retData_cols[cols] %in% config_df$New_name) {
          df <- df[, -c(which(colnames(df) == retData_cols[cols]))]
        }
      }
      
      # EGAUGE: Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
      for (col in 2:ncol(df)) {
        df[[col]] <- na.locf(df[[col]], na.rm = F, maxgap = 5)
      }
    }
    
    # EGAUGE: Different column patterns for different meter types
    if(meter_type == 'neubolt') {
      machine_cols <- paste0(machine_names, "_[kW]")
    } else {
      machine_cols <- paste0(machine_names, " [kW]")  # Egauge uses space
    }
    
    available_cols <- machine_cols[machine_cols %in% names(df)]
    
    if (length(available_cols) == 0) next
    
    # Total power
    total_power <- sum(rowSums(df[, available_cols, drop = FALSE], na.rm = TRUE), na.rm = TRUE)
    total_power_sum <- total_power_sum + total_power
    
    # Each machine's power
    for (m in machine_names) {
      if(meter_type == 'neubolt') {
        col_name <- paste0(m, "_[kW]")
      } else {
        col_name <- paste0(m, " [kW]")  # Egauge uses space
      }
      
      if (col_name %in% names(df)) {
        machine_power_sum[m] <- machine_power_sum[m] + sum(df[[col_name]], na.rm = TRUE)
      }
    }
  }
  
  if (total_power_sum == 0) {
    return(list(error = "No data available for the given date range"))
  }
  
  # Step 9: Calculate contribution 
  machine_power_vec <- as.numeric(unname(machine_power_sum))
  
  if (meter_type == 'neubolt') {
    # NEUBOLT: Hour-wise data columns say [kW] but values are actually in Watts
    contribution_df <- data.frame(
      machine_name = machine_names,
      contribution_kW = round(machine_power_vec / 1000, 2),  # Convert W to kW
      contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
      stringsAsFactors = FALSE
    )
  } else {
    # EGAUGE: Hour-wise data already in kW, no conversion needed
    contribution_df <- data.frame(
      machine_name = machine_names,
      contribution_kW = round(machine_power_vec, 2),  # Already in kW
      contribution_percent = round((machine_power_vec / total_power_sum) * 100, 2),
      stringsAsFactors = FALSE
    )
  }
  
  contribution_df$contribution_kW[is.na(contribution_df$contribution_kW)] <- 0
  contribution_df$contribution_percent[is.na(contribution_df$contribution_percent)] <- 0
  contribution_df$contribution_kW <- paste0(contribution_df$contribution_kW, " kW")
  contribution_df$contribution_percent <- paste0(contribution_df$contribution_percent, "%")
  
  # Step 10: Return clean output
  return(list(
    contributions = unname(split(contribution_df, seq(nrow(contribution_df))))
  ))
}

#* Get detailed appliance data including peak/non-peak consumption
#* @param username User's name
#* @param machine_name Machine name
#* @param start_date Start date (YYYY-MM-DD)
#* @param end_date (optional) End date (YYYY-MM-DD)
#* @get /appliancedata
#* @serializer unboxedJSON
function(username, machine_name, start_date = NULL, end_date = NULL) {
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  if (is.null(username) || is.null(start_date)) {
    return(list(error = "Missing username or start_date"))
  }
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # EGAUGE: Conditional config and RDS paths
  if(meter_type == 'neubolt') {
    config_path <- file.path("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", paste0(meter_id, "_Config.csv"))
    rds_path    <- file.path("/srv/shiny-server/EnergyMonitor2.0/", paste0(meter_id, "_buildingMap.rds"))
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- file.path("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", paste0(first_meter_id, "_New_column_names.csv"))
    rds_path    <- file.path("/srv/shiny-server/EnergyMonitor2.0/", paste0(first_meter_id, "_buildingMap.rds"))
  }
  
  if (!file.exists(config_path) || !file.exists(rds_path)) {
    return(list(error = "Config or BuildingMap file not found"))
  }
  config_df   <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  buildingMap <- readRDS(rds_path)
  
  getUsageColumns <- function(tree, level = 0) {
    retrieve <- function(t, lvl) {
      if (lvl == 0 && !is.null(t$Name)) return(t$Name)
      unlist(lapply(t, function(x) if (is.list(x)) retrieve(x, lvl - 1)))
    }
    repeat {
      usage <- retrieve(tree, level)
      if (any(is.na(usage))) level <- level + 1 else break
    }
    as.character(usage)
  }
  usage_columns <- getUsageColumns(buildingMap)
  
  # EGAUGE: Handle different config column names
  if(meter_type == 'neubolt') {
    cfg_names <- config_df$new_name
  } else {
    # EGAUGE: Use New_name column and extract base names (remove units)
    cfg_names <- gsub(" \\[[A-Za-z]+\\]", "", config_df$New_name)
    cfg_names <- unique(cfg_names)
  }
  
  cfg_names <- cfg_names[!grepl("(?i)phase", cfg_names)]
  machine_names <- setdiff(cfg_names, usage_columns)
  if (length(machine_names) == 0) machine_names <- usage_columns
  
  # EGAUGE: Conditional machine info extraction
  if(meter_type == 'neubolt') {
    machine_info <- config_df[config_df$new_name %in% machine_names,
                              c("new_name", "old_name", "Power", "Working_Hour_Start", 
                                "Working_Hour_End", "Threshold_Power", 
                                "Rated_Power", "Benchmark_Power")]
    names(machine_info)[1] <- "machine_name"
    machine_row <- machine_info[machine_info$machine_name == machine_name, ]
  } else {
    # EGAUGE: Extract machine info with available columns only
    available_cols <- intersect(c("New_name", "Old_name", "Working_Hour_Start", 
                                  "Working_Hour_End", "Threshold_Power", 
                                  "Rated_Power", "Benchmark_Power"), colnames(config_df))
    
    # EGAUGE: Get first occurrence of machine name (since egauge has multiple rows per machine)
    machine_config_rows <- gsub(" \\[[A-Za-z]+\\]", "", config_df$New_name)
    machine_idx <- which(machine_config_rows == machine_name)[1]
    
    if(!is.na(machine_idx)) {
      machine_row <- config_df[machine_idx, available_cols, drop = FALSE]
      names(machine_row)[1] <- "machine_name"
      machine_row$machine_name <- machine_name
    } else {
      machine_row <- data.frame()
    }
  }
  
  if (nrow(machine_row) == 0) return(list(error = "Machine not found"))
  
  sd <- as.Date(start_date)
  ed <- if (is.null(end_date)) sd else as.Date(end_date)
  dates <- seq(sd, ed, by = "day")
  
  peakHoursStart <- c(17,17,18,18,18,19,19,19,18,18,18,17)
  peak_sum <- 0
  nonpeak_sum <- 0
  
  all_current <- c()
  all_power <- c()
  all_voltage <- c()
  all_pf <- c()
  
  for (date in 1:length(dates)) {
    d <- dates[date]
    
    # EGAUGE: Conditional hour-wise file paths
    if(meter_type == 'neubolt') {
      hw_path <- file.path(
        "/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/data/hour-wise Data",
        username,
        paste0(meter_id, "_", d, "_hour-wise.csv")
      )
    } else {
      # EGAUGE: Use egauge hour-wise path
      hw_path <- file.path(
        "/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/hour-wise Data",
        username,
        paste0(first_meter_id, "_", d, "_hour-wise.csv")
      )
    }
    
    if (!file.exists(hw_path)) next
    
    df <- read.csv(hw_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    # EGAUGE: Apply full historic_file_load processing for egauge hour-wise files
    if(meter_type == 'egauge') {
      
      # EGAUGE: Convert DateTime column and rename
      if("Date & Time" %in% colnames(df)) {
        df$`Date & Time` <- as.POSIXct(df$`Date & Time`)
        df <- df[order(df$`Date & Time`), ]
        colnames(df)[1] <- "DateTime"
      }
      
      # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
      df[,2:ncol(df)] = abs(df[,2:ncol(df)])
      
      # EGAUGE: Apply column name mapping from config (same as historic_file_load)
      for (row in 1:nrow(config_df)) {
        old_name <- config_df$Old_name[row]
        new_name <- config_df$New_name[row]
        
        # EGAUGE: If the old name exists in df, rename it to the new name
        if (old_name %in% colnames(df) && old_name != new_name) {
          colnames(df)[which(colnames(df) == old_name)] <- new_name
          next
        }
        
        # EGAUGE: If the old name contains a '+', evaluate it as an expression to create a new column
        if (grepl('\\+', old_name)) {
          expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
          expression_str_fixed <- paste0('`', expression_str_fixed, '`')
          df <- df %>%
            mutate(!!new_name := eval(parse(text = expression_str_fixed)))
        }
      }
      
      # EGAUGE: Remove columns that are neither in Old_name nor New_name from the mapping
      retData_cols <- colnames(df)
      for (cols in 2:length(retData_cols)) {
        if (!retData_cols[cols] %in% config_df$Old_name & !retData_cols[cols] %in% config_df$New_name) {
          df <- df[, -c(which(colnames(df) == retData_cols[cols]))]
        }
      }
      
      # EGAUGE: Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
      for (col in 2:ncol(df)) {
        df[[col]] <- na.locf(df[[col]], na.rm = F, maxgap = 5)
      }
    }
    
    dt <- strptime(df[["Date & Time"]], format = "%Y-%m-%d %H:%M:%S")
    hrs <- dt$hour
    
    # EGAUGE: Different column patterns for different meter types
    if(meter_type == 'neubolt') {
      kw_col <- paste0(machine_name, "_[kW]")
      vcol   <- paste0(machine_name, "_[V]")
      acol   <- paste0(machine_name, "_[A]")
      pfcol  <- paste0(machine_name, "_[PF]")
    } else {
      # EGAUGE: Use space pattern
      kw_col <- paste0(machine_name, " [kW]")
      vcol   <- paste0(machine_name, " [V]")
      acol   <- paste0(machine_name, " [A]")
      pfcol  <- paste0(machine_name, " [PF]")
    }
    
    if (!(kw_col %in% names(df))) next
    
    m <- as.integer(format(as.Date(d), "%m"))
    ph_start <- peakHoursStart[m]
    ph_end <- ph_start + 3
    
    vals <- df[[kw_col]]
    idx_peak <- which(hrs >= ph_start & hrs < ph_end)
    idx_nonpeak <- setdiff(seq_along(hrs), idx_peak)
    peak_sum <- peak_sum + sum(vals[idx_peak], na.rm = TRUE)
    nonpeak_sum <- nonpeak_sum + sum(vals[idx_nonpeak], na.rm = TRUE)
    
    # collect all values
    if (acol %in% names(df))   all_current <- c(all_current, df[[acol]])
    if (kw_col %in% names(df)) all_power   <- c(all_power,   df[[kw_col]])
    if (vcol %in% names(df))   all_voltage <- c(all_voltage, df[[vcol]])
    if (pfcol %in% names(df))  all_pf      <- c(all_pf,      df[[pfcol]])
  }
  
  # aggregate totals
  total_current <- if (length(all_current) > 0) mean(all_current, na.rm = TRUE) else NA
  total_power   <- sum(all_power, na.rm = TRUE)
  avg_voltage   <- if (length(all_voltage) > 0) mean(all_voltage, na.rm = TRUE) else NA
  avg_pf        <- if (length(all_pf) > 0) mean(all_pf, na.rm = TRUE) else NA
  
  all_contributions <- machineContribution(username = username, start_date = start_date, end_date = end_date)
  if (!is.null(all_contributions$error)) return(all_contributions)
  
  result <- Filter(function(x) x$machine_name == machine_name, all_contributions$contributions)
  if (length(result) == 0) return(list(error = "Machine not found in contributions"))
  
  # EGAUGE: Handle machine info with available columns only
  if("Threshold_Power" %in% names(machine_row) && !is.na(machine_row$Threshold_Power)) {
    machine_row$Threshold_Power <- paste0(machine_row$Threshold_Power, " kW")
  }
  if("Rated_Power" %in% names(machine_row) && !is.na(machine_row$Rated_Power)) {
    machine_row$Rated_Power <- paste0(machine_row$Rated_Power, " kW")
  }
  if("Benchmark_Power" %in% names(machine_row) && !is.na(machine_row$Benchmark_Power)) {
    machine_row$Benchmark_Power <- paste0(machine_row$Benchmark_Power, " kW")
  }
  
  # EGAUGE: Handle V/A with conditional units (NA without units)
  if(!is.na(avg_voltage)) {
    avg_voltage <- round(avg_voltage, 2)
    avg_voltage <- paste0(avg_voltage, " V")
  }
  
  if(!is.na(total_current)) {
    total_current <- round(total_current, 2)
    total_current <- paste0(total_current, " A")
  }
  
  # EGAUGE: Conditional unit conversion for power
  if(meter_type == 'neubolt') {
    # NEUBOLT: Convert from watts to kW
    total_power <- round(total_power / 1000, 2)
    peak_sum <- round(peak_sum / 1000, 2)
    nonpeak_sum <- round(nonpeak_sum / 1000, 2)
  } else {
    # EGAUGE: Data already in kW, no conversion needed
    total_power <- round(total_power, 2)
    peak_sum <- round(peak_sum, 2)
    nonpeak_sum <- round(nonpeak_sum, 2)
  }
  
  total_power <- paste0(total_power, " kW")
  peak_sum <- paste0(peak_sum, " kW")
  nonpeak_sum <- paste0(nonpeak_sum, " kW")
  
  # Build response 
  list(
    machine_name        = machine_name,
    Voltage             = avg_voltage,
    Current             = total_current,
    Power               = total_power,
    Power_Factor        = if(!is.na(avg_pf)) round(avg_pf, 2) else NA,
    Peak_Consumption_kW     = peak_sum,
    Non_Peak_Consumption_kW = nonpeak_sum,
    Contribution_kW         = result[[1]],
    Working_Hour_Start  = if("Working_Hour_Start" %in% names(machine_row)) machine_row$Working_Hour_Start else NA,
    Working_Hour_End    = if("Working_Hour_End" %in% names(machine_row)) machine_row$Working_Hour_End else NA,
    Threshold_Power     = if("Threshold_Power" %in% names(machine_row)) machine_row$Threshold_Power else NA,
    Rated_Power         = if("Rated_Power" %in% names(machine_row)) machine_row$Rated_Power else NA,
    Benchmark_Power     = if("Benchmark_Power" %in% names(machine_row)) machine_row$Benchmark_Power else NA
  )
}






# #* Get live appliance snapshot (voltage, current, power, PF, status)
# #* @param username User's name
# #* @param machine_name Machine name
# #* @param is_live logical flag (always TRUE here)
# #* @get /appliancedatalive
# #* @serializer unboxedJSON
# function(username, machine_name) {
# 
#   ## ── 1.  Load required library ────────────────────────────────────────────────
#   library(dplyr)
# 
#   ## ── 2.  look‑ups & validation ───────────────────────────────────────────────
#   profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
#   if (is.null(username) || is.null(machine_name))
#     return(list(error = "username and machine_name are required"))
# 
#   profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
#   user_row <- profiles[profiles$user == username, ]
#   if (nrow(user_row) == 0) return(list(error = "Username not found"))
#   meter_id <- as.character(user_row$Meter_id[1])
# 
#   config_path <- file.path("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
#                            paste0(meter_id, "_Config.csv"))
#   rds_path    <- file.path("/srv/shiny-server/EnergyMonitor2.0/",
#                            paste0(meter_id, "_buildingMap.rds"))
#   if (!file.exists(config_path) || !file.exists(rds_path))
#     return(list(error = "Config or BuildingMap file not found"))
# 
#   config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
# 
#   ## ── 3.  find the machine row & its formulas ─────────────────────────────────
#   machine_row <- config_df[config_df$Display_name == machine_name, ]
#   if (nrow(machine_row) == 0) return(list(error = "Machine not found in config"))
# 
#   old_name      <- machine_row$old_name
#   power_expr    <- machine_row$Power
#   current_expr  <- machine_row$Current
#   voltage_expr  <- machine_row$Voltage
#   pf_expr       <- machine_row$PowerFactor
# 
#   ## ── 4.  resolve live folder from old_name (SM?‑XXX) ─────────────────────────
#   # Extract slot number from meter ID (e.g., SM3-002 -> 3)
#   id_match <- regmatches(
#     old_name,
#     regexpr("(?<=SM)\\d+", old_name, perl = TRUE)
#   )
# 
#   # Determine which live folder to use
#   if (length(id_match) == 0 || id_match == "") {
#     live_folder <- "live1"
#   } else {
#     live_folder <- paste0("live", id_match)
#   }
# 
#   csv_path <- file.path(
#     "/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data",
#     live_folder,
#     paste0(old_name, ".csv")
#   )
#   if (!file.exists(csv_path))
#     return(list(error = paste("CSV not found:", csv_path)))
# 
#   ## ── 5.  read and validate meter data ────────────────────────────────────────
#   meter_df <- read.csv(csv_path, check.names = FALSE, stringsAsFactors = FALSE)
# 
#   # Check if first row is header
#   if (meter_df[1, 1] == 'TimeStamp') {
#     colnames(meter_df) = meter_df[1, ]
#     meter_df <- meter_df[-1, ]
#   }
# 
#   if (nrow(meter_df) == 0) return(list(error = "CSV is empty"))
# 
#   # Convert numeric columns (skip TimeStamp and SensorID)
#   meter_df[, 3:ncol(meter_df)] <- apply(meter_df[, 3:ncol(meter_df)], 2, FUN = as.numeric)
# 
#   # Get the last row as a data frame (important: keep as data frame for mutate)
#   last_meter_df <- meter_df[nrow(meter_df), , drop = FALSE]
# 
#   ## ── 6.  Calculate number of phases/slots from voltage formula ───────────────
#   # This is critical for correct calculations
#   number_of_slots <- length(strsplit(voltage_expr, "\\+")[[1]])
# 
# 
# 
#   # Use mutate to evaluate all formulas
#   calculations <- last_meter_df %>%
#     mutate(
#       voltage_temp = eval(parse(text = voltage_expr)),
#       current_temp = eval(parse(text = current_expr)),
#       power_temp = eval(parse(text = power_expr)),
#       pf_temp = eval(parse(text = pf_expr))
#     )
# 
#   # Extract calculated values
#   voltage_val <- calculations$voltage_temp[1]
#   current_val <- calculations$current_temp[1]
#   power_val <- calculations$power_temp[1]
#   pf_val <- calculations$pf_temp[1]
# 
#   
# 
#   # VOLTAGE - with 3-phase correction
#   if (!is.na(voltage_val)) {
#     # Average across phases
#     voltage_val <- voltage_val / number_of_slots
#     # Apply sqrt(3) for 3-phase systems
#     if (number_of_slots > 2) {
#       voltage_val <- voltage_val * sqrt(3)
#     }
#   }
# 
#   # CURRENT - average per phase
#   if (!is.na(current_val)) {
#     # Check if values seem to be in mA (dashboard comment mentions this)
#     # Assuming if total current > 100, it's likely in mA
#     # if (current_val > 100) {
#     #   current_val <- current_val / 1000  # Convert mA to A
#     # }
#     # Average across phases
#     current_val <- current_val / number_of_slots
#   }
# 
#   # POWER - convert to kW
#   if (!is.na(power_val)) {
#     # Power is already summed in formula, just convert W to kW
#     power_val <- power_val / 1000
#   }
# 
#   # POWER FACTOR - average across phases
#   if (!is.na(pf_val)) {
#     # Average across phases
#     pf_val <- pf_val / number_of_slots
#   }
# 
#   ## ── 9.  Determine status based on power ─────────────────────────────────────
#   status <- ifelse(!is.na(power_val) && power_val > 0.1, "online", "offline")
# 
#   ## ── 10. Get contribution (re‑use existing API) ──────────────────────────────
#   all_contributions <- machineContribution(username = username, is_live = TRUE)
#   if (!is.null(all_contributions$error)) return(all_contributions)
# 
#   result <- Filter(function(x) x$machine_name == machine_name, all_contributions$contributions)
#   if (length(result) == 0) return(list(error = "Machine not found in contributions"))
# 
#   ## ── 11. Format & return (matching dashboard format) ─────────────────────────
#   list(
#     machine_name        = machine_name,
#     status              = status,
#     Working_Hour_Start  = machine_row$Working_Hour_Start,
#     Working_Hour_End    = machine_row$Working_Hour_End,
#     Threshold_Power     = paste0(machine_row$Threshold_Power, " kW"),
#     Rated_Power         = paste0(machine_row$Rated_Power, " kW"),
#     Benchmark_Power     = paste0(machine_row$Benchmark_Power, " kW"),
#     Voltage             = if (!is.na(voltage_val)) paste0(round(voltage_val, 2), " V") else NA,
#     Current             = if (!is.na(current_val)) paste0(round(current_val, 2), " A") else NA,
#     Power               = if (!is.na(power_val)) paste0(round(power_val, 2), " kW") else NA,
#     Power_Factor        = if (!is.na(pf_val)) round(pf_val, 2) else NA,
#     Contribution_kW     = result[[1]]
#   )
# }
#* Get live appliance snapshot (voltage, current, power, PF, status)
#* @param username User's name
#* @param machine_name Machine name
#* @param is_live logical flag (always TRUE here)
#* @get /appliancedatalive
#* @serializer unboxedJSON
function(username, machine_name) {
  
  ## ── 1.  Load required library ────────────────────────────────────────────────
  library(dplyr)
  
  ## ── 2.  look‑ups & validation ───────────────────────────────────────────────
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  if (is.null(username) || is.null(machine_name))
    return(list(error = "username and machine_name are required"))
  
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # EGAUGE: Conditional config and RDS paths
  if(meter_type == 'neubolt') {
    config_path <- file.path("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/",
                             paste0(meter_id, "_Config.csv"))
    rds_path    <- file.path("/srv/shiny-server/EnergyMonitor2.0/",
                             paste0(meter_id, "_buildingMap.rds"))
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- file.path("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/",
                             paste0(first_meter_id, "_New_column_names.csv"))
    rds_path    <- file.path("/srv/shiny-server/EnergyMonitor2.0/",
                             paste0(first_meter_id, "_buildingMap.rds"))
  }
  
  if (!file.exists(config_path) || !file.exists(rds_path))
    return(list(error = "Config or BuildingMap file not found"))
  
  config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  
  ## ── 3.  find the machine row & its formulas ─────────────────────────────────
  # EGAUGE: Conditional machine lookup based on meter type
  if(meter_type == 'neubolt') {
    machine_row <- config_df[config_df$Display_name == machine_name, ]
    if (nrow(machine_row) == 0) return(list(error = "Machine not found in config"))
    
    old_name      <- machine_row$old_name
    power_expr    <- machine_row$Power
    current_expr  <- machine_row$Current
    voltage_expr  <- machine_row$Voltage
    pf_expr       <- machine_row$PowerFactor
  } else {
    # EGAUGE: Find machine in config by base name (remove units)
    config_base_names <- gsub(" \\[[A-Za-z]+\\]", "", config_df$New_name)
    machine_idx <- which(config_base_names == machine_name)[1]
    
    if(is.na(machine_idx)) return(list(error = "Machine not found in config"))
    
    # EGAUGE: For egauge, we don't have formulas, just direct column names
    machine_row <- config_df[machine_idx, , drop = FALSE]
    machine_row$Display_name <- machine_name  # Set display name for consistency
  }
  
  # EGAUGE: Split live data processing based on meter type
  if (meter_type == 'neubolt') {
    
    # NEUBOLT LOGIC
    
    ## ── 4.  resolve live folder from old_name (SM?‑XXX) 
    # Extract slot number from meter ID (e.g., SM3-002 -> 3)
    id_match <- regmatches(
      old_name,
      regexpr("(?<=SM)\\d+", old_name, perl = TRUE)
    )
    
    # Determine which live folder to use
    if (length(id_match) == 0 || id_match == "") {
      live_folder <- "live1"
    } else {
      live_folder <- paste0("live", id_match)
    }
    
    csv_path <- file.path(
      "/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data",
      live_folder,
      paste0(old_name, ".csv")
    )
    if (!file.exists(csv_path))
      return(list(error = paste("CSV not found:", csv_path)))
    
    ## ── 5.  read and validate meter data ────────────────────────────────────────
    meter_df <- read.csv(csv_path, check.names = FALSE, stringsAsFactors = FALSE)
    
    # Check if first row is header
    if (meter_df[1, 1] == 'TimeStamp') {
      colnames(meter_df) = meter_df[1, ]
      meter_df <- meter_df[-1, ]
    }
    
    if (nrow(meter_df) == 0) return(list(error = "CSV is empty"))
    
    # Convert numeric columns (skip TimeStamp and SensorID)
    meter_df[, 3:ncol(meter_df)] <- apply(meter_df[, 3:ncol(meter_df)], 2, FUN = as.numeric)
    
    # Get the last row as a data frame (important: keep as data frame for mutate)
    last_meter_df <- meter_df[nrow(meter_df), , drop = FALSE]
    
    ## ── 6.  Calculate number of phases/slots from voltage formula ───────────────
    # This is critical for correct calculations
    number_of_slots <- length(strsplit(voltage_expr, "\\+")[[1]])
    
    # Use mutate to evaluate all formulas
    calculations <- last_meter_df %>%
      mutate(
        voltage_temp = eval(parse(text = voltage_expr)),
        current_temp = eval(parse(text = current_expr)),
        power_temp = eval(parse(text = power_expr)),
        pf_temp = eval(parse(text = pf_expr))
      )
    
    # Extract calculated values
    voltage_val <- calculations$voltage_temp[1]
    current_val <- calculations$current_temp[1]
    power_val <- calculations$power_temp[1]
    pf_val <- calculations$pf_temp[1]
    
    # VOLTAGE - with 3-phase correction
    if (!is.na(voltage_val)) {
      # Average across phases
      voltage_val <- voltage_val / number_of_slots
      # Apply sqrt(3) for 3-phase systems
      if (number_of_slots > 2) {
        voltage_val <- voltage_val * sqrt(3)
      }
    }
    
    # CURRENT - average per phase
    if (!is.na(current_val)) {
      # Average across phases
      current_val <- current_val / number_of_slots
    }
    
    # POWER - convert to kW
    if (!is.na(power_val)) {
      # Power is already summed in formula, just convert W to kW
      power_val <- power_val / 1000
    }
    
    # POWER FACTOR - average across phases
    if (!is.na(pf_val)) {
      # Average across phases
      pf_val <- pf_val / number_of_slots
    }
    
  } else {
    
    # EGAUGE LOGIC  Use historic_file_load approach 
   
    # EGAUGE: Set directory for egauge minute-wise data
    dir_minutewise <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/minute-wise Data/", username, "/")
    
    # EGAUGE: Load recent files (last 2 days to ensure we have data)
    files <- list.files(path = dir_minutewise, full.names = TRUE)
    if (length(files) == 0) return(list(error = "No egauge data files found"))
    
    # EGAUGE: Sort files and get recent ones
    file_dates <- sapply(files, function(f) {
      base_name <- basename(f)
      date_part <- strsplit(base_name, "_")[[1]][2]
      as.Date(date_part, format="%Y-%m-%d")
    })
    
    # EGAUGE: Get last 2 days of files to ensure we have recent data
    end_date_live <- Sys.Date()
    start_date_live <- end_date_live - 1
    recent_files <- files[file_dates >= start_date_live & file_dates <= end_date_live]
    
    if (length(recent_files) == 0) {
      # EGAUGE: Fallback to most recent file if no recent files found
      recent_files <- files[order(file_dates)]
      recent_files <- tail(recent_files, 1)
    }
    
    # EGAUGE: Load and combine data from multiple files
    combined_data <- data.frame()
    for (file in recent_files) {
      if (file.exists(file)) {
        data <- read.csv(file, check.names = FALSE, stringsAsFactors = FALSE)
        combined_data <- rbind(combined_data, data)
      }
    }
    
    if (nrow(combined_data) == 0) return(list(error = "No data in egauge files"))
    
    # EGAUGE: Process exactly like historic_file_load function
    combined_data$`Date & Time` <- as.POSIXct(combined_data$`Date & Time`)
    combined_data <- combined_data[order(combined_data$`Date & Time`), ]
    colnames(combined_data)[1] <- "DateTime"
    
    # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
    combined_data[,2:ncol(combined_data)] = abs(combined_data[,2:ncol(combined_data)])
    
    # EGAUGE: Apply column name mapping from config
    for (row in 1:nrow(config_df)) {
      old_name_cfg <- config_df$Old_name[row]
      new_name_cfg <- config_df$New_name[row]
      
      if (old_name_cfg %in% colnames(combined_data) && old_name_cfg != new_name_cfg) {
        colnames(combined_data)[which(colnames(combined_data) == old_name_cfg)] <- new_name_cfg
        next
      }
      
      if (grepl('\\+', old_name_cfg)) {
        expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name_cfg)
        expression_str_fixed <- paste0('`', expression_str_fixed, '`')
        combined_data <- combined_data %>%
          mutate(!!new_name_cfg := eval(parse(text = expression_str_fixed)))
      }
    }
    
    # EGAUGE: Get the last row (most recent data)
    last_row <- combined_data[nrow(combined_data), ]
    
    # EGAUGE: Extract values directly from [kW], [V], [A], [PF] columns
    kw_col <- paste0(machine_name, " [kW]")
    v_col  <- paste0(machine_name, " [V]")
    a_col  <- paste0(machine_name, " [A]")
    pf_col <- paste0(machine_name, " [PF]")
    
    # EGAUGE: Get values (already in correct units for egauge)
    power_val   <- if(kw_col %in% colnames(last_row)) as.numeric(last_row[[kw_col]]) else NA
    voltage_val <- if(v_col %in% colnames(last_row)) as.numeric(last_row[[v_col]]) else NA
    current_val <- if(a_col %in% colnames(last_row)) as.numeric(last_row[[a_col]]) else NA
    pf_val      <- if(pf_col %in% colnames(last_row)) as.numeric(last_row[[pf_col]]) else NA
    
    # EGAUGE: For egauge, values are already in correct units (kW, V, A, PF)
    # No unit conversion needed like in neubolt
  }
  
  ## ── 9.  Determine status based on power ─────────────────────────────────────
  status <- ifelse(!is.na(power_val) && power_val > 0.1, "online", "offline")
  
  # ── 10. Get contribution (re‑use existing API) ──────────────────────────────
  all_contributions <- machineContribution(username = username, is_live = TRUE)
  if (!is.null(all_contributions$error)) return(all_contributions)

  result <- Filter(function(x) x$machine_name == machine_name, all_contributions$contributions)
  if (length(result) == 0) return(list(error = "Machine not found in contributions"))
  # all_contributions <- machineContribution(username = username, is_live = TRUE)
  # contribution_result <- NULL
  # 
  # if (is.null(all_contributions$error)) {
  #   result <- Filter(function(x) x$machine_name == machine_name, all_contributions$contributions)
  #   if (length(result) > 0) {
  #     contribution_result <- result[[1]]
  #   }
  # }
  ## ── 11. Format & return 
  # EGAUGE: Build response with conditional fields (handle missing columns for egauge)
  response <- list(
    machine_name        = machine_name,
    status              = status,
    Voltage             = if (!is.na(voltage_val)) paste0(round(voltage_val, 2), " V") else NA,
    Current             = if (!is.na(current_val)) paste0(round(current_val, 2), " A") else NA,
    Power               = if (!is.na(power_val)) paste0(round(power_val, 2), " kW") else NA,
    Power_Factor        = if (!is.na(pf_val)) round(pf_val, 2) else NA,
    Contribution_kW     = result[[1]]
    # Contribution_kW     = if (!is.null(contribution_result)) contribution_result else "N/A"
  )
  
  # EGAUGE: Add machine info fields if they exist
  if(meter_type == 'neubolt') {
    response$Working_Hour_Start <- machine_row$Working_Hour_Start
    response$Working_Hour_End   <- machine_row$Working_Hour_End
    response$Threshold_Power    <- paste0(machine_row$Threshold_Power, " kW")
    response$Rated_Power        <- paste0(machine_row$Rated_Power, " kW")
    response$Benchmark_Power    <- paste0(machine_row$Benchmark_Power, " kW")
  } else {
    # EGAUGE: Add fields if they exist in config
    if("Working_Hour_Start" %in% colnames(machine_row) && !is.na(machine_row$Working_Hour_Start)) {
      response$Working_Hour_Start <- machine_row$Working_Hour_Start
    }
    if("Working_Hour_End" %in% colnames(machine_row) && !is.na(machine_row$Working_Hour_End)) {
      response$Working_Hour_End <- machine_row$Working_Hour_End
    }
    if("Threshold_Power" %in% colnames(machine_row) && !is.na(machine_row$Threshold_Power)) {
      response$Threshold_Power <- paste0(machine_row$Threshold_Power, " kW")
    }
    if("Rated_Power" %in% colnames(machine_row) && !is.na(machine_row$Rated_Power)) {
      response$Rated_Power <- paste0(machine_row$Rated_Power, " kW")
    }
    if("Benchmark_Power" %in% colnames(machine_row) && !is.na(machine_row$Benchmark_Power)) {
      response$Benchmark_Power <- paste0(machine_row$Benchmark_Power, " kW")
    }
  }
  
  return(response)
}
# #* @get /loadDistribution
# #* @serializer unboxedJSON
# function(username) {
#   if (is.null(username)) {
#     return(list(error = "Missing username"))
#   }
#   
#   # 1️⃣ User → meter_id
#   profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
#   profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
#   user_row <- profiles[profiles$user == username, ]
#   if (nrow(user_row) == 0) return(list(error = "Username not found"))
#   meter_id <- as.character(user_row$Meter_id[1])
#   
#   # 2️⃣ Load config file
#   config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
#   if (!file.exists(config_path)) return(list(error = "Config file not found"))
#   config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
#   
#   # 3️⃣ Usage columns from buildingMap
#   getUsageColumns <- function(file_path) {
#     buildingMap <- readRDS(file_path)
#     retrieveNodes <- function(tree, level) {
#       if (level == 0) {
#         # Handle tree might be an atomic vector
#         if (is.atomic(tree)) {
#           return(tree)
#         } else if (is.list(tree) && !is.null(tree$Name)) {
#           return(tree$Name)
#         } else {
#           return(NA)
#         }
#       } else {
#         if (is.list(tree)) {
#           unlist(lapply(tree, retrieveNodes, level = level - 1))
#         } else {
#           return(NA)
#         }
#       }
#     }
#     lvl <- 0
#     repeat {
#       usage_cols <- retrieveNodes(buildingMap, lvl)
#       if (anyNA(usage_cols)) lvl <- lvl + 1 else break
#     }
#     as.character(usage_cols)
#   }
#   
#   rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
#   if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
#   usage_columns <- getUsageColumns(rds_path)
#   
#   # 3.5️⃣ Process Virtual Registers and add to usage columns
#   vr_file <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/", meter_id, "_VirtualRegister.csv")
#   vr_names <- character(0)  # Initialize empty vector for VR names
#   vr_formulas <- list()     # Store VR formulas for later calculation
#   
#   if (file.exists(vr_file)) {
#     vr_data <- read.csv(vr_file, stringsAsFactors = FALSE)
#     if (nrow(vr_data) > 0) {
#       # Extract VR names and formulas
#       vr_names <- vr_data$Name
#       for (i in 1:nrow(vr_data)) {
#         vr_formulas[[vr_data$Name[i]]] <- trimws(vr_data$Formula[i])
#       }
#       # Add VR names to usage columns
#       usage_columns <- c(usage_columns, vr_names)
#     }
#   }
#   
#   # 4️⃣ Prepare metadata (exclude rows with "phase")
#   no_phase <- function(x) !grepl("(?i)phase", x)
#   
#   # For regular usage columns (non-VR)
#   regular_usage_columns <- setdiff(usage_columns, vr_names)
#   usage_metadata <- config_df[config_df$new_name %in% regular_usage_columns & no_phase(config_df$new_name),
#                               c("new_name", "old_name", "Power")]
#   
#   solar_row <- config_df[grepl("(?i)solar", config_df$new_name) & no_phase(config_df$new_name),
#                          c("new_name", "old_name", "Power")]
#   
#   gen_row <- config_df[grepl("(?i)generator", config_df$new_name) & no_phase(config_df$new_name),
#                        c("new_name", "old_name", "Power")]
#   
#   # Combine rows for batch processing (excluding VR for now)
#   all_rows <- rbind(usage_metadata, solar_row, gen_row)
#   
#   # 5️⃣ Helper: last‑value extractor
#   get_live_folder <- function(old_name) {
#     sm_num <- regmatches(old_name, regexpr("(?<=SM)(\\d+)", old_name, perl = TRUE))
#     if (length(sm_num) == 0 || sm_num == "") "live1" else paste0("live", sm_num)
#   }
#   
#   calculate_last_value <- function(old_name, power_formula) {
#     folder    <- get_live_folder(old_name)
#     file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
#     if (!file.exists(file_path)) return(0)
#     
#     df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
#     if (nrow(df) == 0) return(0)
#     
#     cols <- trimws(unlist(strsplit(power_formula, "\\+")))
#     valid_cols <- cols[cols %in% names(df)]
#     if (length(valid_cols) == 0) return(0)
#     
#     sum(as.numeric(df[nrow(df), valid_cols, drop = FALSE]), na.rm = TRUE)
#   }
#   
#   # 6️⃣ Get last values for each row
#   last_values <- numeric(0)
#   appliance_values <- list()  # Store values by appliance name
#   
#   if (nrow(all_rows) > 0) {
#     last_values <- mapply(calculate_last_value, all_rows$old_name, all_rows$Power)
#     # Store values by appliance name for VR calculation
#     for (i in 1:nrow(all_rows)) {
#       appliance_values[[all_rows$new_name[i]]] <- last_values[i]
#     }
#   }
#   
#   # 6.5️⃣ Calculate Virtual Register values
#   vr_values <- numeric(0)
#   if (length(vr_names) > 0) {
#     for (vr_name in vr_names) {
#       formula <- vr_formulas[[vr_name]]
#       
#       # Parse formula to extract components
#       # Handle meter names with hyphens by temporarily replacing them
#       meter_pattern <- "([A-Za-z0-9]+(-[A-Za-z0-9 ]+)+)"
#       meter_matches <- gregexpr(meter_pattern, formula, perl = TRUE)
#       meter_names_in_formula <- unlist(regmatches(formula, meter_matches))
#       
#       # Replace hyphens in meter names temporarily
#       temp_formula <- formula
#       for (meter_name in meter_names_in_formula) {
#         if (grepl("-", meter_name)) {
#           replacement <- gsub("-", "|", meter_name)
#           temp_formula <- gsub(meter_name, replacement, temp_formula, fixed = TRUE)
#         }
#       }
#       
#       # Split formula into components
#       components <- unlist(strsplit(temp_formula, "(?<=\\S)\\s*(?=[+-])", perl = TRUE))
#       
#       # Calculate VR value
#       vr_value <- 0
#       for (comp in components) {
#         comp <- trimws(comp)
#         if (comp %in% c("+", "-")) next
#         
#         # Extract operator and clean component
#         operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else "+"
#         comp_clean <- sub("^[+-]\\s*", "", comp)
#         
#         # Restore hyphens
#         comp_clean <- gsub("\\|", "-", comp_clean)
#         
#         # Get value for this component
#         comp_value <- 0
#         if (comp_clean %in% names(appliance_values)) {
#           comp_value <- appliance_values[[comp_clean]]
#         } else {
#           # Try to find in config and calculate
#           comp_row <- config_df[config_df$new_name == comp_clean & no_phase(config_df$new_name), ]
#           if (nrow(comp_row) > 0) {
#             comp_value <- calculate_last_value(comp_row$old_name[1], comp_row$Power[1])
#             appliance_values[[comp_clean]] <- comp_value
#           }
#         }
#         
#         # Apply operator
#         if (operator == "+") {
#           vr_value <- vr_value + comp_value
#         } else if (operator == "-") {
#           vr_value <- vr_value - comp_value
#         }
#       }
#       
#       vr_values <- c(vr_values, vr_value)
#       appliance_values[[vr_name]] <- vr_value
#     }
#   }
#   
#   # Split into groups (now including VR values)
#   usage_count <- nrow(usage_metadata)
#   solar_count <- nrow(solar_row)
#   gen_count   <- nrow(gen_row)
#   
#   # Get usage values (regular + VR)
#   usage_vals <- numeric(0)
#   if (usage_count > 0) {
#     usage_vals <- last_values[1:usage_count]
#   }
#   # Add VR values to usage
#   if (length(vr_values) > 0) {
#     usage_vals <- c(usage_vals, vr_values)
#   }
#   
#   # Get solar and generator values
#   idx <- usage_count + 1
#   solar_vals <- if (solar_count > 0 && idx <= length(last_values)) last_values[idx:(idx + solar_count - 1)] else numeric(0)
#   idx <- idx + solar_count
#   gen_vals   <- if (gen_count > 0 && idx <= length(last_values)) last_values[idx:(idx + gen_count - 1)] else numeric(0)
#   
#   # 7️⃣ Calculate values
#   load_consumed <- round(sum(usage_vals) / 1000, 3)
#   solar_gen     <- round(sum(solar_vals) / 1000, 3)
#   genset        <- round(sum(gen_vals) / 1000, 3)
#   grid_value    <- round(solar_gen - load_consumed, 3)
#   
#   # 8️⃣ Build response
#   response <- list(
#     load_consumed = paste0(load_consumed, " kW"),
#     solar_gen     = paste0(solar_gen, " kW"),
#     genset        = paste0(genset, " kW"),
#     grid          = paste0(grid_value, " kW")
#   )#* @get /loadDistribution
#* @serializer unboxedJSON
function(username) {
  if (is.null(username)) {
    return(list(error = "Missing username"))
  }
  
  # 1️⃣ User → meter_id & meter_type
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # 2️⃣ Load config file -
  if(meter_type == 'neubolt') {
    config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
    if (!file.exists(config_path)) return(list(error = "Config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", first_meter_id, "_New_column_names.csv")
    if (!file.exists(config_path)) return(list(error = "Egauge config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  }
  
  #3️⃣ Usage columns from buildingMap
  getUsageColumns <- function(file_path) {
    buildingMap <- readRDS(file_path)
    retrieveNodes <- function(tree, level) {
      if (level == 0) {
        # Handle tree might be an atomic vector
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
    lvl <- 0
    repeat {
      usage_cols <- retrieveNodes(buildingMap, lvl)
      if (anyNA(usage_cols)) lvl <- lvl + 1 else break
    }
    as.character(usage_cols)
  }
  
  # EGAUGE: Use first_meter_id for both neubolt and egauge
  rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", first_meter_id, "_buildingMap.rds")
  if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
  usage_columns <<- getUsageColumns(rds_path)
  
  # 4️⃣ Prepare metadata (exclude rows with "phase") 
  no_phase <- function(x) !grepl("(?i)phase", x)
  
  # Split logic based on meter type
  if (meter_type == 'neubolt') {
    
    # NEUBOLT LOGIC
    
    # 3.5️⃣ Process Virtual Registers and add to usage columns
    vr_file <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/", meter_id, "_VirtualRegister.csv")
    vr_names <- character(0)  # Initialize empty vector for VR names
    vr_formulas <- list()     # Store VR formulas for later calculation
    
    if (file.exists(vr_file)) {
      vr_data <- read.csv(vr_file, stringsAsFactors = FALSE)
      if (nrow(vr_data) > 0) {
        # Extract VR names and formulas
        vr_names <- vr_data$Name
        for (i in 1:nrow(vr_data)) {
          vr_formulas[[vr_data$Name[i]]] <- trimws(vr_data$Formula[i])
        }
        # Add VR names to usage columns
        usage_columns <- c(usage_columns, vr_names)
      }
    }
    
    # For regular usage columns (non-VR)
    regular_usage_columns <- setdiff(usage_columns, vr_names)
    usage_metadata <<- config_df[config_df$new_name %in% regular_usage_columns & no_phase(config_df$new_name),
                                c("new_name", "old_name", "Power")]
    
    solar_row <- config_df[grepl("(?i)solar", config_df$new_name) & no_phase(config_df$new_name),
                           c("new_name", "old_name", "Power")]
    
    gen_row <- config_df[grepl("(?i)generator", config_df$new_name) & no_phase(config_df$new_name),
                         c("new_name", "old_name", "Power")]
    
    # Combine rows for batch processing (excluding VR for now)
    all_rows <- rbind(usage_metadata, solar_row, gen_row)
    
    # 5️⃣ Helper: last‑value extractor
    get_live_folder <- function(old_name) {
      sm_num <- regmatches(old_name, regexpr("(?<=SM)(\\d+)", old_name, perl = TRUE))
      if (length(sm_num) == 0 || sm_num == "") "live1" else paste0("live", sm_num)
    }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   
    #   # Check if file exists
    #   if (!file.exists(file_path)) {
    #     return(0)
    #   }
    #   
    #   # Try to read the file with error handling
    #   df <- tryCatch({
    #     read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   }, error = function(e) {
    #     return(NULL)
    #   })
    #   
    #   # Handle empty or NULL dataframe
    #   if (is.null(df) || nrow(df) == 0 || ncol(df) == 0) {
    #     return(0)
    #   }
    #   
    #   # Get last row
    #   last_row <- df[nrow(df), ]
    #   
    #   # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
    #   potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
    #   
    #   # Check which columns exist in the dataframe
    #   valid_cols <- potential_cols[potential_cols %in% names(df)]
    #   
    #   # If no valid columns found, return 0
    #   if (length(valid_cols) == 0) {
    #     return(0)
    #   }
    #   
    #   # Replace column names in formula with their actual values
    #   formula_eval <- power_formula
    #   for (col in valid_cols) {
    #     col_value <- tryCatch({
    #       as.numeric(last_row[[col]])
    #     }, error = function(e) {
    #       NA
    #     })
    #     
    #     # Handle NA or missing values
    #     if (is.na(col_value)) col_value <- 0
    #     
    #     formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
    #   }
    #   
    #   # Evaluate the formula with error handling
    #   result <- tryCatch({
    #     eval(parse(text = formula_eval))
    #   }, error = function(e) {
    #     0
    #   })
    #   
    #   # Handle NA result
    #   if (is.na(result)) result <- 0
    #   
    #   return(as.numeric(result))
    # }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   if (!file.exists(file_path)) return(0)
    #   
    #   df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   if (nrow(df) == 0) return(0)
    #   
    #   cols <- trimws(unlist(strsplit(power_formula, "\\+")))
    #   valid_cols <- cols[cols %in% names(df)]
    #   if (length(valid_cols) == 0) return(0)
    #   
    #   sum(as.numeric(df[nrow(df), valid_cols, drop = FALSE]), na.rm = TRUE)
    # }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   if (!file.exists(file_path)) return(0)
    #   
    #   df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   if (nrow(df) == 0) return(0)
    #   
    #   # Get last row
    #   last_row <- df[nrow(df), ]
    #   
    #   # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
    #   potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
    #   
    #   # Check which columns exist in the dataframe
    #   valid_cols <- potential_cols[potential_cols %in% names(df)]
    #   if (length(valid_cols) == 0) return(0)
    #   
    #   # Replace column names in formula with their actual values
    #   formula_eval <- power_formula
    #   for (col in valid_cols) {
    #     col_value <- as.numeric(last_row[[col]])
    #     if (is.na(col_value)) col_value <- 0
    #     formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
    #   }
    #   
    #   # Evaluate the formula
    #   result <- eval(parse(text = formula_eval))
    #   return(as.numeric(result))
    # }
    calculate_last_value <- function(old_name, power_formula) {
      folder    <- get_live_folder(old_name)
      file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
      
      # Check if file exists
      if (!file.exists(file_path)) return(0)
      
      # Read file
      df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Check if dataframe is valid and not empty
      if (is.null(df) || !is.data.frame(df) || nrow(df) == 0) return(0)
      
      # Get last row
      last_row <- df[nrow(df), ]
      
      # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
      potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
      
      # Check which columns exist in the dataframe
      valid_cols <- potential_cols[potential_cols %in% names(df)]
      if (length(valid_cols) == 0) return(0)
      
      # Replace column names in formula with their actual values
      formula_eval <- power_formula
      for (col in valid_cols) {
        col_value <- as.numeric(last_row[[col]])
        if (is.na(col_value)) col_value <- 0
        formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
      }
      
      # Evaluate the formula
      result <- eval(parse(text = formula_eval))
      if (is.na(result)) result <- 0
      
      return(as.numeric(result))
    }
    # 6️⃣ Get last values for each row
    last_values <- numeric(0)
    appliance_values <- list()  # Store values by appliance name
    
    if (nrow(all_rows) > 0) {
      last_values <- mapply(calculate_last_value, all_rows$old_name, all_rows$Power)
      # Store values by appliance name for VR calculation
      for (i in 1:nrow(all_rows)) {
        appliance_values[[all_rows$new_name[i]]] <- last_values[i]
      }
    }
    
    # 6.5️⃣ Calculate Virtual Register values
    vr_values <- numeric(0)
    if (length(vr_names) > 0) {
      for (vr_name in vr_names) {
        # Extract the Virtual Register name (e.g., "Mani") from the current row
        formula <- vr_formulas[[vr_name]]
        # Extract the formula (e.g., "-Generator phase 1 + Main") and remove leading/trailing whitespace
        original_formula <- trimws(formula)
        
        # Create a list to store meter names with hyphens and their replacements
        meter_names_with_hyphens <- list()
        
        # Identify potential meter names with hyphens
        # This pattern looks for words with hyphens that aren't isolated +/- operators
        meter_pattern <- "([A-Za-z0-9]+(-[A-Za-z0-9 ]+)+)"
        meter_matches <- gregexpr(meter_pattern, original_formula, perl = TRUE)
        meter_names <- unlist(regmatches(original_formula, meter_matches))
        
        # Create a working copy of the formula
        temp_formula <- original_formula
        
        # Replace hyphens in meter names with a special character (|)
        for (meter_name in meter_names) {
          if (grepl("-", meter_name)) {
            replacement <- gsub("-", "|", meter_name)
            meter_names_with_hyphens[[meter_name]] <- replacement
            temp_formula <- gsub(meter_name, replacement, temp_formula, fixed = TRUE)
          }
        }
        
        # Split the formula into components (terms and operators) using a regex
        components <- unlist(strsplit(temp_formula, "(?<=\\S)\\s*(?=[+-])", perl = TRUE))
        
        # Calculate VR value
        vr_value <- 0
        for (comp in components) {
          comp <- trimws(comp)
          if (comp %in% c("+", "-")) next
          
          # Extract operator and clean component
          operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else "+"
          comp_clean <- sub("^[+-]\\s*", "", comp)
          
          # Restore hyphens
          comp_clean <- gsub("\\|", "-", comp_clean)
          
          # Get value for this component
          comp_value <- 0
          if (comp_clean %in% names(appliance_values)) {
            comp_value <- appliance_values[[comp_clean]]
          } else {
            # Try to find in config and calculate
            comp_row <- config_df[config_df$new_name == comp_clean & no_phase(config_df$new_name), ]
            if (nrow(comp_row) > 0) {
              comp_value <- calculate_last_value(comp_row$old_name[1], comp_row$Power[1])
              appliance_values[[comp_clean]] <- comp_value
            }
          }
          
          # Apply operator
          if (operator == "+") {
            vr_value <- vr_value + comp_value
          } else if (operator == "-") {
            vr_value <- vr_value - comp_value
          }
        }
        
        vr_values <- c(vr_values, vr_value)
        appliance_values[[vr_name]] <- vr_value
      }
    }
    
    # Split into groups (now including VR values)
    usage_count <- nrow(usage_metadata)
    solar_count <- nrow(solar_row)
    gen_count   <- nrow(gen_row)
    
    # Get usage values (regular + VR)
    usage_vals <- numeric(0)
    if (usage_count > 0) {
      usage_vals <- last_values[1:usage_count]
    }
    # Add VR values to usage
    if (length(vr_values) > 0) {
      usage_vals <- c(usage_vals, vr_values)
    }
    
    # Get solar and generator values
    idx <- usage_count + 1
    solar_vals <- if (solar_count > 0 && idx <= length(last_values)) last_values[idx:(idx + solar_count - 1)] else numeric(0)
    idx <- idx + solar_count
    gen_vals   <- if (gen_count > 0 && idx <= length(last_values)) last_values[idx:(idx + gen_count - 1)] else numeric(0)
    
  } else {
    
    # EGAUGE LOGIC 
    
    dir_minutewise <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/minute-wise Data/", username, "/")
    
    # EGAUGE: Load recent files last 2 days
    files <- list.files(path = dir_minutewise, full.names = TRUE)
    if (length(files) == 0) return(list(error = "No egauge data files found"))
    
    # EGAUGE: Sort files and get recent ones
    file_dates <- sapply(files, function(f) {
      base_name <- basename(f)
      date_part <- strsplit(base_name, "_")[[1]][2]
      as.Date(date_part, format="%Y-%m-%d")
    })
    
    # EGAUGE: Get last 2 days
    end_date <- Sys.Date()
    start_date <- end_date - 1
    recent_files <- files[file_dates >= start_date & file_dates <= end_date]
    
    if (length(recent_files) == 0) {
      # EGAUGE: Fallback to most recent file if no recent files found
      recent_files <- files[order(file_dates)]
      recent_files <- tail(recent_files, 1)
    }
    
    # EGAUGE: Load and combine data from multiple files
    combined_data <- data.frame()
    for (file in recent_files) {
      if (file.exists(file)) {
        data <- read.csv(file, check.names = FALSE, stringsAsFactors = FALSE)
        combined_data <- rbind(combined_data, data)
      }
    }
    
    if (nrow(combined_data) == 0) return(list(error = "No data in egauge files"))
    
    # EGAUGE: everything same Is like historic_file_load function
    # Convert the Date & Time column to POSIXct format for proper time handling
    combined_data$`Date & Time` <- as.POSIXct(combined_data$`Date & Time`)
    # Sort the data by DateTime to ensure chronological order
    combined_data <- combined_data[order(combined_data$`Date & Time`), ]
    # Rename the first column to 'DateTime' for consistency
    colnames(combined_data)[1] <- "DateTime"
    
    # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
    combined_data[,2:ncol(combined_data)] = abs(combined_data[,2:ncol(combined_data)])
    
    # EGAUGE: Apply column name mapping from config it is  same as historic_file_load
    for (row in 1:nrow(config_df)) {
      old_name <- config_df$Old_name[row]
      new_name <- config_df$New_name[row]
      
      # EGAUGE: If the old name exists in combined_data, rename it to the new name
      if (old_name %in% colnames(combined_data) && old_name != new_name) {
        colnames(combined_data)[which(colnames(combined_data) == old_name)] <- new_name
        next
      }
      
      # EGAUGE: If the old name contains a '+', evaluate it as an expression to create a new column
      if (grepl('\\+', old_name)) {
        expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
        expression_str_fixed <- paste0('`', expression_str_fixed, '`')
        combined_data <- combined_data %>%
          mutate(!!new_name := eval(parse(text = expression_str_fixed)))
      }
    }
    
    # EGAUGE: Remove columns that are neither in Old_name nor New_name from the mapping
    retData_cols <- colnames(combined_data)
    for (cols in 2:length(retData_cols)) {
      if (!retData_cols[cols] %in% config_df$Old_name & !retData_cols[cols] %in% config_df$New_name) {
        combined_data <- combined_data[, -c(which(colnames(combined_data) == retData_cols[cols]))]
      }
    }
    
    # EGAUGE: Ensure a Usage column exists, initialize with NA if not present
    if (!"Usage" %in% colnames(combined_data)) combined_data$Usage <- NA
    # EGAUGE: Move the Usage column to be the second column (after DateTime)
    usage_col_idx <- which(colnames(combined_data) == "Usage")
    if (length(usage_col_idx) > 0) {
      combined_data <- combined_data[, c(1, usage_col_idx, setdiff(2:ncol(combined_data), usage_col_idx))]
    }
    
    # EGAUGE: Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
    for (col in 2:ncol(combined_data)) {
      combined_data[[col]] <- na.locf(combined_data[[col]], na.rm = F, maxgap = 5)
    }
    
    # EGAUGE: Append ' [kW]' to usage columns for egauge meters (same as historic_file_load)
    usage_columns_with_kw <- unlist(lapply(usage_columns, function(x) paste0(x, ' [kW]')))
    
    # EGAUGE: Filter usage columns to only those present in combined_data
    usage_columns_with_kw <- intersect(usage_columns_with_kw, colnames(combined_data))
    
    # EGAUGE: Compute Usage as the sum of usage columns if multiple exist, else use the single column
    if (length(usage_columns_with_kw) > 1) {
      combined_data$Usage = rowSums(combined_data[, usage_columns_with_kw], na.rm = TRUE)
    } else if (length(usage_columns_with_kw) == 1) {
      combined_data$Usage = combined_data[, usage_columns_with_kw]
    } else {
      combined_data$Usage <- 0
    }
    
    # EGAUGE: Get the last row (most recent data)
    last_row <- combined_data[nrow(combined_data), ]
    
    # EGAUGE: Extract usage value (already calculated above)
    usage_vals <- as.numeric(last_row$Usage)
    if (is.na(usage_vals)) usage_vals <- 0
    
    # EGAUGE: Extract solar values (look for columns with "solar" in name and [kW] suffix)
    solar_vals <- 0
    solar_cols <- grep("(?i)solar.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(solar_cols) > 0) {
      solar_values <- sapply(solar_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      solar_vals <- sum(solar_values)
    }
    
    # EGAUGE: Extract generator values (look for columns with "generator" in name and [kW] suffix)
    gen_vals <- 0
    gen_cols <- grep("(?i)generator.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(gen_cols) > 0) {
      gen_values <- sapply(gen_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      gen_vals <- sum(gen_values)
    }
  }
  
  # 7️⃣ Calculate values - EGAUGE: Added conditional unit conversion
  if (meter_type == 'neubolt') {
    # NEUBOLT: Convert from watts to kilowatts by dividing by 1000
    # load_consumed <- round(sum(usage_vals) / 1000, 3)
    # solar_gen     <- round(sum(solar_vals) / 1000, 3)
    # genset        <- round(sum(gen_vals) / 1000, 3)
    load_consumed <- round(sum(usage_vals, na.rm = TRUE) / 1000, 3)
    solar_gen     <- round(sum(solar_vals, na.rm = TRUE) / 1000, 3)
    genset        <- round(sum(gen_vals, na.rm = TRUE) / 1000, 3)
    } else {
    # EGAUGE: Data already in kW, no conversion needed
    load_consumed <- round(usage_vals, 3)
    solar_gen     <- round(solar_vals, 3)
    genset        <- round(gen_vals, 3)
  }
  # # Special case for shangrila
  # if (username == "shangrila") {
  #   # Get Generator VR value from appliance_values
  #   if ("Generator" %in% names(appliance_values)) {
  #     generator_value <- round(appliance_values[["Generator"]] / 1000, 3)
  #     genset <- generator_value
  #     load_consumed <- generator_value  # Use ONLY Generator VR, not sum
  #   }
  #   grid_value <- 0
  # } else {
  #   grid_value <- round(solar_gen - load_consumed, 3)
  # }
  # grid_value <- round(solar_gen - load_consumed, 3)
  # Spcl case for shangrila
  if (username == "shangrila") {
    # Calculate sum of G1, G2, G4 from appliance_values
    g_sum <- 0
    for (g_name in c("G1", "G2", "G4")) {
      if (g_name %in% names(appliance_values)) {
        g_sum <- g_sum + appliance_values[[g_name]]
      }
    }
    
    # Set genset and load_consumed to G1+G2+G4 sum
    genset <- round(g_sum / 1000, 3)
    load_consumed <- round(g_sum / 1000, 3)
    grid_value <- 0
  } else {
    grid_value <- round(solar_gen - load_consumed, 3)
  }
  
  # 8️⃣ Build response
  response <- list(
    load_consumed = paste0(load_consumed, " kW"),
    solar_gen     = paste0(solar_gen, " kW"),
    genset        = paste0(genset, " kW"),
    grid          = paste0(grid_value, " kW")
  )
  
  return(response)
}
#   
#   return(response)
# }
#* @get /loadDistribution
#* @serializer unboxedJSON
function(username) {
  if (is.null(username)) {
    return(list(error = "Missing username"))
  }
  
  # 1️⃣ User → meter_id & meter_type
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge (needed for config and buildingMap)
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # 2️⃣ Load config file -
  if(meter_type == 'neubolt') {
    config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
    if (!file.exists(config_path)) return(list(error = "Config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", first_meter_id, "_New_column_names.csv")
    if (!file.exists(config_path)) return(list(error = "Egauge config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  }
  
  #3️⃣ Usage columns from buildingMap
  getUsageColumns <- function(file_path) {
    buildingMap <- readRDS(file_path)
    retrieveNodes <- function(tree, level) {
      if (level == 0) {
        # Handle tree might be an atomic vector
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
    lvl <- 0
    repeat {
      usage_cols <- retrieveNodes(buildingMap, lvl)
      if (anyNA(usage_cols)) lvl <- lvl + 1 else break
    }
    as.character(usage_cols)
  }
  
  # EGAUGE: Use first_meter_id for both neubolt and egauge
  rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", first_meter_id, "_buildingMap.rds")
  if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
  usage_columns <<- getUsageColumns(rds_path)
  
  # 4️⃣ Prepare metadata (exclude rows with "phase") 
  no_phase <- function(x) !grepl("(?i)phase", x)
  
  # Split logic based on meter type
  if (meter_type == 'neubolt') {
    
    # NEUBOLT LOGIC
    
    # 3.5️⃣ Process Virtual Registers and add to usage columns
    vr_file <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/", meter_id, "_VirtualRegister.csv")
    vr_names <- character(0)  # Initialize empty vector for VR names
    vr_formulas <- list()     # Store VR formulas for later calculation
    
    if (file.exists(vr_file)) {
      vr_data <- read.csv(vr_file, stringsAsFactors = FALSE)
      if (nrow(vr_data) > 0) {
        # Extract VR names and formulas
        vr_names <- vr_data$Name
        for (i in 1:nrow(vr_data)) {
          vr_formulas[[vr_data$Name[i]]] <- trimws(vr_data$Formula[i])
        }
        # Add VR names to usage columns
        usage_columns <- c(usage_columns, vr_names)
      }
    }
    
    # For regular usage columns (non-VR)
    regular_usage_columns <- setdiff(usage_columns, vr_names)
    usage_metadata <<- config_df[config_df$new_name %in% regular_usage_columns & no_phase(config_df$new_name),
                                c("new_name", "old_name", "Power")]
    
    solar_row <- config_df[grepl("(?i)solar", config_df$new_name) & no_phase(config_df$new_name),
                           c("new_name", "old_name", "Power")]
    
    gen_row <- config_df[grepl("(?i)generator", config_df$new_name) & no_phase(config_df$new_name),
                         c("new_name", "old_name", "Power")]
    
    # Combine rows for batch processing (excluding VR for now)
    all_rows <- rbind(usage_metadata, solar_row, gen_row)
    
    # 5️⃣ Helper: last‑value extractor
    get_live_folder <- function(old_name) {
      sm_num <- regmatches(old_name, regexpr("(?<=SM)(\\d+)", old_name, perl = TRUE))
      if (length(sm_num) == 0 || sm_num == "") "live1" else paste0("live", sm_num)
    }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   
    #   # Check if file exists
    #   if (!file.exists(file_path)) {
    #     return(0)
    #   }
    #   
    #   # Try to read the file with error handling
    #   df <- tryCatch({
    #     read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   }, error = function(e) {
    #     return(NULL)
    #   })
    #   
    #   # Handle empty or NULL dataframe
    #   if (is.null(df) || nrow(df) == 0 || ncol(df) == 0) {
    #     return(0)
    #   }
    #   
    #   # Get last row
    #   last_row <- df[nrow(df), ]
    #   
    #   # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
    #   potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
    #   
    #   # Check which columns exist in the dataframe
    #   valid_cols <- potential_cols[potential_cols %in% names(df)]
    #   
    #   # If no valid columns found, return 0
    #   if (length(valid_cols) == 0) {
    #     return(0)
    #   }
    #   
    #   # Replace column names in formula with their actual values
    #   formula_eval <- power_formula
    #   for (col in valid_cols) {
    #     col_value <- tryCatch({
    #       as.numeric(last_row[[col]])
    #     }, error = function(e) {
    #       NA
    #     })
    #     
    #     # Handle NA or missing values
    #     if (is.na(col_value)) col_value <- 0
    #     
    #     formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
    #   }
    #   
    #   # Evaluate the formula with error handling
    #   result <- tryCatch({
    #     eval(parse(text = formula_eval))
    #   }, error = function(e) {
    #     0
    #   })
    #   
    #   # Handle NA result
    #   if (is.na(result)) result <- 0
    #   
    #   return(as.numeric(result))
    # }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   if (!file.exists(file_path)) return(0)
    #   
    #   df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   if (nrow(df) == 0) return(0)
    #   
    #   cols <- trimws(unlist(strsplit(power_formula, "\\+")))
    #   valid_cols <- cols[cols %in% names(df)]
    #   if (length(valid_cols) == 0) return(0)
    #   
    #   sum(as.numeric(df[nrow(df), valid_cols, drop = FALSE]), na.rm = TRUE)
    # }
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder    <- get_live_folder(old_name)
    #   file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   if (!file.exists(file_path)) return(0)
    #   
    #   df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
    #   if (nrow(df) == 0) return(0)
    #   
    #   # Get last row
    #   last_row <- df[nrow(df), ]
    #   
    #   # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
    #   potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
    #   
    #   # Check which columns exist in the dataframe
    #   valid_cols <- potential_cols[potential_cols %in% names(df)]
    #   if (length(valid_cols) == 0) return(0)
    #   
    #   # Replace column names in formula with their actual values
    #   formula_eval <- power_formula
    #   for (col in valid_cols) {
    #     col_value <- as.numeric(last_row[[col]])
    #     if (is.na(col_value)) col_value <- 0
    #     formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
    #   }
    #   
    #   # Evaluate the formula
    #   result <- eval(parse(text = formula_eval))
    #   return(as.numeric(result))
    # }
    calculate_last_value <- function(old_name, power_formula) {
      folder    <- get_live_folder(old_name)
      file_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
      
      # Check if file exists
      if (!file.exists(file_path)) return(0)
      
      # Read file
      df <- read.csv(file_path, check.names = FALSE, stringsAsFactors = FALSE)
      
      # Check if dataframe is valid and not empty
      if (is.null(df) || !is.data.frame(df) || nrow(df) == 0) return(0)
      
      # Get last row
      last_row <- df[nrow(df), ]
      
      # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
      potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
      
      # Check which columns exist in the dataframe
      valid_cols <- potential_cols[potential_cols %in% names(df)]
      if (length(valid_cols) == 0) return(0)
      
      # Replace column names in formula with their actual values
      formula_eval <- power_formula
      for (col in valid_cols) {
        col_value <- as.numeric(last_row[[col]])
        if (is.na(col_value)) col_value <- 0
        formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
      }
      
      # Evaluate the formula
      result <- eval(parse(text = formula_eval))
      if (is.na(result)) result <- 0
      
      return(as.numeric(result))
    }
    # 6️⃣ Get last values for each row
    last_values <- numeric(0)
    appliance_values <- list()  # Store values by appliance name
    
    if (nrow(all_rows) > 0) {
      last_values <- mapply(calculate_last_value, all_rows$old_name, all_rows$Power)
      # Store values by appliance name for VR calculation
      for (i in 1:nrow(all_rows)) {
        appliance_values[[all_rows$new_name[i]]] <- last_values[i]
      }
    }
    
    # 6.5️⃣ Calculate Virtual Register values
    vr_values <- numeric(0)
    if (length(vr_names) > 0) {
      for (vr_name in vr_names) {
        # Extract the Virtual Register name (e.g., "Mani") from the current row
        formula <- vr_formulas[[vr_name]]
        # Extract the formula (e.g., "-Generator phase 1 + Main") and remove leading/trailing whitespace
        original_formula <- trimws(formula)
        
        # Create a list to store meter names with hyphens and their replacements
        meter_names_with_hyphens <- list()
        
        # Identify potential meter names with hyphens
        # This pattern looks for words with hyphens that aren't isolated +/- operators
        meter_pattern <- "([A-Za-z0-9]+(-[A-Za-z0-9 ]+)+)"
        meter_matches <- gregexpr(meter_pattern, original_formula, perl = TRUE)
        meter_names <- unlist(regmatches(original_formula, meter_matches))
        
        # Create a working copy of the formula
        temp_formula <- original_formula
        
        # Replace hyphens in meter names with a special character (|)
        for (meter_name in meter_names) {
          if (grepl("-", meter_name)) {
            replacement <- gsub("-", "|", meter_name)
            meter_names_with_hyphens[[meter_name]] <- replacement
            temp_formula <- gsub(meter_name, replacement, temp_formula, fixed = TRUE)
          }
        }
        
        # Split the formula into components (terms and operators) using a regex
        components <- unlist(strsplit(temp_formula, "(?<=\\S)\\s*(?=[+-])", perl = TRUE))
        
        # Calculate VR value
        vr_value <- 0
        for (comp in components) {
          comp <- trimws(comp)
          if (comp %in% c("+", "-")) next
          
          # Extract operator and clean component
          operator <- if (grepl("^[+-]", comp)) sub("^([+-]).*", "\\1", comp) else "+"
          comp_clean <- sub("^[+-]\\s*", "", comp)
          
          # Restore hyphens
          comp_clean <- gsub("\\|", "-", comp_clean)
          
          # Get value for this component
          comp_value <- 0
          if (comp_clean %in% names(appliance_values)) {
            comp_value <- appliance_values[[comp_clean]]
          } else {
            # Try to find in config and calculate
            comp_row <- config_df[config_df$new_name == comp_clean & no_phase(config_df$new_name), ]
            if (nrow(comp_row) > 0) {
              comp_value <- calculate_last_value(comp_row$old_name[1], comp_row$Power[1])
              appliance_values[[comp_clean]] <- comp_value
            }
          }
          
          # Apply operator
          if (operator == "+") {
            vr_value <- vr_value + comp_value
          } else if (operator == "-") {
            vr_value <- vr_value - comp_value
          }
        }
        
        vr_values <- c(vr_values, vr_value)
        appliance_values[[vr_name]] <- vr_value
      }
    }
    
    # Split into groups (now including VR values)
    usage_count <- nrow(usage_metadata)
    solar_count <- nrow(solar_row)
    gen_count   <- nrow(gen_row)
    
    # Get usage values (regular + VR)
    usage_vals <- numeric(0)
    if (usage_count > 0) {
      usage_vals <- last_values[1:usage_count]
    }
    # Add VR values to usage
    if (length(vr_values) > 0) {
      usage_vals <- c(usage_vals, vr_values)
    }
    
    # Get solar and generator values
    idx <- usage_count + 1
    solar_vals <- if (solar_count > 0 && idx <= length(last_values)) last_values[idx:(idx + solar_count - 1)] else numeric(0)
    idx <- idx + solar_count
    gen_vals   <- if (gen_count > 0 && idx <= length(last_values)) last_values[idx:(idx + gen_count - 1)] else numeric(0)
    
  } else {
    
    # EGAUGE LOGIC 
    
    dir_minutewise <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/minute-wise Data/", username, "/")
    
    # EGAUGE: Load recent files last 2 days
    files <- list.files(path = dir_minutewise, full.names = TRUE)
    if (length(files) == 0) return(list(error = "No egauge data files found"))
    
    # EGAUGE: Sort files and get recent ones
    file_dates <- sapply(files, function(f) {
      base_name <- basename(f)
      date_part <- strsplit(base_name, "_")[[1]][2]
      as.Date(date_part, format="%Y-%m-%d")
    })
    
    # EGAUGE: Get last 2 days
    end_date <- Sys.Date()
    start_date <- end_date - 1
    recent_files <- files[file_dates >= start_date & file_dates <= end_date]
    
    if (length(recent_files) == 0) {
      # EGAUGE: Fallback to most recent file if no recent files found
      recent_files <- files[order(file_dates)]
      recent_files <- tail(recent_files, 1)
    }
    
    # EGAUGE: Load and combine data from multiple files
    combined_data <- data.frame()
    for (file in recent_files) {
      if (file.exists(file)) {
        data <- read.csv(file, check.names = FALSE, stringsAsFactors = FALSE)
        combined_data <- rbind(combined_data, data)
      }
    }
    
    if (nrow(combined_data) == 0) return(list(error = "No data in egauge files"))
    
    # EGAUGE: everything same Is like historic_file_load function
    # Convert the Date & Time column to POSIXct format for proper time handling
    combined_data$`Date & Time` <- as.POSIXct(combined_data$`Date & Time`)
    # Sort the data by DateTime to ensure chronological order
    combined_data <- combined_data[order(combined_data$`Date & Time`), ]
    # Rename the first column to 'DateTime' for consistency
    colnames(combined_data)[1] <- "DateTime"
    
    # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
    combined_data[,2:ncol(combined_data)] = abs(combined_data[,2:ncol(combined_data)])
    
    # EGAUGE: Apply column name mapping from config it is  same as historic_file_load
    for (row in 1:nrow(config_df)) {
      old_name <- config_df$Old_name[row]
      new_name <- config_df$New_name[row]
      
      # EGAUGE: If the old name exists in combined_data, rename it to the new name
      if (old_name %in% colnames(combined_data) && old_name != new_name) {
        colnames(combined_data)[which(colnames(combined_data) == old_name)] <- new_name
        next
      }
      
      # EGAUGE: If the old name contains a '+', evaluate it as an expression to create a new column
      if (grepl('\\+', old_name)) {
        expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
        expression_str_fixed <- paste0('`', expression_str_fixed, '`')
        combined_data <- combined_data %>%
          mutate(!!new_name := eval(parse(text = expression_str_fixed)))
      }
    }
    
    # EGAUGE: Remove columns that are neither in Old_name nor New_name from the mapping
    retData_cols <- colnames(combined_data)
    for (cols in 2:length(retData_cols)) {
      if (!retData_cols[cols] %in% config_df$Old_name & !retData_cols[cols] %in% config_df$New_name) {
        combined_data <- combined_data[, -c(which(colnames(combined_data) == retData_cols[cols]))]
      }
    }
    
    # EGAUGE: Ensure a Usage column exists, initialize with NA if not present
    if (!"Usage" %in% colnames(combined_data)) combined_data$Usage <- NA
    # EGAUGE: Move the Usage column to be the second column (after DateTime)
    usage_col_idx <- which(colnames(combined_data) == "Usage")
    if (length(usage_col_idx) > 0) {
      combined_data <- combined_data[, c(1, usage_col_idx, setdiff(2:ncol(combined_data), usage_col_idx))]
    }
    
    # EGAUGE: Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
    for (col in 2:ncol(combined_data)) {
      combined_data[[col]] <- na.locf(combined_data[[col]], na.rm = F, maxgap = 5)
    }
    
    # EGAUGE: Append ' [kW]' to usage columns for egauge meters (same as historic_file_load)
    usage_columns_with_kw <- unlist(lapply(usage_columns, function(x) paste0(x, ' [kW]')))
    
    # EGAUGE: Filter usage columns to only those present in combined_data
    usage_columns_with_kw <- intersect(usage_columns_with_kw, colnames(combined_data))
    
    # EGAUGE: Compute Usage as the sum of usage columns if multiple exist, else use the single column
    if (length(usage_columns_with_kw) > 1) {
      combined_data$Usage = rowSums(combined_data[, usage_columns_with_kw], na.rm = TRUE)
    } else if (length(usage_columns_with_kw) == 1) {
      combined_data$Usage = combined_data[, usage_columns_with_kw]
    } else {
      combined_data$Usage <- 0
    }
    
    # EGAUGE: Get the last row (most recent data)
    last_row <- combined_data[nrow(combined_data), ]
    
    # EGAUGE: Extract usage value (already calculated above)
    usage_vals <- as.numeric(last_row$Usage)
    if (is.na(usage_vals)) usage_vals <- 0
    
    # EGAUGE: Extract solar values (look for columns with "solar" in name and [kW] suffix)
    solar_vals <- 0
    solar_cols <- grep("(?i)solar.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(solar_cols) > 0) {
      solar_values <- sapply(solar_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      solar_vals <- sum(solar_values)
    }
    
    # EGAUGE: Extract generator values (look for columns with "generator" in name and [kW] suffix)
    gen_vals <- 0
    gen_cols <- grep("(?i)generator.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(gen_cols) > 0) {
      gen_values <- sapply(gen_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      gen_vals <- sum(gen_values)
    }
  }
  
  # 7️⃣ Calculate values - EGAUGE: Added conditional unit conversion
  if (meter_type == 'neubolt') {
    # NEUBOLT: Convert from watts to kilowatts by dividing by 1000
    # load_consumed <- round(sum(usage_vals) / 1000, 3)
    # solar_gen     <- round(sum(solar_vals) / 1000, 3)
    # genset        <- round(sum(gen_vals) / 1000, 3)
    load_consumed <- round(sum(usage_vals, na.rm = TRUE) / 1000, 3)
    solar_gen     <- round(sum(solar_vals, na.rm = TRUE) / 1000, 3)
    genset        <- round(sum(gen_vals, na.rm = TRUE) / 1000, 3)
    } else {
    # EGAUGE: Data already in kW, no conversion needed
    load_consumed <- round(usage_vals, 3)
    solar_gen     <- round(solar_vals, 3)
    genset        <- round(gen_vals, 3)
  }
  # # Special case for shangrila
  # if (username == "shangrila") {
  #   # Get Generator VR value from appliance_values
  #   if ("Generator" %in% names(appliance_values)) {
  #     generator_value <- round(appliance_values[["Generator"]] / 1000, 3)
  #     genset <- generator_value
  #     load_consumed <- generator_value  # Use ONLY Generator VR, not sum
  #   }
  #   grid_value <- 0
  # } else {
  #   grid_value <- round(solar_gen - load_consumed, 3)
  # }
  # grid_value <- round(solar_gen - load_consumed, 3)
  # Spcl case for shangrila
  if (username == "shangrila") {
    # Calculate sum of G1, G2, G4 from appliance_values
    g_sum <- 0
    for (g_name in c("G1", "G2", "G4")) {
      if (g_name %in% names(appliance_values)) {
        g_sum <- g_sum + appliance_values[[g_name]]
      }
    }
    
    # Set genset and load_consumed to G1+G2+G4 sum
    genset <- round(g_sum / 1000, 3)
    load_consumed <- round(g_sum / 1000, 3)
    grid_value <- 0
  } else {
    # grid_value <- round(solar_gen - load_consumed, 3)
    grid_value <- round(solar_gen + genset - load_consumed, 3)
  }
  
  # 8️⃣ Build response
  response <- list(
    load_consumed = paste0(load_consumed, " kW"),
    solar_gen     = paste0(solar_gen, " kW"),
    genset        = paste0(genset, " kW"),
    grid          = paste0(grid_value, " kW")
  )
  
  return(response)
}

#* @get /loadDistribution2
#* @serializer unboxedJSON
function(username) {
  if (is.null(username)) {
    return(list(error = "Missing username"))
  }
  
  # 1️⃣ User → meter_id & meter_type
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  
  meter_id <- as.character(user_row$Meter_id[1])
  meter_type <- as.character(user_row$Meter_type[1])  # EGAUGE: Added meter type detection
  
  # EGAUGE: Extract first meter ID for egauge needed for config and building Map
  first_meter_id <- if(meter_type == 'egauge') strsplit(meter_id, "; ")[[1]][1] else meter_id
  
  # 2️⃣ Load config
  if(meter_type == 'neubolt') {
    config_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
    if (!file.exists(config_path)) return(list(error = "Config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    # EGAUGE: Use first meter ID and egauge config path
    config_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/Egauge Column Names/", first_meter_id, "_New_column_names.csv")
    if (!file.exists(config_path)) return(list(error = "Egauge config file not found"))
    config_df <- read.csv(config_path, check.names = FALSE, stringsAsFactors = FALSE)
  }
  
  # 3️⃣ Usage columns from buildingMap
  getUsageColumns <- function(file_path) {
    buildingMap <- readRDS(file_path)
    retrieveNodes <- function(tree, level) {
      if (level == 0) {
        if (!is.null(tree$Name)) tree$Name else NA
      } else {
        unlist(lapply(tree, retrieveNodes, level = level - 1))
      }
    }
    lvl <- 0
    repeat {
      usage_cols <- retrieveNodes(buildingMap, lvl)
      if (anyNA(usage_cols)) lvl <- lvl + 1 else break
    }
    as.character(usage_cols)
  }
  
  # EGAUGE: Use first_meter_id for both neubolt and egauge
  rds_path <- paste0("/srv/shiny-server/EnergyMonitor2.0/", first_meter_id, "_buildingMap.rds")
  if (!file.exists(rds_path)) return(list(error = "BuildingMap RDS not found"))
  usage_columns <- getUsageColumns(rds_path)
  
  # 4️⃣ Prepare metadata (exclude "phase") 
  no_phase <- function(x) !grepl("(?i)phase", x)
  
  if (meter_type == 'neubolt') {
    
    # NEUBOLT LOGIC 
    
    usage_metadata <- config_df[config_df$new_name %in% usage_columns & no_phase(config_df$new_name),
                                c("new_name", "old_name", "Power")]
    solar_row <- config_df[grepl("(?i)solar",      config_df$new_name) & no_phase(config_df$new_name),
                           c("new_name", "old_name", "Power")]
    gen_row   <- config_df[grepl("(?i)generator",  config_df$new_name) & no_phase(config_df$new_name),
                           c("new_name", "old_name", "Power")]
    
    # Combine rows for batch read
    all_rows <- rbind(usage_metadata, solar_row, gen_row)
    if (nrow(all_rows) == 0)
      return(list(error = "No valid usage / solar / generator rows found"))
    
    # 5️⃣ Helper – fetch last value(s)
    get_live_folder <- function(old_name) {
      sm_num <- regmatches(old_name, regexpr("(?<=SM)(\\d+)", old_name, perl = TRUE))
      if (length(sm_num) == 0 || sm_num == "") "live1" else paste0("live", sm_num)
    }
    
    # calculate_last_value <- function(old_name, power_formula) {
    #   folder <- get_live_folder(old_name)
    #   csv    <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
    #   if (!file.exists(csv)) return(0)
    #   
    #   df <- read.csv(csv, check.names = FALSE, stringsAsFactors = FALSE)
    #   if (nrow(df) == 0) return(0)
    #   
    #   cols <- trimws(unlist(strsplit(power_formula, "\\+")))
    #   cols <- cols[cols %in% names(df)]
    #   if (length(cols) == 0) return(0)
    #   
    #   sum(as.numeric(df[nrow(df), cols, drop = FALSE]), na.rm = TRUE)
    # }
    calculate_last_value <- function(old_name, power_formula) {
      folder <- get_live_folder(old_name)
      csv    <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/db_data/", folder, "/", old_name, ".csv")
      if (!file.exists(csv)) return(0)
      
      df <- read.csv(csv, check.names = FALSE, stringsAsFactors = FALSE)
      if (nrow(df) == 0) return(0)
      
      # Get last row
      last_row <- df[nrow(df), ]
      
      # Extract all column names from formula (P0, P1, P2, L1, L2, L3, etc.)
      potential_cols <- unique(unlist(regmatches(power_formula, gregexpr("\\b[A-Z]\\d+\\b|\\b[LNP]\\d+\\b", power_formula, perl = TRUE))))
      
      # Check which columns exist in the dataframe
      valid_cols <- potential_cols[potential_cols %in% names(df)]
      if (length(valid_cols) == 0) return(0)
      
      # Replace column names in formula with their actual values
      formula_eval <- power_formula
      for (col in valid_cols) {
        col_value <- as.numeric(last_row[[col]])
        if (is.na(col_value)) col_value <- 0
        formula_eval <- gsub(paste0("\\b", col, "\\b"), col_value, formula_eval)
      }
      
      # Evaluate the formula
      result <- eval(parse(text = formula_eval))
      return(as.numeric(result))
    }
    # 6️⃣ Last values
    last_vals <- mapply(calculate_last_value, all_rows$old_name, all_rows$Power)
    
    usage_n <- nrow(usage_metadata)
    solar_n <- nrow(solar_row)
    gen_n   <- nrow(gen_row)
    
    idx <- 1
    usage_vals <<- if (usage_n > 0) last_vals[idx:(idx+usage_n-1)] else numeric(0)
    idx <- idx + usage_n
    solar_vals <- if (solar_n > 0) last_vals[idx:(idx+solar_n-1)] else numeric(0)
    idx <- idx + solar_n
    gen_vals   <- if (gen_n   > 0) last_vals[idx:(idx+gen_n-1)]   else numeric(0)
    
  } else {
    
    
    # EGAUGE: Set directory for egauge minute-wise data
    dir_minutewise <- paste0("/srv/shiny-server/Clients Usage Data/Egauge_Meter_Data/minute-wise Data/", username, "/")
    
    # EGAUGE: Load recent files last 2 days
    files <- list.files(path = dir_minutewise, full.names = TRUE)
    if (length(files) == 0) return(list(error = "No egauge data files found"))
    
    # EGAUGE: Sort files and get recent ones
    file_dates <- sapply(files, function(f) {
      base_name <- basename(f)
      date_part <- strsplit(base_name, "_")[[1]][2]
      as.Date(date_part, format="%Y-%m-%d")
    })
    
    # EGAUGE: Get last 2 days of files To GetRecentData
    end_date <- Sys.Date()
    start_date <- end_date - 1
    recent_files <- files[file_dates >= start_date & file_dates <= end_date]
    
    if (length(recent_files) == 0) {
      # EGAUGE: Fallback to most recent file if no recent files found
      recent_files <- files[order(file_dates)]
      recent_files <- tail(recent_files, 1)
    }
    
    # EGAUGE: Load and combine data from multiple files
    combined_data <- data.frame()
    for (file in recent_files) {
      if (file.exists(file)) {
        data <- read.csv(file, check.names = FALSE, stringsAsFactors = FALSE)
        combined_data <- rbind(combined_data, data)
      }
    }
    
    if (nrow(combined_data) == 0) return(list(error = "No data in egauge files"))
    
    # EGAUGE: historic_file_load function
    # Convert the Date & Time column to POSIXct format for proper time handling
    combined_data$`Date & Time` <- as.POSIXct(combined_data$`Date & Time`)
    # Sort the data by DateTime to ensure chronological order
    combined_data <- combined_data[order(combined_data$`Date & Time`), ]
    # Rename the first column to 'DateTime' for consistency
    colnames(combined_data)[1] <- "DateTime"
    
    # EGAUGE: For non-Neubolt meters, ensure all numeric columns are positive
    combined_data[,2:ncol(combined_data)] = abs(combined_data[,2:ncol(combined_data)])
    
    # EGAUGE: Apply column name mapping from config lke we hve in historic_file_load
    for (row in 1:nrow(config_df)) {
      old_name <- config_df$Old_name[row]
      new_name <- config_df$New_name[row]
      
      # EGAUGE: If the old name exists in combined_data, rename it to the new name
      if (old_name %in% colnames(combined_data) && old_name != new_name) {
        colnames(combined_data)[which(colnames(combined_data) == old_name)] <- new_name
        next
      }
      
      # EGAUGE: If the old name contains a '+', evaluate it as an expression to create a new column
      if (grepl('\\+', old_name)) {
        expression_str_fixed <- gsub(" \\+ ", "`\\1\\+`", old_name)
        expression_str_fixed <- paste0('`', expression_str_fixed, '`')
        combined_data <- combined_data %>%
          mutate(!!new_name := eval(parse(text = expression_str_fixed)))
      }
    }
    
    # EGAUGE: Remove columns that are neither in Old_name nor New_name from the mapping
    retData_cols <- colnames(combined_data)
    for (cols in 2:length(retData_cols)) {
      if (!retData_cols[cols] %in% config_df$Old_name & !retData_cols[cols] %in% config_df$New_name) {
        combined_data <- combined_data[, -c(which(colnames(combined_data) == retData_cols[cols]))]
      }
    }
    
    # EGAUGE: Ensure a Usage column exists, initialize with NA if not present
    if (!"Usage" %in% colnames(combined_data)) combined_data$Usage <- NA
    # EGAUGE: Move the Usage column to be the second column (after DateTime)
    usage_col_idx <- which(colnames(combined_data) == "Usage")
    if (length(usage_col_idx) > 0) {
      combined_data <- combined_data[, c(1, usage_col_idx, setdiff(2:ncol(combined_data), usage_col_idx))]
    }
    
    # EGAUGE: Fill NA values in all columns (except DateTime) using last observation carried forward, up to a gap of 5
    for (col in 2:ncol(combined_data)) {
      combined_data[[col]] <- na.locf(combined_data[[col]], na.rm = F, maxgap = 5)
    }
    
    # EGAUGE: Append ' [kW]' to usage columns for egauge meters 
    usage_columns_with_kw <- unlist(lapply(usage_columns, function(x) paste0(x, ' [kW]')))
    
    # EGAUGE: Filter usage columns to only those present in combined_data
    usage_columns_with_kw <- intersect(usage_columns_with_kw, colnames(combined_data))
    
    # EGAUGE: Compute Usage as the sum of usage columns if multiple exist, else use the single column
    if (length(usage_columns_with_kw) > 1) {
      combined_data$Usage = rowSums(combined_data[, usage_columns_with_kw], na.rm = TRUE)
    } else if (length(usage_columns_with_kw) == 1) {
      combined_data$Usage = combined_data[, usage_columns_with_kw]
    } else {
      combined_data$Usage <- 0
    }
    
    # EGAUGE: Get the last row (most rnt data)
    last_row <- combined_data[nrow(combined_data), ]
    
    # EGAUGE: Extract usage value we already calculated this value abve
    usage_vals <- as.numeric(last_row$Usage)
    if (is.na(usage_vals)) usage_vals <- 0
    
    # EGAUGE: Extract solar values (look for columns with "solar" in name and [kW] suffix)
    solar_vals <- 0
    solar_cols <- grep("(?i)solar.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(solar_cols) > 0) {
      solar_values <- sapply(solar_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      solar_vals <- sum(solar_values)
    }
    
    # EGAUGE: Extract generator values (look for columns with "generator" in name and [kW] suffix)
    gen_vals <- 0
    gen_cols <- grep("(?i)generator.*\\[kW\\]", colnames(combined_data), value = TRUE)
    if (length(gen_cols) > 0) {
      gen_values <- sapply(gen_cols, function(col) {
        val <- as.numeric(last_row[[col]])
        if (is.na(val)) 0 else val
      })
      gen_vals <- sum(gen_values)
    }
    
    # EGAUGE: Set counts for downstream logic compatibility
    usage_n <- if(usage_vals > 0) 1 else 0
    solar_n <- if(solar_vals > 0) 1 else 0
    gen_n   <- if(gen_vals > 0) 1 else 0
  }
  
  # 7️⃣ Numeric totals
  if (meter_type == 'neubolt') {
    # NEUBOLT: Convert from watts to kilowatts by dividing by 1000
    load_kw   <- round(sum(usage_vals,  na.rm = TRUE) / 1000, 3)
    solar_kw  <- round(sum(solar_vals,  na.rm = TRUE) / 1000, 3)
    genset_kw <- round(sum(gen_vals,   na.rm = TRUE) / 1000, 3)
  } else {
    # EGAUGE: Data already in kW, no conversion needed
    load_kw   <- round(usage_vals, 3)
    solar_kw  <- round(solar_vals, 3)
    genset_kw <- round(gen_vals, 3)
  }
  
  # ➡️ If solar OR genset column missing → force solar & export to 0
  if (solar_n == 0 || gen_n == 0) {
    solar_kw <- 0
  }
  
  # 8️⃣ Grid import / export
  grid_val <- round(solar_kw - load_kw, 3)
  
  wapda_export_kw <- if (solar_kw == 0) 0 else if (grid_val >= 0) grid_val else 0
  wapda_import_kw <- if (grid_val < 0) abs(grid_val) else 0
  
  # 9️⃣ Response
  list(
    load_consumed = paste0(load_kw, " kW"),
    solar_gen     = paste0(solar_kw, " kW"),
    wapda_export  = paste0(wapda_export_kw, " kW"),
    wapda_import  = paste0(wapda_import_kw, " kW")
  )
}





#* @post /updated_notification_settings
#* @serializer unboxedJSON
function(
    username,
    machine_name,
    Working_Hour_Start,
    Working_Hour_End,
    Total_Break_Hours,
    Rated_Power,
    Benchmark_Power
) {
  #────────────────────────────────────────────
  # 1️⃣  Validate required params
  #────────────────────────────────────────────
  if (missing(username) || missing(machine_name)) {
    return(list(error = "Missing username or machine_name"))
  }
  
  #────────────────────────────────────────────
  # 2️⃣  Get meter_id for user
  #────────────────────────────────────────────
  profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  if (!file.exists(profiles_path)) return(list(error = "client_profiles.csv not found"))
  profiles <- read.csv(profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  user_row <- profiles[profiles$user == username, ]
  if (nrow(user_row) == 0) return(list(error = "Username not found"))
  meter_id <- as.character(user_row$Meter_id[1])
  
  #────────────────────────────────────────────
  # 3️⃣  Open meter's config file
  #────────────────────────────────────────────
  cfg_path <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
  if (!file.exists(cfg_path)) return(list(error = "Config file not found for meter_id"))
  cfg <- read.csv(cfg_path, check.names = FALSE, stringsAsFactors = FALSE)
  
  #────────────────────────────────────────────
  # 4️⃣  Locate machine row by Display_name (case‑insensitive trim)
  #────────────────────────────────────────────
  clean <- function(x) trimws(tolower(x))
  row_idx <- which(clean(cfg$Display_name) == clean(machine_name))
  if (length(row_idx) == 0) return(list(error = "machine_name not found in Display_name column"))
  
  #────────────────────────────────────────────
  # 5️⃣  Ensure required columns exist; add if missing
  #────────────────────────────────────────────
  req_cols <- c("Working_Hour_Start", "Working_Hour_End", "Total_Break_Hours",
                "Rated_Power", "Benchmark_Power")
  for (col in req_cols) {
    if (!(col %in% names(cfg))) cfg[[col]] <- NA
  }
  
  #────────────────────────────────────────────
  # 6️⃣  Update values (convert to numeric where relevant)
  #────────────────────────────────────────────
  cfg$Working_Hour_Start[row_idx] <- as.character(Working_Hour_Start)
  cfg$Working_Hour_End[row_idx]   <- as.character(Working_Hour_End)
  cfg$Total_Break_Hours[row_idx]  <- as.numeric(Total_Break_Hours)
  cfg$Rated_Power[row_idx]        <- as.numeric(Rated_Power)
  cfg$Benchmark_Power[row_idx]    <- as.numeric(Benchmark_Power)
  
  #────────────────────────────────────────────
  # 7️⃣  Save file back
  #────────────────────────────────────────────
  write.csv(cfg, cfg_path, row.names = FALSE)
  
  return(list(
    success  = TRUE,
    meter_id = meter_id,
    machine_name = machine_name,
    updated_fields = list(
      Working_Hour_Start = Working_Hour_Start,
      Working_Hour_End   = Working_Hour_End,
      Total_Break_Hours  = Total_Break_Hours,
      Rated_Power        = Rated_Power,
      Benchmark_Power    = Benchmark_Power
    )
  ))
}


#### GitHub API Data ####
#* @get /serverData
#* @serializer unboxedJSON
function(fileDir, start_date, end_date) {
  if (is.na(fileDir) | is.na(start_date) | is.na(end_date)) return(list('Error: Missing parameter'))
  
  historic_files = list.files(path = fileDir)
  historic_files = paste0(fileDir, historic_files)
  
  extract_date <- function(file_path) {
    # Extract the base file name
    base_name <- basename(file_path)
    
    # Split the base name by underscores
    parts <- strsplit(base_name, "_")[[1]]
    
    # The date is the second part
    date_string <- parts[2]
    
    # Convert the date string to Date object
    as.Date(date_string, format="%Y-%m-%d")
  }
  
  dates <- sapply(historic_files, extract_date)
  historic_files <- historic_files[order(dates)]
  
  historic_dates = sapply(strsplit(basename(historic_files), '_', fixed = T), `[`, 2)
  
  
  retData <- do.call(rbind.fill, lapply(historic_files[match(start_date, historic_dates):match(end_date, historic_dates)], read.csv, check.names=F))
  
  return(as.list(retData))
}




#### Retrieve Virtual Register Data by Username ####
#* @get /getVirtualRegister
#* @param username:string The username to search for
#* @serializer unboxedJSON
function(username) {
  # Check for missing or empty username
  if (is.null(username) || username == "") {
    return(list(error = "Username is required"))
  }
  
  # Define the path to client_profiles.csv
  client_profiles_path <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  
  # Check if client_profiles.csv exists
  if (!file.exists(client_profiles_path)) {
    return(list(error = "client_profiles.csv file not found"))
  }
  
  # Read client_profiles.csv
  client_profiles <- read.csv(client_profiles_path, check.names = FALSE, stringsAsFactors = FALSE)
  
  # Find the row where the user matches the provided username
  client <- client_profiles[client_profiles$user == username, ]
  
  # Check if the username exists
  if (nrow(client) == 0) {
    return(list(error = "Username not found"))
  }
  
  # Extract the Meter_id
  meter_id <- client$Meter_id[1]
  
  # Define the path to the Virtual Register directory
  virtual_register_dir <- "/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Virtual Register/"
  
  # Check if the Virtual Register directory exists
  if (!dir.exists(virtual_register_dir)) {
    return(list(error = "Virtual Register directory not found"))
  }
  
  # Construct the virtual register file name (e.g., 50086_VirtualRegister.csv)
  virtual_register_file <- paste0(meter_id, "_VirtualRegister.csv")
  virtual_register_path <- paste0(virtual_register_dir, virtual_register_file)
  
  # Check if the virtual register file exists
  if (!file.exists(virtual_register_path)) {
    return(list(error = paste("Virtual register file for Meter_id", meter_id, "not found")))
  }
  
  # Read the virtual register file
  virtual_register_data <- read.csv(virtual_register_path, check.names = FALSE)
  
  # Return the contents of the virtual register file
  return(list(
    data = as.list(virtual_register_data)
  ))
}
#### Step 2: Registration ####
#* Register New User Step 2
#* @post /add_user
#* @param username:string Username of the new user (required)
#* @param password:string Password for the new user (required)
#* @param confirm_password:string Password confirmation (required)
#* @param email:string Email address of the new user (required)
#* @param display_name:string Display name of the user (optional, defaults to username)
#* @param city:string City of the user
#* @param consumerID:string Consumer ID of the user
#* @param meters:integer Number of meters (billCount)
#* @param solar:logical Whether the user has solar (optional, defaults to FALSE)
#* @param sanctioned_load:numeric Sanctioned load
#* @param unit_price:numeric Unit price
#* @param colorBlindFriendly:logical Whether the user prefers colorblind-friendly settings
#* @param theme:string Theme preference for the user
#* @param Meter_type:string Type of meter
#* @param Link:string Link, typically same as email
#* @param Lacuna:logical Lacuna setting
#* @param removed:logical Whether the user is marked as removed
#* @param Lat:numeric Latitude
#* @param Lng:numeric Longitude
#* @param comparativeStart:numeric Comparative start value
#* @param minimumThreshold:numeric Minimum threshold value
#* @param daily_units_limit:numeric Daily units limit
#* @param solar_capacity_kW:numeric Solar capacity in kW
#* @param description:string Description
#* @param Daily_Reports:string Daily reports setting
#* @param Weekly_Reports:string Weekly reports setting
#* @param Monthly_Reports:string Monthly reports setting
#* @param Phone_Number:string Phone number
#* @param Branch_Location:string Branch location
#* @param Branch_Manager_Name:string Branch manager name
#* @param Sanctioned_Load_Alert:string Sanctioned load alert setting
#* @param Minimum_Threshold_Alert:string Minimum threshold alert setting
#* @param Daily_Summary_Alert:string Daily summary alert setting
#* @param Tour_Status:string Tour status
#* @param Machine_Activation_Alerts:string Machine activation alerts setting
#* @param Daily_Units_Consumption_Alert:string Daily units consumption alert setting
#* @param Main_Voltage_Outrange_Alert:string Main voltage outrange alert setting
#* @param Main_Gen_Duality_Alert:string Plant generator duality alert setting
#* @param Solar_Productivity_Alert:string Solar productivity alert setting
#* @param Power_Cutoff_Alert:string Power cutoff alert setting
#* @param Machine_Overload_Alert:string Machine overload alert setting
#* @serializer unboxedJSON
function(req) {
  # Extract body as a list
  body <- req$body
  
  # Check for required parameters
  required_fields <- c("username", "password", "confirm_password", "email")
  if (!all(required_fields %in% names(body))) {
    return(list(status = "error", message = "All required fields (username, password, confirm_password, email) must be provided"))
  }
  
  # Extract parameters with defaults
  username <- body$username
  password <- body$password
  confirm_password <- body$confirm_password
  email <- body$email
  display_name <- if (is.null(body$display_name)) username else body$display_name
  solar <- if (is.null(body$solar)) FALSE else body$solar
  unit_price <- if (is.null(body$unit_price)) 70 else body$unit_price
  city <- if (is.null(body$city)) NA else body$city
  consumerID <- if (is.null(body$consumerID)) "XYZ" else body$consumerID
  meters <- if (is.null(body$meters)) 1 else body$meters
  sanctioned_load <- if (is.null(body$sanctioned_load)) 0 else body$sanctioned_load
  colorBlindFriendly <- if (is.null(body$colorBlindFriendly)) FALSE else body$colorBlindFriendly
  theme <- if (is.null(body$theme)) "default" else body$theme
  Meter_type <- if (is.null(body$Meter_type)) "neubolt" else body$Meter_type
  Link <- if (is.null(body$Link)) email else body$Link
  Lacuna <- if (is.null(body$Lacuna)) FALSE else body$Lacuna
  removed <- if (is.null(body$removed)) FALSE else body$removed
  Lat <- if (is.null(body$Lat)) NA else body$Lat
  Lng <- if (is.null(body$Lng)) NA else body$Lng
  comparativeStart <- if (is.null(body$comparativeStart)) 0 else body$comparativeStart
  minimumThreshold <- if (is.null(body$minimumThreshold)) 10 else body$minimumThreshold
  daily_units_limit <- if (is.null(body$daily_units_limit)) NA else body$daily_units_limit
  solar_capacity_kW <- if (is.null(body$solar_capacity_kW)) NA else body$solar_capacity_kW
  description <- if (is.null(body$description)) NA else body$description
  Daily_Reports <- if (is.null(body$Daily_Reports)) NA else body$Daily_Reports
  Weekly_Reports <- if (is.null(body$Weekly_Reports)) NA else body$Weekly_Reports
  Monthly_Reports <- if (is.null(body$Monthly_Reports)) NA else body$Monthly_Reports
  Phone_Number <- if (is.null(body$Phone_Number)) NA else body$Phone_Number
  Branch_Location <- if (is.null(body$Branch_Location)) NA else body$Branch_Location
  Branch_Manager_Name <- if (is.null(body$Branch_Manager_Name)) NA else body$Branch_Manager_Name
  Sanctioned_Load_Alert <- if (is.null(body$Sanctioned_Load_Alert)) NA else body$Sanctioned_Load_Alert
  Minimum_Threshold_Alert <- if (is.null(body$Minimum_Threshold_Alert)) NA else body$Minimum_Threshold_Alert
  Daily_Summary_Alert <- if (is.null(body$Daily_Summary_Alert)) NA else body$Daily_Summary_Alert
  Tour_Status <- if (is.null(body$Tour_Status)) NA else body$Tour_Status
  Machine_Activation_Alerts <- if (is.null(body$Machine_Activation_Alerts)) NA else body$Machine_Activation_Alerts
  Daily_Units_Consumption_Alert <- if (is.null(body$Daily_Units_Consumption_Alert)) NA else body$Daily_Units_Consumption_Alert
  Main_Voltage_Outrange_Alert <- if (is.null(body$Main_Voltage_Outrange_Alert)) NA else body$Main_Voltage_Outrange_Alert
  Main_Gen_Duality_Alert <- if (is.null(body$Main_Gen_Duality_Alert)) NA else body$Main_Gen_Duality_Alert
  Solar_Productivity_Alert <- if (is.null(body$Solar_Productivity_Alert)) NA else body$Solar_Productivity_Alert
  Power_Cutoff_Alert <- if (is.null(body$Power_Cutoff_Alert)) NA else body$Power_Cutoff_Alert
  Machine_Overload_Alert <- if (is.null(body$Machine_Overload_Alert)) NA else body$Machine_Overload_Alert
  
  # Check for empty required fields
  if (is.null(username) || is.null(password) || is.null(confirm_password) || 
      is.null(email) ||
      username == "" || password == "" || confirm_password == "" || 
      email == "") {
    return(list(status = "error", message = "Required fields cannot be empty"))
  }
  
  # Check if password matches confirm_password
  if (password != confirm_password) {
    return(list(status = "error", message = "Error: Passwords do not match"))
  }
  
  # Validate email format
  if (!grepl("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email)) {
    return(list(status = "error", message = "Invalid email format"))
  }
  
  # Define CSV file paths
  profiles_csv <- "../Clients Usage Data/client_profiles.csv"
  settings_csv <- "../Clients Usage Data/client_settings.csv"
  
  # Check for duplicate username and email in either CSV
  profiles_exists <- file.exists(profiles_csv)
  settings_exists <- file.exists(settings_csv)
  
  if (profiles_exists) {
    profiles <- read.csv(profiles_csv, stringsAsFactors = FALSE, check.names = FALSE)
    if (username %in% profiles$user) {
      return(list(status = "error", message = "Error: User already registered"))
    }
  }
  
  if (settings_exists) {
    settings <- read.csv(settings_csv, stringsAsFactors = FALSE, check.names = FALSE)
    if (email %in% settings$email) {
      return(list(status = "error", message = "Error: Email already registered"))
    }
  }
  
  # Initialize client_profiles.csv if it doesn't exist
  if (!profiles_exists) {
    profiles <- data.frame(
      user = character(),
      Meter_id = character(),
      Installation_date = character(),
      Customer_ID = character(),
      Lat = numeric(),
      Lng = numeric(),
      colorBlindFriendly = logical(),
      theme = character(),
      displayName = character(),
      Meter_type = character(),
      City = character(),
      Link = character(),
      Lacuna = logical(),
      billCount = integer(),
      solar = logical(),
      removed = logical(),
      stringsAsFactors = FALSE
    )
    write.csv(profiles, profiles_csv, row.names = FALSE, quote = FALSE)
  } else {
    profiles <- read.csv(profiles_csv, stringsAsFactors = FALSE, check.names = FALSE)
  }
  
  # Initialize client_settings.csv if it doesn't exist
  if (!settings_exists) {
    settings <- data.frame(
      user = character(),
      password = character(),
      email = character(),
      comparativeStart = numeric(),
      notifEnable = logical(),
      notifHour = integer(),
      notifLimit = numeric(),
      sanctionedLoad = numeric(),
      minimumThreshold = numeric(),
      daily_units_limit = numeric(),
      solar_capacity_kW = numeric(),
      deviceToken = character(),
      description = character(),
      unitPrice = numeric(),
      Daily_Reports = character(),
      Weekly_Reports = character(),
      Monthly_Reports = character(),
      Phone_Number = character(),
      Branch_Location = character(),
      Branch_Manager_Name = character(),
      Sanctioned_Load_Alert = character(),
      Minimum_Threshold_Alert = character(),
      Daily_Summary_Alert = character(),
      Tour_Status = character(),
      Machine_Activation_Alerts = character(),
      Daily_Units_Consumption_Alert = character(),
      Main_Voltage_Outrange_Alert = character(),
      Main_Gen_Duality_Alert = character(),
      Solar_Productivity_Alert = character(),
      Power_Cutoff_Alert = character(),
      Machine_Overload_Alert = character(),
      stringsAsFactors = FALSE
    )
    write.csv(settings, settings_csv, row.names = FALSE, quote = FALSE)
  } else {
    settings <- read.csv(settings_csv, stringsAsFactors = FALSE, check.names = FALSE)
  }
  
  # Calculate Meter_id from client_profiles.csv, restricted to 50000–59999
  Meter_id <- "50000"  # Default starting value
  if (profiles_exists && nrow(profiles) > 0 && "Meter_id" %in% colnames(profiles)) {
    meter_ids <- suppressWarnings(as.numeric(profiles$Meter_id))
    # Filter for values between 50000 and 59999
    valid_ids <- meter_ids[!is.na(meter_ids) & meter_ids >= 50000 & meter_ids <= 59999]
    if (length(valid_ids) > 0) {
      max_id <- max(valid_ids, na.rm = TRUE)
      next_id <- max_id + 1
      if (next_id > 59999) {
        return(list(status = "error", message = "Error: Meter_id limit exceeded (59999)"))
      }
      Meter_id <- as.character(next_id)
    }
  }
  
  # Create new row for client_profiles with expected columns
  expected_profile_cols <- c("user", "Meter_id", "Installation_date", "Customer_ID", 
                             "Lat", "Lng", "colorBlindFriendly", "theme", "displayName", 
                             "Meter_type", "City", "Link", "Lacuna", "billCount", 
                             "solar", "removed")
  profile_row <- data.frame(matrix(NA, nrow = 1, ncol = length(expected_profile_cols)))
  colnames(profile_row) <- expected_profile_cols
  profile_row$user <- username
  profile_row$Meter_id <- Meter_id
  profile_row$Installation_date <- format(Sys.Date(), "%m/%d/%Y")
  profile_row$Customer_ID <- consumerID
  profile_row$Lat <- Lat
  profile_row$Lng <- Lng
  profile_row$colorBlindFriendly <- colorBlindFriendly
  profile_row$theme <- theme
  profile_row$displayName <- display_name
  profile_row$Meter_type <- Meter_type
  profile_row$City <- city
  profile_row$Link <- NA
  profile_row$Lacuna <- Lacuna
  profile_row$billCount <- meters
  profile_row$solar <- solar
  profile_row$removed <- removed
  profile_row$account_type <- "standard"
  
  # Create new row for client_settings with expected columns
  expected_settings_cols <- c("user", "password", "email", "comparativeStart", 
                              "notifEnable", "notifHour", "notifLimit", "sanctionedLoad", 
                              "minimumThreshold", "daily_units_limit", "solar_capacity_kW", 
                              "deviceToken", "description", "unitPrice", "Daily_Reports", 
                              "Weekly_Reports", "Monthly_Reports", "Phone_Number", 
                              "Branch_Location", "Branch_Manager_Name", "Sanctioned_Load_Alert", 
                              "Minimum_Threshold_Alert", "Daily_Summary_Alert", "Tour_Status", 
                              "Machine_Activation_Alerts", "Daily_Units_Consumption_Alert", 
                              "Main_Voltage_Outrange_Alert", "Main_Gen_Duality_Alert", 
                              "Solar_Productivity_Alert", "Power_Cutoff_Alert", 
                              "Machine_Overload_Alert")
  settings_row <- data.frame(matrix(NA, nrow = 1, ncol = length(expected_settings_cols)))
  colnames(settings_row) <- expected_settings_cols
  settings_row$user <- username
  settings_row$password <- password
  settings_row$email <- email
  settings_row$comparativeStart <- comparativeStart
  settings_row$notifEnable <- 0
  settings_row$notifHour <- 10
  settings_row$notifLimit <- 0
  settings_row$sanctionedLoad <- sanctioned_load
  settings_row$minimumThreshold <- minimumThreshold
  settings_row$daily_units_limit <- daily_units_limit
  settings_row$solar_capacity_kW <- solar_capacity_kW
  settings_row$deviceToken <- ""
  settings_row$description <- description
  settings_row$unitPrice <- unit_price
  settings_row$Daily_Reports <- Daily_Reports
  settings_row$Weekly_Reports <- Weekly_Reports
  settings_row$Monthly_Reports <- Monthly_Reports
  settings_row$Phone_Number <- Phone_Number
  settings_row$Branch_Location <- Branch_Location
  settings_row$Branch_Manager_Name <- Branch_Manager_Name
  settings_row$Sanctioned_Load_Alert <- Sanctioned_Load_Alert
  settings_row$Minimum_Threshold_Alert <- Minimum_Threshold_Alert
  settings_row$Daily_Summary_Alert <- Daily_Summary_Alert
  settings_row$Tour_Status <- Tour_Status
  settings_row$Machine_Activation_Alerts <- Machine_Activation_Alerts
  settings_row$Daily_Units_Consumption_Alert <- Daily_Units_Consumption_Alert
  settings_row$Main_Voltage_Outrange_Alert <- Main_Voltage_Outrange_Alert
  settings_row$Main_Gen_Duality_Alert <- Main_Gen_Duality_Alert
  settings_row$Solar_Productivity_Alert <- Solar_Productivity_Alert
  settings_row$Power_Cutoff_Alert <- Power_Cutoff_Alert
  settings_row$Machine_Overload_Alert <- Machine_Overload_Alert
  settings_row$account_type <- "standard"
  
  # Append rows to CSVs with error handling
  tryCatch({
    write.table(profile_row, profiles_csv, append = TRUE, col.names = FALSE, sep = ",", row.names = FALSE, quote = FALSE)
  }, error = function(e) {
    return(list(status = "error", message = paste("Failed to write to profiles CSV:", e$message)))
  })
  tryCatch({
    write.table(settings_row, settings_csv, append = TRUE, col.names = FALSE, sep = ",", row.names = FALSE, quote = FALSE)
  }, error = function(e) {
    return(list(status = "error", message = paste("Failed to write to settings CSV:", e$message)))
  })
  
  # Prepare response (exclude password and confirm_password)
  response_data <- body
  response_data$password <- NULL
  response_data$confirm_password <- NULL
  response_data$Installation_date <- format(Sys.Date(), "%m/%d/%Y")
  response_data$Meter_id <- Meter_id
  
  # Return success response
  return(list(
    status = "success",
    message = "User registered successfully",
    data = response_data
  ))
}
#### Step 2: Appliance Configuration ####
#* Register New Appliance Configuration
#* @post /add_appliance
#* @param client:string Client username (required)
#* @param smartmeter_ID:string Smart meter ID (required)
#* @param appliances:list List of appliances, each containing appliance_name, appliance_type, slotNumber0, and for three-phase: slotNumber1, slotNumber2 (required)
#* @serializer unboxedJSON
function(req) {
  # Debug: Log raw body and content type
  cat("Content-Type:", req$HTTP_CONTENT_TYPE, "\n")
  cat("req$bodyRaw:\n")
  print(req$bodyRaw)
  cat("req$body (pre-parsed):\n")
  str(req$body)
  
  # Try parsing req$bodyRaw, fall back to req$body
  library(jsonlite)
  body <- tryCatch({
    if (!is.null(req$bodyRaw) && is.character(req$bodyRaw) && nchar(req$bodyRaw) > 0) {
      jsonlite::fromJSON(req$bodyRaw, simplifyDataFrame = FALSE, simplifyVector = FALSE)
    } else if (!is.null(req$body)) {
      # Convert req$body appliances to list if it's a data.frame
      if (is.data.frame(req$body$appliances)) {
        body <- req$body
        body$appliances <- lapply(seq_len(nrow(req$body$appliances)), function(i) {
          as.list(req$body$appliances[i, , drop = FALSE])
        })
        body
      } else {
        req$body
      }
    } else {
      stop("No valid request body provided")
    }
  }, error = function(e) {
    return(list(status = "error", message = paste("Failed to parse request body:", e$message)))
  })
  
  # Debug: Log parsed body structure
  cat("Parsed body structure:\n")
  str(body)
  
  # Check for required top-level parameters
  required_fields <- c("client", "smartmeter_ID", "appliances")
  if (!all(required_fields %in% names(body))) {
    return(list(status = "error", message = "All required fields (client, smartmeter_ID, appliances) must be provided"))
  }
  
  # Extract top-level parameters
  client <- body$client
  smartmeter_ID <- body$smartmeter_ID
  appliances <- body$appliances
  
  # Check for empty required fields
  if (is.null(client) || is.null(smartmeter_ID) || is.null(appliances) ||
      client == "" || smartmeter_ID == "" || length(appliances) == 0) {
    return(list(status = "error", message = "Required fields cannot be empty, and appliances list must not be empty"))
  }
  
  # Read client_profiles.csv to get Meter_id
  profiles_csv <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  if (!file.exists(profiles_csv)) {
    return(list(status = "error", message = "Client profiles file not found"))
  }
  
  profiles <- read.csv(profiles_csv, stringsAsFactors = FALSE, check.names = FALSE)
  if (!client %in% profiles$user) {
    return(list(status = "error", message = "Client not found in client_profiles"))
  }
  
  meter_id <- profiles$Meter_id[profiles$user == client]
  if (is.na(meter_id) || meter_id == "") {
    return(list(status = "error", message = "Meter_id not found for the client"))
  }
  
  # Define config file path
  config_file <- paste0("/srv/shiny-server/Clients Usage Data/Neubolt Meter Data/Column Correction/", meter_id, "_Config.csv")
  
  # Check if config file exists
  config_exists <- file.exists(config_file)
  
  # Initialize machine_count for new_name (Machine 1, Machine 2, etc.)
  if (config_exists) {
    config_data <- read.csv(config_file, stringsAsFactors = FALSE, check.names = FALSE)
    machine_count <- nrow(config_data)
  } else {
    machine_count <- 0
  }
  
  # Initialize config file if it doesn't exist
  if (!config_exists) {
    config_data <- data.frame(
      new_name = character(),
      Display_name = character(),
      old_name = character(),
      Voltage = character(),
      Current = character(),
      Power = character(),
      PowerFactor = character(),
      Frequency = character(),
      EnergyPositive = character(),
      EnergyNegative = character(),
      Color = character(),
      Working_Hour_Start = character(),
      Working_Hour_End = character(),
      Total_Break_Hours = numeric(),
      Threshold_Power = numeric(),
      Rated_Power = numeric(),
      Benchmark_Power = numeric(),
      Load_type = character(),
      Working_Hours = numeric(),
      Daily_Benching = logical(),
      Enable_Alert = logical(),
      ON_OFF_Alert = logical(),
      stringsAsFactors = FALSE
    )
    write.csv(config_data, config_file, row.names = FALSE, quote = FALSE)
  } else {
    config_data <- read.csv(config_file, stringsAsFactors = FALSE, check.names = FALSE)
  }
  
  # Initialize response data
  response_data <- list()
  
  # Process each appliance
  for (i in seq_along(appliances)) {
    appliance <- appliances[[i]]
    
    # Debug: Log appliance structure
    cat(paste("Appliance", i, "structure:\n"))
    str(appliance)
    
    # Check for required appliance fields
    appliance_required_fields <- c("appliance_name", "appliance_type", "slotNumber0","scale_factor")
    if (!all(appliance_required_fields %in% names(appliance))) {
      return(list(status = "error", message = paste("Appliance", i, ": All required fields (appliance_name, appliance_type, slotNumber0) must be provided")))
    }
    
    # Extract appliance parameters
    appliance_name <- appliance$appliance_name
    appliance_type <- tolower(appliance$appliance_type) # Normalize case
    slotNumber0 <- appliance$slotNumber0
    scale_factor <- appliance$scale_factor
    # If missing or NULL → default = 1
    if (is.null(scale_factor)) {
      scale_factor <- 1
    }
    scale_factor <- as.numeric(scale_factor)
    if (is.na(scale_factor)) {
      scale_factor <- 1
    }
    
    # Check for empty appliance fields
    if (is.null(appliance_name) || is.null(appliance_type) || is.null(slotNumber0) ||
        appliance_name == "" || appliance_type == "" || slotNumber0 == "") {
      return(list(status = "error", message = paste("Appliance", i, ": Required fields cannot be empty")))
    }
    
    # Validate appliance_type
    if (!appliance_type %in% c("single-phase", "three-phase")) {
      return(list(status = "error", message = paste("Appliance", i, ": appliance_type must be 'single-phase' or 'three-phase' (case-insensitive)")))
    }
    
    # Validate slotNumber0
    slotNumber0 <- as.numeric(slotNumber0)
    if (is.na(slotNumber0) || slotNumber0 <= 0 || slotNumber0 != floor(slotNumber0)) {
      return(list(status = "error", message = paste("Appliance", i, ": slotNumber0 must be a positive integer")))
    }
    
    # Check additional required fields for three-phase
    if (appliance_type == "three-phase") {
      if (!all(c("slotNumber1", "slotNumber2") %in% names(appliance))) {
        return(list(status = "error", message = paste("Appliance", i, ": slotNumber1 and slotNumber2 are required for three-phase appliances")))
      }
      slotNumber1 <- as.numeric(appliance$slotNumber1)
      slotNumber2 <- as.numeric(appliance$slotNumber2)
      
      # Validate slotNumber1 and slotNumber2
      if (is.null(slotNumber1) || is.null(slotNumber2) ||
          is.na(slotNumber1) || is.na(slotNumber2) ||
          slotNumber1 <= 0 || slotNumber2 <= 0 ||
          slotNumber1 != floor(slotNumber1) || slotNumber2 != floor(slotNumber2)) {
        return(list(status = "error", message = paste("Appliance", i, ": slotNumber1 and slotNumber2 must be positive integers")))
      }
    }
    
    # Calculate Voltage, Current, Power, and PowerFactor
    voltage <- paste0("V", slotNumber0 - 1)
    power_factor<- paste0("PF", slotNumber0 - 1)
    frequency <- paste0("F", slotNumber0 - 1)
    
    # SINGLE-PHASE
    current_base  <- paste0("I", slotNumber0 - 1)
    power_base    <- paste0("P", slotNumber0 - 1)
    energyP_base  <- paste0("EP", slotNumber0 - 1)
    energyN_base  <- paste0("EN", slotNumber0 - 1)
    
    # THREE-PHASE 
    if (appliance_type == "three-phase") {
      current_base  <- paste0("I", slotNumber0 - 1, "+I", slotNumber1 - 1, "+I", slotNumber2 - 1)
      power_base    <- paste0("P", slotNumber0 - 1, "+P", slotNumber1 - 1, "+P", slotNumber2 - 1)
      energyP_base  <- paste0("EP", slotNumber0 - 1, "+EP", slotNumber1 - 1, "+EP", slotNumber2 - 1)
      energyN_base  <- paste0("EN", slotNumber0 - 1, "+EN", slotNumber1 - 1, "+EN", slotNumber2 - 1)
    }
    
    # APPLY SCALE FACTOR 
    if (is.null(scale_factor) || is.na(scale_factor) || scale_factor == 1 || scale_factor == 0) {
      current   <- current_base
      power     <- power_base
      energyP   <- energyP_base
      energyN   <- energyN_base
    } else {
      current   <- paste0(scale_factor, "*(", current_base, ")")
      power     <- paste0(scale_factor, "*(", power_base, ")")
      energyP   <- paste0(scale_factor, "*(", energyP_base, ")")
      energyN   <- paste0(scale_factor, "*(", energyN_base, ")")
    }
  
    # Increment machine_count for new_name
    machine_count <- machine_count + 1
    new_name <- paste0("Machine ", machine_count)
    
    # Create new row for config file
    config_cols <- colnames(config_data)
    config_row <- data.frame(matrix(NA, nrow = 1, ncol = length(config_cols)))
    colnames(config_row) <- config_cols
    config_row$new_name <- new_name
    config_row$Display_name <- appliance_name
    config_row$old_name <- smartmeter_ID
    config_row$Voltage <- voltage
    config_row$Current <- current
    config_row$Power <- power
    config_row$PowerFactor <- power_factor
    config_row$Frequency <- frequency 
    config_row$EnergyPositive <- energyP
    config_row$EnergyNegative <- energyN
    
    # Append row to config file
    write.table(config_row, config_file, append = TRUE, col.names = FALSE, sep = ",", row.names = FALSE, quote = FALSE)
    
    # Add to response data
    appliance_response <- list(
      appliance_name = appliance_name,
      appliance_type = appliance_type,
      slotNumber0 = slotNumber0,
      slotNumber1 = if (exists("slotNumber1")) slotNumber1 else NULL,
      slotNumber2 = if (exists("slotNumber2")) slotNumber2 else NULL,
      new_name = new_name,
      Voltage = voltage,
      Current = current,
      Power = power,
      PowerFactor = power_factor,
      Frequency = frequency, 
      EnergyPositive = energyP,
      EnergyNegative = energyN
    )
    response_data[[i]] <- appliance_response
  }
  
  # Return success response
  return(list(
    status = "success",
    message = "All appliance configurations added successfully",
    data = list(
      client = client,
      smartmeter_ID = smartmeter_ID,
      appliances = response_data
    )
  ))
}
#### Building Map POST API ####
#* @apiTitle Building Map API
#* @apiDescription API to create and save building map as .rds file
#* Create and save building map as .rds file
#* @param user:string The client/user identifier
#* @param main The JSON object containing top-level locations and appliances
#* @post /building_map
function(req, user, main) {
  
  # Validate inputs
  if (is.null(user) || user == "") {
    return(list(status = "error", message = "User parameter is missing"))
  }
  
  if (is.null(main) || length(main) == 0) {
    return(list(status = "error", message = "Main parameter is missing or empty"))
  }
  
  # Validate main is a named list with at least one location
  if (!is.list(main) || !all(sapply(names(main), nzchar)) || length(names(main)) == 0) {
    return(list(status = "error", message = "Main parameter must be a non-empty named list of locations"))
  }
  
  # Validate each location has a list or vector of appliances
  for (location in names(main)) {
    if (!is.list(main[[location]]) && !is.character(main[[location]])) {
      return(list(status = "error", message = paste("Appliances for", location, "must be a list or character vector")))
    }
  }
  
  # Read client_profiles.csv to get Meter_id
  profiles_csv <- "/srv/shiny-server/Clients Usage Data/client_profiles.csv"
  if (!file.exists(profiles_csv)) {
    return(list(status = "error", message = "Client profiles file not found"))
  }
  
  profiles <- read.csv(profiles_csv, stringsAsFactors = FALSE, check.names = FALSE)
  if (!user %in% profiles$user) {
    return(list(status = "error", message = "Client not found in client_profiles"))
  }
  
  meter_id <- profiles$Meter_id[profiles$user == user]
  if (is.na(meter_id) || meter_id == "") {
    return(list(status = "error", message = "Meter_id not found for the client"))
  }
  
  # Initialize the Main structure
  building_structure <- list(
    Main = list(
      Name = "Main",
      Current = 0,
      Voltage = 0,
      Power = 0,
      PowerFactor = 0
    )
  )
  
  # Process top-level locations and appliances
  for (location in names(main)) {
    building_structure$Main[[location]] <- list(
      Name = location,
      Current = 0,
      Voltage = 0,
      Power = 0,
      PowerFactor = 0
    )
    
    # Ensure appliances are treated as a character vector
    appliances <- unlist(main[[location]])
    if (length(appliances) == 0) {
      next  # Skip if no appliances
    }
    
    # Add appliances under the location
    for (appliance in appliances) {
      building_structure$Main[[location]][[appliance]] <- list(
        Name = appliance,
        Current = 0,
        Voltage = 0,
        Power = 0,
        PowerFactor = 0
      )
    }
  }
  
  # Define config file path
  config_file <- paste0("/srv/shiny-server/EnergyMonitor2.0/", meter_id, "_buildingMap.rds")
  
  # Save as .rds file
  tryCatch({
    saveRDS(building_structure, file = config_file)
  }, error = function(e) {
    return(list(status = "error", message = paste("Failed to save .rds file:", e$message)))
  })
  
  # Return success response with JSON body
  return(list(
    status = "success",
    message = "Building map saved successfully",
    data = building_structure
  ))
}
        
    

#### Read Notification ####

#* @apiTitle Update ReadStatus API
#* Update ReadStatus to TRUE based on datetime and client_id
#* If read_all = TRUE, mark all rows as TRUE for that client
#* @param datetime:string (optional) Date and time value
#* @param client_id:string Client identifier
#* @param read_all:bool (optional) Mark all as read if TRUE
#* @post /receive-data
function(datetime = NULL, client_id = NULL, read_all = FALSE) {
  tryCatch({
    
    if (is.null(datetime) || datetime == "") {
      datetime <- NA
    }
    
    if (is.null(read_all) || read_all == "") {
      read_all <- "false"
    }
    
    if (is.null(client_id) || client_id == "") {
      return(list(
        success = FALSE,
        message = "Missing or invalid client_id."
      ))
    }
    
    file_path <- paste0("/srv/shiny-server/EnvizReports/alerts_notif/", client_id, "_alerts.csv")
    
    
    if (!file.exists(file_path)) {
      return(list(
        success = FALSE,
        message = paste("Client ID", client_id, "is invalid or file not found.")
      ))
    }
    
    notif_data <- read.csv(file_path, stringsAsFactors = FALSE)

    required_cols <- c("ReadStatus", "date_time")
    
    
    # --- CASE 1: read_all = TRUE  ---
    if (!is.null(read_all) && !is.na(read_all) && (tolower(read_all) == "true" || read_all == TRUE)) {
      notif_data$ReadStatus <- "TRUE"
      write.csv(notif_data, file_path, row.names = FALSE)
      return(list(
        success = TRUE,
        message = "All rows marked as TRUE for this client.",
        total_updated = nrow(notif_data)
      ))
    }
    
    # --- CASE 2: datetime provided ---
    if (!is.null(datetime) && datetime != "") {
      datetime_trimmed <- format(as.POSIXct(datetime), "%Y-%m-%d %H:%M:%S")
      notif_data$date_time_trimmed <- format(as.POSIXct(notif_data$date_time), "%Y-%m-%d %H:%M:%S")
      
      match_row <- which(notif_data$date_time_trimmed == datetime_trimmed)
      
      
      if (length(match_row) > 0) {
        notif_data$ReadStatus[match_row] <- "TRUE"
        write.csv(notif_data, file_path, row.names = FALSE)
        return(list(
          success = TRUE,
          message = "ReadStatus updated successfully for matching datetime.",
          updated_rows = length(match_row)
        ))
      } else {
        return(list(
          success = FALSE,
          message = paste("No matching record found for datetime:", datetime)
        ))
      }
    }
    
    # --- CASE 3: No datetime and read_all is FALSE 
    return(list(
      success = FALSE,
      message = "Please provide either a valid datetime or set read_all = TRUE."
    ))
    
  }, error = function(e) {
    # --- Catch any unhandled server error (500 equivalent) ---
    return(list(
      success = FALSE,
      message = paste("Internal Server Error:", e$message)
    ))
  })
}

#### Upload Logo ####
#* Upload a logo for a specific user
#* @param username:string Username for the logo
#* @post /upload_logo
#* @serializer unboxedJSON
function(req, res, username) {
  # Validate username
  if (is.null(username) || username == "" || nchar(username) == 0) {
    res$status <- 400
    return(list(status = "error", message = "Username is required"))
  }
  
  # Sanitize username to prevent path traversal
  username <- gsub("[^[:alnum:]_-]", "", username)
  if (nchar(username) == 0) {
    res$status <- 400
    return(list(status = "error", message = "Invalid username format"))
  }
  
  # Get file data exactly like /uploadimg does
  bodyfiles <- req$body$imgfile$value
  
  # Simple validation
  if (is.null(bodyfiles)) {
    res$status <- 400
    return(list(status = "error", message = "No file uploaded"))
  }
  
  # Ensure destination directory exists
  dest_dir <- "../EnvizFleet/www"
  if (!dir.exists(dest_dir)) {
    dir.create(dest_dir, recursive = TRUE)
  }
  
  # Construct file path
  dest_path <- file.path(dest_dir, paste0("logo_", username, ".png"))
  
  # Write the file (same as /uploadimg)
  tryCatch({
    writeBin(bodyfiles, dest_path)
  }, error = function(e) {
    res$status <- 500
    return(list(status = "error", message = paste("Error saving file:", e$message)))
  })
  
  list(status = "success", message = "Logo uploaded successfully",
       filename = paste0("logo_", username, ".png"))
}

#### Get logo ####
#* Get user's logo image
#* @param username:string Username for the logo
#* @get /logo
#* @serializer contentType list(type="image/png")
function(res, username) {
  # Validate username
  if (is.null(username) || username == "" || nchar(username) == 0) {
    res$status <- 400
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(list(status = "error", message = "Username is required"), auto_unbox = TRUE)
    return(res)
  }
  
  # Sanitize username to prevent path traversal
  username <- gsub("[^[:alnum:]_-]", "", username)
  if (nchar(username) == 0) {
    res$status <- 400
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(list(status = "error", message = "Invalid username format"), auto_unbox = TRUE)
    return(res)
  }
  
  file_path <- file.path("../EnvizFleet/www", paste0("logo_", username, ".png"))
  
  # Check if logo exists
  if (!file.exists(file_path)) {
    res$status <- 404
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(list(status = "error", message = "Logo not found for this user"), auto_unbox = TRUE)
    return(res)
  }
  
  # Return the logo as binary image with error handling
  tryCatch({
    readBin(file_path, "raw", n = file.info(file_path)$size)
  }, error = function(e) {
    res$status <- 500
    res$setHeader("Content-Type", "application/json")
    res$body <- jsonlite::toJSON(list(status = "error", message = paste("Error reading file:", e$message)), auto_unbox = TRUE)
    return(res)
  })
}
####   Save Hierarchy Data #####
#* Save Hierarchy Data
#* @description Reads CSV locally, maps User to MeterID, saves RDS to specific server path.
#* @parser json
#* @serializer json
#* @post /save_hierarchy
function(req) {
  # ============================================================================
  # 1. CONFIGURATION
  # ============================================================================
  
  PROFILES_PATH <- '/srv/shiny-server/Clients Usage Data/client_profiles.csv'
  BASE_DIR      <- '/srv/shiny-server/EnergyMonitor2.0/'
  STORAGE_DIR   <- file.path(BASE_DIR)
  
  if (!dir.exists(STORAGE_DIR)) {
    dir.create(STORAGE_DIR, recursive = TRUE)
  }
  
  # ============================================================================
  # 2. INTERNAL HELPER FUNCTIONS
  # ============================================================================
  
  add_default_metrics <- function(node) {
    node$Voltage        <- 0
    node$Current        <- 0
    node$Power          <- 0
    node$PowerFactor    <- 0
    node$Frequency      <- 0
    node$EnergyPositive <- 0
    node$EnergyNegative <- 0
  }
  
  build_tree_recursive <- function(parent_node, data) {
    if (is.list(data) && !is.null(names(data))) {
      for (name in names(data)) {
        child_node <- parent_node$AddChild(name)
        add_default_metrics(child_node)
        build_tree_recursive(child_node, data[[name]])
      }
    } else if (is.vector(data) || (is.list(data) && is.null(names(data)))) {
      for (item in data) {
        node_name  <- as.character(item)
        child_node <- parent_node$AddChild(node_name)
        add_default_metrics(child_node)
      }
    }
  }
  
  tree_to_list <- function(node) {
    node_data <- list(
      Name           = node$name,
      Voltage        = node$Voltage,
      Current        = node$Current,
      Power          = node$Power,
      PowerFactor    = node$PowerFactor,
      Frequency      = node$Frequency,
      EnergyPositive = node$EnergyPositive,
      EnergyNegative = node$EnergyNegative
    )
    
    if (node$count > 0) {
      children_data <- lapply(node$children, tree_to_list)
      return(c(node_data, children_data))
    } else {
      return(node_data)
    }
  }
  
  # ============================================================================
  # 3. MAIN EXECUTION LOGIC
  # ============================================================================
  
  tryCatch({
    # --- A. Input Parsing (Standard Production approach) ---
    # We strictly use req$body because the @parser json annotation 
    # automatically puts the JSON content there.
    hierarchy_data <- req$body
    
    # Safety check: if body is empty
    if (is.null(hierarchy_data) || length(hierarchy_data) == 0) {
      stop("No JSON data received in request body.")
    }
    
    # Handle stringified JSON edge cases (Swagger sometimes sends strings)
    if (is.character(hierarchy_data) && length(hierarchy_data) == 1) {
      json_str <- hierarchy_data
      if (!grepl("^\\s*\\{", json_str)) json_str <- paste0("{", json_str, "}")
      hierarchy_data <- jsonlite::fromJSON(json_str, simplifyVector = FALSE)
    }
    
    # --- B. Load CSV & Determine Meter ID ---
    target_meter_id <- "UnknownMeter"
    
    if (!is.null(hierarchy_data$user)) {
      raw_user <- hierarchy_data$user
      if (is.list(raw_user)) raw_user <- unlist(raw_user)
      user_str <- as.character(raw_user)
      clean_user_name <- trimws(strsplit(user_str, ";")[[1]][1])
      
      if (file.exists(PROFILES_PATH)) {
        profiles_db <- read.csv(PROFILES_PATH, stringsAsFactors = FALSE)
        names(profiles_db) <- trimws(names(profiles_db))
        
        match_idx <- which(profiles_db$user == clean_user_name)
        
        if (length(match_idx) > 0) {
          raw_meter_id <- as.character(profiles_db$Meter_id[match_idx[1]])
          ids_list <- strsplit(raw_meter_id, "[;,\\s]+")[[1]]
          valid_ids <- ids_list[ids_list != ""]
          
          if (length(valid_ids) > 0) {
            target_meter_id <- valid_ids[1]
          }
        } else {
          target_meter_id <- paste0(clean_user_name, "_NoID")
        }
      } else {
        # Log error to server console, but don't crash client
        print(paste("ERROR: CSV not found at", PROFILES_PATH))
        target_meter_id <- paste0(clean_user_name, "_CSVMissing")
      }
      
      hierarchy_data$user <- NULL
    }
    
    # --- C. Build Tree ---
    root <- Node$new("Root_Wrapper") 
    
    start_data <- hierarchy_data
    if (!is.null(hierarchy_data$main)) {
      start_data <- hierarchy_data$main
      if (!is.null(start_data$user)) start_data$user <- NULL
    }
    
    build_tree_recursive(root, start_data)
    
    # --- D. Save to RDS ---
    clean_list_to_save <- lapply(root$children, tree_to_list)
    
    safe_id <- gsub("[^a-zA-Z0-9_-]", "_", target_meter_id)
    file_name_base <- paste0(safe_id, "_buildingmap.rds")
    full_file_path <- file.path(STORAGE_DIR, file_name_base)
    
    saveRDS(clean_list_to_save, file = full_file_path)
    
    # --- E. Return Response ---
    list(
      status = "success",
      message = paste0("Saved for Meter ID: ", safe_id),
      file_path = full_file_path,
      preview = clean_list_to_save
    )
    
  }, error = function(e) {
    print(paste("API Error:", e$message))
    list(
      status = "error", 
      message = "An internal error occurred.", 
      details = e$message
    )
  })
}
#* Update Specific Client Profile Entry
#* @description Updates a single cell in the server CSV based on User and Column Name.
#* @param username The client's username (matches 'user' column)
#* @param column_name The CSV column header to update
#* @param new_value The new value to insert
#* @post /update_profile_entry
function(username, column_name, new_value) {
  
  # ============================================================================
  # 1. SERVER CONFIGURATION
  # ============================================================================
  
  # Strict Production Path
  PROFILES_PATH <- '/srv/shiny-server/Clients Usage Data/client_profiles.csv'
  
  # ============================================================================
  # 2. MAIN EXECUTION LOGIC
  # ============================================================================
  
  tryCatch({
    # --- A. Validate Inputs ---
    if (missing(username) || is.null(username) || username == "") {
      return(list(status="error", message="Missing parameter: 'username'"))
    }
    if (missing(column_name) || is.null(column_name) || column_name == "") {
      return(list(status="error", message="Missing parameter: 'column_name'"))
    }
    # Note: We allow new_value to be NULL/empty if the user wants to clear a cell
    if (missing(new_value)) {
      return(list(status="error", message="Missing parameter: 'new_value'"))
    }
    
    # --- B. Read CSV from Server Path ---
    if (!file.exists(PROFILES_PATH)) {
      # Log this error to the server console so admins can see it
      print(paste("CRITICAL ERROR: CSV not found at", PROFILES_PATH))
      return(list(status="error", message="Server Error: Database file not found."))
    }
    
    # Read CSV (check.names=FALSE preserves spaces in headers if any)
    profiles_db <- read.csv(PROFILES_PATH, stringsAsFactors = FALSE, check.names = FALSE)
    names(profiles_db) <- trimws(names(profiles_db)) # Sanitize headers
    
    # --- C. Find Row (User Lookup) ---
    if (!"user" %in% names(profiles_db)) {
      return(list(status="error", message="Database integrity error: 'user' column missing."))
    }
    
    row_idx <- which(profiles_db$user == username)
    
    if (length(row_idx) == 0) {
      return(list(status="error", message=paste("User not found:", username)))
    }
    
    # Handle duplicates: Default to the first match
    if (length(row_idx) > 1) {
      row_idx <- row_idx[1]
    }
    
    # --- D. Find Column ---
    target_col_clean <- trimws(column_name)
    
    if (!target_col_clean %in% names(profiles_db)) {
      return(list(
        status = "error", 
        message = paste("Column not found:", target_col_clean),
        valid_columns = names(profiles_db)
      ))
    }
    
    # --- E. Update Data ---
    # Capture old value for logging
    old_val <- profiles_db[row_idx, target_col_clean]
    
    # Assign new value
    profiles_db[row_idx, target_col_clean] <- new_value
    
    # --- F. Save to Server Disk ---
    write.csv(profiles_db, PROFILES_PATH, row.names = FALSE)
    
    # --- G. Return Success ---
    list(
      status = "success",
      message = "Entry updated successfully.",
      change_log = list(
        user = username,
        column = target_col_clean,
        old_value = old_val,
        new_value = new_value
      )
    )
    
  }, error = function(e) {
    # Print error to R console (Server logs)
    print(paste("Update API Error:", e$message))
    
    list(
      status = "error", 
      message = "An internal error occurred while processing the update.",
      details = e$message
    )
  })
}

# Step 2

#* Get all tickets from all users (Ops Team)
#* @get /get-all-tickets
#* @serializer json list(na='null')
function() {
  tryCatch({
    base_dir <- "/srv/shiny-server/Clients Usage Data/support_tickets"
    
    # Get all user folders
    if (!dir.exists(base_dir)) {
      return(list(success = TRUE, tickets = data.frame(), users = c()))
    }
    
    user_folders <- list.dirs(base_dir, full.names = FALSE, recursive = FALSE)
    user_folders <- user_folders[user_folders != "attachments"]
    
    all_tickets <- data.frame()
    
    for (user_folder in user_folders) {
      tickets_path <- file.path(base_dir, user_folder, paste0(user_folder, "_tickets.csv"))
      
      if (file.exists(tickets_path)) {
        user_tickets <- read.csv(tickets_path, stringsAsFactors = FALSE, check.names = FALSE)
        
        if (nrow(user_tickets) > 0) {
          user_tickets$rv_user <- user_folder
          all_tickets <- rbind(all_tickets, user_tickets)
        }
      }
    }
    
    # Sort by date descending
    if (nrow(all_tickets) > 0) {
      all_tickets <- all_tickets[order(as.Date(all_tickets$created_date, format = "%Y-%m-%d"), decreasing = TRUE), ]
    }
    
    return(list(success = TRUE, tickets = all_tickets, users = user_folders))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Update ticket (Ops Team) - status, category, resolution_notes
#* @post /update-ticket-ops
#* @serializer json list(na='null')
function(req) {
  tryCatch({
    body <- jsonlite::fromJSON(req$postBody)
    
    rv_user <- as.character(body$rv_user)
    ticket_id <- as.character(body$ticket_id)
    status <- as.character(body$status)
    category <- as.character(body$category)
    resolution_notes <- as.character(body$resolution_notes)
    
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
    
    # Update fields
    tickets$status[idx] <- status
    tickets$category[idx] <- category
    tickets$resolution_notes[idx] <- resolution_notes
    
    # Write back
    write.csv(tickets, tickets_path, row.names = FALSE, na = "")
    
    return(list(success = TRUE, message = "Ticket updated successfully"))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

#* Serve ticket image directly (for Ops Team)
#* @get /ticket-image-ops
#* @serializer contentType list(type="image/png")
function(rv_user, filename) {
  file_path <- file.path("/srv/shiny-server/Clients Usage Data/support_tickets", rv_user, "attachments", filename)
  
  if (!file.exists(file_path)) {
    stop("Image not found")
  }
  
  readBin(file_path, "raw", file.info(file_path)$size)
}