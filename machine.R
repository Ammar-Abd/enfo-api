# data_api.R
library(plumber)
library(dplyr)
library(lubridate)
library(jsonlite)


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
    mode <- "hour"
  }
  
  client_profiles <- read.csv("../Clients Usage Data/client_profiles.csv", check.names = F)
  client <- client_profiles[client_profiles$user == username, ]
  print(client)
  dates_list <- if (is.null(end)) start else seq.Date(as.Date(start), as.Date(end), by = "day")
  
  retData <- list()
  if (is.null(end)){
    if (mode == "minute")
      retData[["Date & Time"]] <- seq.POSIXt(as.POSIXct(start), as.POSIXct(start) + hours(24)-1, by = "min")
    else
      retData[["Date & Time"]] <- seq.POSIXt(as.POSIXct(start), as.POSIXct(start) + hours(23), by = "hour")
  } else {
    if (mode == "minute")
      retData[["Date & Time"]] <- seq.POSIXt(as.POSIXct(start), as.POSIXct(end) + hours(24)-1, by = "min")
    else
      retData[["Date & Time"]] <- seq.POSIXt(as.POSIXct(start), as.POSIXct(end) + hours(23), by = "hour")
  }
  
  retData <- data.frame("Date & Time" = retData$`Date & Time`, check.names = F)
  
  dir <- ''
  if (mode == 'minute') { 
    if (client$Meter_type == 'neubolt') { 
      dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/', client$user, "/")
    } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/minute-wise Data/', client$user, "/")
  } else {
    if (client$Meter_type == 'neubolt') { 
      dir <- paste0('../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/', client$user, "/")
    } else dir <- paste0('../Clients Usage Data/Egauge_Meter_Data/hour-wise Data/', client$user, "/")
  }
  
  available_data <- data.frame()
  files <- list.files(dir, full.names = T, pattern = ".*\\.csv$")
  file_dates <- sapply(basename(files), function(x) strsplit(x, "_")[[1]][2])
  filtered_file_names <- files[file_dates %in% dates_list]
  
  available_data <- do.call(rbind.fill, lapply(filtered_file_names, read.csv, check.names=F))
  available_data$`Date & Time` <- as.POSIXct(available_data$`Date & Time`, tz(retData$`Date & Time`))
  retData <- merge(retData, available_data, all.x = T)
  
  if (temp_mode == "day") {
    tz(retData$`Date & Time`) <- "UTC"
    retData$`Date & Time` <- as.Date(retData$`Date & Time`)
    postfix <- if (client$Meter_type == "egauge") " [kW]" else "_[kW]"
    
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
    retData <- retData[,unique(c(1,2,which(colnames(retData) %in% updateColNames$Old_name)))]
    matchedIndices <- match(updateColNames$Old_name, colnames(retData))
    toberemoved <- which(is.na(matchedIndices))
    if (length(toberemoved) > 0) updateColNames <- updateColNames[-c(toberemoved), ]
    matchedIndices <- matchedIndices[!is.na(matchedIndices)]
    colnames(retData)[matchedIndices] = updateColNames$New_name  
  }
  
  if (client$Meter_type == 'neubolt') {
    powerColumns = grep('[kW]', colnames(retData), value = T, fixed = T)
    retData[, powerColumns] = retData[, powerColumns]/1000
  } else {
    print(colnames(retData))
    retData[,2:ncol(retData)] = abs(retData[,2:ncol(retData)])
    colnames(retData) <- gsub(" \\[", "_\\[", colnames(retData))
  } 
  
  rds_path <- paste0("../EnergyMonitor2.0/", meter_id_first, "_buildingMap.rds")
  buildingMap <- readRDS(rds_path)
  
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
  
  curr_level <- 0
  usage_columns <- NULL
  while(TRUE) {
    usage_columns <- getNodesAtLevel(buildingMap, curr_level)
    if (any(is.na(usage_columns))) curr_level <- curr_level + 1
    else break
  }
  
  usage_columns <- paste0(usage_columns, "_[kW]")
  usage_columns <- intersect(usage_columns, colnames(retData))
  
  if (length(usage_columns) > 1)
    retData$Usage <- rowSums(retData[, usage_columns, drop = FALSE], na.rm = TRUE)
  else if (length(usage_columns) == 1)
    retData$Usage <- retData[[usage_columns]]
  
  retData <- retData %>% relocate(Usage, .after = 1)
  
  return(list(data = as.list(retData))) 
}
