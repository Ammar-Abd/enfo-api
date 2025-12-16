library(dplyr)
library(lubridate)
library(zoo)

getMaxLevels <- function(tree) {
  max_levels <- 0
  
  # Recursive function to traverse the nested list
  traverse <- function(node, depth) {
    if (is.list(node)) {
      for (child_node in node) {
        traverse(child_node, depth + 1)
      }
    } else {
      max_levels <- max(max_levels, depth)
    }
  }
  
  traverse(tree, 0)
  
  return(max_levels)
}
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

total_usage <- function(user, date_v) {
  clients <- read.csv("../Clients Usage Data/client_profiles.csv")
  
  client <- clients[clients$user == user, ]
  client_id <- client$Meter_id

  file_path <- paste0("../Clients Usage Data/Neubolt Meter Data/data/hour-wise Data/", user, "/", client_id, "_", date_v, "_hour-wise.csv")
  yday_data <- read.csv(file_path, check.names = F)
  
  buildingMap <- readRDS(paste0("../EnergyMonitor2.0/", client_id, "_buildingMap.rds"))
  
  curr_level = 0
  usage_columns <- NULL
  while(T) {
    usage_columns <- getNodesAtLevel(buildingMap, curr_level)
    if (any(is.na(usage_columns))) curr_level = curr_level + 1
    else break
  }
  usage_columns <- paste0(usage_columns, "_[kW]")
  
  usage <- yday_data %>% select(any_of(usage_columns)) %>% rowSums(na.rm = TRUE)
  
  usage <- na.locf(usage, maxgap = 1)
  usage <- na.locf(usage, fromLast = T, maxgap = 1)
  
  usage <- usage[!is.na(usage)]
  
  return_message = ""
  if (length(usage) == 24) { 
    usage <- round(sum(usage, na.rm = T)/1000, 2)
    return_message <- paste0("Your yesterday's energy consumption: ", usage, " units")
  } else { 
    usage <- round(mean(usage, na.rm = T) * 24/1000, 2)
    return_message <- paste0("Your yesterday's estimated consumption: ", usage, " units")
  }
  return(return_message)
}
  
  
  
  
  
  
  
  
  
  
  