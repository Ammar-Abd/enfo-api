#### Libraries ####
library(tidyr)
library(dplyr)
library(rvest)
library(lubridate)
library(fs)
library(webshot)
library(stringr)

#### Customer ID Input, Defining URL and Extracting HTML Content ####

# Set local time
Sys.setlocale("LC_TIME", "en_US.UTF-8")

# Enter customer ID
customerID <- readline(prompt = "Enter Customer ID: ")

disco <-  readline(prompt = "Enter DISCO: ")
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