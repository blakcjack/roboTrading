suppressPackageStartupMessages({
    library(dplyr) # for data manipulation
    library(httr) # to communicate with server
    library(jsonlite) # to parse various JSON data
    library(lubridate) # to work with date
})
#' @name getter_historical_indodax
#' @author Suberlin Sinaga
#' @description Fetch Historical Price Data for Indodax in Particular
#' @details {
#' Indodax provides public API in order to fetch historical price data.
#' This function accesses that public API to fetch the historical price data.
#' The getter will only accept a coin for each data hit
#' }
#' @param date_ranges Character vector of time span of the desired data.
#' @param pair String that consist of the coin name and currency pair. The coin name and the currency must be fully capitalized.
#' @param resolution The interval of the data that will be fetched, either 1,5,15,30,60,240 mins or D for daily data.
#' @param url The endpoint to fetch the data.
#' @param sleep  Since the URL is a public API, it is limited in the term of hits. Use sleep to limit the number of hits. 
#' @return Data Frame or Simple Error.
getter_historical_indodax <- function(date_ranges, pair, resolution, url, sleep) {
    # splitting date from the date_ranges vector and then change them to numeric form
    date_1 <- as.numeric(as.POSIXct(date_ranges[1]))
    date_2 <- as.numeric(as.POSIXct(date_ranges[2]))
    # make sure that the pair is alway upper case
    pair <- toupper(pair)
    # fetch the data from the server using tryCatch so if the execution
    # is error, we can detect them. This is not included when the error
    # comes from the server note such as invalid symbol error.
    df_temp <- tryCatch(
        GET(paste0(url, pair, "&resolution=", resolution, "&from=", date_1,
                   "&to=", date_2), error = function(e) e)
    )
    # ensure if the GET() expression error executed, then we skip them all and display the error
    if (!inherits(df_temp, "error")) {
        data <- df_temp |> 
            content(type = "text", encoding = "UTF-8") |> 
            fromJSON()

        if (length(data) <= 1) {
            return(NULL)
        }
        
        data <- data |> 
            bind_rows() |> 
            arrange(.data[['t']]) |>
            mutate(s = pair)
    } else {
        return(NULL)
    }
    Sys.sleep(sleep)
    
    return(data)
}

#' @name pooler_historical_indodax
#' @aliases pooler_historical_indodax
#' @author Suberlin Sinaga
#' @description Pool Historical Price Data for Indodax in Particular
#' @details {
#' We use \link{getter_historical_indodax} to fetch historical price data
#' in specific range of date. We use this function to pool, clean
#' and then store those data into our database.
#' }
#' @param date_ranges Character vector of time span of the desired data.
#' @param pair String that consist of the coin name and currency pair. The coin name and the currency must be fully capitalized.
#' @param resolution The interval of the data that will be fetched, either 1,5,15,30,60,240 mins or D for daily data.
#' @param url The endpoint to fetch the data.
#' @param sleep  Since the URL is a public API, it is limited in the term of hits Use sleep to limit the number of hits.
#' @param type Type used to differ the treatment when we fix the data fetch or when we initially get the data.
#' @param direct FALSE by default. We only have to set it to TRUE only when we are looking for the missing data.
#' @return Data Frame or Simple Error.
pooler_historical_indodax <- function(date_ranges, pair,
                                      resolution = c(1, 5, 30, 15, 60, 240, "D"),
                                      url, sleep = 4,
                                      type = c("initial", "getter"),
                                      direct = FALSE) {
    type <- match.arg(type, c("initial", "getter"))
    resolution <- match.arg(as.character(resolution), c(1, 5, 30, 15, 60, 240, "D"))
    if (missing(url)) {
        url <- "https://indodax.com/tradingview/history?symbol="
    }
    
    if (direct) {
        return(getter_historical_indodax(date_ranges, pair, resolution, url, sleep))
    } else {
        cat(paste0("Collecting data for ", pair, "...\n"))
        date_1 <- as_date(date_ranges[[1]])
        date_2 <- as_date(date_ranges[[2]])
        date_ranges <- seq(floor_date(date_1, "month"), floor_date(date_2, "month"), "months")
        
        # ensure if there is more than 1 month data to be fetched.
        if (length(date_ranges) > 1) {
            # to ensure that data will range from date_1 to date_2 as specified,
            # we need to reassign those date into first and last data
            date_ranges[1] <- date_1
            date_ranges[length(date_ranges)] <- date_2
            
            # for easy capture of the data, we will change the date into dataframe
            # so we can use looping function such as apply or map to generate the data
            date_ranges <- data.frame(date_1 = date_ranges) |> 
                mutate(date_2 = ceiling_date(date_1, "months") - days(1))
            
            final_data <- apply(date_ranges, 1, getter_historical_indodax,
                                pair = pair, resolution = resolution, url = url,
                                sleep = sleep) |>
                bind_rows() |> 
                arrange(.data[['t']])
        } else {
            final_data <- getter_historical_indodax(c(date_1, date_2), pair,
                                                    resolution, url, sleep) |> 
                bind_rows() |> 
                arrange(.data[['t']])
        }
    }
    
    cat("Finish collecting data for ", pair, "...\n")
    return(final_data)
}

#' @name fix_historical_indodax
#' @author Suberlin Sinaga
#' @description Data Completeness Validator
#' @details {
#' When fetching data from Indodax server, very often we find out that not all
#' the data fetched. This causes hole in the data where certain time frame has
#' missing data. However, if we manually fetch them, we will find them available.
#' Hence, we need this function to get all them out.
#' }
fix_historical_indodax <- function(pair, resolution, interval, data,
                                   type = "getter", direct = TRUE, ...) {
    data_manipulated <- data
    cat(paste0("Collecting missing data for ", pair, "...\n"))
    if (missing(data)) {
        stop("Please attach the data.")
    } else {
        cat(paste0("Total row of the initial data: ", nrow(data), "\n"))
    }
    
    if (!all(c("t", "o", "l", "h", "c", "v") %in% names(data))) {
        stop("Please use tolhcv data frame as input.")
    }
    
    incomplete_seq <- which(diff(data$t) != interval)
    cat(paste0("There are ", length(incomplete_seq), " incomplete data.\n"))
    
    if (length(incomplete_seq) > 0) {
        for (i in incomplete_seq) {
            date_range <- as.POSIXct(c(data$t[i], data$t[i + 1]), origin = "1970-01-01")
            cat("Processing data index ", i, " with time from ", date_range[1], " to ", date_range[2], "\n")
            
            fix_data <- pooler_historical_indodax(date_range, pair, resolution, type = type, direct = direct, ...)
            if (inherits(fix_data, "data.frame")) {
                if (nrow(fix_data) > 0 && all(c("t", "o", "l", "h", "c", "v") %in% names(data_manipulated))) {
                    data_manipulated <- data_manipulated |> 
                        rbind(fix_data) |> 
                        distinct(.data[["t"]], .keep_all = TRUE)
                }
            } else {
                next
            }
        }
    }
    data_manipulated <- data_manipulated |> 
        arrange(.data[['t']]) |> 
        mutate(timeframe = c(interval, diff(.data[['t']])),
               interval = interval)
    
    cat("Finish fixing data for ", pair, "! Quiting...\n", sep = "")
    if (nrow(data) != nrow(data_manipulated)) {
        cat("New data has :", nrow(data_manipulated), " rows.\n")
    } else {
        cat("No new insertion.")
    }
    return(data_manipulated)
}
