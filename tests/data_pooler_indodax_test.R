source("scripts/helper/data_pooler_indodax.R")
suppressPackageStartupMessages({
    library(assertthat)
})

date_ranges <- c("2021-01-01", "2021-04-30")
pair <- "ETH_IDR"
resolution <- c(1, 5, 15, 60, 240, "D")
sleep <- 4

for (res in resolution) {
    cat("Testing logic for resolution of ", res, "\n", sep = "")
    df_historical_ <- pooler_historical_indodax(date_ranges, pair, res, sleep = sleep)
    
    assert_that(inherits(df_historical_, "data.frame"), msg = "Data is not a data frame.")
    
    df_fix <- fix_historical_indodax(pair, res, if_else(res == "D", 24*60*60, as.numeric(res) * 60), sleep = sleep, data = df_historical_)
    
    assert_that(nrow(df_historical_) != nrow(df_fix), msg = "No repairment created")
}
