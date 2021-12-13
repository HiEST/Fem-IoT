# Remove incorrect data found with Rosner filter
# Code: IHSAIS_fixes and Data: doc/possible_outliers.ods

library(plyr)
library(purrr)

rosner_err_data <- list(
    "9263954" = list(list(v = "inst_pow_ae")),
    "9512587" = list(
                     list(v = "inst_pow_me"),
                     list(v = "inst_pow_ae")),
    "9262211" = list(list(v = "me_rpm")),
    "9127382" = list(list(v = "me_rpm")),
    "9391177" = list(list(v = "me_rpm")),
    "9192399" = list(list(v = "me_rpm")),
    "9320087" = list(list(v = "inst_pow_ae")),
    "9320099" = list(list(v = "inst_pow_ae")),
    "9203162" = list(list(v = "type", val = "Fishing Support Vessel")),
    "9439723" = list(list(v = "t")),
    "1008102" = list(list(v = "inst_pow_me")),
    "7222786" = list(list(v = "me_rpm")),
    "8982307" = list(list(v = "inst_pow_me")),
    "9053452" = list(list(v = "me_rpm")),
    "9661792" = list(list(v = "inst_pow_me"))
)

rm_rosner_bad_data <- function(d, key = "imo", err_data = rosner_err_data) {
    # Iterate over rows
    for (i in seq_len(nrow(d))) {
        # Get the key
        key_id <- unlist(d[i, key])
        for (v in err_data[[as.character(key_id)]]) {
            if (!is.null(v)) {
                if (is.null(v$val)) v$val <- NA
                d[i, v$v] <- v$val
            }
        }
    }
    return(d)
}
