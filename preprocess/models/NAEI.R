# SFOC tables

# Diesel depends on engine age and engine type


#' Assign SFOC to each ship using NAEI tables 11 and 12 along with static LNG
#' value. It uses engine type, fuel type and build year of the ship.
#' *It requieres features calculated with trozzi.r engine code, i.e.
#' trozzi_fuel_type and eng_type2.*
#'
#' (NAEI Tab. 11)
#'
#' | Engine age  | SSD | MSD | HSD |
#' | ----------- | --- | --- | --- |
#' | Before 1983 | 205 | 215 | 225 |
#' | 1984-2000   | 185 | 195 | 205 |
#' | post 2001   | 175 | 185 | 195 |
#'
#' (NAEI Tab. 12) Gas turbine, steam boilers, auxiliary engines
#'
#' | Engine type | HFO | MDO |
#' | ----------- | --- | --- |
#' | Gas Turbine | 305 | 300 |
#' | Steam boiler| 305 | 300 |
#' | Aux. Engine | 225 | 255 |
#'
#' LNG is assumed to have a SFOC of 166g/kWh
#'
#' @return Input tbl with a column *naei_sfoc_me* and *naei_sfoc_ae* containing
#' the assignation.
#' @export
#'
#' @examples
#' [TODO:example]

naei_sfoc <- function(df) {
    df %>%
        rowwise %>%
        mutate(
            naei_sfoc_me = naei_sfoc_me(eng_type2, trozzi_fuel_type,
                                         build_year),
            naei_sfoc_ae = naei_sfoc_ae(trozzi_fuel_type)
        ) %>%
        ungroup
}


#' Apply NAEI's Auxiliary Engine table 12 criteria to assign SFOC
#'
#' @param fuel Fuel type
#'
#' @return Auxiliary Engine SFOC for the given fuel type
#'
#' @examples
#' [TODO:example]
naei_sfoc_ae <- function(fuel) {
    if (is.na(fuel)) {
        sfoc <- NA
    }
    else if (fuel == "MDO") {
        sfoc <- 255
    } else if (fuel == "LNG") {
        sfoc <- 166
    } else { # We assume that HFO and BFO have the same SFOC
        sfoc <- 225
    }
    return(sfoc)
}

#' Apply NAEI's Main Engine tables 11 and 12 criteria to assign SFOC
#'
#' @param eng Engine type (Following Trozzi's table)
#' @param fuel Fuel type
#' @param year Ship build year
#'
#' @return Main Engine SFOC for the given engine, fuel and build year
#'
#' @examples
#' [TODO:example]
naei_sfoc_me <- function(eng, fuel, year) {
    sfoc <- NA
    if (!is.na(fuel) && !is.na(eng)) {
        if (fuel == "LNG") {
            sfoc <- 166
        } else {
            if (eng == "GT" || eng == "ST") {
                # Gas or Steam Turbine
                if (fuel == "MDO") {
                    sfoc <- 300
                } else {
                    sfoc <- 305
                }
            } else {
                # Diesel
                sfoc <- 0
                # Set post 2001 as base
                if (eng == "SSD") {
                    sfoc <- 175
                } else if (eng == "MSD") {
                    sfoc <- 185
                } else {
                    sfoc <- 195
                }
                # Older equipment uses 30 g/kWh more than post 2001.
                if (year <= 2000) sfoc <- sfoc + 10
                if (year <= 1983) sfoc <- sfoc + 20
            }
        }
    }
    return(sfoc)
}
