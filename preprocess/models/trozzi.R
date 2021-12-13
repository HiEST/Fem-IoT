source("../preprocess/preprocess.R", chdir = T)

read_trozzi_tables <- function(file_path) {
    read_data <- function(file) {
        read.csv(file.path(file_path, file), header = T, sep = ",")
    }

    trozzi <- list()
    trozzi[["eng_fuel"]]        <- read_data("trozzi2010_table7.csv")
    trozzi[["sfoc_ef"]]         <- read_data("trozzi2010_table4.csv")
    trozzi[["ihs_trozzi_type"]] <- read_data(
                                    "ihs_trozzi_ship_type_correspondence.csv")

    return(trozzi)
}

diesel_engine_type <- function(rpm) {
    # From EMEP/EEA 2016
    ifelse(is.na(rpm), "xSD",
        ifelse(rpm > 900, "HSD",
               ifelse(rpm > 300, "MSD", "SSD")
        )
    )
}


expand_engine_types <- function(ihs) {
    ihs$eng_type2 <- as.character(ihs$eng_type)

    # Resolve Oil engine type
    oil_idx <- ihs$eng_type == "Oil"
    ihs[oil_idx, "eng_type2"] <- diesel_engine_type(ihs[oil_idx, ]$me_rpm)

    ihs$eng_type2 <- as.factor(ihs$eng_type2)

    # Gas Turbine as GT
    levels(ihs$eng_type2)[levels(ihs$eng_type2) == "Gas"] <- "GT"

    # Steam Turbine as ST
    levels(ihs$eng_type2)[levels(ihs$eng_type2) == "Turbine"] <- "ST"

    return(ihs)
 }

add_trozzi_type_column <- function(ihs, ihs_trozzi) {
    ihs_trozzi_l <- setNames(as.list(ihs_trozzi$trozzi_2010_types),
                             ihs_trozzi$ShiptypeLevel4)
    ihs$trozzi_type <- apply_dict_list_to_vector(ihs$type, ihs_trozzi_l)
    return(ihs)
}

impute_one_fuel_row <- function(fuel_type, type, eng_type, eng_fuel) {
    # Impute from table - Get most probable fuel from table
    if (is.na(fuel_type)) {
        res <- eng_fuel %>%
           filter(ship_type == type, engine_type == eng_type)
        fuel_type <- as.character(res[which.max(res$probability), ]$fuel_type)
        if (length(fuel_type) == 0) {
            msg <- paste(
                "Couldn't find fuel_type in the table for ship type", type,
                ", engine type", eng_type,
                ". Returning NA."
            )
            warning(msg)
            print(msg)
            fuel_type <- NA
        }
    }
    # Use the original value
    else {
        fuel_type <- as.character(fuel_type)
    }
    return(as.character(fuel_type))
}

impute_fuel_na <-  function(ihs, eng_fuel) {
    ihs <- ihs %>%
        rowwise %>%
        mutate(trozzi_fuel_type =
               impute_one_fuel_row(trozzi_fuel_type, trozzi_type,
                                    eng_type2, eng_fuel = eng_fuel
                                    )
        ) %>%
        ungroup
    ihs$trozzi_fuel_type <-  as.factor(ihs$trozzi_fuel_type)
    return(ihs)
}


# | FuelType1Code | FuelType1First  | # Ships | Our variable |
# |---------------|-----------------|---------|--------------|
# | DF            | Distillate Fuel | 3336    | MDO/MGO      |
# | GB            | Gas Boil Off    | 1       | LNG          |
# | LG            | LNG             | 17      | LNG          |
# | RF            | Residual Fuel   | 20      | HFO          |
# | YY            | Type Not Known  | 444     | NA           |

fuel_list <- list(
                  DF = "MDO",
                  GB = "LNG",
                  LG = "LNG",
                  RF = "HFO",
                  YY = NA
)

rename_fuel <- function(ihs) {
    fu <- sapply(ihs$fuel_type, function(x) { fuel_list[[x]]})
    fu[is.null(fu)] <- NA
    ihs$trozzi_fuel_type <- as.factor(fu)
    return(ihs)
}
