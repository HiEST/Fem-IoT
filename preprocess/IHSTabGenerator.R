#!/usr/bin/env Rscript
library(magrittr)
library(tidyr)
source("preprocess/preprocess.R", chdir = T)
source("models/STEAM.R")
source("models/STEAM2.R", chdir = T)
source("models/trozzi.R", chdir = T)
source("models/HERMES.R", chdir = T)
source("models/NAEI.R", chdir = T)
source("tools/csv_to_psql.R", chdir = T)

# Compute in parallel?

parallel <- F
export_sql <- F
imputation <- "none"

if (parallel) {
    cl <- parallel::makeCluster(4L, type = "FORK")
    doParallel::registerDoParallel(cl)
}

## IHS TABLE GENERATION
ihs_tables          <- read_ihs_tables(file_path = "~/data/IHS")
trozzi_tables       <- read_trozzi_tables(file_path = "emis_tables/")
hermes_tables       <- read_hermes_tables(file_path = "emis_tables/")
merged_table_path   <- "~/data/IHS/raw_ihs_data.csv"


# Join tables and calculate basic assumptions
# If the file already exists, it is not calculated
if (!file.exists(merged_table_path)) {
    # Note: It may take a while
    ihs_data_base <- ihs_tables  %>% join_tables_ships

    write.table(ihs_data_base, file = "merged_table_path", sep = "\t",
                row.names = F)
}


# Read the calculated basic assumptions and joined tables
ihs_data_base <- read.table(file = "~/data/IHS/raw_ihs_data.csv", sep = "\t",
                            header = T)



print("Generating features")
# Clean variables and compute new ones

ihs_data <- ihs_data_base %>%
    # Basic cleaning
    set_le_zero_na(design_speed) %>%
    set_le_zero_na(inst_pow_me) %>%
    set_le_zero_na(inst_pow_ae) %>%
    # Note: The fuel is saved in another variable
    rename_fuel %>%
    # Remove/Change erroneous data found by data exploration
    rm_chg_bad_data


    # Base imputation cleaning
if (imputation == "basic") {
    ihs_data <- ihs_data %>%
        imputate_by_group_mean(type, c(me_rpm, inst_pow_me, design_speed)) %>%
        # Default fillers
        fill_class_na(design_speed, type,
                      c("Supply Tender", "Naval/Naval Auxiliary"), 20) %>%
        set_steam_def_vals
}

# Feature derivation
ihs_data <- ihs_data %>%
    # Fuel processing
    add_hermes_type_column(hermes_tables$ihs_hermes_type) %>%
    add_trozzi_type_column(trozzi_tables$ihs_trozzi_type) %>%
    expand_engine_types %>%
    impute_fuel_na(trozzi_tables$eng_fuel) %>%
    # New feature derivation
    ## NAEI
    naei_sfoc %>%
    ## STEAM
    calculate_steam_variables %>%
    calc_steam2_charact

print("Writting to CSV")
# Write to CSV
write.table(ihs_data,
            file = glue::glue(
                    "~/data/IHS/{Sys.Date()}_ihs_imp_{imputation}_data.csv"),
            sep = ",", row.names = F)

# Write to PSQL
if (export_sql) {
    print("Writting to PostgreSQL")
    ihs_data %>%
        store_in_db(ihs,
                    glue::glue("{Sys.Date()}_ihs_imp_{imputation}"),
                    overwrite = T)
}

print("Done!")
