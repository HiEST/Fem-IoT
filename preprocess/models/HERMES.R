source("../preprocess/preprocess.R", chdir = T)

read_hermes_tables <- function(file_path) {
    read_data <- function(file) {
        read.csv(file.path(file_path, file), header = T, sep = ",")
    }

    hermes <- list()
    hermes[["ihs_hermes_type"]] <- read_data(
                                    "ihs_hermes_ship_type_correspondence.csv")

    return(hermes)
}

add_hermes_type_column <- function(ihs, ihs_hermes) {
    ihs_hermes_l <- setNames(as.list(ihs_hermes$hermes_code),
                             ihs_hermes$ShiptypeLevel4)
    ihs$hermes_type <- apply_dict_list_to_vector(ihs$type, ihs_hermes_l)
    return(ihs)
}
