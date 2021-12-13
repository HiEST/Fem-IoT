library(magrittr)
library(plyr)
library(dplyr)
source("rosner_outliers.R")

max_inf_na <- function(vec) {
    if (is.null(vec)) { # | is.na(vec)) {
        res <- NA
    } else {
        res <- suppressWarnings(max(vec, na.rm = TRUE))
        if (is.infinite(res)) res <- NA
        # If max had no number (all vector is NA)
    }
    return(res)
}

le_zero_na <- function(val) {
    if (is.null(val) | is.na(val)) {
        val <- NA
    } else if (val <= 0) {
        val <- NA
    }
    val
    }

    set_le_zero_na <- function(d, v) {
    v       <- rlang::enquo(v)

    d %>%
        mutate(!! v := ifelse(!! v <= 0, NA, !!v))
}


calculate_mode <- function(x) {
    uniqx <- unique(na.omit(x))
    uniqx[which.max(tabulate(match(x, uniqx)))]
}


apply_dict_list_to_vector <-  function(v, d) {
    # WARNING: This assumes that the ship type is in the list!
    if (sum(!v %in% names(d)) >  0) {
        warning(paste0("apply_dict_list: There are keywords that are not present
                       in the dictionary. Keywords: \"",
                       v[which(!v %in% names(d))], "\"\n"))
    }
    r <- unlist(d[v])
    names(r) <- NULL
    return(r)
}


ship_tables_to_row <- function(ship, prop, m_gen, m_eng, boil, a_gen, a_eng,
                       cargo) {
    # Create the type to return
    d <- list()
    ######################## SCHEMA ############################################
    ## Ship Table
    d$imo <- NA
    d$name <- NA
    d$type <- NA
    d$me_stroke <- NA
    d$me_rpm <- NA
    d$prop_rpm <- NA
    d$loa <- NA
    d$lbp <- NA
    d$l <- NA
    d$b <- NA
    d$t <- NA
    d$los <- NA
    d$lwl <- NA
    d$ta <- NA
    d$tf <- NA
    d$bulbous_bow <- NA
    d$n_thru <- NA
    d$inst_pow_me <- NA
    d$n_inst_me <- NA
    d$single_pow_me <- NA
    d$eng_type <- NA
    d$eng_type <- NA
    d$inst_pow_ae <- NA
    d$design_speed <- NA
    d$n_ref_teu <- NA
    d$n_cabin <- NA
    d$serv_pow_me <- NA
    d$serv_single_pow_me <- NA
    d$ae_rpm <- NA
    d$n_screw <- NA
    d$n_rudd <- NA
    d$n_brac <- NA
    d$n_boss <- NA
    d$design_draft <- NA

    d$build_year <- NA
    d$fuel_type <- NA
    d$vapour_recovery <- NA
    d$mg_total_pow <- NA
    d$ag_total_pow <- NA

    ## Aux. eng. table
    d$ae_stroke <- NA


    ## Main generators
    d$mg_pow <- NA

    ## Auxiliary generators
    d$ag_pow <- NA

    ####################### SHIP TABLE #########################################
    if (is.data.frame(ship)) {
        d$imo   <- ship$LRIMOShipNo
        d$name  <- ship$ShipName

        # TODO: check attribute values (NAs and so on)
        # Determine ship type
        d$type <- ship$ShiptypeLevel4

        # Determine Main and Aux Engine stroke type and RPM
        # Notice that there may be more than one engine. Jalkanen 2012 assumes
        # that there are not mixed engine models on multi-engine setups.
        d$me_stroke <- as.character(ship$MainEngineStrokeType) %>%
                            { ifelse(. == "Y", NA, as.numeric(.)) } %>%
                            max_inf_na

        d$me_rpm <- ship$MainEngineRPM %>% le_zero_na
                            # This attribute is more reliable than getting the
                            # data from m_eng$RPM
                            #  as it covers properly hybrid engines
                            #   (0 cylinders gas engines are not considered).



        d$loa <- le_zero_na(ship$LengthOverallLOA)
        d$lbp <- le_zero_na(ship$LengthBetweenPerpendicularsLBP)


        d$l <- le_zero_na(ship$Length)		# Length in general
        d$b <- le_zero_na(ship$Breadth)		# Width in general
        d$t <- le_zero_na(ship$Draught)		# Draught in general

        d$los    <- le_zero_na(mean(c(d$loa, d$lbp)))	# Length over surface
        d$lwl    <- d$los				                # Length waterline
        d$ta     <- d$t		# Draft in Aft perpendicular
        d$tf     <- d$t		# Draft in Forward perpendicular

        # Does the ship have bulbous bow or not? Y is TRUE, else False
        # (missing or "N")
        d$bulbous_bow <- (ship$BulbousBow == "Y")

        d$vapour_recovery <- (ship$VapourRecoverySystem == "Y")

        d$build_year <- ship$YearOfBuild

        d$fuel_type <- ship$FuelType1Code

        d$n_thru <- ship$NumberofThrusters		# Number of thrusters

        #  TODO: Does this need to be a vector??
        # breadth should be ship$Breadth?
        # draught should be ship$Draught?

        # Installed power
        d$inst_pow_me   <- ship$TotalKilowattsofMainEngines %>% le_zero_na
        d$n_inst_me     <- ship$NumberofMainEngines %>% le_zero_na

        d$single_pow_me <- m_eng$PowerKW %>% max_inf_na %>% le_zero_na

        d$eng_type <- m_eng$EngineType %>% calculate_mode

        d$inst_pow_ae <- ship$TotalPowerOfAuxiliaryEngines %>% le_zero_na
        # TODO: Does service power 80% apply also to AE?

        # Design speed
        d$design_speed <- ship$Speed

        # TEU and Cabins
        d$n_ref_teu <- cargo$RefrigeratedTEU
        d$n_cabin   <- ship$NumberofCabins

        # N. Screws
        d$n_screw   <- ship$NumberOfPropulsionUnits %>% le_zero_na

        # Generators - This is horsepower, we need kW
        # d$mg_total_pow <- ship$TotalHorsepowerofMainGenerators
        # d$ag_total_pow <- ship$TotalHorsepowerofAuxiliaryGenerators
    }

    ####################### Auxiliary Engine Table #############################
    if (is.data.frame(a_eng)) {
        d$ae_stroke <- max_inf_na(as.numeric(as.character(a_eng$StrokeType)))
    }

    ####################### Main Generators Table ##############################
    if (is.data.frame(m_gen)) {
        d$mg_pow <- sum(m_gen$Number * m_gen$KW)
    }

    ####################### Aux. Generators Table ##############################
    if (is.data.frame(a_gen)) {
        d$ag_pow <- sum(a_gen$Number * a_gen$KWEach)
    }

    ####################### Propeller Table ####################################
    if (is.data.frame(prop)) {
        d$prop_rpm <- prop$RPMMaximum %>% max_inf_na %>% le_zero_na
        # We could use RPMService but it has a lot of missing values
    }


    ####################### ASSUMPTIONS ########################################
    # Jalkanen 2009: Service power (80% of the Inst. pow)
    d$serv_pow_me           <- d$inst_pow_me * 0.8
    d$serv_single_pow_me    <- d$single_pow_me * 0.8

    # TODO: Jalkanen assumption
    # *** This is not listed in the IHS data. We assume medium speed aux
    # engines with 514 rpm, IMO Tiers are applied depending on build year
    d$ae_rpm <- NA

    # Jalkanen assumption: The number of the following elements are close to the
    #		number of propellers.?
    d$n_rudd    <- d$n_screw		# Number of Rudders
    d$n_brac    <- d$n_screw		# Number of Brackets
    d$n_boss    <- d$n_screw		# Number of Bossings

    d$design_draft <- T	        # Assuming Design draft (Jalkanen).
                                # TODO: AIS draught is ballast draft?


    # Engine RPM. If 0 (Missing), we assume a 500RPM diesel engine
    #   (Jalkanen 2009)

    # Fn # Froude Number. CALC
    # Dp # Propeller diameter. CALC
    # Cb # Block Coefficient . CALC
    return(as.data.frame(d))
}

# imputate_by_group_mean <- function(d, group, v) {
#     group   <- rlang::enquo(group)
#     v       <- rlang::enquo(v)
# 
#     d %>%
#         group_by(!! group) %>%
#         mutate(!! v := ifelse(is.na(!! v), mean(!! v, na.rm=TRUE), !! v)) %>%
#         ungroup
# }

fill_class_na <- function(d, v, cl, classes, fill_val) {
    v   <- rlang::enquo(v)
    cl  <- rlang::enquo(cl)
    d %>% mutate(!! v :=
           ifelse(is.na(!!v) & (!! cl %in% classes), fill_val, !!v))
    }

    imputate_by_group_mean <- function(d, group, columns) {
    group   <- rlang::enquo(group)

    d %>%
        group_by(!! group) %>%
        mutate_at(rlang::enquos(columns),
                  ~ifelse(is.na(.), mean(., na.rm = TRUE), .)) %>%
        ungroup
}


read_ihs_tables <- function(file_path="~/data/IHS") {
    # Read IHS tables
    read_data <- function(file) {
        read.csv(file.path(file_path, file), header = T, sep = "\t")
    }

    dfs <- list()
    dfs[["ship"]]       <- read_data("ShipData.csv")
    dfs[["propell"]]    <- read_data("tblPropellers.csv")
    dfs[["m_gen"]]      <- read_data("tblMainGenerators.csv")
    dfs[["m_eng"]]      <- read_data("tblMainEngines.csv")
    dfs[["boilers"]]    <- read_data("tblBoilers.csv")
    dfs[["a_gen"]]      <- read_data("tblAuxiliaryGenerators.csv")
    dfs[["a_eng"]]      <- read_data("tblAuxEngines.csv")
    dfs[["cargo"]]      <- read_data("tblCargoData.csv")

    return(dfs)
}

data_or_na <- function(df) {
    if (nrow(df) <= 0) df <- NA
    return(df)
}

# Apply preprocess
preprocess_one_ship <- function(imo, dfs) {
    ship    <- dfs[["ship"]]      %>% filter(LRIMOShipNo == imo) %>% data_or_na
    prop    <- dfs[["propell"]]   %>% filter(LRNO == imo)        %>% data_or_na
    m_gen   <- dfs[["m_gen"]]     %>% filter(LRNO == imo)        %>% data_or_na
    m_eng   <- dfs[["m_eng"]]     %>% filter(LRNO == imo)        %>% data_or_na
    boil    <- dfs[["boilers"]]   %>% filter(LRNO == imo)        %>% data_or_na
    a_gen   <- dfs[["a_gen"]]     %>% filter(LRNO == imo)        %>% data_or_na
    a_eng   <- dfs[["a_eng"]]     %>% filter(LRNO == imo)        %>% data_or_na
    cargo   <- dfs[["cargo"]]     %>% filter(LRIMOShipNo == imo) %>% data_or_na

    ship_tables_to_row(ship, prop, m_gen, m_eng, boil, a_gen, a_eng, cargo)
}


join_tables_ships <-  function(dfs) {
    imos <- dfs[["ship"]]$LRIMOShipNo
    parallel <- foreach::getDoParWorkers() > 1
    ldply(imos, preprocess_one_ship, dfs = dfs, .progress = "text",
            .parallel = parallel)
}

rm_chg_bad_data <- function(d) {
    # Remove or change data found by data exploration
    rm_rosner_bad_data(d)
}
