set_steam_def_vals <- . %>%
    replace_na(list(
        me_rpm = 514,
        ae_rpm = 514,
        inst_pow_me = 2380,
        design_speed = 45 # The max speed found in AIS dataset
    ))

calculate_steam_variables <- function(ihs) {
    # Base SFOC
    # From the paper: For simplicity, it has been assumed that engine load and
    # SFOC-dependence from Eqs. (11) and (12) applies to all engines. For
    # turbine machinery, SFOCbase of 260 gkWh−1 is used. Auxiliary engine
    # SFOCbase was set to 220 gkWh−1 and the same load dependency was applied.

    # We're setting this in the model folder
    ihs$steam_sfocbase_me <- 260
    ihs$steam_sfocbase_ae <- 220
    return(ihs)
}
