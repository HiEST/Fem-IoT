source("STEAM2/hollenbach.R")


#' Propeller diameter (Eq. 5)
#'
#'	d = 16.2 * (P_{s}^0.2/N^0.6)
#'	Where:
#'		- P_{s}: Service power of the main engine, 80% of Max. Cont. Rating (kW)
#' 		- N: Propeller angular velocity (rpm)
#'	Notes:
#'		This formula is applied only to single-propeller ships. If multi, use
#'		appendix A
#'		If number of propellers unknown, assume 1.
#'
#' @param serv_pow Ship service power
#' @param propel_rpm Propeller RPM
#'
#' @return Propeller diameter
#' @export
calc_propeller_diameter <- function(serv_pow, propel_rpm) {
    16.2 * (serv_pow^0.2 / propel_rpm^0.6)
}

#' Quasi Propulsive Constant
#'
#' Used to describe the effectiveness of converting the main engine power to
#' actual propelling power. Watson (1998)
#' eta_{qpc} = 0.84 - (N * sqrt(LBP) / 10000)
#' Where:
#' 	- N: rpm of the propeller.
#'	- LBP: length between perpendiculars (m)
#'
#' @param prop_rpm Propeller RPM
#' @param lbp Lenght Between Perpendiculars (in meters)
#'
#' @return Quasi propulsive constant
#' @export
calc_quasi_propulsive_cte <- function(prop_rpm, lbp) {
    0.84 - (prop_rpm * sqrt(lbp) / 10000)
}

#' STEAM2 characteristics
#'
#' Calculate STEAM2 requiered characteristics that are based on the ship
#' properties.
#'
#' Assumption: Water density calculated as the average for the mediterranean sea
#' using "kinematicViscosity.R"
#'
#' @param sp Ship parameters
#' @param water_density waterDensity in kg/m^3. Mediterranean sea density
#' assumed 1038 kg/m^3 (calculated using "kinematicViscosity.R")
#'
#' @return Input dataframe with the new characteristics
#' @export
calc_steam2_charact <- function(ihs, water_density = 1038) {
    # Calculate waterline. Assumption: waterline is the average of LOA and LBP

    # Note: wet_surface_coef is defined in hollenbach.R

    # TODO: Change the parameters
    # TODO: wet_surface & res_coef constants for later calculations
    ihs <- ihs %>%
        mutate(waterline = rowMeans(cbind(loa, lbp))) %>%
        rowwise %>%
        mutate(dp = calc_propeller_diameter(serv_pow_me, prop_rpm)) %>%
        mutate(
            wet_surf_a3 = wet_surface_a3(l, b, t, bulbous_bow, design_draft,
                                         n_screw),
            wet_surf_k  = wet_surface_k(l, b, t, los, lwl, ta, tf, dp, n_rudd,
                                        n_brac, n_boss, n_screw, bulbous_bow,
                                        design_draft),
            cr_nofn     = calc_cr_nofn(l, b, t, los, lwl, ta, tf, dp, n_rudd,
                                       n_brac, n_boss, n_thru, n_screw,
                                       design_draft),
            qpc         = calc_quasi_propulsive_cte(prop_rpm, lbp)
        ) %>%
        ungroup

    return(ihs)
}

