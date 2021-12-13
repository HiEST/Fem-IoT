# Hollenbach Method

# Method for calculating the wet surface and the residual resistance coefficient
# of a ship. All information extracted from Ship Design for Efficiency and
# Economy Schneekluth, H. and Bertram, V. (1998) if not explicited.


# Wet surface

# Notice that if it's single screw, the number of bossings, rudders and brackets
# is not taken into account (Because they are one).
wet_surface_coef <- data.frame(
                         single_s_design = c(-0.6837, 0.2771, 0.6542, 0.6422,
                                         0.0075, 0.0275, -0.0045, -0.4798,
                                         0.0376, 0, 0, 0),
                         single_s_ballast = c(-0.8037, 0.2726, 0.7133, 0.6699,
                                          0.0243, 0.0265, -0.0061, 0.2349,
                                          0.0131, 0, 0, 0),
                         twin_s_bulbous = c(-0.4319, 0.1685, 0.5637, 0.5891,
                                        0.0033, 0.0134, -0.0006, -2.7932,
                                        0.0072, 0.0131, -0.0030, 0.0061),
                         twin_s_nobulb = c(-0.0887, 0.0000, 0.5192, 0.5839,
                                       -0.0130, 0.0050, -0.0007, -0.9486,
                                       0.0506, 0.0076, -0.0036, 0.0049)
                    )


estimate_wet_surface <- function(l, b, t, los, lwl, cb, ta, tf, dp, n_rudd,
                                 n_brac, n_boss, n_screw, bulbous_bow,
                                 design_draft) {
    # Jalkanen 2012 (using Hollenbach method):
    #   Lwl and Los = mean(LOA,LBP)
    #   L = Length in general. In this case Lpp
    #   B = Width in general
    #   T = Draugth in general
    #   Dp = Propeller diameter
    #   Ta = Draft at aft perpendicular
    #   Tf = Draft at forward perpendicular
    #   Cb = Block coefficient


    # Select coefficients
    if (n_screw < 2) {
        if (design_draft) {
            coef <- wet_surface_coef$single_s_design
        } else { # Ballast draft
            coef <- wet_surface_coef$single_s_ballast
        }
    } else {
        if (bulbous_bow) {
            coef <- wet_surface_coef$twin_s_bulbous
        } else {
            coef <- wet_surface_coef$twin_s_nobulb
        }
    }



    # Calculate k constant
    # k <- a0 + a1 * Los/Lwl + a2*Lwl/L + a3*Cb + a4*B/T +
    #   a6*L/T + a7*(Ta-Tf)/L + a8*Dp/T +
    #   kRudd*NRudd + kBrac*NBrac + kBoss*NBoss  :)

    k <- coef[1] + coef[2] * los / lwl + coef[3] * lwl / l + coef[4] * cb +
        coef[5] * b / t + coef[7] * l / t + coef[8] * (ta - tf) / l +
        coef[9] * dp / t + coef[10] * n_rudd + coef[11] * n_brac +
        coef[12] * n_boss


    # Total wetted surface
    return(k * l * (b + 2 * t))
}

wet_surface_k <- function(l, b, t, los, lwl, ta, tf, dp, n_rudd, n_brac,
                          n_boss, n_screw, bulbous_bow, design_draft) {
    # Jalkanen 2012 (using Hollenbach method):
    #   Lwl and Los = mean(LOA,LBP)
    #   L = Length in general. In this case Lpp
    #   B = Width in general
    #   T = Draugth in general
    #   Dp = Propeller diameter
    #   Ta = Draft at aft perpendicular
    #   Tf = Draft at forward perpendicular
    #   Cb = Block coefficient

    # Select coefficients
    if (n_screw < 2) {
        if (design_draft) {
            coef <- wet_surface_coef$single_s_design
        } else { # Ballast draft
            coef <- wet_surface_coef$single_s_ballast
        }
    } else {
        if (bulbous_bow) {
            coef <- wet_surface_coef$twin_s_bulbous
        } else {
            coef <- wet_surface_coef$twin_s_nobulb
        }
    }

    # Calculate k constant without block coefficient
    # k_nocb <- a0 + a1 * Los/Lwl + a2*Lwl/L + a4*B/T +
    #   a6*L/T + a7*(Ta-Tf)/L + a8*Dp/T +
    #   kRudd*NRudd + kBrac*NBrac + kBoss*NBoss  :)

    k_nocb <- coef[1] + coef[2] * los / lwl + coef[3] * lwl / l +
        coef[5] * b / t + coef[7] * l / t + coef[8] * (ta - tf) / l +
        coef[9] * dp / t + coef[10] * n_rudd + coef[11] * n_brac +
        coef[12] * n_boss

    c_area <- l * (b + 2 * t)
    k_area <- k_nocb * c_area

    # Total wetted surface
    return(k_area)
}

wet_surface_a3 <- function(l, b, t, bulbous_bow, design_draft, n_screw) {
    # Jalkanen 2012 (using Hollenbach method):
    #   Lwl and Los = mean(LOA,LBP)
    #   L = Length in general. In this case Lpp
    #   B = Width in general
    #   T = Draugth in general
    #   Dp = Propeller diameter
    #   Ta = Draft at aft perpendicular
    #   Tf = Draft at forward perpendicular
    #   Cb = Block coefficient


    # Select coefficients
    if (n_screw < 2) {
        if (design_draft) {
            coef <- wet_surface_coef$single_s_design
        } else { # Ballast draft
            coef <- wet_surface_coef$single_s_ballast
        }
    } else {
        if (bulbous_bow) {
            coef <- wet_surface_coef$twin_s_bulbous
        } else {
            coef <- wet_surface_coef$twin_s_nobulb
        }
    }

    c_area <- l * (b + 2 * t)
    a3_area <- c_area * coef[4] # a3 is coef[4]

    return(a3_area)
}



# Resistance Coefficient

resistance_b_coef <- data.frame(
                         single_s_design = c(-0.3382, 0.8086, -6.0258, -3.5632,
                                         9.4405, 0.0146, 0, 0, 0, 0),
                         single_s_ballast = c(-0.7139, 0.2558, -1.1606, 0.4534,
                                          11.222, 0.4524, 0, 0, 0, 0),
                         twin_screw = c(-0.2748, 0.5747, -6.7610, -4.3834,
                                     8.8158, -0.1418, -0.1258, 0.0481,
                                     0.1699, 0.0728)
                    )


resistance_c_coef <- data.frame(
                             single_s_design = c(-0.5742, 13.3893, 90.596,
                                             4.6614, -39.721, -351.483,
                                             -1.14215, -12.3296, 459.254),
                             single_s_ballast = c(-1.50162, 12.9678, -36.7985,
                                              5.55536, -45.8815, 121.82,
                                              -4.33571, 36.0782, -85.3741),
                             twin_screw = c(-5.3475, 55.6532, -114.905,
                                         19.2714, -192.388, 388.333,
                                         -14.3571, 142.738, -254.762)
                            )

resistance_d_coef <- data.frame(
                             single_s_design = c(0.854, -1.228, 0.497),
                             single_s_ballast = c(0.032, 0.803, -0.739),
                             twin_screw = c(0.897, -1.457, 0.767)
                            )

resistance_e_coef <- data.frame(
                             single_s_design = c(2.1701, -0.1602),
                             single_s_ballast = c(1.9994, -0.1446),
                             twin_screw = c(1.8319, -0.1237)
                            )


calc_resistance_coef <- function(l, b, t, los, lwl, cb, ta, tf, dp, n_rudd,
                                 n_brac, n_boss, n_thru, n_screw,
                                         design_draft, fn) {
    # Select coefficients
    if (n_screw < 2) {
        if (design_draft) {
            coef_b <- resistance_b_coef$single_s_design
            coef_c <- t(matrix(resistance_c_coef$single_s_design, nrow = 3))
            coef_d <- resistance_d_coef$single_s_design
            coef_e <- resistance_e_coef$single_s_design
        } else { # Ballast draft
            coef_b <- resistance_b_coef$single_s_ballast
            coef_c <- t(matrix(resistance_c_coef$single_s_ballast, nrow = 3))
            coef_d <- resistance_d_coef$single_s_ballast
            coef_e <- resistance_e_coef$single_s_ballast
        }
    } else {
        coef_b <- resistance_b_coef$twinScrew
        coef_c <- t(matrix(resistance_c_coef$twinScrew, nrow = 3))
        coef_d <- resistance_d_coef$twinScrew
        coef_e <- resistance_e_coef$twinScrew
    }

    # >> Eq: Kl <- e1*L^e2
    kl <- coef_e[1] * l^coef_e[2]

    # >> Eq: fnkrit <- d1 + d2*Cb + d3*Cb^2
    fn_krit <- coef_d[1] + coef_d[2] * cb + coef_d[3] * cb^2

    # >> Eq: CrStandard <- c11 + c12*fn + c13*fn^2 +
    # >>       Cb * (c21 + c22*fn + c23*fn^2) +
    # >>       Cb^2 * (c31 + c32*fn + c33*fn^2)
    cr_standard <- coef_c[1, 1] + coef_c[1, 2] * fn + coef_c[1, 3] * fn^2 +
                Cb * (coef_c[2, 1] + coef_c[2, 2] * fn + coef_c[2, 3] * fn^2) +
                Cb^2 * (coef_c[3, 1] + coef_c[3, 2] * fn + coef_c[3, 3] * fn^2)

    # >> Crfnkrit <- max(1.0, (fn/fnkrit)^f1)
    if (n_screw < 2 & !design_draft) { # If Ballast Draft
        coef_f <- 10 * cb * (fn / fn_krit - 1)
    } else { # DesignDraft and TwinScrew
        coef_f <- fn / fn_krit
    }
    cr_fn_krit <- max(1.0, (fn / fn_krit)^coef_f)


    # >> Cr <- CrStandard * Crfnkrit * Kl * (T/B)^b1 * (B/L)^b2 * (Los/Lwl)^b3 *
    # >>   (Lwl/L)^b4 * (1+(TA-TF)/L)^b5 * (Dp/Ta)^b6 * (1+NRudd)^b7 *
    # >>   (1+NBrac)^b8 * (1+NBoss)^b9 * (1+NThruster)^b10
    cr <- cr_standard * cr_fn_krit * kl * (T / B)^coef_b[1] *
        (b / l)^coef_b[2] *  (los / lwl)^coef_b[3] * (lwl / l)^coef_b[4] *
        (1 + (ta - tf) / l)^coef_b[5] * (dp / ta)^coef_b[6] *
        (1 + n_rudd)^coef_b[7] * (1 + n_brac)^coef_b[8] *
        (1 + n_boss)^coef_b[9] * (1 + n_thru)^coef_b[10]

    return(cr)
}


calc_cr_nofn <- function(l, b, t, los, lwl, ta, tf, dp, n_rudd, n_brac, n_boss,
                         n_thru, n_screw, design_draft) {
    # Select coefficients
    if (n_screw < 2) {
        if (design_draft) {
            coef_b <- resistance_b_coef$single_s_design
            coef_e <- resistance_e_coef$single_s_design
        } else { # Ballast draft
            coef_b <- resistance_b_coef$single_s_ballast
            coef_e <- resistance_e_coef$single_s_ballast
        }
    } else {
        coef_b <- resistance_b_coef$twin_screw
        coef_e <- resistance_e_coef$twin_screw
    }

    # >> Eq: Kl <- e1*L^e2
    kl <- coef_e[1] * l^coef_e[2]

    # >> Cr_nofn <- Kl * (T/B)^b1 * (B/L)^b2 * (Los/Lwl)^b3 *
    # >>   (Lwl/L)^b4 * (1+(TA-TF)/L)^b5 * (Dp/Ta)^b6 * (1+NRudd)^b7 *
    # >>   (1+NBrac)^b8 * (1+NBoss)^b9 * (1+NThruster)^b10
    cr_nofn <- kl * (t / b)^coef_b[1] * (b / l)^coef_b[2] *
        (los / lwl)^coef_b[3] * (lwl / l)^coef_b[4] *
        (1 + (ta - tf) / l)^coef_b[5] * (dp / ta)^coef_b[6] *
        (1 + n_rudd)^coef_b[7] * (1 + n_brac)^coef_b[8] *
        (1 + n_boss)^coef_b[9] * (1 + n_thru)^coef_b[10]

    return(cr_nofn)
}
