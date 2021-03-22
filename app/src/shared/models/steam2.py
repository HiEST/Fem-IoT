# -*- coding: utf-8 -*-
from pandas import isnull
import math
from shared.models.steam import transient_power_me_steam

# Block coefficient & Propeller diameter


def calc_block_coeff(fn):
    """
    Block coeff. (Eq. 4)
    Coefficient that describes the hull's shape.
        C_{b} = 0.7 + 1/8 * atan((23-100*F_{n})/4)
    Where:
        - F_{n}: Froude number

    :param fn [double]: Froude number
    """
    return 0.7 + 1/8 * math.atan((23-100*fn)/4)


def calc_froude_number(msSpeed, waterline, gravity_ct=9.8):
    """
    Froude number
        F_{n} = speed / sqrt(gravity constant * waterline length)
        waterline length ~= avg(LOA,LBP)
        NOTICE:
        - Waterline length not available in DB (Jalkanen 2012), therefore they
        use an average value of overall length in meters (LOA) and length
        between perpendiculars in meters (LBP).
        https://www.globalsecurity.org/military/systems/ship/images/image1445.gif

        WARNING: This equation is incorrect in STEAM2 paper

        Gravity ~= 9.8 m/s^2

    :param msSpeed [double]: Speed in m/s.
    :param waterline [double]: Ship's waterline. In Jalkanen 2012 it is the
    average of Lenght Over All and Length Between Perpendicular.
    :param gravity_ct [double]: Gravity constant. Usually 9.8 m/s^2, but who
    knows about the future constants...
    """
    return msSpeed / math.sqrt(gravity_ct * waterline)


# Evaluation of resistance and ship specifications

def calc_total_resistance(fric_r, res_r):
    """
    Jalkanen 2012 (Eq. 1)
    Total resistance of a moving marine vessel (in kN) can be estimated with:
        Rtotal ~= R_{F} + R_{R}  (in Newtons)
        Where:
                - R_{F}: Frictional resistance acting on the wet surface of the
                            vessel.
                - R_{R}: Residual resistance


    :param fric_r [double]: Friction resistance
    :param res_r [double]: Residual resistance
    """
    return (fric_r+res_r)/1000  # Return in kN


def calc_reynolds_number(ms_speed, length, kinematicViscosityC=1.069461e-06):
    """
    Reynolds number
    ITTC - Recommended Procedures and Guidelines: index
    https://www.ittc.info/media/7943/0_0.pdf
    ITTC - Recommended Procedures and Guidelines: Resistance test
    https://www.ittc.info/media/8001/75-02-02-01.pdf

    The used Kinematic Viscosity is the mean of the viscosities during the year
    In 2016 the temperature of the Mediterranean sea ranged from 14º to 25º.
    For further information check kinematicViscosity.R and ITTC - Fresh water
    and seawater properties (2011).

    TODO: Use the actual value for each month.

    :param msSpeed [double]: Speed in m/s.
    :param length [double]: Ship length (L).
    :param kinematicViscosityC [double]: Kinematic viscosity as defined in the
    description.
    """
    return (ms_speed*length)/kinematicViscosityC


def calc_frictional_resistance(reynolds_n, sea_dens, ms_speed, wet_surf):
    """
    R_{F} as in International Towing Tank Conference procedure (ITTC 1999)
    (Eq. 2)
        R_{F} = C_{F} * (rho/2) * v^2 * S
        Where:
                - C_{F}: Friction resistance coefficient
                - rho: Seawater density (km/m^3)
                - v: Vessel speed (m/s)
                - S: Wet surface (m^2) [Evaluated according to Schneekluth and
                    Bertram (1998) and Hollenbach (1998)]

        C_{F} = 0.075/(log10 R_{n} - 2)^2
        Where:
                - R_{n}: Reynolds number


    :param reynolds_n [double]: Reynolds number.
    :param sea_dens [double]: Sea density in km/m^3
    :param speed [double]: Ship's speed in m/s
    :param wet_surf [double]: Wet surface of the ship in m^2. In this project
    it is estimated using the Hollenbach (1998) approach.
    """
    fric_c = 0.075/((math.log(reynolds_n, 10) - 2)**2)
    return fric_c*(sea_dens/2)*(ms_speed**2)*wet_surf


def calc_residual_resistance(cr, sea_dens, b, t, ms_speed):
    """
    R_{R} Residual resistance (Eq. 3)
        R_{R} = C_{R} * (rho/2) * v^2 * (B*T/10)
        Where:
                - B: Vessel breadth (m)
                - rho: Seawater density (km/m^3)
                - T: Vessel draught (m)
                - C_{R}: Residual resistence coeficient [Schneekluth and
                    Bertram (1998) and Hollenbach (1998)]
                - v: Vessel speed

    :param cr [double]: Residual resistance coefficient. It may be calculated
    using the method from Schneekluth and Bertram (1998).
    :param sea_dens [double]: Seawater density in km/m^3.
    :param b [double]: Ship's breadth in meters.
    :param t [double]: Ship's draught in meters.
    :param ms_speed [double]: Ship's speed in m/s.
    """
    return cr * (sea_dens/2) * (ms_speed**2) * (b*t/10)


def calc_propelling_power(total_res, ms_speed):
    """
    Calculated the propelling power (in kW) using STEAM2 with the total
    resistance and the current speed.

    :param total_res [double]: Total resitance factor.
    :param ms_speed [double]: Ship's speed in m/s.
    """
    return total_res*ms_speed


# Total Power (kW)

# P_{total} = P_{propel} / eta_{qpc}
def calcTotalPower(propelPow, qpc):
    """
    Calculate total power

    :param propelPow [double]: Propelling power.
    :param qpc [double]: Quasi Propulsive Constant, i.e. transmission.
    efficiency
    """
    return propelPow/qpc


# Engine load balancing


def calc_n_operative_engines(total_power, engine_power):
    """
    Number of engines operational (eq. 9):

    n_{OE} = P_{total}/P_{e} + 1
    Where:
        - P_{total} = Total power requiered (calc. before)
        - P_{e} = Installed engine power at MRC (Max. Continuous Rating)

    :param total_power [double]: Total transient power that is being used.
    :param engine_power [double]: Ship's single engine power. STEAM2 assumes
    that all the main engines are identical. Load values <= 85%.
    """
    # We assume that n_{OE} is an integer, so we need to truncate.
    return math.trunc(total_power/engine_power) + 1


def calcEngineLoad(total_power, engine_power, n_op_eng):
    """
    Engine load (eq. 10):
    The model assumes that all main engines are identical. Load values <= 85%.

    EL = P_{total}/(P_{OE}*n_{OE})
    Where:
        - n_{OE} = number of operating engines.

    :param total_power [double]: Ship's transient power
    :param engine_power [double]: Ship's single installed engine power
    :param n_op_eng [int]: Number of currently operative engines
    """
    return total_power/(engine_power*n_op_eng)


# ENGINE LOAD & SPECIFIC FUEL OIL CONSUMPTION

def calc_relative_sfoc(EL):
    """
    Minimizing fuel oil consumption requieres engine loads approximately from
    70 to 80%, which represents the optimum regine in terms of both comsumption
    and performance.

    STEAM2 model assumes a parabolic function for all engines [see Jalkanen
    2012 for further information].

    Relative SFOC:
        SFOC_{relative} = 0.455*EL^2 - 0.71*EL + 1.28
    Where:
        - EL: Engine load ranging from 0 to 1.


    :param EL [TODO:type]: [TODO:description]
    """
    return 0.455*(EL**2) - 0.71*EL + 1.28


def calcAbsoluteSFOC(sfoc_relative, sfoc_base):
    """
    Absolute SFOC:
    SFOC = SFOC_{relative}*SFOC_{base}
    Where:

        - SFOC_{base}: base values for SFOC. Constant for each engine.
            In Jalkanen 2012 is set as follows:
            Turbine machinery -> 260 g/kWh
            Auxiliary machinery -> 220 g/kW
            We can use this approach or NAEI's which give more details

    Notes:
        - We do not have SFOC from the engine manufacturers.
        - Therefore, value is evaluated according to the IMO GHG2 report
        (Buhaug et al., 2009) or NAEI's inventory approach (NAEI 2017)


    :param sfoc_relative [double]: SFOC relative coefficient calculated from
    engine load.
    :param sfoc_base [double]: Static base SFOC coeeficient.
    """
    return sfoc_relative*sfoc_base


# AUXILIARY ENGINE EVALUATION

# Cruise ships, RoRo/Passenger and yatch:
#   - Base 750kW on all operating modes
#   - 3kW per cabin
# Reefers and container ships:
#   - Base :
#       + 750kW on cruising (<5 knots)
#       + 1000kW on hotelling (> 1 knot)
#       + 1250kW on maneuvering ( 1 < n < 5 knots)
#   - Each regrigerated TEU (Twenty-foot Equivalent Unit, standard cargo cont.)
#       additional 4 kW required.
# All others:
#   - 750 kW Cruising
#   - 1000 kW Hotelling
#   - 1250 kW Maneuvering
#

def transient_power_ae_steam2(knot_speed, type, inst_pow, n_cabin, ref_teu):
    # If installed pow is not set or incorrectly set, pow is none
    if inst_pow is None or inst_pow <= 0.0:
        return(None)

    if type in [
                 "Passenger (Cruise) Ship",
                 "Passenger/Ro-Ro Cargo Ship",
                 "Passenger Ship",
                 "Yacht",
                 "Yacht Carrier, semi submersible",
                 "Yacht (Sailing)"]:
        # TODO: Are yatch really in this cathegory?
        pow = 750.0 + 3 * n_cabin
    else:  # General case
        if knot_speed > 5:      # Sailing
            pow = 750.0
        elif knot_speed > 1:    # Maneuvering
            pow = 1250.0
        else:                   # Hotelling
            pow = 1000.0
        if ref_teu > 0:
            # As this calculation only involves refrigerated TEUs
            #  we can directly check this avoiding type check.
            pow += (4*ref_teu)

    # Limit to installed power
    pow = min(inst_pow, pow)

    return(pow)


def estimate_main_engine_power(ms_speed, total_res, qpc):
    """
    Estimate the main engine power (in kW) using the current speed and total
    resistance taking into account the transmission efficiency with the Quasi
    Propulsive Constant.

    :param ms_speed [double]: Ship's speed in m/s
    :param total_res [double]: Total resitance estimated with STEAM2 method
    :param qpc [double]: Quasi Propulsive Constant, i.e. transmission.
    """
    propel_power = calc_propelling_power(total_res, ms_speed)
    # Calculate how much of the total power is needed for the given propelling
    # power.
    return propel_power/qpc


def estimate_resistance_coef(fn, cb, cr_nofn, n_screw, design_draft):
    # TODO: Mayyyyyyybe this should be given to spark and then be broadcasted?
    res_coef_c = [
        [[-0.5742, 13.3893, 90.596],
         [4.6614, -39.721, -351.483],
         [-1.14215, -12.3296, 459.254]],  # Single screw, design draft
        [[-1.50162, 12.9678, -36.7985],
         [5.55536, -45.8815, 121.82],
         [-4.33571, 36.0782, -85.3741]],  # Single screw, ballast draft
        [[-5.3475, 55.6532, -114.905],
         [19.2714, -192.388, 388.333],
         [-14.3571, 142.738, -254.762]]   # Twin screw
    ]
    res_coef_d = [
        [0.854, -1.228, 0.497],  # Single screw, design draft
        [0.032, 0.803, -0.739],  # Single screw, ballast draft
        [0.897, -1.457, 0.767]   # Twin screw
    ]
    # Single screw design draft by default as most of them are like this
    coef_id = 0
    if n_screw > 1:
        coef_id = 2  # Twin screw
    elif not design_draft:
        coef_id = 1  # Single screw ballast draft

    # Select coef
    c = res_coef_c[coef_id]
    d = res_coef_d[coef_id]

    fn_krit = d[0] + d[1]*cb + d[2]*(cb**2)

    f1 = fn/fn_krit  # DesignDraft and TwinScrew
    if (coef_id == 1):  # If Ballast Draft
        f1 = 10*cb*(f1 - 1)

    cr_fn_krit = max(1.0, (fn/fn_krit)**f1)

    cr_std = \
        c[0][0] + c[0][1]*fn + c[0][2]*(fn**2) +\
        (c[1][0] + c[1][1]*fn + c[1][2]*(fn**2)) * cb +\
        (c[2][0] + c[2][1]*fn + c[2][2]*(fn**2)) * (cb**2)

    return cr_std * cr_fn_krit * cr_nofn


def estimate_wet_surf(k_area, a3_area, cb):
    """
    Estimates the ship's wet surface using Hollenbach 1998.

    k_{noCB} = a_0 + a_1 * L_{os}/L_{wl} + a_2 * L_wl/L + a_4 * B/T + a_6 *
    L/T + a_7 * (T_A - T_F)/L + a_8 * Dp/T +
    k_{Rudd}*N_{Rudd} + k_{Brac}*N_{Brac} + k_{Boss}*N_{Boss}


    First we calculate the measure related to the overall ship area:
    C_{area} = L*(B+2*T)


    Therefore our main equation is this:
    S_{total} = (k_{noCB} + a_3 * C_B) * C_{area}

    S_{total} = k_{noCB} * C_{area} + a_3 * C_{area} * C_B

    We define new constants:

    k_{area} = k_{noCB} * C_{area}
    a_{3area} = a_3 * C_{area}

    Final equation is:

    S_{total} = k_{area} + a_{3area} * C_B

    :param k_area [double]: Static coefficients multiplied by ship's area
    constant as defined in the documentation.
    :param a3_area [double]: a3 regression coefficient multiplied by the ship's
    area constant.
    :param cb [double]: Ship's block coefficient.
    """
    return k_area + (a3_area * cb)


def transient_power_me_steam2(model, knot_speed, v_design, inst_pow, L, b, t,
                              qpc, k_area, a3_area, cr_nofn, n_screw,
                              design_draft, waterline, v_safety=0.5,
                              sea_dens=1038):

    """
    Does all the calculations with the static data and the speed to estimate
    the main engine power (in kW). If qpc is not a value, STEAM is used
    instead.

    :param model [integer]: STEAM model to be used.
    :param L [double]: Ship's length.
    :param b [double]: Ship's beam.
    :param t [double]: Ship's draught.
    :param knot_speed [double]: Ship's speed in knots
    :param qpc [double]: Quasi Propulsive Constant, i.e. transmission.
    :param k_area [double]: k_area constant used for wet surface estimation.
    :param a3_area [double]: a3_area constant used for wet surface estimation.
    :param cr_nofn [double]: cr_nofn constant used for resistance coefficient
    estimation.
    :param n_screw [double]: Number of screws that the ship has.
    :param design_draft [boolean]: True if the ship has a design draft.
    :param waterline [double]: Ship's waterline length, i.e. the length of the
    ship at the water surface. Usually estimated as the mean of Length Between
    Perpendiculars and Length Over All.
    :param EpsilonP [double]: [TODO:description]
    :param VSafety [double]: [TODO:description]
    :param sea_dens [double]: Sea density in kg/m^3. Mediterranean sea density
    assumed 1038 kg/m^3
    """
    # If installed pow is not set or incorrectly set, pow is none
    if inst_pow is None or inst_pow <= 0.0:
        return None

    # Limit speed
    if knot_speed > (v_safety + v_design):
        knot_speed = (v_design + v_safety)
    if knot_speed < 0.0:
        knot_speed = 0.0

    if knot_speed == 0.0:
        pow = 0.0
    else:
        if model == 1:
            # STEAM
            pow = transient_power_me_steam(knot_speed, v_design, inst_pow)
        else:
            # STEAM2
            ms_speed = knot_speed * 0.514
            fn = calc_froude_number(ms_speed, waterline)
            cb = calc_block_coeff(fn)

            cr = estimate_resistance_coef(fn, cb, cr_nofn, n_screw,
                                          design_draft)
            wet_surf = estimate_wet_surf(k_area, a3_area, cb)
            reynolds_n = calc_reynolds_number(ms_speed, L)
            res_r = calc_residual_resistance(cr, sea_dens, b, t, ms_speed)
            fric_r = calc_frictional_resistance(reynolds_n, sea_dens, ms_speed,
                                                wet_surf)
            total_res = calc_total_resistance(fric_r, res_r)
            pow = estimate_main_engine_power(ms_speed, total_res, qpc)
            pow = min(inst_pow, pow)  # Limit to installed power

    return pow
