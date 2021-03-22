from pandas import isnull


def transient_power_me_steam(VTransient, VDesign, PInstalled, EpsilonP=0.8,
                             VSafety=0.5):
    """
    Calculates the transient main engine power using the current speed vs the
    design speed as specified in STEAM (Jalkanen et al. 2009).

    :param VTransient [double]: Current ship speed at a given point.
    :param VDesign [doble]: Ship's maximum speed by design.
    :param PInstalled [double]: Installed main engine power.
    :param EpsilonP [double]: Engine efficiency rate. How much of the engine
    power is effective. STEAM assumes 80%.
    :param VSafety [double]: Safety extra limit established in STEAM becuase
    ships may go faster than design speed just by a little. STEAM sets 0.5
    knots.
    """
    if PInstalled is None or PInstalled <= 0.0:
        return None

    if VTransient > (VSafety + VDesign):
        VTransient = (VDesign + VSafety)
    if VTransient < 0:
        VTransient = 0
    k = (EpsilonP * PInstalled) / (VDesign + VSafety) ** 3
    return k * (VTransient ** 3)


def transient_power_ae_steam(speed, shipType, instPow):
    """
    Calculates the transient power for Auxiliary Engines using the installed
    power, speed and ship type as specified in STEAM (Jalkanen et al. 2009).

    :param speed [double]: Current ship speed at a given point.
    :param shipType [string]: Type of the current ship.
    :param instPow [double]: Installed auxiliary engine power.
    """
    if instPow is None:
        return None

    # Cruisers, Ro-Ro and passenger ships
    if (shipType == "Passenger (Cruise) Ship" or
            shipType == "Passenger/Ro-Ro Cargo Ship" or
            shipType == "Passenger Ship"):
        if instPow <= 0:
            return 4000.0
        return min(4000.0, instPow)
    # The rest
    res = 1000.0
    if speed > 1 and speed <= 5:
        res = 1250.0
    if speed > 5:
        res = 750.0
    if instPow > 0 and res > instPow:
        res = instPow
    return res
