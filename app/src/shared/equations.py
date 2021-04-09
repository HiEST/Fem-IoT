def calcSOxEmissionFactor(SFOC=200, SC=0.001):
    """
    Calculate SOx Emission factor using Jalkanen 2009 Appendix.

    :param SFOC [int]:  Specific Fuel Oil Consumtion in g/kWh
    :param SC [double]: Percentage of Sulfur Content of the fuel. Defaults to
        0.1%, which is the limit established for SECA zones and EU Sulfur
        directives.
    """
    nS = (SFOC * SC) / 32.0655
    mSOx = 64.06436 * nS
    return mSOx


def calcCO2EmissionFactor(SFOC=200, CC=0.85):
    """
    Calculate CO2 Emission factor using Jalkanen 2009 Appendix.

    :param SFOC [int]:  Specific Fuel Oil Consumtion in g/kWh
    :param CC [double]: Percentage of Carbon Content of the fuel. Defaults to
        0.85%, as specified in STEAM.
    """
    nC = (SFOC * CC) / 12.01
    mCO2 = 44.00886 * nC
    return mCO2


def calcNOxEmissionFactor(rpm):
    """
    Calculate NOx Emission factor using Jalkanen 2009 Appendix.

    :param rpm [int]: Engine's Revolutions Per Minute.
    """
    res = 0
    if rpm > 0:
        if rpm < 130:
            res = 17.0
        else:
            if rpm < 2000:
                res = 45 * (rpm ** (-0.2))
            else:
                res = 9.8
    return res


# Default sampling per minute and unit is kg -> kg/minute
def estimateEmission(factor, power, sampleGranularity=60, unit=1000):
    if power is None or factor is None:
        return None
    return ((factor * power / 3600.0) / unit * sampleGranularity)
