# Jalkanen 2012

source("tools/interpolation.R")
source("jalkanen2012/powerEstimation.R")
source("jalkanen2012/emissionEstimation.R")
source("jalkanen2012/hollenbach.R")
source("jalkanen2009/emissionEstimation.R")


################################################################################
############################ Data flow chart ###################################
################################################################################

# 1 - [DATA] Receive AIS message
# 2 - [DATA] Get technical information from IHS tables
# 3 - [DATA] Determine ship type
#               (Default: Small craft. GT 620, main engine 2380 kW)
#   If insufficient data: use AIS data.
# 4 - [DATA] Determine Main Engine stroke type and RPM
#   Default: Medium speed diesel 500 RPM
# 5 - [CALC] Calculate Instantaneos Propeling Power. [Optional: Wave penalty]
# 6 - [CALC/DATA] Estimate Auxiliary Engine usage (including Boilers)
#   Passenger, RoPax and Cruise ships: 4000kW always
#       ShiptypeLevel4:
#       [24] "Passenger (Cruise) Ship"
#       [25] "Passenger/Ro-Ro Cargo Ship"
#       [26] "Passenger Ship"
#   Others:
#       - Cruising (> 5 knots): 750 kW
#       - Manouvering (1-5 knots): 1250 kW
#       - Hotelling (< 1 knot): 1000 kW
#   (Do not exceed installed maximum in both cases)
# 7 - [DATA?] Apply measured emission values or emission abatement
#   If not persent: calculate NOx from IMO curve
# 8 - [CALC] Calculate NOx & SOx
# 9 - Put emissions to grid and plot.

# waterDensity in kg/m^3. Mediterranean sea density assumed 1038 kg/m^3
estimateShipEmissions2012 <- function(dAISInt, shipParameters, sampleGranularity,
                                      waterDensity = 1038, unit = "g") {
################################################################################
#                           Preprocess                                         #
################################################################################
  # Scale the output. grams, kilograms and tonnes
  unit <- switch(unit, g = 1, kg = 10^3, t = 10^6)
  # Retrieve IHS preprocessed parameters.
  attach(shipParameters, warn.conflicts = FALSE)

    # Time series attributes
    # Speed: knots to m/s
    msSpeed <- dAISInt$sog*0.514
    knotSpeed <- dAISInt$sog


################################################################################
#                       Vessel's hull parameterss                              #
################################################################################
    Fn <- calcFroudeNumber(msSpeed, LOA, LBP)
    Cb <- calcBlockCoeff(Fn)


################################################################################
#                       Propeller characteristics                              #
################################################################################
    Dp <- calcPropellerDiameter(servicePowerME, propellerRPM) # For Hollenbach

################################################################################
#                       Resistance evaluation                                  #
################################################################################

    # Calculate ResidualCoefficient and WetSurface with Hollenbach (1999)
    wetSurface <- estimateWetSurfaceHollenbach(L, B, T, Los, Lwl, Cb, Ta, Tf,
                                               Dp, NRudd, NBrac, NBoss, NScrew,
                                               bulbousBow, designDraft)
    Cr <- estimateResitanceCoefficient(L, B, T, Los, Lwl, Cb, Ta, Tf, Dp, NRudd,
                                       NBrac, NBoss, NThruster,  NScrew,
                                       designDraft, Fn)

    reynoldsNumber <- calcReynoldsNumber(msSpeed, L)
    fricRes <- calcFrictionalResistance(reynoldsNumber, waterDensity, msSpeed, wetSurface)
    resRes <- calcResidualResistance(Cr, waterDensity, B, T, msSpeed)
    totalRes <- calcTotalResistance(fricRes, resRes)



################################################################################
#                           Power Estimation                                   #
################################################################################
    ### MAIN ENGINE POWER EVALUATION
    mEnginePower <- estimateMainEnginePower(msSpeed, totalRes, LBP, propellerRPM)

    ### AUX ENGINE POWER EVALUATION
    auxEnginePower <- estimateAuxiliaryEnginePower(knotSpeed, type, installedPowerAE,
                                                   nCabins, nRefrigeratedTEU)


    ### LOAD BALANCING
    nOperativeEngines <- calcNumOperativeEngines(mEnginePower, singleEnginePowerME)

    # engineLoadME <- calcEngineLoad(mEnginePower, servicePowerME, nInstalledME)
    engineLoadME <- calcEngineLoad(mEnginePower, singleEnginePowerME, nOperativeEngines)

################################################################################
#                           Fuel Estimation                                    #
################################################################################

    ### SFOC
    relativeSFOCME <- calcRelativeSFOC(engineLoadME)

    # BASE SFOC: turbine eng 260 g/kWh, aux eng 220 g/kWh
    absoluteSFOCME <- calcAbsoluteSFOC(relativeSFOCME, SFOCBaseME)

    # TODO: AE?


    # TODO: Apply SFOC
    transPME <- mEnginePower
    transPAE <- auxEnginePower

################################################################################
#                       Emission Factor calculation                            #
################################################################################

    # Emission abatement or measured values?

    # TODO: This part

    # Calculate CO2, SOx and NOx factors. Initially they are g/Kwh.
    SOxFactME <- calcSOxEmissionFactor(SC = 0.001) #TODO: Set SFOC y SC/CC
    SOxFactAE <- calcSOxEmissionFactor(SC = 0.001) #	Now using paper assump.
    CO2Fact <- calcCO2EmissionFactor()

    # We calculate the factor for all the engines and then sum it.
    NOxFactME <- calcNOxEmissionFactor(MainEngineRPM)
    NOxFactAE <- calcNOxEmissionFactor(AuxiliaryEngineRPM)

################################################################################
#                           Emission Estimation                                #
################################################################################


    # Each sample is treated as an independent second.
    # (g/Kwh * kW)/ h/s = g/s -> Grams for this second.
    # If the samples are not taken by second, we multiply them by the space
    # left between samples (sampleGranularity)
    SOxME <- (SOxFactME * transPME  /  3600) / unit * sampleGranularity
    SOxAE <- (SOxFactAE * transPAE / 3600) / unit * sampleGranularity

    CO2ME <- (CO2Fact * transPME / 3600) / unit * sampleGranularity
    CO2AE <- (CO2Fact * transPAE / 3600) / unit * sampleGranularity

    NOxME <- (NOxFactME * transPME / 3600) / unit * sampleGranularity
    NOxAE <- (NOxFactAE * transPAE / 3600) / unit * sampleGranularity

    #return(list(IntpData = dAISInt, SOxME = SOxME, SOxAE = SOAE, CO2ME = CO2ME,
    #            CO2AE=CO2AE,
    #			NOxME=NOxME, transPME=transPME, transPAE=transPAE,
    #		  SOXFactME=SOxFactME, SOxFactAE=SOxFactAE, CO2Fact=CO2Fact,
    #			NOxMEFact=NOxMEFact)) #, NOxAE=NOxAE))
    return(
        list(
            emissions =
                data.frame(
                    dAISInt, SOxME = SOxME, SOxAE = SOxAE,
                    CO2ME = CO2ME, CO2AE = CO2AE, NOxME = NOxME, NOxAE = NOxAE,
                    transPME = transPME, transPAE = transPAE
                ),
            emissionFactors =
                data.frame(
                    SOXFactME = SOxFactME, SOxFactAE = SOxFactAE,
                    CO2Fact = CO2Fact,
                    NOxFactME=NOxFactME, NOxFactAE=NOxFactAE
                )
        )
    )
}
