####################### MAIN ENGINE EVALUATION #################################

################################################################################
#################  Block coefficient & Propeller diameter ######################
################################################################################

# Block coeff. (Eq. 4)
# Coefficient that describes the hull's shape.
# 	C_{b} = 0.7 + 1/8 * atan((23-100*F_{n})/4)
#	Where:
#		- F_{n}: Froude number

calcBlockCoeff <- function(fN) {
	0.7 + 1/8 * atan((23-100*fN)/4)
}

# Froude number
#	F_{n} = speed / (gravity constant * waterline length)
#	waterline length ~= avg(LOA,LBP)
#	NOTICE:
#		- Waterline length not available in DB (Jalkanen 2012), therefore they
#			use an average value of overall length in meters (LOA) and length
#			between perpendiculars in meters (LBP).
#	 https://www.globalsecurity.org/military/systems/ship/images/image1445.gif
#
#	Gravity ~= 9.8 m/s^2

calcFroudeNumber <- function(msSpeed, LOA, LBP, gravityCt = 9.8) {
	waterlineL <- mean(c(LOA,LBP))
	msSpeed / (gravityCt * waterlineL)
}

# Propeller diameter (Eq. 5)
#	d = 16.2 * (P_{s}^0.2/N^0.6)
#	Where:
#		- P_{s}: Service power of the main engine, 80% of Max. Cont. Rating (kW)
# 		- N: Propeller angular velocity (rpm)
#	Notes: 
#		This formula is applied only to single-propeller ships. If multi, use
#		appendix A
#		If number of propellers unknown, assume 1.

calcPropellerDiameter <- function(servPow, propelRPM) {
	16.2 * (servPow^0.2/propelRPM^0.6)
}

################################################################################
############# Evaluation of resistance and ship specifications ################
################################################################################

# Hollenbach (1998) (Eq. 1)
# Total resistance of a moving marine vessel (in kN) can be estimated with:
#	Rtotal ~= R_{F} + R_{R}
#	Where:
#		- R_{F}: Frictional resistance acting on the wet surface of the vessel.
#		- R_{R}: Residual resistance
#
#
calcTotalResistance <- function(fricR, resR) {
	fricR+resR
}


# Reynolds number
# ITTC - Recommended Procedures and Guidelines: index
# https://www.ittc.info/media/7943/0_0.pdf
# ITTC - Recommended Procedures and Guidelines: Resistance test
# https://www.ittc.info/media/8001/75-02-02-01.pdf

# The used Kinematic Viscosity is the mean of the viscosities during the year.
# In 2016 the temperature of the Mediterranean sea ranged from 14º to 25º.
# For further information check kinematicViscosity.R and ITTC - Fresh water and
# seawater properties (2011).
# TODO: Use the actual value for each month.
calcReynoldsNumber <- function(msSpeed, length, kinematicViscosityC =  1.069461e-06) {
	(msSpeed*length)/kinematicViscosityC
}


# R_{F} as in International Towing Tank Conference procedure (ITTC 1999) (Eq. 2)
#	R_{F} = C_{F} * (rho/2) * v^2 * S
#	Where:
#		- C_{F}: Friction resistance coefficient
#		- rho: Seawater density (km/m^3)
#		- v: Vessel speed (m/s)
#		- S: Wet surface (m^2) [Evaluated according to Schneekluth and Bertram 
#								(1998) and Hollenbach (1998)]								
#
#	C_{F} = 0.075/(log R_{n} - 2)^2
#	Where:
#		- R_{n}: Reynolds number
#

calcFrictionalResistance <- function(reynoldsN, seaDens, speed, wetS) {
	fricC <- 0.075/(log(reynoldsN) - 2)^2

	fricC * (seaDens/2) * speed^2 * wetS
}

# R_{R} Residual resistance (Eq. 3)
#	R_{R} = C_{R} * (rho/2) * v^2 * (B*T/10)
#	Where:
#		- B: Vessel breadth (m)
#		- rho: Seawater density (km/m^3)
#		- T: Vessel draught (m)
#		- C_{R}: Residual resistence coeficient [Schneekluth and Bertram (1998)
#													and Hollenbach (1998)]
#		- v: Vessel speed

calcResidualResistance <- function(cR, seaDens, B, T, msSpeed) {
	cR * (seaDens/2) * msSpeed^2 * (B*T/10)
}

################################################################################
######################### Propelling power (kW) ################################
################################################################################

# P_{Propel} = R_{total} * v
# Where:
#	- v: Instantaneous speed (m/s)

calcPropellingPower <- function(totalRes, msSpeed) {
	totalRes*msSpeed
}

################################################################################
###################### Quasi Propulsive Constant ###############################
################################################################################

# Used to describe the effectiveness of converting the main engine power to 
# actual propelling power. Watson (1998)
#
# eta_{qpc} = 0.84 - (N * sqrt(LBP) / 10000)
# Where:
# 	- N: rpm of the propeller.
#	- LBP: length between perpendiculars (m)
#
#

calcQuasiPropulsiveCte <- function(propelRPM, LBP) {
	0.84 - ( propelRPM * sqrt(LBP) / 10000)
}

################################################################################
########################## Total Power (kW) ####################################
################################################################################

# P_{total} = P_{propel} / eta_{qpc}
calcTotalPower <- function (propelPow, qpc) {
	propelPow/qpc
}


estimateMainEnginePower <- function(msSpeed, totalRes, LBP, propelRPM) {
	### POWER
	propelPower <- calcPropellingPower(totalRes,msSpeed)
	# Calculate transmission efficiency
	qpc <- calcQuasiPropulsiveCte(propelRPM, LBP)

	# Calculate how much of the total power is needed for the given propelling
	#	power.
	totalPower <- calcTotalPower(propelPower, qpc)
}


################################################################################
######################## Engine load balancing #################################
################################################################################

# Model assumes that all main engines are identical. Load values <= 85%.

# Number of engines operational (eq. 9):
#
# n_{OE} = P_{total}/P_{e} + 1
# Where:
#	- P_{total} = Total power requiered (calc. before)
#	- P_{e} = Installed engine power at MRC (Max. Continuous Rating)

calcNumOperativeEngines <- function(totalPower, enginePower) {
	# TODO: We assume that n_{OE} is an integer, so we need to truncate.
	trunc(totalPower/enginePower + 1)
}

# Engine load (eq. 10):
# 
# EL = P_{total}/(P_{e}*n_{e})
# Where:
#	- n_{e} = number of installed engines. #TODO: Check if n_{e} or n_{OE}


calcEngineLoad <- function(totalPower, enginePower, nInstalledEngines) {
	totalPower/(enginePower*nInstalledEngines)
}



###################### AUXILIARY ENGINE EVALUATION #############################

# Cruise ships, RoRo/Passenger and yatch:
#	- Base 750kW on all operating modes
#	- 3kW per cabin
# Reefers and container ships:
#	- Base :
#		+ 750kW on cruising (<5 knots)
#		+ 1000kW on hotelling (> 1 knot)
#		+ 1250kW on maneuvering ( 1 < n < 5 knots)
#	- Each regrigerated TEU (Twenty-foot Equivalent Unit, standard cargo cont.)
#		additional 4 kW required. 
# All others:
#	- 750 kW Cruising
#	- 1000 kW Hotelling
#	- 1250 kW Maneuvering
#

speedToUsedAuxBasePower <- function(knotSpeed, maxPower=NULL) {
	res <- rep(1000, length(knotSpeed)) #Hotelling as default	
	res[knotSpeed > 1 & knotSpeed <= 5] <- 1250
	res[knotSpeed > 5] <- 750
	if (!is.null(maxPower)) res[res > maxPower] <- maxPower #Limit max power
	res
}


estimateAuxiliaryEnginePower <- function(knotSpeed, type, instPow, nCabin, refTEU) {
	cat("[Estimation] Auxiliary Engines Power\n")
	if (is.na(instPow) | instPow <= 0) instPow <- Inf

	if (type %in% c("Passenger (Cruise) Ship", "Passenger/Ro-Ro Cargo Ship",
					"Passenger Ship", "Yacht", "Yacht Carrier, semi submersible",
					"Yacht (Sailing)")) 
	{ #TODO: Are yatch really in this cathegory?                                                                                      		
		# Return a vector of the size of input with minimum of 4000 or Max Power.   
	   	p <- 750 + 3 * nCabin
		pow <- rep(min(p,instPow), length(knotSpeed))                            

	}
	else { # General case
		pow <- speedToUsedAuxBasePower(knotSpeed, instPow)
		if (refTEU > 0) { # As this calculation only involves refrigerated TEUs
						  #  we can directly check this avoiding type check.
			pow <- pow + (4*refTEU)
			pow[pow > instPow] <- instPow #Limit max power
		}	
  	}                                                                                                                     
	return(pow)
}

############ ENGINE LOAD & SPECIFIC FUEL OIL CONSUMPTIONN ######################

# Minimizing fuel oil consumption requieres engine loads approximately from
#  70 to 80%, which represents the optimum regine in terms of both comsumption
#  and performance.
#
# STEAM2 model assumes a parabolic function for all engines [See article].

# Relative SFOC:
# SFOC_{relative} = 0.455*EL^2 - 0.71*EL + 1.28
# Where:
#	- EL: Engine load ranging from 0 to 1.

calcRelativeSFOC <- function(EL) {
	0.455*(EL^2) - 0.71*EL + 1.28
}

# Absolute SFOC:
# SFOC = SFOC_{relative}*SFOC_{base}
# Where:
#	- SFOC_{base}: base values for SFOC. Constant for each engine
#					Turbine machinery -> 260 g/kWh
#					Auxiliary machinery -> 220 g/kW
#					This is for simplicity as in Jalkanen 2012
#					TODO:
#						- We do not have SFOC from the engine manufacturers.
#						- Therefore, value is evaluated according to the IMO 
#							GHG2 report (Buhaug et al., 2009).


calcAbsoluteSFOC <- function(SFOCRelative, SFOCBase) {
	SFOCRelative*SFOCBase
}

