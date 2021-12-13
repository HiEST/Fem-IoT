# Kinematic Viscosity
# ITTC - Fresh water and seawater properties (2011)

temp <- 1:30
v <- c(1.7926E-06, 1.7341E-06, 1.6787E-06, 1.6262E-06, 1.5762E-06, 1.5288E-06, 1.4836E-06, 1.4406E-06, 1.3995E-06, 1.3604E-06, 1.3230E-06, 1.2873E-06, 1.2532E-06, 1.2205E-06, 1.1892E-06, 1.1592E-06, 1.1304E-06, 1.1028E-06, 1.0763E-06, 1.0508E-06, 1.0263E-06, 1.0027E-06, 9.8002E-07, 9.5818E-07, 9.3713E-07, 9.1683E-07, 8.9726E-07, 8.7837E-07, 8.6014E-07, 8.4253E-07)
kV <- data.frame(temperature=temp, kinematicViscosity=v)

plot(kV, type="l")
abline(v=14, col="red")
abline(v=25, col="red")

# Get Mediterraneo temperatures
med <- kV[kV$temperature >= 14 & kV$temperature <= 25,]

m <- mean(med$kinematicViscosity)
abline(h=m, col="blue")
