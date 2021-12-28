#!/usr/bin/end Rscript

library(readr)
library(dplyr)
library(tidyr)
library(magrittr)
library(ggplot2)

fp <- "calc_emis/emis_STEAM_94300f8bf795471c9fbd99f2a3011435.csv/part-00000-f1d612fa-0dec-4edd-8ff9-c8265933755b-c000.csv" 

pollutant_histogram <- function(df2) {
    df2 %>%
        group_by(pollutant) %>%
        summarise(grams = sum(grams)) %>%
        ggplot(aes(x = pollutant, y = grams)) +  geom_col(position = "dodge") + 
            ggtitle("Amount of emissions per pollutant")
}

df <- readr::read_csv(fp) %>%
    mutate(NOx = nox_me + nox_ae, SOx = sox_me + sox_ae, CO2 = co2_me + co2_ae) %>%
    mutate(NOx = NOx/60, SOx = SOx/60, CO2 = CO2/60) %>% # Unit change, g/h.
    mutate(trans_p_ae = trans_p_ae/60)                   # Unit change, kWh

# Preprocess
pvt <- df %>% 
    pivot_longer(c(NOx, SOx, CO2), names_to = "pollutant", values_to = "grams")



# Pollutants - Plot and aggregation

pvt %>%
    group_by(pollutant) %>%
    summarise(sum=sum(grams), max=max(grams), mean=mean(grams)) %T>%
    readr::write_csv("agg_pollution.csv")

pvt %>%
    pollutant_histogram %T>%
    ggsave(file="pollutant_histogram.png" , width=4, height=4)


# Power consumption at port - aggregation

df %>%
    filter(sog <= 0.5) %>%
    summarise(AuxPowPort=sum(trans_p_ae)) %>%
    readr::write_csv("agg_power_ae.csv")
