ships_data_catalog = ''.join("""
        {
            "table": {
                "namespace": "default",
                "name": "ships_data"
            },
            "rowkey": "id",
            "columns": {
                "id": {"cf":"rowkey", "col":"id", "type":"string"},
                "nombre": {"cf":"data", "col":"name", "type":"string"},
                "imo": {"cf":"data", "col":"imo", "type":"int"},
                "mmsi": {"cf":"data", "col":"mmsi", "type":"int"},
                "size_a": {"cf":"data", "col":"size_a", "type":"int"},
                "size_b": {"cf":"data", "col":"size_b", "type":"int"},
                "size_c": {"cf":"data", "col":"size_c", "type":"int"},
                "size_d": {"cf":"data", "col":"size_d", "type":"int"},
                "eslora": {"cf":"data", "col":"eslora", "type":"int"},
                "manga": {"cf":"data", "col":"manga", "type":"int"},
                "draught": {"cf":"data", "col":"draught", "type":"float"},
                "sog": {"cf":"data", "col":"sog", "type":"float"},
                "cog": {"cf":"data", "col":"cog", "type":"int"},
                "rot": {"cf":"data", "col":"rot", "type":"int"},
                "heading": {"cf":"data", "col":"heading", "type":"int"},
                "navstatus": {"cf":"data", "col":"navstatus", "type":"int"},
                "typeofshipandcargo": {"cf":"data", "col":"typeofshipandcargo", "type":"int"},
                "latitude": {"cf":"data", "col":"lat", "type":"float"},
                "longitude": {"cf":"data", "col":"lon", "type":"float"},
                "time": {"cf":"data", "col":"time", "type":"int"}
            }
        }
    """.split())

ihs_data_catalog = ''.join("""
        {
            "table": {
                "namespace": "default",
                "name": "ihs_data"
            },
            "rowkey": "LRIMOShipNO",
            "columns": {
                "LRIMOShipNO": {"cf":"rowkey", "col":"LRIMOShipNO", "type":"int"},
                "ShipName": {"cf":"data", "col":"ShipName", "type":"string"},
                "type": {"cf":"data", "col":"type", "type":"string"},
                "stroke": {"cf":"data", "col":"stroke", "type":"int"},
                "MainEngineRPM": {"cf":"data", "col":"MainEngineRPM", "type":"int"},
                "AuxiliaryEngineRPM": {"cf":"data", "col":"AuxiliaryEngineRPM", "type":"int"},
                "propellerRPM": {"cf":"data", "col":"propellerRPM", "type":"int"},
                "LOA": {"cf":"data", "col":"LOA", "type":"float"},
                "LBP": {"cf":"data", "col":"LBP", "type":"float"},
                "L": {"cf":"data", "col":"L", "type":"float"},
                "B": {"cf":"data", "col":"B", "type":"float"},
                "T": {"cf":"data", "col":"T", "type":"float"},
                "Los": {"cf":"data", "col":"Los", "type":"float"},
                "Lwl": {"cf":"data", "col":"Lwl", "type":"float"},
                "Ta": {"cf":"data", "col":"Ta", "type":"float"},
                "Tf": {"cf":"data", "col":"Tf", "type":"float"},
                "NScrew": {"cf":"data", "col":"NScrew", "type":"int"},
                "bulbousBow": {"cf":"data", "col":"bulbousBow", "type":"boolean"},
                "designDraft": {"cf":"data", "col":"designDraft", "type":"boolean"},
                "NRudd": {"cf":"data", "col":"NRudd", "type":"int"},
                "NBrac": {"cf":"data", "col":"NBrac", "type":"int"},
                "NBoss": {"cf":"data", "col":"NBoss", "type":"int"},
                "NThruster": {"cf":"data", "col":"NThruster", "type":"int"},
                "installedPowerME": {"cf":"data", "col":"installedPowerME", "type":"float"},
                "servicePowerME": {"cf":"data", "col":"servicePowerME", "type":"float"},
                "nInstalledME": {"cf":"data", "col":"nInstalledME", "type":"int"},
                "singleEnginePowerME": {"cf":"data", "col":"singleEnginePowerME", "type":"float"},
                "singleEngineServicePowerME": {"cf":"data", "col":"singleEngineServicePowerME", "type":"float"},
                "installedPowerAE": {"cf":"data", "col":"installedPowerAE", "type":"float"},
                "designSpeed": {"cf":"data", "col":"designSpeed", "type":"float"},
                "nRefrigeratedTEU": {"cf":"data", "col":"nRefrigeratedTEU", "type":"int"},
                "nCabins": {"cf":"data", "col":"nCabins", "type":"int"},
                "SFOCBaseME": {"cf":"data", "col":"SFOCBaseME", "type":"int"},
                "SFOCBaseAux": {"cf":"data", "col":"SFOCBaseAux", "type":"int"}
            }
        }
    """.split())

emissions_data_catalog = ''.join("""
        {
            "table": {
                "namespace": "default",
                "name": "emissions_data"
            },
            "rowkey": "id",
            "columns": {
                "id": {"cf":"rowkey", "col":"id", "type":"string"},
                "imo": {"cf":"data", "col":"imo", "type":"int"},
                "sog": {"cf":"data", "col":"sog", "type":"float"},
                "latitude": {"cf":"data", "col":"lat", "type":"float"},
                "longitude": {"cf":"data", "col":"lon", "type":"float"},
                "time": {"cf":"data", "col":"time", "type":"int"},
                "type": {"cf":"data", "col":"type", "type":"string"},
                "MainEngineRPM": {"cf":"data", "col":"MainEngineRPM", "type":"int"},
                "AuxiliaryEngineRPM": {"cf":"data", "col":"AuxiliaryEngineRPM", "type":"int"},
                "installedPowerME": {"cf":"data", "col":"installedPowerME", "type":"float"},
                "installedPowerAE": {"cf":"data", "col":"installedPowerAE", "type":"float"},
                "designSpeed": {"cf":"data", "col":"designSpeed", "type":"float"},
                "transPME": {"cf": "data", "col": "transPME", "type": "float"},
                "transPAE": {"cf": "data", "col": "transPAE", "type": "float"},
                "SOxFactME": {"cf": "data", "col": "SOxFactME", "type": "float"},
                "SOxFactAE": {"cf": "data", "col": "SOxFactAE", "type": "float"},
                "CO2Fact": {"cf": "data", "col": "CO2Fact", "type": "float"},
                "NOxFactME": {"cf": "data", "col": "NOxFactME", "type": "float"},
                "NOxFactAE": {"cf": "data", "col": "NOxFactAE", "type": "float"},
                "SOxME": {"cf": "data", "col": "SOxME", "type": "float"},
                "SOxAE": {"cf": "data", "col": "SOxAE", "type": "float"},
                "CO2ME": {"cf": "data", "col": "CO2ME", "type": "float"},
                "CO2AE": {"cf": "data", "col": "CO2AE", "type": "float"},
                "NOxME": {"cf": "data", "col": "NOxME", "type": "float"},
                "NOxAE": {"cf": "data", "col": "NOxAE", "type": "float"}
            }
        }
    """.split())

rasters_data_catalog = ''.join("""
        {
            "table": {
                "namespace": "default",
                "name": "rasters_data"
            },
            "rowkey": "id",
            "columns": {
                "id": {"cf":"rowkey", "col":"id", "type":"string"},
                "hour": {"cf":"data", "col":"hour", "type":"int"},
                "cell": {"cf":"data", "col":"cell", "type":"int"},
                "sample_count": {"cf":"data", "col":"sample_count", "type":"int"},
                "SOxME": {"cf":"data", "col":"SOxME", "type":"float"},
                "SOxAE": {"cf":"data", "col":"SOxAE", "type":"float"},
                "CO2ME": {"cf":"data", "col":"CO2ME", "type":"float"},
                "CO2AE": {"cf":"data", "col":"CO2AE", "type":"float"},
                "NOxME": {"cf":"data", "col":"NOxME", "type":"float"},
                "NOxAE": {"cf":"data", "col":"NOxAE", "type":"float"}
            }
        }
    """.split())
