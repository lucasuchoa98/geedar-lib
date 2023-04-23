from fastkml import kml
import pandas as pd
import ee

ee.Initialize() #realmente necessário?

# Dictionary for the GEEDaR products.
## Product ID format: FP
### F: sensor "Family" (1 = MODIS, 2 = Sentinel, 3 = Landsat, 4 = VIIRS...)
### P: product (e.g. 01 = MOD09GA, 02 = MYD09GA, 03 = MOD09GQ, ...)
PRODUCT_SPECS = {
    101: {
        "productName": "MOD09GA",
        "sensor": "MODIS/Terra",
        "description": "Daily MODIS/Terra 500-m images, bands 1-7, processing version 6",
        "collectionID": ["MODIS/006/MOD09GA"],
        "collection": ee.ImageCollection("MODIS/006/MOD09GA"),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 500,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QC_500m", "state_1km", "SensorZenith", "SensorAzimuth", "SolarZenith", "SolarAzimuth"],
        "qaLayer": ["state_1km"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    102: {
        "productName": "MYD09GA",
        "sensor": "MODIS/Aqua",
        "description": "Daily MODIS/Aqua 500-m images, bands 1-7, processing version 6",
        "collectionID": ["MODIS/006/MYD09GA"],
        "collection": ee.ImageCollection("MODIS/006/MYD09GA"),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 500,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QC_500m", "state_1km", "SensorZenith", "SensorAzimuth", "SolarZenith", "SolarAzimuth"],
        "qaLayer": ["state_1km"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    103: {
        "productName": "MOD09GQ",
        "sensor": "MODIS/Terra",
        "description": "Daily MODIS/Terra 250-m images, bands 1-2, processing version 6",
        "collectionID": ["MODIS/006/MOD09GQ"],
        "collection": ee.ImageCollection("MODIS/006/MOD09GQ"),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "QC_250m"],
        "qaLayer": [],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(2)],
        "commonBands": {"blue": -1, "green": -1, "red": 0, "NIR": 1, "SWIR": -1, "wl400": -1, "wl440": -1, "wl490": -1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": -1, "wl1500": -1, "wl2000": -1, "wl10500": -1, "wl11500": -1}
    },
    104: {
        "productName": "MYD09GQ",
        "sensor": "MODIS/Aqua",
        "description": "Daily MODIS/Aqua 250-m images, bands 1-2, processing version 6",
        "collectionID": ["MODIS/006/MYD09GQ"],
        "collection": ee.ImageCollection("MODIS/006/MYD09GQ"),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "QC_250m"],
        "qaLayer": [],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(2)],
        "commonBands": {"blue": -1, "green": -1, "red": 0, "NIR": 1, "SWIR": -1, "wl400": -1, "wl440": -1, "wl490": -1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": -1, "wl1500": -1, "wl2000": -1, "wl10500": -1, "wl11500": -1}
    },
    105: {
        "productName": "MOD09GAGQ",
        "sensor": "MODIS/Terra",
        "description": "Daily MODIS/Terra images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MOD09GA", "MODIS/006/MOD09GQ"],
        "collection": ee.ImageCollection("MODIS/006/MOD09GA").combine(ee.ImageCollection("MODIS/006/MOD09GQ"), True),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QC_500m", "state_1km", "SensorZenith", "SensorAzimuth", "SolarZenith", "SolarAzimuth", "QC_250m"],
        "qaLayer": ["state_1km"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    106: {
        "productName": "MYD09GAGQ",
        "sensor": "MODIS/Aqua",
        "description": "Daily MODIS/Aqua images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MYD09GA", "MODIS/006/MYD09GQ"],
        "collection": ee.ImageCollection("MODIS/006/MYD09GA").combine(ee.ImageCollection("MODIS/006/MYD09GQ"), True),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QC_500m", "state_1km", "SensorZenith", "SensorAzimuth", "SolarZenith", "SolarAzimuth", "QC_250m"],
        "qaLayer": ["state_1km"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    107: {
        "productName": "MODMYD09GAGQ",
        "sensor": "MODIS/Terra&Aqua",
        "description": "Combined MODIS/Aqua and MODIS/Terra daily images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MOD09GA", "MODIS/006/MOD09GQ", "MODIS/006/MYD09GA", "MODIS/006/MYD09GQ"],
        "collection": ee.ImageCollection("MODIS/006/MOD09GA").combine(ee.ImageCollection("MODIS/006/MOD09GQ"), True).merge(ee.ImageCollection("MODIS/006/MYD09GA").combine(ee.ImageCollection("MODIS/006/MYD09GQ"), True)).sort('system:time_start'),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QC_500m", "state_1km", "SensorZenith", "SensorAzimuth", "SolarZenith", "SolarAzimuth"],
        "qaLayer": ["state_1km"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    111: {
        "productName": "MOD09A1",
        "sensor": "MODIS/Terra",
        "description": "8-day composite MODIS/Terra 500-m images, bands 1-7, processing version 6",
        "collectionID": ["MODIS/006/MOD09A1"],
        "collection": ee.ImageCollection("MODIS/006/MOD09A1"),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 500,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QA", "SolarZenith", "ViewZenith", "RelativeAzimuth", "StateQA", "DayOfYear"],
        "qaLayer": ["StateQA"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    112: {
        "productName": "MYD09A1",
        "sensor": "MODIS/Aqua",
        "description": "8-day composite MODIS/Aqua 500-m images, bands 1-7, processing version 6",
        "collectionID": ["MODIS/006/MYD09A1"],
        "collection": ee.ImageCollection("MODIS/006/MYD09A1"),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 500,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QA", "SolarZenith", "ViewZenith", "RelativeAzimuth", "StateQA", "DayOfYear"],
        "qaLayer": ["StateQA"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    113: {
        "productName": "MOD09Q1",
        "sensor": "MODIS/Terra",
        "description": "8-day composite MODIS/Terra 250-m images, bands 1-2, processing version 6",
        "collectionID": ["MODIS/006/MOD09Q1"],
        "collection": ee.ImageCollection("MODIS/006/MOD09Q1"),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "State", "QA"],
        "qaLayer": ["State"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(2)],
        "commonBands": {"blue": -1, "green": -1, "red": 0, "NIR": 1, "SWIR": -1, "wl400": -1, "wl440": -1, "wl490": -1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": -1, "wl1500": -1, "wl2000": -1, "wl10500": -1, "wl11500": -1}
    },
    114: {
        "productName": "MYD09Q1",
        "sensor": "MODIS/Aqua",
        "description": "8-day composite MODIS/Aqua 250-m images, bands 1-2, processing version 6",
        "collectionID": ["MODIS/006/MYD09Q1"],
        "collection": ee.ImageCollection("MODIS/006/MYD09Q1"),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "State", "QA"],
        "qaLayer": ["State"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(2)],
        "commonBands": {"blue": -1, "green": -1, "red": 0, "NIR": 1, "SWIR": -1, "wl400": -1, "wl440": -1, "wl490": -1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": -1, "wl1500": -1, "wl2000": -1, "wl10500": -1, "wl11500": -1}
    },
    115: {
        "productName": "MOD09A1Q1",
        "sensor": "MODIS/Terra",
        "description": "8-day composite MODIS/Terra images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MOD09A1", "MODIS/006/MOD09Q1"],
        "collection": ee.ImageCollection("MODIS/006/MOD09A1").combine(ee.ImageCollection("MODIS/006/MOD09Q1"), True),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QA", "SolarZenith", "ViewZenith", "RelativeAzimuth", "StateQA", "DayOfYear", "State"],
        "qaLayer": ["State"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    116: {
        "productName": "MYD09A1Q1",
        "sensor": "MODIS/Aqua",
        "description": "8-day composite MODIS/Aqua images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MYD09A1", "MODIS/006/MYD09Q1"],
        "collection": ee.ImageCollection("MODIS/006/MYD09A1").combine(ee.ImageCollection("MODIS/006/MYD09Q1"), True),
        "startDate": "2002-07-04",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QA", "SolarZenith", "ViewZenith", "RelativeAzimuth", "StateQA", "DayOfYear", "State"],
        "qaLayer": ["State"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    117: {
        "productName": "MODMYD09A1Q1",
        "sensor": "MODIS/Terra&Aqua",
        "description": "Combined MODIS/Aqua and MODIS/Terra 8-day composite images, with bands 1-2 in 250 m and bands 3-7 in 500 m, processing version 6",
        "collectionID": ["MODIS/006/MOD09A1", "MODIS/006/MOD09Q1", "MODIS/006/MYD09A1", "MODIS/006/MYD09Q1"],
        "collection": ee.ImageCollection("MODIS/006/MOD09A1").combine(ee.ImageCollection("MODIS/006/MOD09Q1"), True).merge(ee.ImageCollection("MODIS/006/MYD09A1").combine(ee.ImageCollection("MODIS/006/MYD09Q1"), True)).sort('system:time_start'),
        "startDate": "2000-02-24",
        "scaleRefBand": "sur_refl_b01",
        "roughScale": 250,
        "bandList": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "sur_refl_b04", "sur_refl_b05", "sur_refl_b06", "sur_refl_b07", "QA", "SolarZenith", "ViewZenith", "RelativeAzimuth", "StateQA", "DayOfYear", "State"],
        "qaLayer": ["State"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 2, "green": 3, "red": 0, "NIR": 1, "SWIR": 5, "wl400": -1, "wl440": -1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 1, "wl900": -1, "wl1200": 4, "wl1500": 5, "wl2000": 6, "wl10500": -1, "wl11500": -1}
    },
    151: {
        "productName": "VNP09GA",
        "sensor": "VIIRS",
        "description": "VIIRS Surface Reflectance Daily with standard blue, green, red and NIR bands set to M3, M4 (1 km), I1 and I2 (500m).",
        "collectionID": ["NOAA/VIIRS/001/VNP09GA"],
        "collection": ee.ImageCollection("NOAA/VIIRS/001/VNP09GA"),
        "startDate": "2012-01-19",
        "scaleRefBand": "I1",
        "roughScale": 500,
        "bandList": ["M1", "M2", "M3", "M4", "M5", "M7", "M8", "M10", "M11", "I1", "I2", "I3", "SensorAzimuth", "SensorZenith", "SolarAzimuth", "SolarZenith", "iobs_res", "num_observations_1km", "num_observations_500m", "obscov_1km", "obscov_500m", "orbit_pnt", "QF1", "QF2", "QF3", "QF4", "QF5", "QF6", "QF7"],
        "qaLayer": ["QF1", "QF2"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(12)],
        "commonBands": {"blue": 2, "green": 3, "red": 9, "NIR": 10, "SWIR": 11, "wl400": 0, "wl440": 1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": 4, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 10, "wl900": -1, "wl1200": 6, "wl1500": 11, "wl2000": 8, "wl10500": -1, "wl11500": -1}
    },
    152: {
        "productName": "VNP09GA",
        "sensor": "VIIRS",
        "description": "VIIRS Surface Reflectance Daily with standard blue, green, red and NIR bands set to M3, M4, M5 and M10 (1 km).",
        "collectionID": ["NOAA/VIIRS/001/VNP09GA"],
        "collection": ee.ImageCollection("NOAA/VIIRS/001/VNP09GA"),
        "startDate": "2012-01-19",
        "scaleRefBand": "M5",
        "roughScale": 1000,
        "bandList": ["M1", "M2", "M3", "M4", "M5", "M7", "M8", "M10", "M11", "SensorAzimuth", "SensorZenith", "SolarAzimuth", "SolarZenith", "iobs_res", "num_observations_1km", "num_observations_500m", "obscov_1km", "obscov_500m", "orbit_pnt", "QF1", "QF2", "QF3", "QF4", "QF5", "QF6", "QF7"],
        "qaLayer": ["QF1", "QF2"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(9)],
        "commonBands": {"blue": 2, "green": 3, "red": 4, "NIR": 5, "SWIR": 7, "wl400": 0, "wl440": 1, "wl490": 2, "wl620": -1, "wl665": -1, "wl675": 4, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 5, "wl900": -1, "wl1200": 6, "wl1500": 7, "wl2000": 8, "wl10500": -1, "wl11500": -1}
    },
    201: {
        "productName": "S2_L2A",
        "sensor": "MSI/Sentinel-2",
        "description": "Sentinel-2 L2A images provided by ESA.",
        "collectionID": ["COPERNICUS/S2_SR_HARMONIZED"],
        "collection": ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED"),
        "startDate": "2015-06-27",
        "scaleRefBand": "B4",
        "roughScale": 10,
        "bandList": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12", "AOT", "WVP", "SCL", "TCI_R", "TCI_G", "TCI_B", "MSK_CLDPRB", "MSK_SNWPRB", "QA10", "QA20", "QA60"],
        "qaLayer": ["SCL"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(12)],
        "commonBands": {"blue": 1, "green": 2, "red": 3, "NIR": 7, "SWIR": 10, "wl400": -1, "wl440": 0, "wl490": 1, "wl620": -1, "wl665": 3, "wl675": -1, "wl680": -1, "wl705": 4, "wl740": 5, "wl780": 6, "wl800": 8, "wl900": 9, "wl1200": -1, "wl1500": 10, "wl2000": 11, "wl10500": -1, "wl11500": -1}
    },
    202: {
        "productName": "S2_L1C",
        "sensor": "MSI/Sentinel-2",
        "description": "Sentinel-2 L1C images provided by ESA.",
        "collectionID": ["COPERNICUS/S2_HARMONIZED"],
        "collection": ee.ImageCollection("COPERNICUS/S2_HARMONIZED"),
        "startDate": "2015-06-27",
        "scaleRefBand": "B4",
        "roughScale": 10,
        "bandList": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12", "QA10", "QA20", "QA60"],
        "qaLayer": ["QA60"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(12)],
        "commonBands": {"blue": 1, "green": 2, "red": 3, "NIR": 7, "SWIR": 10, "wl400": -1, "wl440": 0, "wl490": 1, "wl620": -1, "wl665": 3, "wl675": -1, "wl680": -1, "wl705": 4, "wl740": 5, "wl780": 6, "wl800": 8, "wl900": 9, "wl1200": -1, "wl1500": 10, "wl2000": 11, "wl10500": -1, "wl11500": -1}
    },
    301: {
        "productName": "Landsat_5_SR_Collection1",
        "sensor": "TM/Landsat 5",
        "description": "Landsat 5 surface reflectance images (Collection 1).",
        "collectionID": ["LANDSAT/LT05/C01/T1_SR", "LANDSAT/LT05/C01/T2_SR"],
        "collection": ee.ImageCollection("LANDSAT/LT05/C01/T1_SR").merge(ee.ImageCollection("LANDSAT/LT05/C01/T2_SR")),
        "startDate": "1984-03-16",
        "scaleRefBand": "B3",
        "roughScale": 30,
        "bandList": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "sr_atmos_opacity", "sr_cloud_qa", "pixel_qa", "radsat_qa"],
        "qaLayer": ["pixel_qa"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 0, "green": 1, "red": 2, "NIR": 3, "SWIR": 4, "wl400": -1, "wl440": -1, "wl490": 0, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 3, "wl900": -1, "wl1200": -1, "wl1500": 4, "wl2000": 6, "wl10500": -1, "wl11500": 5}
    },
    302: {
        "productName": "Landsat_7_SR_Collection1",
        "sensor": "ETM+/Landsat 7",
        "description": "Landsat 7 surface reflectance images (Collection 1).",
        "collectionID": ["LANDSAT/LE07/C01/T1_SR", "LANDSAT/LE07/C01/T2_SR"],
        "collection": ee.ImageCollection("LANDSAT/LE07/C01/T1_SR").merge(ee.ImageCollection("LANDSAT/LE07/C01/T2_SR")),
        "startDate": "1999-05-28",
        "scaleRefBand": "B3",
        "roughScale": 30,
        "bandList": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "sr_atmos_opacity", "sr_cloud_qa", "pixel_qa", "radsat_qa"],
        "qaLayer": ["pixel_qa"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(7)],
        "commonBands": {"blue": 0, "green": 1, "red": 2, "NIR": 3, "SWIR": 4, "wl400": -1, "wl440": -1, "wl490": 0, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 3, "wl900": -1, "wl1200": -1, "wl1500": 4, "wl2000": 6, "wl10500": -1, "wl11500": 5}
    },
    303: {
        "productName": "Landsat_8_SR_Collection1",
        "sensor": "OLI/Landsat 8",
        "description": "Landsat 8 surface reflectance images (Collection 1).",
        "collectionID": ["LANDSAT/LC08/C01/T1_SR", "LANDSAT/LC08/C01/T2_SR"],
        "collection": ee.ImageCollection("LANDSAT/LC08/C01/T1_SR").merge(ee.ImageCollection("LANDSAT/LC08/C01/T2_SR")),
        "startDate": "2013-03-18",
        "scaleRefBand": "B4",
        "roughScale": 30,
        "bandList": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B10", "B11", "sr_aerosol", "pixel_qa", "radsat_qa"],
        "qaLayer": ["pixel_qa", "sr_aerosol"],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [*range(9)],
        "commonBands": {"blue": 1, "green": 2, "red": 3, "NIR": 4, "SWIR": 5, "wl400": -1, "wl440": 0, "wl490": 1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 4, "wl900": -1, "wl1200": -1, "wl1500": 5, "wl2000": 6, "wl10500": 7, "wl11500": 8}
    },
    311: {
        "productName": "Landsat_4_L2_Collection2",
        "sensor": "TM/Landsat 4",
        "description": "Landsat 4 surface reflectance images (Collection 2) provided by the USGS.",
        "collectionID": ["LANDSAT/LT04/C02/T1_L2", "LANDSAT/LT04/C02/T2_L2"],
        "collection": ee.ImageCollection("LANDSAT/LT04/C02/T1_L2").merge(ee.ImageCollection("LANDSAT/LT04/C02/T2_L2")),
        "startDate": "1982-08-22",
        "scaleRefBand": "SR_B3",
        "roughScale": 30,
        "bandList": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "SR_ATMOS_OPACITY", "SR_CLOUD_QA", "ST_B6", "ST_ATRAN", "ST_CDIST", "ST_DRAD", "ST_EMIS", "ST_EMSD", "ST_QA", "ST_TRAD", "ST_URAD", "QA_PIXEL", "QA_RADSAT"],
        "qaLayer": ["QA_PIXEL"],
        "scalingFactor": [0.275] * 6 + [0.001, 1, 0.00341802, 0.0001, 0.01, 0.001, 0.0001, 0.0001, 0.01, 0.001, 0.001, 1, 1],
        "offset": [-2000] * 6 + [0, 0, -124, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "spectralBandInds": [*range(6)] + [8],
        "commonBands": {"blue": 0, "green": 1, "red": 2, "NIR": 3, "SWIR": 4, "wl400": -1, "wl440": -1, "wl490": 0, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 3, "wl900": -1, "wl1200": -1, "wl1500": 4, "wl2000": 5, "wl10500": -1, "wl11500": 8}
    },
    312: {
        "productName": "Landsat_5_L2_Collection2",
        "sensor": "TM/Landsat 5",
        "description": "Landsat 5 surface reflectance images (Collection 2) provided by the USGS.",
        "collectionID": ["LANDSAT/LT05/C02/T1_L2", "LANDSAT/LT05/C02/T2_L2"],
        "collection": ee.ImageCollection("LANDSAT/LT05/C02/T1_L2").merge(ee.ImageCollection("LANDSAT/LT05/C02/T2_L2")),
        "startDate": "1984-03-16",
        "scaleRefBand": "SR_B3",
        "roughScale": 30,
        "bandList": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "SR_ATMOS_OPACITY", "SR_CLOUD_QA", "ST_B6", "ST_ATRAN", "ST_CDIST", "ST_DRAD", "ST_EMIS", "ST_EMSD", "ST_QA", "ST_TRAD", "ST_URAD", "QA_PIXEL", "QA_RADSAT"],
        "qaLayer": ["QA_PIXEL"],
        "scalingFactor": [0.275] * 6 + [0.001, 1, 0.00341802, 0.0001, 0.01, 0.001, 0.0001, 0.0001, 0.01, 0.001, 0.001, 1, 1],
        "offset": [-2000] * 6 + [0, 0, -124, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "spectralBandInds": [*range(6)] + [8],
        "commonBands": {"blue": 0, "green": 1, "red": 2, "NIR": 3, "SWIR": 4, "wl400": -1, "wl440": -1, "wl490": 0, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 3, "wl900": -1, "wl1200": -1, "wl1500": 4, "wl2000": 5, "wl10500": -1, "wl11500": 8}
    },
    313: {
        "productName": "Landsat_7_L2_Collection2",
        "sensor": "ETM+/Landsat 7",
        "description": "Landsat 7 surface reflectance images (Collection 2) provided by the USGS.",
        "collectionID": ["LANDSAT/LE07/C02/T1_L2", "LANDSAT/LE07/C02/T2_L2"],
        "collection": ee.ImageCollection("LANDSAT/LE07/C02/T1_L2").merge(ee.ImageCollection("LANDSAT/LE07/C02/T2_L2")),
        "startDate": "1999-05-28",
        "scaleRefBand": "SR_B3",
        "roughScale": 30,
        "bandList": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "SR_ATMOS_OPACITY", "SR_CLOUD_QA", "ST_B6", "ST_ATRAN", "ST_CDIST", "ST_DRAD", "ST_EMIS", "ST_EMSD", "ST_QA", "ST_TRAD", "ST_URAD", "QA_PIXEL", "QA_RADSAT"],
        "qaLayer": ["QA_PIXEL"],
        "scalingFactor": [0.275] * 6 + [0.001, 1, 0.00341802, 0.0001, 0.01, 0.001, 0.0001, 0.0001, 0.01, 0.001, 0.001, 1, 1],
        "offset": [-2000] * 6 + [0, 0, -124, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "spectralBandInds": [*range(6)] + [8],
        "commonBands": {"blue": 0, "green": 1, "red": 2, "NIR": 3, "SWIR": 4, "wl400": -1, "wl440": -1, "wl490": 0, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 3, "wl900": -1, "wl1200": -1, "wl1500": 4, "wl2000": 5, "wl10500": -1, "wl11500": 8}
    },
    314: {
        "productName": "Landsat_8_L2_Collection2",
        "sensor": "OLI/Landsat 8",
        "description": "Landsat 8 surface reflectance images (Collection 2) provided by the USGS.",
        "collectionID": ["LANDSAT/LC08/C02/T1_L2","LANDSAT/LC08/C02/T2_L2"],
        "collection": ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").merge(ee.ImageCollection("LANDSAT/LC08/C02/T2_L2")),
        "startDate": "2013-03-18",
        "scaleRefBand": "SR_B4",
        "roughScale": 30,
        "bandList": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7", "SR_QA_AEROSOL", "ST_B10", "ST_ATRAN", "ST_CDIST", "ST_DRAD", "ST_EMIS", "ST_EMSD", "ST_QA", "ST_TRAD", "ST_URAD", "QA_PIXEL", "QA_RADSAT"],
        "qaLayer": ["QA_PIXEL","SR_QA_AEROSOL"],
        "scalingFactor": [0.275] * 7 + [1, 0.00341802, 0.0001, 0.01, 0.001, 0.0001, 0.0001, 0.01, 0.001, 0.001, 1, 1],
        "offset": [-2000] * 7 + [0, -124, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "spectralBandInds": [*range(7)] + [8],
        "commonBands": {"blue": 1, "green": 2, "red": 3, "NIR": 4, "SWIR": 5, "wl400": -1, "wl440": 0, "wl490": 1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 4, "wl900": -1, "wl1200": -1, "wl1500": 5, "wl2000": 6, "wl10500": 8, "wl11500": -1}
    },
    315: {
        "productName": "Landsat_9_L2_Collection2",
        "sensor": "OLI/Landsat 9",
        "description": "Landsat 8 surface reflectance images (Collection 2) provided by the USGS.",
        "collectionID": ["LANDSAT/LC09/C02/T1_L2","LANDSAT/LC09/C02/T2_L2"],
        "collection": ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").merge(ee.ImageCollection("LANDSAT/LC09/C02/T2_L2")),
        "startDate": "2021-10-31",
        "scaleRefBand": "SR_B4",
        "roughScale": 30,
        "bandList": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7", "SR_QA_AEROSOL", "ST_B10", "ST_ATRAN", "ST_CDIST", "ST_DRAD", "ST_EMIS", "ST_EMSD", "ST_QA", "ST_TRAD", "ST_URAD", "QA_PIXEL", "QA_RADSAT"],
        "qaLayer": ["QA_PIXEL","SR_QA_AEROSOL"],
        "scalingFactor": [0.275] * 7 + [1, 0.00341802, 0.0001, 0.01, 0.001, 0.0001, 0.0001, 0.01, 0.001, 0.001, 1, 1],
        "offset": [-2000] * 7 + [0, -124, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "spectralBandInds": [*range(7)] + [8],
        "commonBands": {"blue": 1, "green": 2, "red": 3, "NIR": 4, "SWIR": 5, "wl400": -1, "wl440": 0, "wl490": 1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": 4, "wl900": -1, "wl1200": -1, "wl1500": 5, "wl2000": 6, "wl10500": 8, "wl11500": -1}
    },
    901: {
        "productName": "GPM_Precipitation_v6",
        "sensor": "GPM",
        "description": "Global Precipitation Measurement (GPM) v6.",
        "collectionID": ["NASA/GPM_L3/IMERG_V06"],
        "collection": ee.ImageCollection("NASA/GPM_L3/IMERG_V06"),
        "startDate": "2000-06-01",
        "scaleRefBand": "precipitationCal",
        "roughScale": 11132,
        "bandList": ["HQobservationTime", "HQprecipSource", "HQprecipitation", "IRkalmanFilterWeight", "IRprecipitation", "precipitationCal", "precipitationUncal", "probabilityLiquidPrecipitation", "randomError"],
        "qaLayer": [],
        "scalingFactor": None,
        "offset": None,
        "spectralBandInds": [5],
        "commonBands": {"blue": -1, "green": -1, "red": -1, "NIR": -1, "SWIR": -1, "wl400": -1, "wl440": -1, "wl490": -1, "wl620": -1, "wl665": -1, "wl675": -1, "wl680": -1, "wl705": -1, "wl740": -1, "wl780": -1, "wl800": -1, "wl900": -1, "wl1200": -1, "wl1500": -1, "wl2000": -1, "wl10500": -1, "wl11500": -1}
    }
}
AVAILABLE_PRODUCTS = [*PRODUCT_SPECS]


# Image processing (atmospheric correction and unwanted pixels' exclusion) algorithms:
IMG_PROC_ALGO_SPECS = {
    0: {
        "name": "None",
        "description": "This algorithm makes no change to the image data.",
        "ref": "",
        "nSimImgs": 500,
        "applicableTo": AVAILABLE_PRODUCTS
    },
    1: {
        "name": "StdCloudMask",
        "description": "This algorithm removes pixels with cloud, cloud shadow or high aerosol, based on the product's pixel quality layer. It works better for Modis and Landsat.",
        "ref": "",
        "nSimImgs": 500, # confirm it!
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152,201,202,301,302,303,311,312,313,314,315]
    },
    2: {
        "name": "MOD3R",
        "description": "This algorithm replicates, to the possible extent, the MOD3R algorithm, developed by researchers from the IRD French institute.",
        "ref": "Espinoza-Villar, R. 2013. Suivi de la dynamique spatiale et temporelle des flux se´dimentaires dans le bassin de l’Amazone a` partir d’images satellite. PhD thesis, Université Toulouse III - Paul Sabatier, Toulouse, France.",
        "nSimImgs": 40,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    3: {
        "name": "MOD3R_minNDVI",
        "description": "It is a modification of the MOD3R algorithm, defining as the water-representative cluster the one with the lowest NDVI.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 60,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    4: {
        "name": "MOD3R_minIR",
        "description": "It is a modification of the MOD3R algorithm, defining as the water-representative cluster the one with the lowest reflectance in the near infrared.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 60,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    5: {
        "name": "Ventura2018",
        "description": "It is simply a threshold (400) in the near infrared.",
        "ref": "VENTURA, D.L.T. 2018. Water quality and temporal dynamics of the phytoplankton biomass in man-made lakes of the Brazilian semiarid region: an optical approach. Thesis. University of Brasilia.",
        "nSimImgs": 500, # test it!
        "applicableTo": [*range(100, 120)] + [151,152]
    },
    6: {
        "name": "S2WP_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the water pixels not affected by cloud, cirrus, shadow, sunglint and adjacency effects. It selects both 'bright' and 'dark' water pixels. The latter may incorrectly include shaded water pixels.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    7: {
        "name": "S2WP_Bright_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the 'bright' water pixels (which includes most types of water) not affected by cloud, cirrus, shadow, sunglint and adjacency effects. 'Dark' water pixels, which may me mixed with shaded water pixels, are excluded.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    8: {
        "name": "S2WP_Dark_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the 'dark' water pixels (such as waters rich in dissolved organic compounds) not affected by cloud, cirrus, sunglint and adjacency effects. 'Dark' water pixels may me mixed with shaded water pixels.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    9: {
        "name": "S2WP_v7",
        "description": "Selects, on an atmospherically corrected Sentinel-2 or Landsat image, the water pixels not affected by cloud, cirrus and sunglint, as well as pixels not strongly affected by shadow, aerosol and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 120,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    10: {
        "name": "S2WP_v7_MODIS",
        "description": "Selects, on an atmospherically corrected Modis image, the water pixels not affected by cloud, cirrus and sunglint, as well as pixels not strongly affected by shadow, aerosol and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    11: {
        "name": "RICO",
        "description": "For products with only the red and NIR bands, selects water pixels. Not appropriate for eutrophic conditions or for extreme inorganic turbidity.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 30,
        "applicableTo": [101,102,103,104,105,106,107,111,112,113,114,115,116,117,151,152,201,202,301,302,303,311,312,313,314,315]
    },
    12: {
        "name": "S2WP_v8",
        "description": "Selects, on an atmospherically corrected image, the water pixels unaffected by cloud, cirrus, sunglint, aerosol, shadow and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 120,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    13: {
        "name": "minNDVI + Wang2016",
        "description": "Selects the pixel cluster with the lowest NDVI and reduces reflectance noise by subtracting the minimum value in the NIR-SWIR range from all bands, excluding pixels with high NIR or SWIR.",
        "ref": "WANG, S. et al. 2016. A simple correction method for the MODIS surface reflectance product over typical inland waters in China. Int. J. Remote Sens. 37 (24), 6076–6096.",
        "nSimImgs": 30,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    14: {
        "name": "GPM daily precipitation",
        "description": "Average the calibrated precipitation in 24 hours inside the area of interest.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 48,
        "applicableTo": [901]
    }
}
IMG_PROC_ALGO_LIST = [*IMG_PROC_ALGO_SPECS]


ESTIMATION_ALGO_SPECS = {
    0: {
        "name": "None",
        "description": "This algorithm makes no calculations and no changes to the images.",
        "model": "",
        "ref": "",
        "paramName": [""],
        "requiredBands": []
    },
    1: {
        "name": "Former HidroSat chla",
        "description": "Estima a concentração de clorofila (ug/L) em açudes do Semiárido.",
        "model": "4.3957 + 0.213*(R - R^2/G) + 0.0004*(R - R^2/G)^2",
        "ref": "",
        "paramName": ["chla_surf"],
        "requiredBands": ["red", "green"]
    },
    2: {
        "name": "SSS Solimões",
        "description": "Estimates the surface suspended solids concentration in the Solimões River.",
        "model": "759.12*(NIR/red)^1.9189",
        "ref": "Villar, R.E.; Martinez, J.M; Armijos, E.; Espinoza, J.C.; Filizola, N.; Dos Santos, A.; Willems, B.; Fraizy, P.; Santini, W.; Vauchel, P. Spatio-temporal monitoring of suspended sediments in the Solimoes River (2000-2014). Comptes Rendus Geoscience, v. 350, n. 1-2, p. 4-12, 2018.",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "NIR"]
    },
    3: {
        "name": "SSS Madeira",
        "description": "Estimates the surface suspended solids concentration in the Madeira River.",
        "model": "1020*(NIR/red)^2.94",
        "ref": "Villar, R.E.; Martinez, J.M.; Le Texier, M.; Guyot, J.L.; Fraizy, P.; Meneses, P.R.; Oliveira, E. A study of sediment transport in the Madeira River, Brazil, using MODIS remote-sensing images. Journal of South American Earth Sciences, v. 44, p. 45-54, 2013.",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "NIR"]
    },
    4: {
        "name": "SSS Óbidos",
        "description": "Estimates the surface suspended solids concentration in the Amazon River, near Óbidos.",
        "model": "0.2019*NIR - 14.222",
        "ref": "Martinez, J. M.; Guyot, J.L.; Filizola, N.; Sondag, F. Increase in suspended sediment discharge of the Amazon River assessed by monitoring network and satellite data. Catena, v. 79, n. 3, p. 257-264, 2009.",
        "paramName": ["SS_surf"],
        "requiredBands": ["NIR"]
    },
    5: {
        "name": "Turb Paranapanema",
        "description": "Estimates the surface turbidity in reservoirs along the Paranapnema river.",
        "model": "2.45*EXP(0.00223*red)",
        "ref": "Condé, R.C.; Martinez, J.M.; Pessotto, M.A.; Villar, R.; Cochonneau, G.; Henry, R.; Lopes, W.; Nogueira, M. Indirect Assessment of Sedimentation in Hydropower Dams Using MODIS Remote Sensing Images. Remote Sensing, v.11, n. 3, 2019.",
        "paramName": ["Turb_surf"],
        "requiredBands": ["red"]
    },
    10: {
        "name": "Brumadinho_2020simp",
        "description": "Estimates the surface suspended solids concentration in the Paraopeba River, accounting for the presence of mining waste after the 2019 disaster.",
        "model": "more than one",
        "ref": "VENTURA, 2020 (Unpublished).",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "green", "NIR"]
    },
    11: {
        "name": "Açudes SSS-ISS-OSS-Chla",
        "description": "Estimates four parameters for the waters of Brazilian semiarid reservoirs: surface suspended solids, its organic and inorganic fractions, and chlorophyll-a.",
        "model": "more than one",
        "ref": "VENTURA, 2020 (Unpublished).",
        "paramName": ["SS_surf","ISS_surf","OSS_surf","chla_surf","biomass_surf"],
        "requiredBands": ["blue", "green", "red", "NIR"]
    },
    12: {
        "name": "Açudes Chla 2022",
        "description": "Estimates chlorophyll-a in Brazilian semiarid reservoirs.",
        "model": "-4.227 + 0.1396*G + -0.1006*R",
        "ref": "VENTURA, 2022 (Unpublished).",
        "paramName": ["chla_surf"],
        "requiredBands": ["green", "red"]
    },
    99: {
        "name": "Test",
        "description": "This algorithm is for test only. It adds a band 'turb_surf' with a constant value of 1234.",
        "ref": "",
        "model": "",
        "paramName": ["turb_surf"],
        "requiredBands": ["red", "NIR"]
    }
}
ESTIMATION_ALGO_LIST = [*ESTIMATION_ALGO_SPECS]


REDUCTION_SPECS = {
    0: {
        "description": "none",
        "sufix": [""]
    },
    1: {
        "description": "median",
        "sufix": ["median"]
    },
    2: {
        "description": "mean",
        "sufix": ["mean"]
    },
    3: {
        "description": "mean & stdDev",
        "sufix": ["mean", "stdDev"]
    },
    4: {
        "description": "min & max",
        "sufix": ["min", "max"]
    },
    5: {
        "description": "count",
        "sufix": ["count"]
    },
    6: {
        "description": "sum",
        "sufix": ["sum"]
    },
    7: {
        "description": "median, mean, stdDev, min & max",
        "sufix": ["median", "mean", "stdDev", "min", "max"]
    }
}
REDUCER_LIST = [(str(k) + " (" + REDUCTION_SPECS[k]["description"] + ")") for k in range(len(REDUCTION_SPECS))]    

def which(self):
    """
    Essa função é uma adaptação da função "with" da linguagem R
    """
    try:
        self = list(iter(self))
    except TypeError as e:
        raise Exception("""'which' method can only be applied to iterables.
        {}""".format(str(e)))
    indices = [i for i, x in enumerate(self) if bool(x) == True]
    return(indices)


def writeToLogFile(lines, entryType, identifier):
    """
    Essa função escreve um log file
    """

    log_file = "GEEDaR_log.txt"

    if not isinstance(lines, list):
        lines = [lines]
    try:
        dateAndTime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        f = open(log_file, "a")
        for line in lines:
            f.write(dateAndTime + "," + str(entryType) + "," + str(identifier) + "," + line + "\n")
        f.close()
    except:
        print("(!)")
        print("The message(s) below could not be written to the log file (" + log_file + "):")
        for line in lines:
            print(line)
        print("(.)")

def polygonFromKML(kmlFile):
    """
    Essa função extrai um polygono (gee object) a partir de um kml
    """
    try:
        # Read the file as a string.
        with open(kmlFile, 'rt', encoding="utf-8") as file:
            doc = file.read()   
        # Create the KML object to store the parsed result.
        k = kml.KML()
        # Read the KML string.
        k.from_string(doc)
        structDict = {0: list(k.features())}
    except:
        return []
    
    # Search for polygons.
    polygons = []
    idList = [0]
    curID = 0
    lastID = 0
    try:
        while curID <= lastID:
            curFeatures = structDict[curID]
            for curFeature in curFeatures:
                if "_features" in [*vars(curFeature)]:
                    lastID = idList[-1] + 1
                    idList.append(lastID)
                    structDict[lastID] = list(curFeature.features())
                elif "_geometry" in [*vars(curFeature)]:
                    geom = curFeature.geometry
                    if geom.geom_type == "Polygon":
                        coords = [list(point[0:2]) for point in geom.exterior.coords]
                        if coords == []:
                            coords = [list(point[0:2]) for point in geom.interiors.coords]
                        if coords != []:
                            polygons.append([coords])
            curID = curID + 1
    except:
        pass
    
    return polygons

# Unfold the processing code into the IDs of the product and of the pixel selection and inversion algorithms.
def unfoldProcessingCode(fullCode:int, silent:bool = False):
    """
    Desempacota o código de processamento no ID dos produtos na 
    seleção de pixels e no algorítmo de inversão.

    Args:
        fullCode: Código de execução GEEDaR
    
    Returns:
        Uma tupla com os códigos de processamento, ID dos produtos, algoritmos de processamento e estimação e redutores.
        
    Examples:
        >>>unfoldProcessingCode(90114001)
        ([90114001], [901], [14], [0], [1])
    """
    failValues = (None, None, None, None, None)
    fullCode = str(fullCode)
    
    if len(fullCode) < 8:
        if not silent:
            raise Exception("Unrecognized processing code: '" 
                + fullCode 
                + "'. It must be a list of integers in the form PPPSSRRA '"
                + "'(PPP is one of the product IDs listed by '-h:products';'"
                + "' SS is the code of the pixel selection algorithm; '"
                + "' RR, the code of the processing algorithm; '"
                + "' and A, the code of the reducer.)."
                )
        else:
            return failValues
    
    if fullCode[0] == "[" and fullCode[-1] == "]":
        fullCode = fullCode[1:-1]
        
    strCodes = fullCode.replace(" ", "").split(",")
    
    processingCodes = []
    productIDs = []
    imgProcAlgos = []
    estimationAlgos = []
    reducers = []
    
    for strCode in strCodes:
        try:
            code = int(strCode)
        except:
            if not silent:
                print("(!)")
                raise Exception("Unrecognized processing code: '" 
                    + strCode 
                    + "'. It should be an integer in the form PPPSSRRA '"
                    + "'(PPP is one of the product IDs listed by '-h:products';'"
                    + "' SS is the code of the pixel selection algorithm; RR, '"
                    + "' the code of the processing algorithm; and A, the code of the reducer.)."
                    )
            else:
                return failValues
        if code < 10000000:
            if not silent:
                print("(!)")
                raise Exception("Unrecognized processing code: '" 
                    + strCode 
                    + "'."
                    )
            else:
                return failValues
        
        processingCodes.append(code)
        
        productID = int(strCode[0:3])
        if not productID in AVAILABLE_PRODUCTS:
            if not silent:
                print("(!)")
                raise Exception("The product ID '" 
                    + str(productID) 
                    + "' derived from the processing code '" 
                    + strCode 
                    + "' was not recognized."
                    )
            else:
                return failValues
        productIDs.append(productID)
        
        imgProcAlgo = int(strCode[3:5])
        if not imgProcAlgo in IMG_PROC_ALGO_LIST:
            if not silent:
                print("(!)")
                raise Exception("The image processing algorithm ID '" 
                    + str(imgProcAlgo) 
                    + "' derived from the processing code '" 
                    + strCode 
                    + "' was not recognized."
                    )
            else:
                return failValues
        imgProcAlgos.append(imgProcAlgo)
        
        estimationAlgo = int(strCode[5:7])
        if not estimationAlgo in ESTIMATION_ALGO_LIST:
            if not silent:
                print("(!)")
                raise Exception("The estimation algorithm ID '" 
                    + str(estimationAlgo) 
                    + "' derived from the processing code '" 
                    + strCode 
                    + "' was not recognized."
                    )
            else:
                return failValues
        estimationAlgos.append(estimationAlgo)       
        reducer = int(strCode[-1])

        if not reducer in range(len(REDUCER_LIST)):
            if not silent:
                print("(!)")
                raise Exception("The reducer code '" 
                    + str(reducer) 
                    + "' in the processing code '" 
                    + strCode 
                    + "' was not recognized. The reducer code must correspond to an index of the reducer list: " 
                    + str(REDUCER_LIST) + "."
                    )
            
            else:
                return failValues
        reducers.append(reducer)
    
    return processingCodes, productIDs, imgProcAlgos, estimationAlgos, reducers