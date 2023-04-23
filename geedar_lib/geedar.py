import sys
import os
import math
from time import sleep
import sqlite3
import pandas as pd
from shutil import copyfile
from fastkml import kml
import ee

from utils import (wich, writeToLogFile, polygonFromKML)

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


# Get the GEEDaR product list.
def listAvailableProducts() -> list:
    """Essa função retorna uma lista com todos os produtos de satelites
    disponíveis.
    """
    return AVAILABLE_PRODUCTS


# Get the list of image processing algorithms.
def listProcessingAlgos() -> list:
    """Essa função retorna uma lista com todos os algoritmos de 
    processamento disponíveis.
    """
    return IMG_PROC_ALGO_LIST


# Get the list of estimation (inversion) algorithms.
def listEstimationAlgos() -> list:
    """Essa função retorna a lista de algorítmos de estimação 
    (inversão).
    """
    return ESTIMATION_ALGO_LIST


# Get the list of GEE image collection IDs related to a given GEEDaR product.
def getCollection(productID) -> list:
    """Essa função retorna uma lista com uma coleção de imagens GEE 
    relacionadas a determinado produto GEEDaR.
    """
    return PRODUCT_SPECS[productID]["collection"].set("product_id", productID)


# Given a product ID, get a dictionary with the band names corresponding to spectral regions (blue, green, red, ...).
def getSpectralBands(productID:int) -> dict:
    """
    Essa função retorna um dicionario com os nomes das bandas 
    correspondentes a região espectral a partir de determinada ID de um
    produto.
    """
    commonBandsDict = {k: PRODUCT_SPECS[productID]["bandList"][v] for k, v in PRODUCT_SPECS[productID]["commonBands"].items() if v >= 0}
    spectralBandsList = [PRODUCT_SPECS[productID]["bandList"][v] for v in PRODUCT_SPECS[productID]["spectralBandInds"]]
    spectralBandsDict = {k: k for k in spectralBandsList}
    return {**commonBandsDict, **spectralBandsDict}


# Unfold the processing code into the IDs of the product and of the pixel selection and inversion algorithms.
def unfoldProcessingCode(fullCode:int, silent:bool = False):
    """
    Desempacota o código de processamento no ID dos produtos na 
    seleção de pixels e no algorítmo de inversão.
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


# Mask bad pixels based on the respective "pixel quality assurance" layer.
def qaMask_collection(productID, imageCollection, addBand = False):
    """
    Retorna a uma coleção de imagens baseada na definição de qualidade pixel
    determinada pelo usuário
    """
    qaLayerName = PRODUCT_SPECS[productID]["qaLayer"]

    if qaLayerName == "" or qaLayerName == []:

        if addBand:
            return ee.ImageCollection(imageCollection).map(
                lambda image: image.addBands(
                    ee.Image(1).rename("qa_mask"))
                    )
        
        else:
            return ee.ImageCollection(imageCollection)
    
    # MODIS bands 1-7 (Terra and Aqua)
    if productID in range(100, 120):
        qaLayer = [qaLayerName[0], qaLayerName[0], qaLayerName[0]]
        startBit = [0, 6, 8]
        endBit = [2, 7, 9]
        testExpression = ["b(0) == 0", "b(0) < 2", "b(0) == 0"]
    # Sentinel-2 L2A
    elif productID == 201:
        qaLayer = [qaLayerName[0]]
        startBit = [0]
        endBit = [7]
        testExpression = ["b(0) >= 4 && b(0) <= 7"]
    # Sentinel-2 L1C
    elif productID == 202:
        qaLayer = [qaLayerName[0]]
        startBit = [10]
        endBit = [11]
        testExpression = ["b(0) == 0"]
    # Landsat 5 and 7 SR Collection 1
    elif productID in [301,302]:
        qaLayer = [qaLayerName[0]]
        startBit = [3]
        endBit = [5]
        testExpression = ["b(0) == 0"]
    # Landsat 8 SR Collection 1
    elif productID in [303]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [3,6]
        endBit = [5,7]
        testExpression = ["b(0) == 0", "b(0) <= 1"]
    # Landsat 4, 5 and 7 Level 2 Collection 2
    elif productID in [311,312,313]:
        qaLayer = [qaLayerName[0]]
        startBit = [1]
        endBit = [5]
        testExpression = ["b(0) == 0"]
    # Landsat 8 and 9 Level 2 Collection 2
    elif productID in [314,315]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [1,6]
        endBit = [5,7]
        testExpression = ["b(0) == 0", "b(0) <= 1"]
    # VIIRS
    elif productID in [151,152]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [2,3]
        endBit = [4,7]
        testExpression = ["b(0) == 0", "b(0) == 0"]
    else:
        if addBand:
            return ee.ImageCollection(imageCollection).map(
                lambda image: image.addBands(
                    ee.Image(1).rename("qa_mask"))
                    )
        else:
            return ee.ImageCollection(imageCollection)
    
    maskVals = []
    for i in range(len(startBit)):
        bitToInt = 0
        for j in range(startBit[i], endBit[i] + 1):
            bitToInt = bitToInt + int(math.pow(2, j))
        maskVals.append(bitToInt)
    
    def qaMask(image):
      mask = ee.Image(1)
      for i in range(len(maskVals)):
        mask = mask.And(image.select(qaLayer[i]).int().bitwiseAnd(
            maskVals[i]
            ).rightShift(startBit[i]).expression(testExpression[i]))
      if addBand:
        image = image.addBands(mask.rename("qa_mask"))
      return image.updateMask(mask);

    return ee.ImageCollection(imageCollection).map(qaMask)


# Get the dates of the images in the collection which match AOI and user dates.
def getAvailableDates(productID:int, dateList:list):
    """Retorna algo que eu ainda não descobri"""
    aoi = None
    dateMin = dateList[0]
    dateMax = (pd.Timestamp(dateList[-1]) 
               + pd.Timedelta(1, "day")).strftime("%Y-%m-%d")
    imageCollection = ee.ImageCollection(getCollection(productID)) \
        .filterBounds(aoi) \
        .filterDate(dateMin, dateMax) \
        .map(lambda image: image.set(
            "img_date", ee.Image(image).date().format("YYYY-MM-dd"))) \
        .filter(ee.Filter.inList("img_date", dateList))
    return imageCollection.aggregate_array("img_date").getInfo()

# Apply an image processing algorithm to the image collection to get spectral data.
def imageProcessing(algo, productID, dateList, clip = True):
    """
    Aplica um algoritmo as coleções de imagens para conseguir os dados espectrais
    """
    global image_collection
    global bands
    global export_vars, export_bands

    # Adicionado a partir dos objetos globais usados entre as funções
    aoi = None

    # Band dictio/lists:
    bands = getSpectralBands(productID)
    irBands = [bands[band] for band in ["wl740", "wl780", "wl800", "wl900", "wl1200", "wl1500", "wl2000"] if band in bands]
    spectralBands = [PRODUCT_SPECS[productID]["bandList"][i] for i in PRODUCT_SPECS[productID]["spectralBandInds"]]

    # Reference band:
    refBand = PRODUCT_SPECS[productID]["scaleRefBand"]

    # Lists of bands and variables which will be calculated and must be exported to the result data frame.
    export_vars = ["img_time"]
    export_bands = []

    # Filter and prepare the image collection.
    dateMin = dateList[0]
    dateMax = (pd.Timestamp(dateList[-1]) 
               + pd.Timedelta(1, "day")).strftime("%Y-%m-%d")
    image_collection = ee.ImageCollection(
        getCollection(productID)
        ).filterBounds(aoi).filterDate(dateMin, dateMax)
    # Set image date and time (manually set time for Modis products).
    if productID in [101,103,105,111,113,115]:
        image_collection = image_collection.map(
            lambda image: image.set(
                "img_date", ee.Image(image).date().format("YYYY-MM-dd"), 
                "img_time", "10:30"
                ))
    elif productID in [102,104,106,112,114,116]:
        image_collection = image_collection.map(
            lambda image: image.set(
                "img_date", ee.Image(image).date().format("YYYY-MM-dd"), 
                "img_time", "13:30"
                ))
    elif productID in [107,117]:
        image_collection = image_collection.map(
            lambda image: image.set(
                "img_date", ee.Image(image).date().format("YYYY-MM-dd"), 
                "img_time", "12:00"
                ))    
    else:
        image_collection = image_collection.map(
            lambda image: image.set(
                "img_date", ee.Image(image).date().format("YYYY-MM-dd"),
                "img_time", ee.Image(image).date().format("HH:mm")
                ))
    image_collection = image_collection.filter(
        ee.Filter.inList("img_date", dateList)
        )
    sortedCollection = image_collection.sort("img_date")
    imageCollection_list = sortedCollection.toList(5000)
    imgDates = ee.List(sortedCollection.aggregate_array("img_date"))
    distinctDates = imgDates.distinct()
    dateFreq = distinctDates.map(lambda d: imgDates.frequency(d))
    # Function to be mapped to the image list and mosaic same-date images.
    def oneImgPerDate(freq, imgList):
        freq = ee.Number(freq)
        localImgList = ee.List(imgList).slice(0, freq)
        firstImg = ee.Image(localImgList.get(0))
        properties = firstImg.toDictionary(
                firstImg.propertyNames()
            ).remove(["system:footprint"], True)
        proj = firstImg.select(refBand).projection()
        #mosaic = ee.Image(qaMask_collection(productID, ee.ImageCollection(localImgList), True).qualityMosaic("qa_mask").setMulti(properties)).setDefaultProjection(proj).select(firstImg.bandNames())
        mosaic = ee.Image(ee.ImageCollection(localImgList).reduce(
            ee.Reducer.mean()).setMulti(
                properties)).setDefaultProjection(proj).rename(
                    firstImg.bandNames()
                    )
        singleImg = ee.Image(
            ee.Algorithms.If(freq.gt(1), mosaic, firstImg)
            )        
        return ee.List(imgList).splice(0, freq).add(singleImg)
    mosaicImgList = ee.List(dateFreq.iterate(
        oneImgPerDate, imageCollection_list)
        )
    mosaicCollection = ee.ImageCollection(
        mosaicImgList).copyProperties(image_collection)
    image_collection = ee.ImageCollection(
        ee.Algorithms.If(imgDates.length().gt(distinctDates.length()), 
            mosaicCollection, image_collection)
            )
    
    # Clip the images.
    if clip:
        image_collection = image_collection.map(
            lambda image: ee.Image(image).clip(aoi)
            )

    # Rescale the spectral bands.
    def rescaleSpectralBands(image):
        finalImage = image.multiply(
            PRODUCT_SPECS[productID]["scalingFactor"]
            ).add(PRODUCT_SPECS[productID]["offset"]).copyProperties(image)
        return finalImage
            
    if (PRODUCT_SPECS[productID]["scalingFactor"] 
        and PRODUCT_SPECS[productID]["offset"]):
        image_collection = image_collection.map(rescaleSpectralBands)

    # Reusable functions:
    
    # Set the number of unmasked pixels as an image property.
    def nSelecPixels(image):
        scale = image.select(refBand).projection().nominalScale()
        nSelecPixels = image.select(
            refBand).reduceRegion(
                ee.Reducer.count(), aoi
                ).values().getNumber(0)
        return image.set("n_selected_pixels", nSelecPixels)

    # minNDVI clustering: select the cluster with the lowest NDVI.
    def minNDVI(image):
        
        nClusters = 20
        targetBands = [bands["red"], bands["NIR"]]
        redNIRimage = ee.Image(image).select(targetBands)
        ndviImage = redNIRimage.normalizedDifference([bands["NIR"], bands["red"]])
        
        # Make the training dataset for the clusterer.
        trainingData = redNIRimage.sample()
        clusterer = ee.Clusterer.wekaCascadeKMeans(2, nClusters).train(trainingData)
        resultImage = redNIRimage.cluster(clusterer)
    
        # Update the clusters (classes).
        maxID = resultImage.reduceRegion(ee.Reducer.max(), aoi).values().getNumber(0)
        clusterIDs = ee.List.sequence(0, maxID)
                   
        # Pick the class with the smallest NDVI.
        ndviList = clusterIDs.map(
            lambda id: ndviImage.updateMask(
                resultImage.eq(ee.Image(ee.Number(id)))
                ).reduceRegion(
                    ee.Reducer.mean(), aoi
                ).values().getNumber(0)
                )
        minNDVI = ndviList.sort().getNumber(0)
        waterClusterID = ndviList.indexOf(minNDVI)

        return image.updateMask(resultImage.eq(waterClusterID))

    # RICO algorithm.
    def rico(image):
        firstCut = image.updateMask(
                image.select(bands["NIR"]).lt(2000).And(
                    image.select(bands["NIR"]).gte(0)
                    ).And(image.select(bands["red"]).gte(0))
            )
        newRed = firstCut.select(bands["red"]).subtract(
            firstCut.select(bands["NIR"])
            ).unitScale(-500,500).rename("R")
        newGreen = firstCut.select(bands["NIR"]).unitScale(0,2000).rename("G")
        newBlue = firstCut.select(bands["NIR"]).subtract(500).unitScale(0,1500).rename("B")
        redwaterImg = newRed.addBands(newGreen).addBands(newBlue)
        hsvImg = redwaterImg.rgbToHsv()
        waterMask = hsvImg.select("hue").lt(0.08).selfMask()
        value = hsvImg.select("value").updateMask(waterMask)
        valueRef = value.reduceRegion(
            reducer=ee.Reducer.median(), geometry=aoi, bestEffort=True
            ).values().getNumber(0)
        statMask = ee.Image(ee.Algorithms.If(
            valueRef, value.gte(valueRef.multiply(0.95)).And(
                value.lte(valueRef.multiply(1.05))), waterMask
                ))
        waterMask = waterMask.updateMask(statMask)
        hsvImg = hsvImg.updateMask(statMask)
        hue = hsvImg.select("hue")
        saturation = hsvImg.select("saturation")
        trustIndex = saturation.subtract(hue)
        trustIndexRefs = trustIndex.reduceRegion(
            reducer=ee.Reducer.percentile([40,95]), geometry=aoi, 
            bestEffort=True
            ).values()
        trustIndexRef1 = trustIndexRefs.getNumber(0)
        trustIndexRef2 = trustIndexRefs.getNumber(1)
        sunglintMask = ee.Image(ee.Algorithms.If(
                    trustIndexRef1, trustIndex.gte(
                        trustIndexRef1).And(
                            trustIndex.gte(trustIndexRef2.multiply(0.8))
                            ), waterMask
                    ))
        waterMask = waterMask.updateMask(sunglintMask)
        return image.updateMask(waterMask)
    
    # Statistic filter to remove mixed (outlier) pixels.
    def mod3rStatFilter(image):
        redNIRRatio = image.select(
            bands["red"]).divide(image.select(bands["NIR"]).add(1)
            )
        redNIRRatioRef = redNIRRatio.reduceRegion(
            reducer=ee.Reducer.median(), geometry=aoi
            ).values().getNumber(0)
        statMask = ee.Image(ee.Algorithms.If(
            redNIRRatioRef, redNIRRatio.gte(
                redNIRRatioRef.multiply(0.95)
                ), image.mask())
            )
        return image.updateMask(statMask)

    # Function to calculate a quality flag for Modis images.
    def mod3rQualFlag(image):
        tmpImage = image
        tmpImage.set("qual_flag", 0)
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        scale = image.select(refBand).projection().nominalScale()
        meanVals = image.select(
            [bands["red"], bands["NIR"]]
            ).reduceRegion(ee.Reducer.mean(), aoi).values()
        redMean = meanVals.getNumber(0)
        nirMean = meanVals.getNumber(1)
        convrad = ee.Number(math.pi / 180)
        
        if productID < 110 or productID in [151,152]:
            vzen = image.select("SensorZenith").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale = scale
                ).getNumber("SensorZenith").divide(100).multiply(convrad)
            szen = image.select("SolarZenith").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("SolarZenith").divide(100).multiply(convrad)
            solaz = image.select("SolarAzimuth").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("SolarAzimuth").divide(100).multiply(convrad)
            senaz = image.select("SensorAzimuth").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("SensorAzimuth").divide(100).multiply(convrad)
            delta = solaz.subtract(senaz)
            delta = ee.Number(ee.Algorithms.If(
                delta.gte(360), delta.subtract(360), delta)
                )
            delta = ee.Number(ee.Algorithms.If(
                delta.lt(0), delta.add(360), delta)
                )
            raz = delta.subtract(180).abs()

        elif productID in range(111,120):
            vzen = image.select("ViewZenith").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("ViewZenith").divide(100).multiply(convrad)
            szen = image.select("SolarZenith").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("SolarZenith").divide(100).multiply(convrad)
            raz = image.select("RelativeAzimuth").reduceRegion(
                reducer=ee.Reducer.mean(), geometry=aoi, scale=scale
                ).getNumber("RelativeAzimuth").divide(100).multiply(convrad)
        sunglint = vzen.cos().multiply(szen.cos()).subtract(
                vzen.sin().multiply(szen.sin()).multiply(raz.cos())
            ).acos().divide(convrad)
        sunglint = sunglint.min(ee.Number(180).subtract(sunglint))
        qual = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.05) \
            .Or(nSelecPixels.divide(nValidPixels).lt(0.1)) \
            .Or(nSelecPixels.lt(10)) \
        ).add( \
            vzen.divide(convrad).gte(45) \
            .Or(sunglint.lte(25)) \
        ).add( \
            nirMean.gte(1000) \
            .Or(nirMean.subtract(redMean).gte(300)) \
            .add(nirMean.gte(2000).multiply(2)) \
        )
        image = image.set(
            "vzen", vzen.divide(convrad), "sunglint", 
            sunglint, "qual_flag", qual.min(3)
            )
        
        image = ee.Image(ee.Algorithms.If(
            nSelecPixels.gt(0), image, tmpImage)
            )
        return image;
    
    # Calculates a quality flag for algorithms that distinguish 
    # the numbers of total, valid, water and selected pixels, 
    # such as the S2WP algorithms.
    def s2wpQualFlag(image):        
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nWaterPixels = ee.Number(image.get("n_water_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        qualFlag = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nWaterPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nWaterPixels).lt(0.01) \
        ).min(3).multiply(nSelecPixels.min(1))
        return image.set("qual_flag", qualFlag)

    # Calculates a generic quality flag.
    def genericQualFlag(image):        
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        qualFlag = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nValidPixels).lt(0.1) \
        ).add( \
            nSelecPixels.divide(nValidPixels).lt(0.01) \
        ).min(3).multiply(nSelecPixels.min(1))
        return image.set("qual_flag", qualFlag)
       
    # Algorithms:
    
    # 00 is the most simple one. It does nothing to the images.
    if algo == 0:
        pass
    # Simply removes pixels with cloud, cloud shadow or high aerosol.
    if algo == 1:
        image_collection = qaMask_collection(
            productID, image_collection
            )
        
    # MOD3R and its variations
    elif algo in [2, 3, 4]:
        export_vars = list(set(export_vars).union({
            "n_selected_pixels", "n_valid_pixels", 
            "n_total_pixels", "vzen", 
            "sunglint", "qual_flag"}
            )
        )
        
        # Set the number of total pixels and remove unlinkely water pixels.
        image_collection = image_collection.map(
            lambda image: ee.Image(image).set(
                    "n_total_pixels", ee.Image(image).select(
                        bands["red"]
                    ).reduceRegion(
                        ee.Reducer.count(), aoi
                    ).values().getNumber(0)
                ).updateMask(ee.Image(image).select(
                    bands["red"]).gte(0).And(
                        ee.Image(image).select(bands["red"]).lt(3000)
                            ).And(ee.Image(image).select(bands["NIR"]).gte(0)
                    )
                )
            )
        # Remove bad pixels (cloud, cloud shadow, high aerosol and 
        # acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Filter out images with too few valid pixels.
        image_collection = image_collection.map(
            lambda image: ee.Image(image).set(
                "n_valid_pixels", ee.Image(image).select(
                    bands["red"]
                    ).reduceRegion(
                        ee.Reducer.count(), aoi
                    ).values().getNumber(0)
                )
        )
        image_collection_out = ee.ImageCollection(
            image_collection.filterMetadata(
                "n_valid_pixels", "less_than", 10
                ).map(
                    lambda image: ee.Image(image).set(
                            "n_selected_pixels", 0, 
                            "qual_flag", 0
                        ).updateMask(ee.Image(0))
                    )
                )
        image_collection_in = ee.ImageCollection(
            image_collection.filterMetadata(
                "n_valid_pixels", "greater_than", 9
                )
            )

        if algo == 2:
            # MOD3R clusterer/cassifier.
            ## Run k-means with up to 5 clusters and choose the cluster 
            # which most likley represents water.
            ## For such choice, first define the cluster which probably 
            # represents soil or vegetation.
            ## Such cluster is the one with the largest difference 
            # between red and NIR.
            ## Then test every other cluster as a possible water endmember, 
            # choosing the one which yields the smaller error.
            def mod3r(image):
                nClusters = 20
                targetBands = [bands["red"], bands["NIR"]]
                redNIRimage = ee.Image(image).select(targetBands)
                
                # Make the training dataset for the clusterer.
                trainingData = redNIRimage.sample()
                clusterer = ee.Clusterer.wekaCascadeKMeans(
                    2, nClusters).train(trainingData)
                resultImage = redNIRimage.cluster(clusterer)
            
                # Update the clusters (classes).
                maxID = ee.Image(resultImage).reduceRegion(
                    ee.Reducer.max(), aoi).values().get(0)
                clusterIDs = ee.List.sequence(0, ee.Number(maxID))
                
                # Get the mean band values for each cluster.
                clusterBandVals = clusterIDs.map(
                    lambda id: redNIRimage.updateMask(
                        resultImage.eq(ee.Image(ee.Number(id)))
                        ).reduceRegion(ee.Reducer.mean(), aoi)
                    )
            
                # Get a red-NIR difference list.
                redNIRDiffList = clusterBandVals.map(
                    lambda vals: ee.Number(
                        ee.Dictionary(vals).get(bands["NIR"])
                        ).subtract(
                    ee.Number(ee.Dictionary(vals).get(bands["red"]))
                        )
                    )
            
                # Pick the class with the greatest difference to be the land endmember.
                greatestDiff = redNIRDiffList.sort().reverse().get(0)
                landClusterID = redNIRDiffList.indexOf(greatestDiff)
                # The other clusters are candidates for water endmembers.
                waterCandidateIDs = clusterIDs.splice(landClusterID, 1)
            
                # Apply, for every water candidate cluster, an unmix 
                # procedure with non-negative-values constraints.
                # Then choose as water representative the one which 
                # yielded the smaller prediction error.
                landEndmember = ee.Dictionary(clusterBandVals.get(
                        landClusterID
                    )).values(targetBands)
                landEndmember_red = ee.Number(landEndmember.get(0))
                landEndmember_nir = ee.Number(landEndmember.get(1))
                landImage = ee.Image(landEndmember_red).addBands(
                    ee.Image(landEndmember_nir)
                    ).rename(targetBands)
                minError = ee.Dictionary().set(
                        "id", ee.Number(waterCandidateIDs.get(0))
                        ).set("val", ee.Number(2147483647)
                    )
                
                # Function for getting the best water candidate.
                def pickWaterCluster(id, errorDict):
                    candidateWaterEndmember = ee.Dictionary(
                        clusterBandVals.get(ee.Number(id))
                        ).values(targetBands)
                    candidateWaterEndmember_red = ee.Number(
                        candidateWaterEndmember.get(0)
                        )
                    candidateWaterEndmember_nir = ee.Number(
                        candidateWaterEndmember.get(1)
                        )
                    candidateWaterImage = ee.Image(
                        candidateWaterEndmember_red
                        ).addBands(
                        ee.Image(candidateWaterEndmember_nir)
                        ).rename(targetBands)
                    otherCandidatesIDs = waterCandidateIDs.splice(
                        ee.Number(id), 1
                        )
                    def testCluster(otherID, accum):
                        maskedImage = redNIRimage.updateMask(
                            resultImage.eq(ee.Number(otherID))
                            )
                        fractions = maskedImage.unmix([
                            landEndmember, candidateWaterEndmember], 
                            True, True
                            )
                        predicted = landImage.multiply(
                            fractions.select("band_0")
                            ).add(candidateWaterImage.multiply(
                                fractions.select("band_1"))
                                )
                        return ee.Number(
                            maskedImage.subtract(
                                predicted
                            ).pow(2).reduce(
                                ee.Reducer.sum()
                            ).reduceRegion(
                                ee.Reducer.mean(), aoi
                            ).values().get(0)).add(ee.Number(accum)
                        )
                    errorSum = otherCandidatesIDs.iterate(testCluster, 0)
                    errorDict = ee.Dictionary(errorDict)
                    prevError = ee.Number(errorDict.get("val"))
                    prevID = ee.Number(errorDict.get("id"))
                    newError = ee.Algorithms.If(
                        ee.Number(errorSum).lt(prevError), errorSum, prevError
                        )
                    newID = ee.Algorithms.If(
                        ee.Number(errorSum).lt(prevError), ee.Number(id), prevID
                        )    
                    return errorDict.set(
                        "id", newID).set("val", newError)
                
                waterClusterID = ee.Number(
                    ee.Dictionary(
                        waterCandidateIDs.iterate(
                            pickWaterCluster, minError)).get("id")
                        )
                
                # Return the image with non-water clusters masked, 
                # with the clustering result as a band and with the water 
                # cluster ID as a property.
                return image.updateMask(
                    resultImage.eq(
                        ee.Image(waterClusterID))
                        )
    
        elif algo == 3:
            # minNDVI: a MOD3R modification. Get the lowest-NDVI cluster.
            mod3r = minNDVI
   
        elif algo == 4:
            # minNIR: a MOD3R modification. Get the lowest-NIR cluster.
            def mod3r(image):
                nClusters = 20
                targetBands = [bands["red"], bands["NIR"]]
                redNIRimage = ee.Image(image).select(targetBands)
                
                # Make the training dataset for the clusterer.
                trainingData = redNIRimage.sample()
                clusterer = ee.Clusterer.wekaCascadeKMeans(
                    2, nClusters).train(trainingData)
                resultImage = redNIRimage.cluster(clusterer)
            
                # Update the clusters (classes).
                maxID = resultImage.reduceRegion(ee.Reducer.max(), aoi).values().getNumber(0)
                clusterIDs = ee.List.sequence(0, maxID)
                           
                # Pick the class with the smallest NDVI.
                nirList = clusterIDs.map(
                    lambda id: redNIRimage.select(
                        bands["NIR"]).updateMask(resultImage.eq(
                    ee.Image(ee.Number(id))
                        )
                    ).reduceRegion(
                        ee.Reducer.mean(), aoi
                        ).values().getNumber(0)
                    )
                minNIR = nirList.sort().getNumber(0)
                waterClusterID = nirList.indexOf(minNIR)

                return ee.Image(image).updateMask(
                    resultImage.eq(waterClusterID)
                    )

        # Run the modified MOD3R algorithm and set the quality flag.
        image_collection_in = image_collection_in.map(mod3r) \
            .map(mod3rStatFilter) \
            .map(nSelecPixels) \
            .map(mod3rQualFlag)
        
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(
            image_collection_in.merge(image_collection_out)
            ).copyProperties(image_collection)
    
    # Ventura 2018 (Açudes)
    elif algo == 5:
        # Remove bad pixels (cloud, cloud shadow, high aerosol and 
        # acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Remove pixels with NIR > 400.
        image_collection = ee.ImageCollection(image_collection).map(
            lambda image: ee.Image(image).updateMask(
                ee.Image(image).select(bands["NIR"]).lte(400).And(
                    ee.Image(image).select(bands["NIR"]).gte(0))
                )
            )
    
    # Sentinel-2 Water Processing (S2WP) algorithm version 6.
    elif algo in [6, 7, 8]:
        export_vars = list(set(export_vars).union({"n_selected_pixels"}))
        def s2wp6(image):
            blue = image.select(bands["blue"])
            green = image.select(bands["green"])
            nir = image.select(bands["NIR"])
            swir2 = image.select(bands["wl2000"])
            minSWIR = image.select(
                [bands["wl1500"],bands["wl2000"]]
                ).reduce(ee.Reducer.min())
            maxGR = image.select(
                [bands["green"], bands["red"]]
                ).reduce(ee.Reducer.max())
            blueNIRratio = blue.divide(nir)
            b1pred = blue.multiply(1.1470590).add(
                green.multiply(-0.24835489)
                ).add(38.96482)
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            maxV = vis.reduce(ee.Reducer.max())
            minV = vis.reduce(ee.Reducer.min())
            maxDiffV = maxV.subtract(minV)
            ndwi = image.normalizedDifference([bands["green"], bands["NIR"]])
            ci = image.normalizedDifference([bands["red"], bands["green"]])
            rg = image.select(bands["red"]).divide(image.select(bands["green"]))
            ndwihvt2 = maxGR.addBands(minSWIR).normalizedDifference()
            aeib2 = maxDiffV.subtract(blue)
            aeib1 = maxDiffV.subtract(b1pred)
            nirMaxVratio = nir.divide(maxV)
            predNIRmaxVratioHighR = rg.multiply(1.45589130421).exp().multiply(0.0636397716305)
            nirMaxVratioDevHighR = nirMaxVratio.subtract(predNIRmaxVratioHighR)
            
            image = image.updateMask(ndwihvt2.gte(0).And(minSWIR.lt(420)))
            darkAndInterW = maxDiffV.lt(250) \
                .And(nir.lt(300)) \
                .And(aeib2.gte(-450)) \
                .And(maxDiffV.gte(120) \
                .And(swir2.lt(125)) \
                .Or(ci.lt(0.08).And(swir2.lt(60)))) \
                .And(ndwihvt2.gte(0.78) \
                    .Or(aeib2.subtract(ci.polynomial(
                        [-151.17, -359.17])).abs().lt(80)))
            darkW = darkAndInterW.And(maxDiffV.lt(120))
            interW = darkAndInterW.And(maxDiffV.gte(120).And(maxDiffV.lt(250)))
            brightW = interW.Or(maxDiffV.gte(220) \
              .And(aeib2.gte(-350)) \
              .And(ndwihvt2.gte(0.4)) \
              .And(nirMaxVratioDevHighR.lt(0.5)) \
              .And(ndwi.gte(-0.1).Or(maxDiffV.gte(420).And(ci.gte(0.3).Or(maxDiffV.gte(715))))) \
              .And(ci.lt(0.23).And(aeib2.gte(
                ci.polynomial([-881.33, 5266.7])
                )).Or(ci.gte(0.23).And(aeib2.gte(ci.polynomial([519.41, -823.53]))))) 
              .And( \
                ci.lt(-0.35).And(ndwihvt2.gte(0.78).Or(aeib1.gte(-5)).Or(blueNIRratio.gte(4))) \
                .Or(ci.gte(-0.35).And(ci.lt(-0.2)).And(
                    ndwihvt2.gte(0.78).Or(aeib1.gte(-15)).Or(blueNIRratio.gte(5)))) \
                .Or(ci.gte(-0.2).And(ci.lt(0.3)).And(ndwihvt2.gte(0.78).Or(aeib1.gte(0)))) \
                .Or(ci.gte(0.3).And(aeib2.gte(220))) \
              ) \
            )
            # Bright + dark waters:
            if algo == 6:
                image = image.updateMask(darkW.Or(brightW))
            # Only bright waters:
            elif algo == 7:
                image = image.updateMask(brightW)
            # Only dark waters:
            elif algo == 8:
                image = image.updateMask(darkW)
            return image.set("n_selected_pixels", image.select(
                bands["red"]).reduceRegion(
                    ee.Reducer.count(), aoi).values().get(0))
        image_collection = image_collection.map(s2wp6)

    # Sentinel-2 Water Processing (S2WP) algorithm versions 7 and 8.
    elif algo in [9,10,12]:
        export_vars = list(set(export_vars).union({
                "n_selected_pixels", "n_valid_pixels", 
                "n_total_pixels", "n_water_pixels", 
                "qual_flag"}
                )
            )
        
        # Set the total number of pixels in the aoi as an image property:
        def totalPixels(image):
            scale = image.select(refBand).projection().nominalScale()       
            return image.set(
                "n_total_pixels", image.select(refBand).reduceRegion(
                    ee.Reducer.count(), aoi, scale
                    ).values().getNumber(0)
                    )
        image_collection = image_collection.map(totalPixels)
        
        # Mask clouds and set the number of valid (non-cloudy) pixels.
        def validPixels(image):        
            vis = image.select([
                bands["blue"], bands["green"], bands["red"]]
                )
            maxV = vis.reduce(ee.Reducer.max())
            minV = vis.reduce(ee.Reducer.min())
            maxDiffV = maxV.subtract(minV)
            atmIndex = maxDiffV.subtract(minV)            
            # Exclude cloud pixels (it will inadvertedly pick very bright pixels):
            validPixels = atmIndex.gte(-1150)
            image = image.updateMask(validPixels)
            scale = image.select(refBand).projection().nominalScale()       
            nValidPixels = image.select(refBand).reduceRegion(
                ee.Reducer.count(), aoi, scale
                ).values().getNumber(0)
            return image.set("n_valid_pixels", nValidPixels)
        image_collection = image_collection.map(validPixels)       

        # Select potential water pixels.
        def waterPixels(image):
            swir1 = image.select(bands["wl1500"])
            swir2 = image.select(bands["wl2000"])
            ndwihvt = image.select(bands["green"]).max(
                        image.select(bands["red"])
                    ).addBands(swir2).normalizedDifference()
            waterMask = ndwihvt.gte(0).And(swir1.lt(680))
            scale = image.select(refBand).projection().nominalScale()       
            nWaterPixels = waterMask.reduceRegion(
                ee.Reducer.count(), aoi, scale
                ).values().getNumber(0)
            return image.updateMask(waterMask).set("n_water_pixels", nWaterPixels)            
        image_collection = image_collection.map(waterPixels)

        # Remove border (spectrally mixed) pixels (only work for Sentinel-2).
        # "B8" in bands.values() and "B8A" in bands.values():
        if productID in [201,151,152]: 
            if productID == 201:
                def maskBorder(image):
                    smi = image.normalizedDifference(["B8","B8A"])
                    return image.updateMask(smi.abs().lt(0.2))
                
            else:
                def maskBorder(image):
                    smi = image.select("I3").divide(image.select("M10"))
                    return image.updateMask(smi.lte(1))
                
            image_collection = image_collection.map(maskBorder)
        
        if algo == 9:
            # More appropriate for Sentinel-2 and Landsat:
            nir_thr = 2000
            blue_thr = 2000
            ndwihvt_thr_bright = 0.2
            ndwi_thr_dark = -0.15
            maxOffset = 30

        elif algo == 10:
            # More appropriate for MODIS:
            nir_thr = 1500
            blue_thr = 800
            ndwihvt_thr_bright = 0.4
            ndwi_thr_dark = 0
            maxOffset = 0

        # Algorithm - version 7.
        def s2wp7(image):
            swir2 = image.select(bands["wl2000"])
            vnir = image.select([
                bands["blue"], bands["green"], 
                bands["red"], bands["NIR"]]
                )
            offset = vnir.reduce(ee.Reducer.min()).min(0).abs()
            blue = image.select(bands["blue"]).add(offset)
            green = image.select(bands["green"]).add(offset)
            red = image.select(bands["red"]).add(offset)
            nir = image.select(bands["NIR"]).add(offset)
            vnir_offset = blue.addBands(green).addBands(red).addBands(nir)
            vis = vnir_offset.select(
                [bands["blue"], bands["green"], 
                bands["red"]]
                )
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            maxDiffV = maxV.subtract(minV)
            ci = vnir_offset.normalizedDifference(
                [bands["red"], bands["green"]]
                )
            ndwi = vnir_offset.normalizedDifference(
                [bands["green"], bands["NIR"]]
                )
            ngbdi = vnir_offset.normalizedDifference(
                [bands["green"], bands["blue"]]
                )
            ndwihvt = green.max(red).addBands(swir2).normalizedDifference()
            # An index helpful to detect clouds (+ bright pixels), cirrus and aerosol:
            saturationIndex = maxDiffV.subtract(minV)            
            # CI-Saturation Index curves.
            curveCI_SI1 = ci.polynomial([-370, -800])
            curveCI_SI2 = -290
            curveCI_SI3 = ci.polynomial([-378.57, 1771.4])
            # A visible-spectrum-based filter which removes pixels strongly 
            # affected by aerosol, sungling and cirrus.
            saturationFilter = saturationIndex.gte(curveCI_SI1).And(
                saturationIndex.gte(curveCI_SI2)).And(
                saturationIndex.gte(curveCI_SI3))
            # CI-NDWI curves to detect sunglint and cirrus:
            curveHighR1a = ci.polynomial([0.745, 0.575])
            curveHighR1b = ci.polynomial([0.3115, -1.5926])
            curveHighR1c = ci.polynomial([0.4158, -3.0833])
            curveLowR1 = ci.polynomial([-0.3875, -2.9688])            
            # A visible & infrared filter for sunglint, cirrus and dark land pixels.
            # The filter is applied separately to low and high reflectance pixels.
            multiFilter = maxV.gte(200).And(
                            ndwihvt.gte(ndwihvt_thr_bright).And(
                                ndwi.gte(0.6).Or(
                                    ndwi.gte(curveHighR1a)).Or(
                                        ndwi.gte(curveHighR1b)
                                        ).Or(ndwi.gte(curveHighR1c)
                                             ).Or(ngbdi.gte(0.25).And(
                                                ndwihvt.gte(0.7)
                                                )))
                            ).Or(maxV.lt(250).And(
                                ndwi.gte(ndwi_thr_dark).And(
                                    ndwi.gte(curveLowR1)).Or(
                                        ndwi.gte(0.6)))
                                        )
            # "Good" water pixels:
            waterMask = saturationFilter.And(
                multiFilter).And(
                nir.lt(nir_thr)).And(
                blue.lt(blue_thr)).And(
                offset.lte(maxOffset)
                ).selfMask()
            # Filter shadow by comparing each pixel to the median of the 
            # area of interest.
            # It must be applied to a small water surface area so to 
            # avoid shadow misclassification due to heterogeneity.
            shadowFilter = waterMask
            indicator = maxV.updateMask(waterMask)
            indicator_ref = indicator.reduceRegion(
                reducer = ee.Reducer.median(), geometry = aoi, 
                bestEffort = True
                ).values().getNumber(0)
            proportionToRef = indicator.divide(indicator_ref);
            shadowFilter = ee.Image(ee.Algorithms.If(
                indicator_ref, proportionToRef.gte(0.8), shadowFilter)
                )
            waterMask = waterMask.updateMask(shadowFilter)
            return image.updateMask(waterMask)
        
        # Algorithm - version 8.2
        def s2wp8(image):
            # Bands and indices:
            blue = image.select(bands["blue"])
            green = image.select(bands["green"])
            red = image.select(bands["red"])
            nir = image.select(bands["NIR"])
            swir2 = image.select(bands["wl2000"])
            vis = image.select([
                bands["blue"], bands["green"], bands["red"]]
                )
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            maxDiffV = maxV.subtract(minV)
            ci = image.normalizedDifference([bands["red"],bands["green"]])
            ndwihvt = green.max(red).addBands(swir2).normalizedDifference()
            # Remove negative-reflectance pixels.
            ndwihvt = ndwihvt.updateMask(minV.gte(0).And(nir.gte(0)))
            # Atmospheric Index (for detection of cloud, cirrus and aerosol).
            atmIndex2 = green.subtract(blue.multiply(2))
            # Filter pixels affected by glint, cirrus or aerosol.
            atm2ndwihvtMask = atmIndex2.gte(ndwihvt.multiply(-500).add(100))
            # "Good" water pixels:
            waterMask = atm2ndwihvtMask.And(ndwihvt.gte(0.6)).selfMask()
            # Filter shaded pixels statistically. For it to work 
            # properly, the water must be homogeneous.
            shadowFilter = waterMask
            indicator = maxV.updateMask(waterMask)
            indicator_ref = indicator.reduceRegion(
                    reducer = ee.Reducer.median(), 
                    geometry = aoi, bestEffort = True
                ).values().getNumber(0)
            proportionToRef = indicator.divide(indicator_ref)
            shadowFilter = ee.Image(ee.Algorithms.If(
                indicator_ref, proportionToRef.gte(0.5), shadowFilter)
                )
            # Final mask:
            waterMask = waterMask.updateMask(shadowFilter)
            return image.updateMask(waterMask)
        
        if algo in [9,10]:
            image_collection = image_collection.map(s2wp7)

        elif algo == 12:
            image_collection = image_collection.map(s2wp8)
        
        # Set the final number of pixels as an image property:
        def selecPixels(image):
            scale = image.select(refBand).projection().nominalScale()
            return image.set(
                "n_selected_pixels", image.select(refBand)
                    .reduceRegion(
                        ee.Reducer.count(), aoi, scale
                        ).values().getNumber(0)
                        )
        image_collection = image_collection.map(selecPixels)
        image_collection = image_collection.map(s2wpQualFlag)         # Quality flag.
    
    # RICO (Red In Cyan Out)
    elif algo == 11:
        if(productID < 200 and not productID in [103,104,113,114]):
            export_vars = list(set(export_vars).union({
                "n_selected_pixels", "n_valid_pixels", 
                "n_total_pixels", "vzen", 
                "sunglint", "qual_flag"}
                    )
                )
        else:
            export_vars = list(set(export_vars).union({
                    "n_selected_pixels", "n_valid_pixels", 
                    "n_total_pixels", "qual_flag"}
                    )
                )
        # Set the number of total pixels.
        image_collection = image_collection.map(
            lambda image: image.set(
            "n_total_pixels", image.select(refBand).reduceRegion(
                ee.Reducer.count(), aoi).values().getNumber(0)
                )
            )
        # Mask bad pixels.
        image_collection = qaMask_collection(productID, image_collection)
        # Set the number of valid (remainging) pixels.
        image_collection = image_collection.map(
            lambda image: image.set(
                "n_valid_pixels", image.select(refBand).reduceRegion(
                ee.Reducer.count(), aoi).values().getNumber(0)
                )
            )
        # Apply the algorithm.
        image_collection = image_collection.map(rico).map(nSelecPixels)
        # Filter images with no good pixels.
        image_collection_out = ee.ImageCollection(
            image_collection.filterMetadata(
                "n_selected_pixels", "less_than", 1
                ).map(lambda image: ee.Image(image).set(
                    "n_selected_pixels", 0, 
                    "qual_flag", 0).updateMask(ee.Image(0))
                )
            )
        image_collection_in = ee.ImageCollection(
            image_collection.filterMetadata("n_selected_pixels", "greater_than", 0)
            )
        # Quality flag:
        if(productID < 200 and not productID in [103,104,113,114]):
            image_collection_in = image_collection_in.map(mod3rQualFlag)

        else:
            image_collection_in = image_collection_in.map(
                genericQualFlag
                )
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(
            image_collection_in.merge(image_collection_out)
            ).copyProperties(image_collection)

    # minNDVI + Wang et al. 2016
    elif algo == 13:
        if(productID < 200 and not productID in [103,104,113,114]):
            export_vars = list(set(export_vars).union({
                "n_selected_pixels", "n_valid_pixels", 
                "n_total_pixels", "vzen","sunglint", 
                "qual_flag"}
                )
            )

        else:
            export_vars = list(set(export_vars).union({
                "n_selected_pixels", "n_valid_pixels", 
                "n_total_pixels", "qual_flag"}
                ))        

        # Set the number of total pixels and remove unlinkely water pixels.
        image_collection = image_collection.map( \
            lambda image: ee.Image(image) \
                .set("n_total_pixels", ee.Image(image).select(refBand)
                     .reduceRegion(ee.Reducer.count(), aoi)
                     .values().getNumber(0)) \
                .updateMask( \
                    ee.Image(image).select(bands["red"]).gte(0) \
                    .And(ee.Image(image).select(bands["red"]).lt(3000)) \
                    .And(ee.Image(image).select(bands["NIR"]).gte(0)) \
                ) \
        )
        # Remove bad pixels (cloud, cloud shadow, high aerosol and acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Filter out images with too few valid pixels.
        image_collection = image_collection.map(
            lambda image: ee.Image(image) \
                .set("n_valid_pixels", ee.Image(image)
                     .select(refBand).reduceRegion(
                        ee.Reducer.count(), aoi).values().getNumber(0)
                        )
        )
        image_collection_out = ee.ImageCollection(
            image_collection.filterMetadata(
                "n_valid_pixels", "less_than", 10).map(
                    lambda image: ee.Image(image).set(
                        "n_selected_pixels", 0, "qual_flag", 0
                        ).updateMask(ee.Image(0)))
                        )
        image_collection_in = ee.ImageCollection(
            image_collection.filterMetadata(
                "n_valid_pixels", "greater_than", 9)
            )

        # Clustering.
        image_collection_in = image_collection_in.map(minNDVI)

        def wang2016(image):
            allBands = image.bandNames()
            noCorrBands = allBands.removeAll(ee.List(spectralBands))
            image = image.updateMask(image.select(spectralBands)
                                     .reduce(ee.Reducer.min()).gte(0)) # Mask negative pixels
            minIR = image.select(irBands).reduce(ee.Reducer.min())
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            image = image.updateMask(minV.gte(minIR))
            corrImage = image.select(
                spectralBands).subtract(minIR).rename(spectralBands)
            finalImage = ee.Image(corrImage.addBands(
                image.select(noCorrBands)).copyProperties(image)
                )
            return finalImage
        image_collection_in = image_collection_in.map(wang2016).map(nSelecPixels)
        
        # Update the separate collections:
        image_collection_out = ee.ImageCollection(
            image_collection_out.merge(ee.ImageCollection(
                image_collection_in.filterMetadata(
                    "n_selected_pixels", "less_than", 1)
                    ).map(
                lambda image: ee.Image(image).set(
                    "n_selected_pixels", 0, "qual_flag", 0).updateMask(
                        ee.Image(0))
                        ).copyProperties(image_collection)
                    )
                )
        image_collection_in = ee.ImageCollection(
            image_collection_in.filterMetadata(
            "n_selected_pixels", "greater_than", 0)
            )     
        
        # Quality flag:
        if(productID < 200 and not productID in [103,104,113,114]):
            image_collection_in = image_collection_in.map(
                mod3rQualFlag
                )
            
        else:
            image_collection_in = image_collection_in.map(
                genericQualFlag
                )
            
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(
            image_collection_in.merge(image_collection_out)
            ).copyProperties(image_collection)
        
    # GPM daily precipitation
    elif algo == 14:
        export_vars = list(set(export_vars).union(
            {"n_selected_pixels", "area"})
            )
        area = aoi.area()
        image_collection = image_collection.map(
            nSelecPixels).map(
            lambda image: ee.Image(image).set("area", ee.Number(area))
            )
    
    #---
        
    # If not already added, add the final number of pixels selected by the algorithm as an image propoerty.
    if not "n_selected_pixels" in export_vars:
        export_vars.append("n_selected_pixels")
        image_collection = image_collection.map(
            lambda image: image.set(
                "n_selected_pixels", image.select(refBand).reduceRegion(
                    ee.Reducer.count(), aoi, 
                    image.select(
                    refBand).projection().nominalScale()
                    ).values().getNumber(0)
                )
            )


# Essa função depende do running mode mas queremos mudar o modo como o GEEDaR
# funciona, dessa forma eu acredito passar o running modes como argumento de
# uma função seja o mais interessante para o objetivo. Se der errado podemos
# retornar o modelo de execução antigo, mas preciso tentar antes.
def estimation(algos:int, productID:int, demandIDs = [-1], running_mode:int = 1):
    """ Essa função executa um conjunto de algoritmos de estimação."""
    global image_collection
    global anyError

    if not isinstance(algos, list):
        algos = [algos]
    
    productBands = list(set(list(bands.values())))
    image_collection = ee.ImageCollection(image_collection).select(
        productBands + export_bands
        )
    
    for algo_i in range(len(algos)):
        algo = algos[algo_i]

        # Check if the required bands for running the estimation algorithm are prensent in the product.
        requiredBands = ESTIMATION_ALGO_SPECS[algo]["requiredBands"]

        if not all(band in list(bands.keys()) for band in requiredBands):
            msg = "(!) The product #" 
            + str(productID) 
            + " does not contain all the bands required to run the estimation algorithm #" 
            + str(algo) + ": " 
            + str(requiredBands) + "."

            if running_mode < 3:
                print(msg)

            elif running_mode >= 3:
                anyError = True
                print("[DEMANDID " + str(demandIDs[algo_i]) + "] " + msg)
                writeToLogFile(msg, "Error", "DEMANDID " + str(demandIDs[algo_i]))

            continue
    
        # Add the estimated variable to the list of reduction.
        varName = ESTIMATION_ALGO_SPECS[algo]["paramName"]

        if not isinstance(varName, list):
            varName = [varName]

        if not varName == [""]:
            export_bands.extend(varName)
        
        # 00 is the most simple one. It does nothing with the images.
        if algo == 0:
            pass

        # Conc. de clorofila-a em açudes do Nordeste.

        elif algo == 1:
            def estim(image):
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                ind = red.subtract(red.pow(2).divide(green))
                return image.addBands(ind.pow(2).multiply(0.0004).add(
                        ind.multiply(0.213)).add(4.3957).rename(varName[0]
                    ))
            image_collection = image_collection.map(estim)

        # Sedimentos em Suspensão na Superfície no Solimões.

        elif algo == 2:
            image_collection = image_collection.map(
                lambda image: image.addBands(
                    image.select(bands["NIR"]).divide(
                    image.select(bands["red"])).pow(1.9189).multiply(759.12)
                    .rename(varName[0])
                    )
                )
            
        # Sedimentos em Suspensão na Superfície do Rio Madeira.
        elif algo == 3:
            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                nirRedRatio = nir.divide(red)
                filter = nirRedRatio.pow(2).multiply(421.63).add(
                    nirRedRatio.multiply(1027.6)
                    ).subtract(nir).abs()
                sss = nirRedRatio.updateMask(
                    filter.lt(200)).pow(2.94).multiply(1020).rename(varName[0])
                return image.addBands(sss)
            image_collection = image_collection.map(estim)                                
        
        # Sedimentos em Suspensão na Superfície em Óbidos, no rio Amazonas.
        elif algo == 4:
            image_collection = image_collection.map(
                lambda image: image.addBands(
                    image.select(bands["NIR"]).multiply(0.2019).add(-14.222).rename(varName[0])
                    )
                )
        
        # Turbidez nos reservatórios do Paranapanema.
        elif algo == 5:
            image_collection = image_collection.map(
                lambda image: image.addBands(
                    image.select(bands["red"]).multiply(0.00223).exp()
                    .multiply(2.45).rename(varName[0])
                    )
                )
        
        # SSS no Paraopeba.
        elif algo == 10:

            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                rejeito = green.divide(
                    math.pi * 10000).pow(-1).subtract(
                    red.divide(math.pi * 10000).pow(-1)
                    )
                ind1 = nir.divide(math.pi*10000).multiply(red.divide(green))
                ind2 = nir.divide(red)
                normalCase = ind1.pow(2).multiply(18381).add(
                    ind1.multiply(3874.8)
                    )
                specialCase = ind2.pow(2).multiply(9205.5).add(
                    ind2.multiply(-9253.8)
                    )
                sss = normalCase.where(
                    ind2.gte(0.9).And(rejeito), 
                    specialCase).rename(varName[0]
                    )
                return image.addBands(sss)
            
            image_collection = image_collection.map(estim)

        # SSS, ISS, OSS and chla in Brazilian semiarid reservoirs.
        elif algo == 11:

            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                blue = image.select(bands["blue"])
                iss = red.subtract(nir).multiply(0.059).add(
                    green.subtract(nir).multiply(-0.0245)).add(0.74)
                iss = iss.where(iss.lt(0), 0).rename(varName[1])
                sss = red.subtract(blue).multiply(0.06318).add(
                    green.multiply(0.009793)).add(1.363)
                sss = sss.where(iss.gt(sss), iss).rename(varName[0])
                oss = sss.subtract(iss).rename(varName[2])
                chla = green.multiply(0.0937).add(
                    iss.multiply(-3.752)).add(-10.92)
                chla = chla.where(chla.lt(0), 0).rename(varName[3])
                biomass = chla.multiply(0.02386).exp().multiply(
                    1.55465).rename(varName[4])
                return image.addBands(sss).addBands(iss).addBands(
                    oss).addBands(chla).addBands(biomass)
            
            image_collection = image_collection.map(estim)

        # Chla in Brazilian semiarid reservoirs.
        elif algo == 12:

            def estim(image):
                chla = image.select(bands["green"]).multiply(
                    0.1396).add(image.select(
                    bands["red"]).multiply(-0.1006)).add(
                    -4.227).rename(varName[0])
                
                return image.addBands(chla)
            image_collection = image_collection.map(estim)

        # 99 is for tests only.
        elif algo == 99:
            image_collection = image_collection.map(
                lambda image: image.addBands(
                ee.Image(1234).rename(varName[0]))
                )
