import sys
import os
import math
from time import sleep
import sqlite3
import pandas as pd
from shutil import copyfile
from fastkml import kml
import ee

from utils import (which, writeToLogFile, 
                              polygonFromKML, unfoldProcessingCode)

from utils import (PRODUCT_SPECS, AVAILABLE_PRODUCTS,
                              IMG_PROC_ALGO_SPECS, IMG_PROC_ALGO_LIST,
                              ESTIMATION_ALGO_SPECS, ESTIMATION_ALGO_LIST,
                              REDUCTION_SPECS)

ee.Initialize()

# Global objects used among functions.
image_collection = ee.ImageCollection(ee.Image())
aoi = None
ee_reducer = ee.Reducer.median()
bands = {}
input_df = pd.DataFrame()
user_df = pd.DataFrame()
export_vars = []
export_bands = []
log_file = "GEEDaR_log.txt"
anyError = False

# Get the GEEDaR product list.
def listAvailableProducts() -> list:
    """
    Retorna uma lista com todos os produtos de satelites disponíveis.

    Returns:
        Uma lista com os produtos de satélites disponíveis

    Examples:
        >>> listAvailableProducts()
        [101, 102, 103, 104, 105, 106, 107, 111, 112, 113, 114, 115, 116, 117, 151, 152, 201, 202, 301, 302, 303, 311, 312, 313, 314, 315, 901]
    """
    return AVAILABLE_PRODUCTS


# Get the list of image processing algorithms.
def listProcessingAlgos() -> list:
    """
    Retorna uma lista com os algoritmos de processamento disponíveis.

    Returns:
        Uma lista com os algoritmos de processamento disponíveis.

    Examples:
        >>> listProcessingAlgos()
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    """
    return IMG_PROC_ALGO_LIST


# Get the list of estimation (inversion) algorithms.
def listEstimationAlgos() -> list:
    """
    Retorna a lista de algorítmos de estimação (inversão).

    Returns:
        Uma lista com os algoritmos de processamento disponíveis.

    Examples:
        >>> listEstimationAlgos()
        [0, 1, 2, 3, 4, 5, 10, 11, 12, 99]
    """
    return ESTIMATION_ALGO_LIST


# Get the list of GEE image collection IDs related to a given GEEDaR product.
def getCollection(productID:int) -> ee.imagecollection.ImageCollection:
    """
    Returna uma lista com uma coleção de imagens GEE de determinado produto GEEDaR.

    Args:
        productID: Identificação do produto espectral.
    
    Returns:
        Uma lista com uma coleção de imagens do GEE relacionadas a determinado produto GEEDaR.

    Examples:
        >>> getCollection(101)
        ee.ImageCollection({"functionInvocationValue": {"functionName": "Element.set","arguments": {"key": {"constantValue": "product_id"},"object": {"functionInvocationValue": {"functionName": "ImageCollection.load","arguments": {"id": {"constantValue": "MODIS/006/MOD09GA"}}}},"value": {"constantValue": 101}}}})
    """
    return PRODUCT_SPECS[productID]["collection"].set("product_id", productID)


# Given a product ID, get a dictionary with the band names corresponding to spectral regions (blue, green, red, ...).
def getSpectralBands(productID:int) -> dict:
    """
    Retorna um dicionario com os nomes das bandas de uma determinada região espectrais
    
    Args:
        productID: Identificação do produto espectral.
    
    Returns:
        Um dicionário com o nome das bandas espectrais de um produto GEEDaR.

    Examples:
        >>> getSpectralBands(101)
        {'blue': 'sur_refl_b03', 'green': 'sur_refl_b04', 'red': 'sur_refl_b01', 'NIR': 'sur_refl_b02', 'SWIR': 'sur_refl_b06', 'wl490': 'sur_refl_b03', 'wl800': 'sur_refl_b02', 'wl1200': 'sur_refl_b05', 'wl1500': 'sur_refl_b06', 'wl2000': 'sur_refl_b07', 'sur_refl_b01': 'sur_refl_b01', 'sur_refl_b02': 'sur_refl_b02', 'sur_refl_b03': 'sur_refl_b03', 'sur_refl_b04': 'sur_refl_b04', 'sur_refl_b05': 'sur_refl_b05', 'sur_refl_b06': 'sur_refl_b06', 'sur_refl_b07': 'sur_refl_b07'}
    """
    commonBandsDict = {k: PRODUCT_SPECS[productID]["bandList"][v] for k, v in PRODUCT_SPECS[productID]["commonBands"].items() if v >= 0}
    spectralBandsList = [PRODUCT_SPECS[productID]["bandList"][v] for v in PRODUCT_SPECS[productID]["spectralBandInds"]]
    spectralBandsDict = {k: k for k in spectralBandsList}
    return {**commonBandsDict, **spectralBandsDict}


# Mask bad pixels based on the respective "pixel quality assurance" layer.
def qaMask_collection(productID:int, imageCollection:ee.ImageCollection, addBand:bool = False):
    """
    Retorna a uma coleção de imagens baseada na definição de qualidade pixel
    determinada pelo usuário.
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
def getAvailableDates(productID:int, dateList:list) -> list:
    """
    Retorna um array de valores da propriedade "img_date" de cada imagem da coleção de imagens.
    
    Args:
        productID: Identificação do produto espectral.
        dateList: Lista de datas para verificação de dados disponíveis
    
    Returns:
        Um array com valores da propriedade "img_date" de cada imagem da coleção de imagens. 

    Examples:
        >>> getAvailableDates(101, ['2020-01-01'])
        
    """
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
        
    # If not already added, add the final number of pixels selected 
    # by the algorithm as an image propoerty.
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

# Apply a estimation (inversion) algorithm to the image collection to estimate 
# a parameter (e.g. water turbidity).
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

# Function for reducing the values of each image (previously masked) in a 
# collection applying the predefined reducer (mean, median, ...)
def reduction(reducer, productID, aoi=None):
    """
    Essa função reduz o valor de cada imagem previamente mascadara em
    uma coletação aplicando o redutor predefinido
    """
    global image_collection
    global ee_reducer
    
    # Parameters to include in the result data frame:
    paramList = ee.List(export_vars)   

    def getParamVals(image, result):
        return ee.Dictionary(result).set(
            ee.Image(image).get("img_date"), ee.Dictionary.fromLists(
                paramList, paramList.map(
                    lambda paramName: ee.Image(image).get(ee.String(paramName))
                    )
                )
            )   
    first = ee.Dictionary()
    paramDict = ee.Dictionary(
        ee.ImageCollection(image_collection).iterate(getParamVals, first))
  
    if reducer == 0:
        return paramDict

    else:
        if reducer == 1:
            ee_reducer = ee.Reducer.median()

        elif reducer == 2:
            ee_reducer = ee.Reducer.mean()

        elif reducer == 3:
            ee_reducer = ee.Reducer.mean().combine(
                reducer2=ee.Reducer.stdDev(),sharedInputs=True
                )

        elif reducer == 4:
            ee_reducer = ee.Reducer.minMax()

        elif reducer == 5:
            ee_reducer = ee.Reducer.count()

        elif reducer == 6:
            ee_reducer = ee.Reducer.sum()

        elif reducer == 7:
            ee_reducer = ee.Reducer.median() \
                .combine(reducer2 = ee.Reducer.mean(), sharedInputs = True) \
                .combine(reducer2 = ee.Reducer.stdDev(), sharedInputs = True) \
                .combine(reducer2 = ee.Reducer.minMax(), sharedInputs = True)
       
    band = PRODUCT_SPECS[productID]["scaleRefBand"]
       
    # Combine the dictionaries of parameters and of band values.
    def combDicts(key, subDict):
        return ee.Dictionary(
            subDict).combine(ee.Dictionary(paramDict).get(key)
                             )
    
    successful = False
    timeoutcounts = 0
    tileScale = 1

    for c in range(3):
        
        def reduce(image, result):
            scale = image.select(band).projection().nominalScale()
            return ee.Dictionary(result).set(
                ee.Image(image).get("img_date"), ee.Image(image).reduceRegion(
                    reducer=ee.Reducer(ee_reducer), geometry = aoi, 
                    scale=scale, bestEffort = True, 
                    tileScale=tileScale
                    )
                )
        #first = ee.Dictionary()
        bandDict = ee.Dictionary(
            ee.ImageCollection(image_collection).iterate(reduce, first)
            )
        
        try:
            result = bandDict.map(combDicts).getInfo()
            successful = True
            #if c > 0:
            #print("Successful retrieval.")
            break
        
        except Exception as e:
            print("(!)")
            print(e)
            if str(e) == "Computation timed out.":
                if c < 2:
                    print("Trying again...")                    
                timeoutcounts = timeoutcounts + 1
                if(timeoutcounts >= 2):
                    # On the second failure for computation timeout, process images one by one:
                    localDateList = image_collection.aggregate_array("img_date").getInfo()
                    if len(localDateList) > 1:
                        print("This time processing images one by one:")                    
                        result = ee.Dictionary()
                        for localDate in localDateList:
                            localImageCollection = image_collection.filterDate(
                                localDate, (pd.Timestamp(localDate) 
                                            + pd.Timedelta(1, "day")
                                            ).strftime("%Y-%m-%d")
                                            )
                            #first = ee.Dictionary()
                            paramDict = ee.Dictionary(
                                ee.ImageCollection(localImageCollection).iterate(getParamVals, first)
                                )
                            bandDict = ee.Dictionary(
                                ee.ImageCollection(localImageCollection).iterate(reduce, first)
                                )
                            localResult = bandDict.map(combDicts)
                            print(localDate + ": ", end = '')
                            try:
                                result = ee.Dictionary(result).combine(localResult).getInfo()
                                print("successful retrieval.")
                                successful = True
                            except:
                                print("Failed.")
                        break
            elif str(e)[:40] == "Output of image computation is too large":
                if c < 2:
                    print("Trying with a different tileScale parameter: " 
                          + str(tileScale) + "...")
                    tileScale = tileScale * 2
                else:
                    print("Failed.")
            else:
                if c < 2:
                    print("Trying again in 30 seconds...")
                    sleep(30)
                else:
                    print("Failed.")
                    
    if not successful:
        return
        
    reducedBands = list({*bands.values()}) + export_bands
    sufix = REDUCTION_SPECS[reducer]["sufix"][0]

    if len(REDUCTION_SPECS[reducer]["sufix"]) == 1:
        for k1 in result:
            for k2 in [*result[k1]]:
                if k2 in reducedBands:
                    result[k1][k2 + "_" + sufix] = result[k1].pop(k2)
    #print("Successful retrieval.")
    return result

def loadInputDF(running_mode, input_file, input_path, input_dir):       
    if running_mode < 3:
        # If a kml was pointed as input file...
        if input_file[-4:] == ".kml":
            print("Building input data frame...")
            aoi_mode = "kml"
            running_mode = 2
            
            if input_file == "*.kml":
                kmlFiles = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f)) and f[-4:] == ".kml"]
            else:
                kmlFiles = [input_file]
            user_df = pd.DataFrame(columns = ["id","start_date","end_date"])
            nKmlFiles = len(kmlFiles)
            if nKmlFiles == 0:
                print("(!) No kml file was found in the folder '" + input_dir + "'.")
                sys.exit(1)
            for i in range(nKmlFiles):
                siteID = kmlFiles[i][:-4]
                user_df.loc[i] = [siteID, "auto", None]
        else:
            print("Opening the input file...")
            # Read the CSV file.
            try:
                user_df = pd.read_csv(input_path)
                print(user_df.dtypes)

            except Exception as e:
                print("(!) Could not read the input file.")
                raise Exception(e)
        input_df = user_df.copy()
        colnames = [c.lower() for c in [*input_df.columns]]
        if all(col in colnames for col in ["start_date", "end_date"]) and running_mode == 0:
            running_mode = 2
        elif running_mode == 0:
            running_mode = 1

# Convert a 'date-ranges' to a 'specific-dates' data frame.
def toSpecificDatesDF(input_df):
    """
    Essa função converte uma série temporal em datas específicas em um data frame
    """
    product_ids = [101,102,301,302,303,201]
    colnames = [c.lower() for c in [*input_df.columns]]

    for i in colnames:
        print(i, type(i))

    
    if (all(col in colnames for col in ["lat", "long", 
                                        "start_date", "end_date"]
        ) or all(col in colnames for col in ["id", "start_date", 
                                             "end_date"])
        ):
        print("(!)")
        raise Exception(
            "The input CSV file should have the columns 'start_date', 'end_date' and 'id' or 'lat' and 'long'."
            )
    
    startDate_col = colnames.index("start_date")
    endDate_col = colnames.index("end_date")

    exportColumns = []

    # ID:
    try:
        id_col = colnames.index("id")
    except:
        pass
    else:
        exportColumns.append(id_col)

    # Lat/Long:
    try:
        lat_col = colnames.index("lat")
        long_col = colnames.index("long")
    except:
        pass
    else:
        exportColumns.extend([lat_col, long_col])

    nrows = input_df.shape[0]
    tmpList = []

    for row_i in range(nrows):
        nDates = 0
        try:
            # Get the optimal start date, discarding the dates of the period 
            # before the beginning of the sensor operation.
            userStartDateStr = input_df.iloc[row_i, startDate_col]

            if (not isinstance(userStartDateStr, str)
                ) or (userStartDateStr.lower() == "auto"
                      ) or (userStartDateStr.replace(" ", "") == ""):
                userStartDateStr = "1960-01-01"

            userStartDate = pd.to_datetime(userStartDateStr).date()
            earliestSensorDate = pd.to_datetime('today').date()
            for prodID in product_ids:
                collectionStartDate = pd.to_datetime(
                        PRODUCT_SPECS[prodID]["startDate"]
                    ).date()
                earliestSensorDate = min(earliestSensorDate, collectionStartDate)
            optimalStartDate = max(userStartDate, earliestSensorDate)
            dates = [*pd.Series(pd.date_range(
                optimalStartDate, pd.to_datetime(input_df.iloc[row_i, endDate_col])
                )).astype("str")]            
            nDates = len(dates)
        except:
            pass

        if not nDates > 0:
            print("(!) Could not interpret the date range defined by 'start_date' and 'end_date' in row #" 
                  + str(row_i + 1) + " of the input CSV file. The row was ignored.")
            continue
        
        tmpDF = pd.DataFrame({"date": dates})
        for c in exportColumns:
            tmpDF[input_df.columns[c]] = input_df.iloc[row_i, c]
        tmpList.append(tmpDF)
    
    input_df = pd.concat(tmpList)


# Retrieve data in the 'speficic-dates' mode.
## Ideally, the CSV file must include the columns 'date', 'id', 'lat' and long in such order.
def specificDatesRetrieval(
        date_col:int = 0, 
        id_col:int = 1, 
        lat_col:int = 2, 
        long_col:int = 3,  
        running_mode:int = 1,
        input_dir:str = "", 
        aoi_mode:str = 'kml',
        append_mode:bool = False,
        max_n_proc_pixels:int = 25000,
        estimation_algos:list = [0]*6,
        reducers:list = [1]*6,
        img_proc_algos:list = [10,10,
                               9,9,9,9], 
        aoi_radius:int = 1000, 
        product_ids:list = [101,102,301,
                            302,303,201], 
        processing_codes:list = [10110001,10210001,30109001,
                                                  30209001,30309001,20109001]
        ):
    """
    Recupera dados no modo específico de datas
    """

    #global image_collection
    
    global aoi, export_bands, export_vars
    global input_df
    global time_window

    nProcCodes = len(processing_codes)
    export_bands = []
    export_vars = []

    if running_mode == 2:
        time_window = 0
        print("Converting the date-range format to the specific-dates format...")
        toSpecificDatesDF()
    
    print("Checking data in the input file...")
    
    # Data frame attributes:
    colnames = [c.lower() for c in [*input_df.columns]]
    nrows = input_df.shape[0]
    ncols = input_df.shape[1]

    # Check if the data frame has enough rows and columns:
    if nrows < 1:
        print("(!)")
        raise Exception(
            "The input CSV file must have a header row and at least one data row.")
    
    if ncols < 3 and aoi_mode != "kml":
        print("(!)")
        raise Exception(
            "The input CSV file must have a header and at least three columns "
            + "(date, lat, long), unless you are defining your sites trough KML "
            + "files (option -k), in which case the minimum required columns are"
            + " 'date' and 'id'.")
    
    if ncols < 2 and aoi_mode == "kml":
        print("(!)")
        raise Exception(
            "If you choose to define the regions of interest trough KML files,"
            + "the input CSV file must include, at least, the columns 'date'"
            +" and 'id'. The KML files' names must be equal to the corresponding"
            +" 'id' plus the extension '.kml' and the files must be in the same "
            + "folder as the CSV file.")
    
    # Update, if possible, the index of the "date" column.
    try:
        date_col = colnames.index("date")
    except ValueError:
        pass
    
    # Check the date values:
    try:
        pdDates = pd.to_datetime(input_df.iloc[:,date_col])
        input_df.iloc[:, date_col] = pd.Series(pdDates).dt.date
    except:
        print("(!)")
        raise Exception(
            "The date column in the input file must have valid date values "
            + "in the format yyyy-mm-dd.")
        
    # Update, if possible, the index of the (site) "id" column.
    try:
        id_col = colnames.index("id")
    except ValueError:
        if ncols < 4 and aoi_mode != "kml":
            id_col = -1
            lat_col = lat_col - 1
            long_col = long_col - 1
    else:
        if ncols < 4 and aoi_mode != "kml":
            print("(!)")
            raise Exception(
                "The input CSV file must include, at least, the columns "
                + " 'date', 'lat' and 'long', unless you define your sites "
                + "of interest through kml files (option -k), in which case the"
                + "'date' and 'id' columns are enough.") 
    
    # Update, if possible, the index of the lat column.
    try:
        lat_col = colnames.index("lat")
    except ValueError:
        if aoi_mode == "kml":
            lat_col = -1
        
    # Update, if possible, the index of the long column.
    try:
        long_col = colnames.index("long")
    except ValueError:
        if aoi_mode == "kml":
            long_col = -1

    # Unknown id column?
    if id_col == date_col or id_col == lat_col or id_col == long_col:
        if aoi_mode == "kml":
            print("(!)")
            raise Exception(
                "The column containing the sites' name could not be identified." 
                + " Please, name it as 'id'.")
        else:
            id_col = -1

    # Check the lat/long values:
    if aoi_mode != "kml":
        if (not pd.api.types.is_numeric_dtype(input_df.iloc[:, lat_col])
            ) or ((not pd.api.types.is_numeric_dtype(input_df.iloc[:, long_col]))):
            print("(!)")
            raise Exception(
                "'lat' and 'long' values in the input file must be in decimal degrees.")
    
    # Get the indices of the valid rows (no NaN nor None):
    if aoi_mode == "kml":
        validIDs = input_df.iloc[:,id_col].notna()
        validLats = True
        validLongs = True

    else:
        validIDs = True
        validLats = input_df.iloc[:,lat_col].notna()
        validLongs = input_df.iloc[:,long_col].notna()

    validRows = which(input_df.iloc[:,date_col].notna() 
                      & validLats & validLongs & validIDs)
    
    if len(validRows) < 1:
        print("(!)")
        raise Exception(
            "The input CSV file has no valid rows (rows with no missing data).")

    # Results' data frame template:
    resultDF_template = input_df.copy()
    # Add the adjacents dates according to the time window.
    nrows_result = nrows

    if time_window != 0:
        print("Expanding the input data to meet the time_window parameter (" 
              + str(time_window) + ")...")
        window_size = 1 + (time_window * 2)
        nrows_tmp = len(validRows) * window_size + (nrows - len(validRows))
        tmpDF = pd.DataFrame(index=range(nrows_tmp), columns=resultDF_template.columns)
        imgDate = pd.Series(index=range(nrows_tmp), name="img_date", dtype="float64")
        row_j = 0
        validRows_new = []

        for row_i in range(nrows):
            if row_i in validRows:
                date_j = pd.Timestamp(
                    resultDF_template.iloc[row_i, date_col]
                    ) - pd.Timedelta(time_window, "day")
                
                for window_i in range(window_size):
                    validRows_new.append(row_j)
                    tmpDF.iloc[row_j] = resultDF_template.iloc[row_i]
                    imgDate[row_j] = date_j.date()
                    date_j = date_j + pd.Timedelta(1, "day")
                    row_j = row_j + 1

            else:            
                tmpDF.iloc[row_j] = resultDF_template.iloc[row_i]
                row_j = row_j + 1

        tmpDF.insert(date_col + 1, "img_date", imgDate)
        ncols = ncols + 1
        date_col = date_col + 1

        if date_col <= id_col:
            id_col = id_col + 1

        if date_col <= lat_col:
            lat_col = lat_col + 1

        if date_col <= long_col:
            long_col = long_col + 1   

        resultDF_template = tmpDF.copy()        
        nrows_result = nrows_tmp
        validRows = validRows_new
    
    # Get the unique site IDs:
    if id_col >= 0:
        siteSeries = resultDF_template.iloc[:,id_col].astype(str)

    else:
        siteSeries = pd.Series(
            [str([*resultDF_template.iloc[:, lat_col]][i]) 
            + str([*resultDF_template.iloc[:, long_col]][i]) 
            for i in range(nrows_result)]
            )
    
    siteList = siteSeries.iloc[validRows].unique().tolist()

    # Result dictionary.
    resultDFs_dictio = {}

    for code_i in range(nProcCodes):
        processingCode = processing_codes[code_i]
        resultDFs_dictio[processingCode] = pd.DataFrame(
            data=None, index=range(nrows_result))
                 
    # Data retrieval grouped by GEEDaR product and by site.
    print("Processing started at " + str(pd.Timestamp.now()) + ".")
    dataRetrieved = False
    
    for site in siteList:
        print("")
        print("[Site] " + str(site))
        targetRows = [i for i in which(siteSeries == site) if i in validRows]
        dateList = [*pd.to_datetime(
            resultDF_template.iloc[targetRows, date_col].sort_values()
            ).dt.strftime("%Y-%m-%d").unique()]
        aoi = None

        if aoi_mode == "kml":
            kmlFile = ""
            searchPath1 = os.path.join(input_dir, site + ".kml")
            searchPath2 = os.path.join(input_dir, "KML", site + ".kml")

            if os.path.isfile(searchPath1):
                kmlFile = searchPath1

            elif os.path.isfile(searchPath2):
                kmlFile = searchPath2
                
            if kmlFile == "":
                print("(!) File " 
                      + kmlFile + " was not found. The site was ignored.")
                
            else:
                coords = polygonFromKML(kmlFile)
                if coords != []:
                    aoi = ee.Geometry.MultiPolygon(coords)
                else:
                    print("(!) A polygon could not be extracted from the file " 
                          + kmlFile + ". The site was ignored.")
        
        else:                
            # Check if lat/long coordinates are the same for the site.
            lats = [*resultDF_template.iloc[targetRows, lat_col]]
            firstLat = lats[0]
            longs = [*resultDF_template.iloc[targetRows, long_col]]
            firstLong = longs[0]

            if (not all(i == firstLat for i in lats)
                ) or (not all(i == firstLong for i in longs)):
                print("(!) Coordinates were not all the same. The first pair was used.")
            # Define the region of interest.
            aoi = ee.Geometry.Point(coords = [firstLong, firstLat]).buffer(aoi_radius)
        
        if not aoi is None:
            # If more than one processing code was provided, run one by one.
            for code_i in range(nProcCodes):
                processingCode = processing_codes[code_i]
                productID = product_ids[code_i]
                imgProcAlgo = img_proc_algos[code_i]
                estimationAlgo = estimation_algos[code_i]
                reducer = reducers[code_i]
                print("\n(" + str(processingCode) + ")")
                
                if not productID in IMG_PROC_ALGO_SPECS[
                    imgProcAlgo]["applicableTo"]:
                    print("(!) The image processing algorithm #" 
                          + str(imgProcAlgo) 
                          + " is not applicable to the product " 
                          + str(productID) 
                          + ". This data demand was ignored.")
                    continue

                # Get the available dates.
                tmpDateList = getAvailableDates(productID, dateList)
                availableDates = [d for d in dateList if d in tmpDateList]                
                #if not len(availableDates) == 0:
                #    availableDates = list(set(availableDates.sort()))
                nAvailableDates = len(availableDates)
                if nAvailableDates == 0:
                    print("No available data.")
                elif append_mode:
                    # Get common band names (e.g. 'red', 'blue', etc.).
                    commonBandNames = [k for k,v in PRODUCT_SPECS[productID][
                        "commonBands"].items() if v >= 0]
                    commonBandInds = [PRODUCT_SPECS[productID][
                        "commonBands"][k] for k in commonBandNames]
                    realBandNames = [PRODUCT_SPECS[productID][
                        "bandList"][i] for i in commonBandInds]
                    #commonBandsDictio = {PRODUCT_SPECS[productID]["bandList"][v]:k for k,v in PRODUCT_SPECS[productID]["commonBands"].items() if v >= 0 and k in commonBandNames}

                # Divide the request in groups to avoid exceeding GEE capacity.
                # First, calculate the number of pixels in the region of interest.
                # Then determine the number of images which correspond to a total of 100 000 pixels.
                nPixelsInAoI = aoi.area().divide(math.pow(
                    PRODUCT_SPECS[productID]["roughScale"], 2)).getInfo()
                maxNImgs = math.ceil(max_n_proc_pixels/nPixelsInAoI)
                group_len = min(maxNImgs, IMG_PROC_ALGO_SPECS[imgProcAlgo]["nSimImgs"])
                nGroups = math.ceil(nAvailableDates / group_len)

                for g in range(nGroups):
                    dateSublist_inds = range(g * group_len, min(
                        g * group_len + group_len, nAvailableDates))
                    dateSublist = [availableDates[i] for i in dateSublist_inds]
                    print("Requesting data for days " 
                          + str(g * group_len + 1) 
                          + "-" + str(min(g * group_len + group_len, nAvailableDates)) 
                          + "/" + str(nAvailableDates) + "...")
                    # Image processing, parameter estimation and reduction.
                    imageProcessing(imgProcAlgo, productID, dateSublist)
                    estimation(estimationAlgo, productID)
                    result = reduction(reducer, productID)

                    if result is None:
                        print("(!) Failed to retrieve data.")

                    elif result == {}:
                        print("No data retrieved.")

                    else:
                        dataRetrieved = True
                        # Save the retrieved data in the result data frame.

                        for date in [*result]:
                            sameDateRows = [i for i in which(
                                resultDF_template.iloc[:,date_col
                                                       ].astype("str") == date) if i in targetRows]
                            
                            for band in [*result[date]]:
                                colNames = []
                                if append_mode:
                                    for i in range(len(commonBandNames)):
                                        if realBandNames[i] + "_" in band:
                                            colNames.append(band.replace(
                                                realBandNames[i], commonBandNames[i]))
                                elif nProcCodes > 1:
                                    colNames = [str(processingCode) + "_" + band]
                                if len(colNames) == 0:
                                    colNames = [band]
                                for row_i in sameDateRows:
                                    for colName in colNames:
                                        resultDFs_dictio[processingCode].loc[row_i, colName] = result[date][band]
                        print("Data successfully retrieved.")

    print("Processing finished at " + str(pd.Timestamp.now()) + ".")

    if dataRetrieved:
        print("Consolidating results...")
        resultDF_template.reset_index(inplace = True, drop = True)

        if append_mode:
            # Get all column names.
            cols = []
            for k in resultDFs_dictio:
                cols.extend([*resultDFs_dictio[k].columns])
            cols = set(cols)
            commonBandNames = [*PRODUCT_SPECS[101]["commonBands"].keys()]
            # Reorder columns.

            for k in resultDFs_dictio:
                tmpDF = pd.DataFrame()
                for col in cols:
                    wosuffix = col.split("_")[0]                    
                    if not wosuffix in commonBandNames:
                        if col in [*resultDFs_dictio[k].columns]:
                            tmpDF[col] = resultDFs_dictio[k][col]
                        else:
                            tmpDF[col] = math.nan
                tmpDF = tmpDF.reindex(sorted(tmpDF.columns), axis=1)

                for band in commonBandNames:
                    matches = [col for col in cols if (band + "_") in col]
                    for col in matches:
                        if col in [*resultDFs_dictio[k].columns]:
                            tmpDF[col] = resultDFs_dictio[k][col]
                        else:
                            tmpDF[col] = math.nan
                dataColNames = tmpDF.columns
                resultDFs_dictio[k] = tmpDF
                prodID = int(str(k)[0:3])
                sensor = PRODUCT_SPECS[prodID]["sensor"]
                resultDFs_dictio[k] = pd.concat([
                    pd.DataFrame({"ProcCode": [k] * nrows_result, 
                                  "Source": [sensor] * nrows_result}), 
                                  resultDFs_dictio[k]], axis = 1, sort = False)
                resultDFs_dictio[k] = pd.concat([
                    resultDF_template, resultDFs_dictio[k]], 
                    axis = 1, sort = False)
                
                if(running_mode == 2):
                    resultDFs_dictio[k].dropna(
                        subset = dataColNames, how = "all", inplace = True)
            resultDF = pd.concat([*resultDFs_dictio.values()], sort = False)
        else:
            dataDF = pd.concat([*resultDFs_dictio.values()], axis = 1, sort = False)
            resultDF = pd.concat([resultDF_template, dataDF], axis = 1, sort = False)
            # Remove empty rows (if in running mode 2):
            if(running_mode == 2):
                resultDF.dropna(subset = dataDF.columns, how = "all", inplace = True)
    else:
        resultDF = None
    
    return resultDF