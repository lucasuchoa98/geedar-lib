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