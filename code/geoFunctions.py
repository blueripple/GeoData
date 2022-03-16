import geopandas
import pandas
import tobler
import re
import numpy
#from quilt3 import Package #for nlcd
geopandas.options.use_pygeos = True

class ACSData:
    """Container for specifics of ACS shapes and data for aggregation"""
    def __init__(self, dataCSVs, dataShapes, nlcdFile, totalPopCol, pcIncomeCol):
        self.dataCSVs = dataCSVs
        self.dataShapes = dataShapes
        self.nlcdFile = nlcdFile
        self.totalPopCol = totalPopCol
        self.pcIncomeCol = pcIncomeCol


def dataAndColsForMerge(fp, pat=re.compile('^[A-Z0-9]+E\d+$')):
    print("Loading tract data from ", fp)
#    pat = re.compile('^[A-Z0-9]+E\d+$')
    df = pandas.read_csv(fp, encoding='latin-1')
    dataCols = [s for s in df.columns if pat.match(s)]
#    dataCols = list(filter(pat.search, df.columns))
    colVals = ['GISJOIN'] + dataCols
    return df.filter(colVals, axis=1), dataCols

def loadAndJoinData(fps, colPat=re.compile('^[A-Z0-9]+E\d+$'), joinCol='GISJOIN'):
    if len(fps) == 0:
        raise RuntimeError("No files given to loadAndJoinData")
    print("Loading tract data tables")
    fps_local = fps.copy()
    fp0 = fps_local.pop()
    df, dataCols = dataAndColsForMerge(fp0, colPat)
    for fp in fps_local:
        dfA, dataColsA = dataAndColsForMerge(fp, colPat)
        print ("joining data on", joinCol)
        df = df.set_index(joinCol).join(dfA.set_index(joinCol))
        dataCols = dataCols + dataColsA
    return df, dataCols




acs2018 = ACSData(["input_data/NHGIS/US_2018_tract_csv/nhgis0027_ds240_20185_2018_tract_E.csv"
                   , "input_data/NHGIS/US_2018_tract_csv/nhgis0022_ds239_20185_2018_tract_E.csv"]
                  , "input_data/NHGIS/US_2018_tract_shapefile/US_tract_2018.shp"
                  , "nlcd_2016.tif"
                  , 'AJWME001'
                  , 'AJ0EE001'
                  )

acs2016 = ACSData (["input_data/NHGIS/US_2016_tract_csv/nhgis0028_ds226_20165_2016_tract_E.csv"
                    , "input_data/NHGIS/US_2016_tract_csv/nhgis0023_ds225_20165_2016_tract_E.csv"]
                   , "input_data/NHGIS/US_2016_tract_shapefile/US_tract_2016.shp"
                   , "nlcd_2016.tif"
                   , 'AF2LE001'
                   , 'AF6AE001'
                   )


acs2014 = ACSData (["input_data/NHGIS/US_2014_tract_csv/nhgis0029_ds207_20145_2014_tract_E.csv"
                    , "input_data/NHGIS/US_2014_tract_csv/nhgis0024_ds206_20145_2014_tract_E.csv"]
                   , "input_data/NHGIS/US_2014_tract_shapefile/US_tract_2014.shp"
                   , "nlcd_2011.tif"
                   , 'ABA1E001'
                   , 'ABFIE001'
                   )

acs2012 = ACSData (["input_data/NHGIS/US_2012_tract_csv/nhgis0030_ds192_20125_2012_tract_E.csv"
                    , "input_data/NHGIS/US_2012_tract_csv/nhgis0026_ds191_20125_2012_tract_E.csv"]
                   , "input_data/NHGIS/US_2012_tract_shapefile/US_tract_2012.shp"
                   ,  "nlcd_2011.tif"
                   , 'QSPE001'
                   , 'QWUE001'
                   )
