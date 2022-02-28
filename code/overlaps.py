import geopandas
import pandas
import tobler
import re
import numpy
#from quilt3 import Package #for nlcd
from geoFunctions import *
geopandas.options.use_pygeos = True


class NamedShapes:
    """Container for fields required to specify overlap computation"""
    def __init__(self, stateFIPS, shpFile, nameCol):
        self.stateFIPS = stateFIPS
        self.shpFile = shpFile
        self.nameCol = nameCol

azCongressional = NamedShapes(4, "input_data/CongressionalDistricts/cd117-proposed/AZ.geojson", "NAME")

azSLD = NamedShapes(4, "input_data/StateLegDistricts/AZ/slds_2022.geojson", "NAME")

def populationOverlaps(acsData, nsComponents, nsToDecompose, outCSV, joinCol='GISJOIN'):
    df_acs_dat, dataCols = loadAndJoinData(acsData.dataCSVs, re.compile(acsData.totalPopCol), joinCol)
    print(dataCols)
    print(df_acs_dat.head())
    df_acs_dat["TotalPopulation"] = df_acs_dat[acsData.totalPopCol]
    print("Loading tract shapefile")
    df_acs_geo = geopandas.read_file(acsData.dataShapes)
    df_acs_geo['STATEFP'] = df_acs_geo['STATEFP'].astype(int)
    df_acs_geo.query('STATEFP == @nsComponents.stateFIPS', inplace=True)
    print ("projecting ACS to EPSG:3857")
    df_acs_geo = df_acs_geo.to_crs('EPSG:3857')
    print(df_acs_geo.head())
    print("Merging data into shapes")
    df_acs_geo = df_acs_geo.merge(df_acs_dat, on=joinCol)
    print("Loading named shapes from ", nsComponents.shpFile)
    df_comp_geo = geopandas.read_file(nsComponents.shpFile)
    shapeNames = list(map(lambda name: 'S' + name, df_comp_geo[nsComponents.nameCol].to_list()))
    nShapes = len(shapeNames)
    for k in range(0, nShapes) :
        col = [0]*nShapes
        col[k] = 100
        df_comp_geo[shapeNames[k]] = col
    print (df_comp_geo.head())
    print ("projecting component shapes to EPSG:3857")
    df_comp_geo = df_comp_geo.to_crs('EPSG:3857')
    df_comp_interp = tobler.area_weighted.area_interpolate(df_comp_geo, df_acs_geo,intensive_variables=shapeNames)
    df_comp_interp = pandas.concat([df_acs_geo["TotalPopulation"], df_comp_interp],axis=1) # put population back
    for sn in shapeNames :
       df_comp_interp[sn] = df_comp_interp[sn] * df_comp_interp['TotalPopulation']/100
    print (df_comp_interp.head())
    print("Loading named shapes from ", nsToDecompose.shpFile)
    df_to_geo = geopandas.read_file(nsToDecompose.shpFile)
    print ("projecting shapes to EPSG:3857")
    df_to_geo = df_to_geo.to_crs('EPSG:3857')
    df_comp_interp = df_comp_interp.to_crs('EPSG:3857')
    cols = ['TotalPopulation'] + shapeNames
    df_to_interp = tobler.area_weighted.area_interpolate(df_comp_interp, df_to_geo, extensive_variables=cols)
    df_to_interp = pandas.concat([df_to_geo[nsToDecompose.nameCol], df_to_interp], axis=1)
    print(df_to_interp.head())
    to_write = df_to_interp[[nsToDecompose.nameCol] + cols]
    for sn in shapeNames :
       to_write[sn] = to_write[sn].astype(int) #df_comp_interp[sn] * df_comp_interp['TotalPopulation']/100
    to_write['TotalPopulation'] = to_write['TotalPopulation'].astype(int)
    print ("Writing ", outCSV)
    to_write.to_csv(outCSV,index=False)

populationOverlaps(acs2018,azCongressional,azSLD, "test.csv")
