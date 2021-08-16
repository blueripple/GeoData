import geopandas
import pandas
import tobler
import re
#from quilt3 import Package
geopandas.options.use_pygeos = True

# Some notes
# 5-year ACS has much at the tract level
# Comes in 2 sets so code below can merge those

# VA Lower
dataCSVs = ["input_data/NHGIS/US_2018_tract_csv/nhgis0027_ds240_20185_2018_tract_E.csv"
           , "input_data/NHGIS/US_2018_tract_csv/nhgis0022_ds239_20185_2018_tract_E.csv"]
dataShapes = "input_data/NHGIS/US_2018_tract_shapefile/US_tract_2018.shp"
aggregateToShape = "input_data/StateLegDistricts/tl_2020_51_sldl20/tl_2017_51_sldl.shp"
aggToCol = 'SLDLST'
distCol = 'DistrictNumber'
outCSV = "output_data/VA_2018_sldl/va_sldl_Raw.csv"
totalPopCol = 'AJWME001'
pcIncomeCol = 'AJ0EE001'

'''2018
dataCSVs = ["input_data/NHGIS/US_2018_tract_csv/nhgis0027_ds240_20185_2018_tract_E.csv"
           , "input_data/NHGIS/US_2018_tract_csv/nhgis0022_ds239_20185_2018_tract_E.csv"]
dataShapes = "input_data/NHGIS/US_2018_tract_shapefile/US_tract_2018.shp"
aggregateToShape = "input_data/CongressionalDistricts/cd116/tl_2018_us_cd116.shp"
aggToCol = 'CD116FP'
distCol = 'CongressionalDistrict'
outCSV = "output_data/US_2018_cd115/cd115Raw.csv"
totalPopCol = 'AJWME001'
pcIncomeCol = 'AJ0EE001'
#nlcd = "nlcd_2011.tif"
'''

'''2016
dataCSVs = ["input_data/NHGIS/US_2016_tract_csv/nhgis0015_ds226_20165_2016_tract_E.csv"
            , "input_data/NHGIS/US_2016_tract_csv/nhgis0023_ds225_20165_2016_tract_E.csv"]
dataShapes = "input_data/NHGIS/US_2016_tract_shapefile/US_tract_2016.shp"
aggregateToShape = "input_data/CongressionalDistricts/cd115/tl_2016_us_cd115.shp"
outCSV = "output_data/US_2016_cd115/cd115Raw.csv"
aggToCol = 'CD115FP'
distCol = 'DistrictNumber'
totalPopCol = 'AF2LE001'
pcIncomeCol = 'AF6AE001'
nlcd = "nlcd_2016.tif"
'''

'''2014
dataCSVs = ["input_data/NHGIS/US_2014_tract_csv/nhgis0017_ds207_20145_2014_tract_E.csv"
            , "input_data/NHGIS/US_2014_tract_csv/nhgis0024_ds206_20145_2014_tract_E.csv"]
dataShapes = "input_data/NHGIS/US_2014_tract_shapefile/US_tract_2014.shp"
aggregateToShape = "input_data/CongressionalDistricts/cd114/tl_2014_us_cd114.shp"
outCSV = "output_data/US_2014_cd114/cd114Raw.csv"
aggToCol = 'CD114FP'
distCol = 'DistrictNumber'
totalPopCol = 'ABA1E001'
pcIncomeCol = 'ABFIE001'
nlcd = "nlcd_2011.tif"
'''

'''2012
dataCSVs = ["input_data/NHGIS/US_2012_tract_csv/nhgis0018_ds192_20125_2012_tract_E.csv"
            , "input_data/NHGIS/US_2012_tract_csv/nhgis0026_ds191_20125_2012_tract_E.csv"]
dataShapes = "input_data/NHGIS/US_2012_tract_shapefile/US_tract_2012.shp"
aggregateToShape = "input_data/CongressionalDistricts/cd113/tl_2013_us_cd113.shp" # this is weird, the 2013 bit
outCSV = "output_data/US_2012_cd113/cd113Raw.csv"
aggToCol = 'CD113FP'
distCol = 'DistrictNumber'
totalPopCol = 'QSPE001'
pcIncomeCol = 'QWUE001'
nlcd = "nlcd_2011.tif"
'''

extraIntCols =['TotalPopulation']
extraFloatCols = ['PerCapitaIncome','SqKm','SqMiles','PopPerSqMile','pwPopPerSqMile','SqKmPop']

def dataAndColsForMerge(fp):
    print("Loading tract data from ", fp)
    pat = re.compile('^[A-Z0-9]+E\d+$')
    df = pandas.read_csv(fp, encoding='latin-1')
    dataCols = [s for s in df.columns if pat.match(s)]
    colVals = ['GISJOIN'] + dataCols
    return df.filter(colVals, axis=1), dataCols

def loadAndJoinData(fps, joinCol='GISJOIN'):
    if len(fps) == 0:
        raise RuntimeError("No files given to loadAndJoinData")
    print("Loading tract data tables")
    fp0 = fps.pop()
    df, dataCols = dataAndColsForMerge(fp0)
    for fp in fps:
        dfA, dataColsA = dataAndColsForMerge(fp)
        print ("joining data on", joinCol)
        df = df.set_index(joinCol).join(dfA.set_index(joinCol))
        dataCols = dataCols + dataColsA
    return df, dataCols

def addPopAndIncome(df_dat, popC, pcIncomeC):
    df_dat["TotalPopulation"] = df_dat[popC]
    df_dat["TotalIncome"] = df_dat[popC] * df_dat[pcIncomeC]
    return df_dat

def loadShapesAndData(dataFPS, shapeFP, popC, pcIncomeC, joinCol='GISJOIN'):
    df_dat, dataCols = loadAndJoinData(dataFPS, joinCol)
    df_dat = addPopAndIncome(df_dat, popC, pcIncomeC)
    print (df_dat.head())
    print("Loading tract shapefile")
    df_geo = geopandas.read_file(shapeFP)
    print ("Adding area*pop, after projecting to CEA")
    df_geo = df_geo.to_crs({'proj':'cea'})
    print(df_geo.head())
    print("Merging data into shapes")
    df_geo = df_geo.merge(df_dat, on=joinCol)
    print ("Adding (CEA) area-weighted pop")
    sq_meters_per_sq_km = 1e6
    df_geo["SqKmPop"] = df_geo["TotalPopulation"] * df_geo['geometry'].area / sq_meters_per_sq_km
    return df_geo, dataCols

def loadAggregateToShapes(fp):
    print("Loading aggregate-to shapefile")
    agg_geo = geopandas.read_file(fp)
    print ("Adding areas, after projecting to CEA")
    agg_geo = agg_geo.to_crs({'proj':'cea'})
    print ("Computing areas")
    sq_meters_per_sq_mile = 1609.34 * 1609.34
    sq_meters_per_sq_km = 1e6
    agg_geo["SqMiles"] = agg_geo['geometry'].area / sq_meters_per_sq_mile
    agg_geo["SqKm"] = agg_geo['geometry'].area / sq_meters_per_sq_km
    return agg_geo

def reformat(x):
    if (isinstance(x, float)):
        return '{:.0f}'.format(x)
    else:
        return x

def reProjectBoth(df_dat, df_agg, crs='EPSG:3857'):
    print("Projecting small areas to ", crs)
    df_dat = df_dat.to_crs(crs)
    print("Projecting aggregate-to areas to ",crs)
    df_agg = df_agg.to_crs(crs)
    return df_dat, df_agg

def aggregate_simple(df_dat, df_agg, dataCols, districtFIPSCol, stateFIPSCol='STATEFP', nJobs=-1):
    crs = 'EPSG:3857'
    df_dat, df_agg = reProjectBoth(df_dat, df_agg, crs)
    print("Aggregating small areas (via areal interpolation)")
    df_interp = tobler.area_weighted.area_interpolate(df_dat
                                                      , df_agg
                                                      , extensive_variables=(['TotalPopulation', 'TotalIncome','SqKmPop'] + dataCols)
                                                      , n_jobs=nJobs)
    df_interp = pandas.concat([df_agg[[stateFIPSCol, districtFIPSCol] + ['SqMiles','SqKm']], df_interp],axis=1) # put the keys + areas back
    print("Removing ZZ entries")
    df_interp = df_interp[(df_interp[districtFIPSCol] != "ZZ")]
    df_interp = df_interp.rename(columns={stateFIPSCol: "StateFIPS", districtFIPSCol: distCol})
    df_interp["PerCapitaIncome"] = df_interp["TotalIncome"]/df_interp["TotalPopulation"]
    df_interp["PopPerSqMile"] = df_interp["TotalPopulation"]/df_interp["SqMiles"]
    sqKm_per_sqMi = 2.58999
    df_interp["pwPopPerSqMile"] = (df_interp["TotalPopulation"] * df_interp["TotalPopulation"]/df_interp["SqKmPop"]) * sqKm_per_sqMi
    print ("Reformatting numbers...")
    for col in (dataCols + extraIntCols):
        df_interp[col] = df_interp[col].map(lambda x: reformat(x))
    for col in extraFloatCols:
        df_interp[col] = df_interp[col].map(lambda x:  '{:.2f}'.format(x))
    return df_interp

'''
def aggregate_dasymmetric(nlcd, df_dat, df_agg, dataCols, districtFIPSCol, stateFIPSCol='STATEFP', nJobs=-1):
    crs = "EPSG:4326"
    df_dat, df_agg = reProjectBoth(df_dat, df_agg, crs)
    print("loading nlcd raster data via quilt")
    p = Package.browse("rasters/nlcd", "s3://spatial-ucr")
    p[nlcd].fetch()
    print("Aggregating small areas (via dasymetric areal interpolation using NLCD raster data)")
    df_dat.geometry = df_dat.buffer(0)
    df_agg.geometry = df_agg.buffer(0)
    df_interp = tobler.dasymetric.masked_area_interpolate(raster=nlcd
                                                          , source_df=df_dat
                                                          , target_df=df_agg
                                                          , extensive_variables=(['TotalPopulation', 'TotalIncome','SqKmPop'] + dataCols))
    df_interp = pandas.concat([df_agg[[stateFIPSCol, districtFIPSCol] + ['SqMiles','SqKm']], df_interp],axis=1) # put the keys + areas back
    print("Removing ZZ entries")
    df_interp = df_interp[(df_interp[districtFIPSCol] != "ZZ")]
    df_interp = df_interp.rename(columns={stateFIPSCol: "StateFIPS", districtFIPSCol: "District"})
    df_interp["PerCapitaIncome"] = df_interp["TotalIncome"]/df_interp["TotalPopulation"]
    df_interp["PopPerSqMile"] = df_interp["TotalPopulation"]/df_interp["SqMiles"]
    sqKm_per_sqMi = 2.58999
    df_interp["pwPopPerSqMile"] = (df_interp["TotalPopulation"] * df_interp["TotalPopulation"]/df_interp["SqKmPop"]) * sqKm_per_sqMi
    print ("Reformatting numbers...")
    for col in (dataCols + extraIntCols):
        df_interp[col] = df_interp[col].map(lambda x: reformat(x))
    for col in extraFloatCols:
        df_interp[col] = df_interp[col].map(lambda x:  '{:.2f}'.format(x))
    return df_interp
'''

df_tracts, tract_dataCols = loadShapesAndData(dataCSVs, dataShapes, totalPopCol, pcIncomeCol)
df_cds = loadAggregateToShapes(aggregateToShape)
df_aggregated = aggregate_simple(df_tracts, df_cds, tract_dataCols, aggToCol, 'STATEFP')
#df_aggregated = aggregate_dasymmetric(nlcd, df_tracts, df_cds, tract_dataCols, aggToCol)

outCols = ['StateFIPS',distCol] + extraIntCols + extraFloatCols + tract_dataCols

print ("Writing ", outCSV)
df_aggregated[outCols].to_csv(outCSV, index=False)
print ("done.")



#bg_dataTable, bg_dataCols = dataAndColsForMerge("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0003_ds176_20105_2010_blck_grp.csv")
#print (bg_dataTable.head())


#print("Loading block group shapefile")
#blk_grp_geo = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
#print(blk_grp_geo.head())
#print(blk_grp_geo.crs)

#print("Merging block group data into block group shapefile")
#blk_grp_geo = blk_grp_geo.merge(bg_dataTable, on='GISJOIN')
#print(blk_grp_geo.head())
