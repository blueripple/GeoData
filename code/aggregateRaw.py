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

# Some notes
# 5-year ACS has much at the tract level
# Comes in 2 sets so code below can merge those
class AggregateTo:
    """Container For Fields required to Aggregate ACS block-group data geographically"""
    def __init__(self, stateFP, districtType, aggToShpFile, aggToCol, distCol, outCSV):
        self.stateFP = stateFP # col name for state FIPS or integer value of state FIPS for a single state input
        self.districtType = districtType
        self.aggToShpFile = aggToShpFile
        self.aggToCol = aggToCol # Column Name for the district number ?
        self.distCol = distCol #output column name for district number ?
        self.outCSV = outCSV


ncLower = AggregateTo(37
                      ,"StateLower"
                      ,"input_data/StateLegDistricts/NC/NC_2022_lower.geojson"
                      ,"NAME"
                      ,"DistrictNumber"
                      ,"output_data/StateLegDistricts/nc_2022_sldl.csv")

ncUpper = AggregateTo(37
                      ,"StateUpper"
                      ,"input_data/StateLegDistricts/NC/NC_2022_upper.geojson"
                      ,"NAME"
                      ,"DistrictNumber"
                      ,"output_data/StateLegDistricts/mc_2022_sldu.csv")



ncProposed = AggregateTo(37
                         ,"Congressional"
                         ,"input_data/CongressionalDistricts/cd117-proposed/NC-CST-13.geojson"
                         ,"NAME"
                         ,"DistrictNumber"
                         ,"output_data/US_2020_cd117P/cd117_NC.csv")

azCongressional = AggregateTo(4
                              ,"Congressional"
                              ,"input_data/CongressionalDistricts/cd117-proposed/AZ.geojson"
                              ,"NAME"
                              ,"DistrictNumber"
                              ,"output_data/US_2020_cd117P/cd117_AZ.csv")

azSLD = AggregateTo(4
                    ,"StateUpper"
                    ,"input_data/StateLegDistricts/AZ/slds_2022.geojson"
                    ,"NAME"
                    ,"DistrictNumber"
                    ,"output_data/StateLegislativeDistricts/az_sld.csv")

txProposed = AggregateTo(48
                         ,"Congressional"
                         ,"input_data/CongressionalDistricts/cd117-proposed/TX-proposed.geojson"
                         ,"NAME"
                         ,"DistrictNumber"
                         ,"output_data/US_2020_cd117P/cd117_TX.csv")

vaLower = AggregateTo("STATEFP"
                      , "StateLower"
                      , "input_data/StateLegDistricts/VA/tl_2020_51_sldl20/tl_2017_51_sldl.shp"
                      , 'SLDLST'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/va_2018_sldl.csv")

vaUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/VA/tl_2020_51_sldu20/tl_2020_51_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/va_2020_sldu.csv")

txLower = AggregateTo("STATEFP20"
                      , "StateLower"
                      , "input_data/StateLegDistricts/TX/tl_2020_48_sldl20/tl_2020_48_sldl20.shp"
                      , 'SLDLST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/tx_2020_sldl.csv")


txUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/TX/tl_2020_48_sldu20/tl_2020_48_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/tx_2020_sldu.csv")


gaLower = AggregateTo("STATEFP20"
                      , "StateLower"
                      , "input_data/StateLegDistricts/GA/tl_2020_13_sldl20/tl_2020_13_sldl20.shp"
                      , 'SLDLST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/ga_2020_sldl.csv")


gaUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/GA/tl_2020_13_sldu20/tl_2020_13_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/ga_2020_sldu.csv")

azLower = AggregateTo("STATEFP20"
                      , "StateLower"
                      , "input_data/StateLegDistricts/AZ/tl_2020_04_sldl20/tl_2020_04_sldl20.shp"
                      , 'SLDLST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/az_2020_sldl.csv")


azUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/AZ/tl_2020_04_sldu20/tl_2020_04_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/az_2020_sldu.csv")


nvLower = AggregateTo("STATEFP20"
                      , "StateLower"
                      , "input_data/StateLegDistricts/NV/tl_2020_32_sldl20/tl_2020_32_sldl20.shp"
                      , 'SLDLST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/nv_2020_sldl.csv")


nvUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/NV/tl_2020_32_sldu20/tl_2020_32_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/nv_2020_sldu.csv")


ohLower = AggregateTo("STATEFP20"
                      , "StateLower"
                      , "input_data/StateLegDistricts/OH/tl_2020_39_sldl20/tl_2020_39_sldl20.shp"
                      , 'SLDLST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/oh_2020_sldl.csv")


ohUpper = AggregateTo("STATEFP20"
                      , "StateUpper"
                      , "input_data/StateLegDistricts/OH/tl_2020_39_sldu20/tl_2020_39_sldu20.shp"
                      , 'SLDUST20'
                      , 'DistrictNumber'
                      , "output_data/StateLegDistricts/oh_2020_sldu.csv")

cd116NC = AggregateTo(37
                      ,"Congressional"
                      ,"input_data/CongressionalDistricts/DRA-cd116/NC.geojson"
                      , 'NAME'
                      , 'DistrictNumber'
                      , "output_data/US_2018_cd116/NC_DRA.csv")

cd116GA = AggregateTo(37
                      ,"Congressional"
                      ,"input_data/CongressionalDistricts/DRA-cd116/GA.geojson"
                      , 'NAME'
                      , 'DistrictNumber'
                      , "output_data/US_2018_cd116/GA_DRA.csv")

cd116 = AggregateTo("STATEFP"
                    ,"Congressional"
                    ,"input_data/CongressionalDistricts/cd116/tl_2018_us_cd116.shp"
                    , 'CD116FP'
                    , 'DistrictNumber'
                    , "output_data/US_2018_cd116/cd116Raw.csv")

cd115 = AggregateTo("STATEFP"
                    ,"Congressional"
                    ,"input_data/CongressionalDistricts/cd115/tl_2016_us_cd115.shp"
                    , 'CD115FP'
                    , 'DistrictNumber'
                    , "output_data/US_2016_cd115/cd115Raw.csv")

cd114 = AggregateTo("STATEFP"
                    ,"Congressional"
                    ,"input_data/CongressionalDistricts/cd114/tl_2014_us_cd114.shp"
                    , 'CD114FP'
                    , 'DistrictNumber'
                    , "output_data/US_2014_cd114/cd114Raw.csv")

cd113 = AggregateTo("STATEFP"
                    ,"Congressional"
                    ,"input_data/CongressionalDistricts/cd113/tl_2013_us_cd113.shp" # this is weird, the 2013 bit
                    , 'CD113FP'
                    , 'DistrictNumber'
                    , "output_data/US_2012_cd113/cd113Raw.csv")


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
    print ("Adding (CEA) area-weighted pop & pop weighted log density")
    sq_meters_per_sq_km = 1e6
    df_geo["SqKmPop"] = df_geo["TotalPopulation"] * df_geo['geometry'].area / sq_meters_per_sq_km
    df_geo["PWLogPopPerSqKm"] = df_geo["TotalPopulation"] * numpy.log(df_geo["TotalPopulation"] / (df_geo['geometry'].area / sq_meters_per_sq_km))
    return df_geo, dataCols

def loadAggregateToShapes(fp, stateFIPS):
    print("Loading aggregate-to shapefile")
    agg_geo = geopandas.read_file(fp)
    if type(stateFIPS) is int:
        print ("Manually adding state FIPS column to input shapes")
        agg_geo["STATEFP"] = stateFIPS
    print ("Adding areas, after projecting to CEA")
    agg_geo = agg_geo.to_crs({'proj':'cea'})
    print ("Computing areas")
    sq_meters_per_sq_mile = 1609.34 * 1609.34
    sq_meters_per_sq_km = 1e6
    agg_geo["SqMiles"] = agg_geo['geometry'].area / sq_meters_per_sq_mile
    agg_geo["SqKm"] = agg_geo['geometry'].area / sq_meters_per_sq_km
    print(agg_geo.head())
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

def aggregate_simple(df_dat, df_agg, dataCols, districtFIPSInCol, districtFIPSOutCol, stateFIPSCol='STATEFP', nJobs=-1):
    crs = 'EPSG:3857'
    df_dat, df_agg = reProjectBoth(df_dat, df_agg, crs)
    print("Aggregating small areas (via areal interpolation)")
    df_interp = tobler.area_weighted.area_interpolate(df_dat
                                                      , df_agg
                                                      , extensive_variables=(['TotalPopulation', 'TotalIncome','SqKmPop', 'PWLogPopPerSqKm'] + dataCols)
                                                      , n_jobs=nJobs)
    df_interp = pandas.concat([df_agg[[stateFIPSCol, districtFIPSInCol] + ['SqMiles','SqKm']], df_interp],axis=1) # put the keys + areas back
    print("Removing ZZ entries")
    df_interp = df_interp[(df_interp[districtFIPSInCol] != "ZZ")]
    df_interp = df_interp.rename(columns={stateFIPSCol: "StateFIPS", districtFIPSInCol: districtFIPSOutCol})
    df_interp["PerCapitaIncome"] = df_interp["TotalIncome"]/df_interp["TotalPopulation"]
    df_interp["PopPerSqMile"] = df_interp["TotalPopulation"]/df_interp["SqMiles"]
    sqKm_per_sqMi = 2.58999
#    df_interp["pwPopPerSqMile"] = (df_interp["TotalPopulation"] * df_interp["TotalPopulation"]/df_interp["SqKmPop"]) * sqKm_per_sqMi
    df_interp["pwPopPerSqMile"] = numpy.exp((df_interp["PWLogPopPerSqKm"]/df_interp["TotalPopulation"])) * sqKm_per_sqMi
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

def doAggregation(acsData, aggTo):
    df_tracts, tract_dataCols = loadShapesAndData(acsData.dataCSVs, acsData.dataShapes, acsData.totalPopCol, acsData.pcIncomeCol)
    df_cds = loadAggregateToShapes(aggTo.aggToShpFile,aggTo.stateFP)
    if type(aggTo.stateFP) is int:
        stateFPCol = "STATEFP"
    else:
        stateFPCol = aggTo.stateFP

    df_aggregated = aggregate_simple(df_tracts, df_cds, tract_dataCols, aggTo.aggToCol, aggTo.distCol, stateFPCol)
    outCols = ['StateFIPS',aggTo.distCol] + extraIntCols + extraFloatCols + tract_dataCols
    print ("Writing ", aggTo.outCSV)
    toWrite = df_aggregated[outCols]
    toWrite.insert(1,'DistrictType',aggTo.districtType)
    toWrite.to_csv(aggTo.outCSV, index=False)
    print ("done.")

doAggregation(acs2018,azSLD)
