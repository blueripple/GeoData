import geopandas
import pandas
import tobler
import re
import numpy
#from quilt3 import Package #for nlcd
from os.path import exists
from geoFunctions import *
geopandas.options.use_pygeos = True

# Some notes
# 5-year ACS has much at the tract level
# Comes in 2 sets so code below can merge those
class AggregateTo:
    """Container For Fields required to Aggregate ACS block-group data geographically"""
    def __init__(self, stateFP, districtType, aggToShpFile, aggToCol, distCol, outCSV, outStats):
        self.stateFP = stateFP # col name for state FIPS or integer value of state FIPS for a single state input
        self.districtType = districtType
        self.aggToShpFile = aggToShpFile
        self.aggToCol = aggToCol # Column Name for the district number ?
        self.distCol = distCol #output column name for district number ?
        self.outCSV = outCSV
        self.outStats = outStats

def aggCongressional(stateAbbreviation, stateFIPS):
    print("Building district demographics for ", stateAbbreviation, " congressional districts.")
    aggTo = AggregateTo(stateFIPS
                        ,"Congressional"
                        ,"input_data/CongressionalDistricts/cd2024/" + stateAbbreviation + ".geojson"
                        ,"NAME"
                        ,"DistrictName"
                        ,"../bigData/Census/cd2024_ACS2022/" + stateAbbreviation + ".csv"
                        ,"../research/data/districtStats/2024/" + stateAbbreviation + "_congressional.csv"
                        )
    doAggregation(acs2022, aggTo, stateFIPS)
    print(stateAbbreviation, " done.")

def aggSLD(stateAbbreviation, stateFIPS, upperOnly):
    print("Building district demographics for ", stateAbbreviation, " state-leg districts.")
    print("Upper")
    aggTo = AggregateTo(stateFIPS
                        ,"StateUpper"
                        ,"input_data/StateLegDistricts/2024/" + stateAbbreviation + "_sldu.geojson"
                        ,"NAME"
                        ,"DistrictName"
                        ,"../bigData/Census/sldu2024_ACS2022/" + stateAbbreviation + ".csv"
                        ,"../research/data/districtStats/2024/" + stateAbbreviation + "_sldu.csv"

                        )
    doAggregation(acs2022, aggTo, stateFIPS)
    print("done")
    if not(stateAbbreviation in upperOnly):
        print("Lower")
        aggTo = AggregateTo(stateFIPS
                        ,"StateLower"
                        ,"input_data/StateLegDistricts/2024/" + stateAbbreviation + "_sldl.geojson"
                        ,"NAME"
                        ,"DistrictName"
                        ,"../bigData/Census/sldl2024_ACS2022/" + stateAbbreviation + ".csv"
                        ,"../research/data/districtStats/2024/" + stateAbbreviation + "_sldl.csv"
                        )
        doAggregation(acs2022, aggTo, stateFIPS)
        print("done")



extraIntCols =['TotalPopulation']
extraFloatCols = ['PerCapitaIncome','SqKm','SqMiles','PopPerSqMile','pwPopPerSqMile','SqKmPop']



def addPopAndIncome(df_dat, popC, pcIncomeC):
    df_dat2 = df_dat.copy()
    df_dat2["TotalPopulation"] = df_dat2[popC]
    df_dat2["TotalIncome"] = df_dat2[popC] * df_dat2[pcIncomeC]
    return df_dat2

def loadShapesAndData(dataFPS, shapeFP, popC, pcIncomeC, colPat= re.compile('^[A-Z0-9]+E\d+$'), joinCol='GISJOIN',stateFIPS=''):
    df_dat, dataCols = loadAndJoinData(dataFPS, colPat, joinCol)
    df_dat = addPopAndIncome(df_dat, popC, pcIncomeC)
    print (df_dat.head())
    print("Loading tract shapefile")
    df_geo = geopandas.read_file(shapeFP)
    df_geo['STATEFP'] = df_geo['STATEFP'].astype(int)
#    stateFIPSInt = stateFIPS.astype(int)
    if stateFIPS:
        df_geo.query('STATEFP == @stateFIPS', inplace=True)
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
    df_dat2 = df_dat.copy().to_crs(crs)
    print("Projecting aggregate-to areas to ",crs)
    df_agg2 = df_agg.copy().to_crs(crs)
    return df_dat2, df_agg2

def aggregate_simple(df_dat, df_agg, dataCols, districtFIPSInCol, districtFIPSOutCol, stateFIPSCol='STATEFP', nJobs=-1):
    crs = 'EPSG:3857'
    df_dat2, df_agg2 = reProjectBoth(df_dat, df_agg, crs)
    print("Aggregating small areas (via areal interpolation)")
    df_interp = tobler.area_weighted.area_interpolate(df_dat2
                                                      , df_agg2
                                                      , extensive_variables=(['TotalPopulation', 'TotalIncome','SqKmPop', 'PWLogPopPerSqKm'] + dataCols)
                                                      , n_jobs=nJobs)
    df_interp = pandas.concat([df_agg2[[stateFIPSCol, districtFIPSInCol] + ['SqMiles','SqKm']], df_interp],axis=1) # put the keys + areas back
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

def doAggregation(acsData, aggTo, stateFIPS=''):
    inputFPs = acsData.dataCSVs.copy()
    inputFPs.append(acsData.dataShapes)
    inputFPs.append(aggTo.aggToShpFile)
    if resultIsOlderOrMissing(aggTo.outCSV,inputFPs):
        df_tracts, tract_dataCols = loadShapesAndData(acsData.dataCSVs, acsData.dataShapes, acsData.totalPopCol, acsData.pcIncomeCol, stateFIPS=stateFIPS)
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
    else:
        print(aggTo.outCSV + " exists and is current with inputs.  Skipping.")

    if resultIsOlderOrMissing(aggTo.outStats,[aggTo.aggToShpFile]):
        print(aggTo.outStats + " is missing or out of date. Extracting from map...")
        gp = geopandas.read_file(aggTo.aggToShpFile)
        geopandaToStatsCSV(gp, aggTo.outStats)
    else:
        print(aggTo.outStats + " exists and is current with input map. Leaving in place.")

si = loadStatesInfo()

cdStatesAndFIPS = si.fipsFromAbbr.copy()
[cdStatesAndFIPS.pop(key) for key in si.oneDistrict.copy().union(si.noMaps)]
list(map(lambda t:aggCongressional(t[0], t[1]), cdStatesAndFIPS.items()))

sldStatesAndFIPS = si.fipsFromAbbr.copy()
[sldStatesAndFIPS.pop(key) for key in si.noMaps]
sldStatesAndFIPS.pop("DC")
list(map(lambda t:aggSLD(t[0], t[1],si.sldUpperOnly), sldStatesAndFIPS.items()))

# this one requires a separate run since it's for extant districts
#doAggregation(acs2020,cd116)
#doAggregation(acs2020,cd116NC)
#doAggregation(acs2020,cd116GA)
