import geopandas
import pandas
import tobler
import re
geopandas.options.use_pygeos = True

# Some notes
# All but citizenshep available from census at block-group level
# Citizenship available from 5-year ACS at Tract level
# So we build each, Use areal weighting to aggregate to districts and then merge

print("Loading block group data tables")
bg_df = pandas.read_csv("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0003_ds176_20105_2010_blck_grp.csv", encoding='latin-1')
print(bg_df.head())


def dataAndColsForMerge(df):
    pat = re.compile('^[A-Z0-9]+E\d+$')
    dataCols = [s for s in df.columns if pat.match(s)]
    colVals = ['GISJOIN'] + dataCols
    return df.filter(colVals, axis=1), dataCols


bg_dataTable, bg_dataCols = dataAndColsForMerge(bg_df)
print (bg_dataTable.head())


print("Loading block group shapefile")
blk_grp_geo = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
print(blk_grp_geo.head())
print(blk_grp_geo.crs)

print("Merging block group data into block group shapefile")
blk_grp_geo = blk_grp_geo.merge(bg_dataTable, on='GISJOIN')
print(blk_grp_geo.head())

print("Loading tract data tables")
tract_pop_df, tractPopCols = dataAndColsForMerge(pandas.read_csv("input_data/NHGIS/US_2010_tract_csv/nhgis0005_ds176_20105_2010_tract.csv"))
tract_cit_df, tractCitCols = dataAndColsForMerge(pandas.read_csv("input_data/NHGIS/US_2010_tract_csv/nhgis0005_ds177_20105_2010_tract.csv"))
tract_dataCols = tractPopCols + tractCitCols

print("Joining tract-level data")
tract_df = tract_pop_df.set_index('GISJOIN').join(tract_cit_df.set_index('GISJOIN'))
print(tract_df.head())

print("Loading tract shapefile")
tract_geo = geopandas.read_file("input_data/NHGIS/US_2010_tract_shapefile/US_tract_2010.shp")
print(tract_geo.head())
print(tract_geo.crs)

print("Merging tract-level-data into tract shapefile")
tract_geo = tract_geo.merge(tract_df, on='GISJOIN')

print("Loading congressional district shapefile")
cd_geo = geopandas.read_file("input_data/CongressionalDistricts/cd116/tl_2018_us_cd116.shp")

# There are entries with CD116FP == 'ZZ' representing census areas with no people.  Leave them in until later!
# Otherwise tobler gets confused somehow

print ("Adding areas to districts")
print ("Re-projecting to CEA")
cd_geo = cd_geo.to_crs({'proj':'cea'})
print ("Computing areas")
sq_meters_per_sq_mile = 1609.34 * 1609.34
cd_geo["SqMiles"] = cd_geo['geometry'].area/ sq_meters_per_sq_mile

crs = "EPSG:3857"
print("Projecting block groups to ", crs)
blk_grp_geo = blk_grp_geo.to_crs(crs)
print("Projecting tracts to ", crs)
tract_geo = tract_geo.to_crs(crs)
print("Projecting districts to ",crs)
cd_geo = cd_geo.to_crs(crs)


print("Aggregating block group data into districts (via areal interpolation)")
cd_bg_interp = tobler.area_weighted.area_interpolate(blk_grp_geo, cd_geo, extensive_variables=bg_dataCols, n_jobs=-1)
cd_bg_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP','SqMiles']], cd_bg_interp],axis=1) # put the keys + areas back

print("Aggregating tract data into districts (via areal interpolation)")
cd_tract_interp = tobler.area_weighted.area_interpolate(tract_geo, cd_geo, extensive_variables=tract_dataCols, n_jobs=-1)
#cd_tract_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP'], tract_interp],axis=1) # put the keys + areas back

cd_interp = cd_bg_interp_rekeyed.join(cd_tract_interp[tract_dataCols],rsuffix="_tract")

print("Removing ZZ entries")
cd_interp = cd_interp[(cd_interp.CD116FP != "ZZ")]

#cd_interp[["STATEFP","CD116FP","SqMiles","BG_Population"]].to_csv("output_data/tmp.csv")
#print(cd_interp_rekeyed.head())

#blk_grps_with_cd = geopandas.sjoin(blk_grp_geo, cd_geo, how="inner", op="intersects")
#print(blk_grps_with_cd.head())

#blk_grp_data = blk_grps_with_cd[["GISJOIN", "BLKGRPA", "StateName", "STATEFP", "CongressionalDistrict"
#                                 , "Population", "S_Male", "S_Female"
#                                 , "A_Under18", "A_18To24", "A_25To44", "A_45To64", "A_65AndOver"
#                                 , "R_White", "R_Black", "R_Asian", "R_Other"
#                                 , "Eth_Hispanic", "Eth_NotHispanic"
#                                 , "E_L9", "E_9To12", "E_HSGrad", "E_AS", "E_BA", "E_AD"
#                                 , "TotalIncome"
#
#]]
##print (blk_grp_data.head())
cd_interp = cd_interp.rename(columns={"STATEFP": "StateFIPS", "CD116FP": "CongressionalDistrict"})

def format_float(value):
    if (value > 100):
        return f'{value: ,.0f}'
    elif (value > 10):
        return f'{value: ,.1f}'
    return f'{value: ,.0f}'

#float_cols = cd_interp_rekeyed.select_dtypes(float).columns
#formatDict = {}
#for key in float_cols:
#    formatDict[key] = format_float
#cd_interp_rekeyed = cd_interp_rekeyed.style.format(formatDict)
outCols = ['StateFIPS','CongressionalDistrict'] + bg_dataCols + tract_dataCols + ['SqMiles']
cd_interp[outCols].to_csv("output_data/US_2010_cd116/cd116Raw.csv", index=False, float_format="%.1f")
#cd_interp.to_csv("output_data/US_2010_bg_cd116/cd116.csv", index=False, float_format="%.1f")

#print("dissolving block groups to districts")
#cds_with_data_geo = blk_grps_with_cd.dissolve(by=["GEOID"], as_index=False, aggfunc='sum')

#print("Writing merged/dissolved shapefile")
#cds_with_data_geo.to_file("output_data/US_2010_districts/US_2010_districts.shp")
