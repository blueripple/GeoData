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

def dataAndColsForMerge(fp):
    pat = re.compile('^[A-Z0-9]+E\d+$')
    df = pandas.read_csv(fp, encoding='latin-1')
    dataCols = [s for s in df.columns if pat.match(s)]
    colVals = ['GISJOIN'] + dataCols
    return df.filter(colVals, axis=1), dataCols


#bg_dataTable, bg_dataCols = dataAndColsForMerge("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0003_ds176_20105_2010_blck_grp.csv")
#print (bg_dataTable.head())


#print("Loading block group shapefile")
#blk_grp_geo = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
#print(blk_grp_geo.head())
#print(blk_grp_geo.crs)

#print("Merging block group data into block group shapefile")
#blk_grp_geo = blk_grp_geo.merge(bg_dataTable, on='GISJOIN')
#print(blk_grp_geo.head())

print("Loading tract data tables")
tractA_df, tractACols = dataAndColsForMerge("input_data/NHGIS/US_2018_tract_csv/nhgis0006_ds239_20185_2018_tract_E.csv")
tractB_df, tractBCols = dataAndColsForMerge("input_data/NHGIS/US_2018_tract_csv/nhgis0006_ds240_20185_2018_tract_E.csv")

tract_dataCols = tractACols + tractBCols

print("Joining tract-level data")
tract_df = tractA_df.set_index('GISJOIN').join(tractB_df.set_index('GISJOIN'))
print(tract_df.head())

print("Loading tract shapefile")
tract_geo = geopandas.read_file("input_data/NHGIS/US_2018_tract_shapefile/US_tract_2018.shp")
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
sq_meters_per_sq_km = 1e6
cd_geo["SqMiles"] = cd_geo['geometry'].area / sq_meters_per_sq_mile
cd_geo["SqKm"] = cd_geo['geometry'].area / sq_meters_per_sq_km

crs = "EPSG:3857"
#print("Projecting block groups to ", crs)
#blk_grp_geo = blk_grp_geo.to_crs(crs)
print("Projecting tracts to ", crs)
tract_geo = tract_geo.to_crs(crs)
print("Projecting districts to ",crs)
cd_geo = cd_geo.to_crs(crs)


#print("Aggregating block group data into districts (via areal interpolation)")
#cd_bg_interp = tobler.area_weighted.area_interpolate(blk_grp_geo, cd_geo, extensive_variables=bg_dataCols, n_jobs=-1)
#cd_bg_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP','SqMiles','SqKm']], cd_bg_interp],axis=1) # put the keys + areas back

print("Aggregating tract data into districts (via areal interpolation)")
cd_tract_interp = tobler.area_weighted.area_interpolate(tract_geo, cd_geo, extensive_variables=tract_dataCols, n_jobs=-1)
cd_tract_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP','SqMiles','SqKm']], cd_tract_interp],axis=1) # put the keys + areas back
cd_interp = cd_tract_interp_rekeyed #cd_bg_interp_rekeyed.join(cd_tract_interp[tract_dataCols],rsuffix="_tract")

print("Removing ZZ entries")
cd_interp = cd_interp[(cd_interp.CD116FP != "ZZ")]

cd_interp = cd_interp.rename(columns={"STATEFP": "StateFIPS", "CD116FP": "CongressionalDistrict"})

outCols = ['StateFIPS','CongressionalDistrict','SqMiles','SqKm'] + tract_dataCols

print ("Reformatting numbers...")

def reformat(x):
    if (isinstance(x, float)):
        return '{:.0f}'.format(x)
    else:
        return x

for col in tract_dataCols:
    cd_interp[col] = cd_interp[col].map(lambda x: reformat(x))

cd_interp["SqMiles"] = cd_interp["SqMiles"].map(lambda x: '{:.2f}'.format(x))
cd_interp["SqKm"] = cd_interp["SqKm"].map(lambda x: '{:.2f}'.format(x))

print ("Saving CSV")
cd_interp[outCols].to_csv("output_data/US_2018_cd116/cd116Raw.csv", index=False) # , float_format="%.1f")
print ("done.")
