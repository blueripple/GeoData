import geopandas
import pandas
import tobler
geopandas.options.use_pygeos = True

# Some notes
# All but citizenshep available from census at block-group level
# Citizenship available from 5-year ACS at Tract level
# So we build each, Use areal weighting to aggregate to districts and then merge

print("Loading block group data tables")
bg_df = pandas.read_csv("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0003_ds176_20105_2010_blck_grp.csv", encoding='latin-1')
print(bg_df.head())

print("Simpifying block group data")
bg_df["StateFIPS"] = bg_df["STATEA"]
bg_df["StateName"] = bg_df["STATE"]
bg_df["BG_Population"] = bg_df["JLZE001"]
bg_df["S_Male"] = bg_df["JLZE002"]
bg_df["S_Female"] = bg_df["JLZE026"]
bg_df["A_Under18"] = bg_df["JLZE003"] + bg_df["JLZE004"] + bg_df["JLZE005"] + bg_df["JLZE006"] + bg_df["JLZE027"] + bg_df["JLZE028"] + bg_df["JLZE029"] + bg_df["JLZE030"]
bg_df["A_18To24"] = bg_df["JLZE007"] + bg_df["JLZE008"] + bg_df["JLZE009"] + bg_df["JLZE010"] + bg_df["JLZE031"] + bg_df["JLZE032"] + bg_df["JLZE033"] + bg_df["JLZE034"]
bg_df["A_25To44"] = bg_df["JLZE011"] + bg_df["JLZE012"] + bg_df["JLZE013"] + bg_df["JLZE014"] + bg_df["JLZE035"] + bg_df["JLZE036"] + bg_df["JLZE037"]+ bg_df["JLZE038"]
bg_df["A_45To64"] = bg_df["JLZE015"] + bg_df["JLZE016"] + bg_df["JLZE017"] + bg_df["JLZE018"] + bg_df["JLZE019"] + bg_df["JLZE039"] + bg_df["JLZE040"] + bg_df["JLZE041"] + bg_df["JLZE042"] + bg_df["JLZE043"]
bg_df["A_65AndOver"] = bg_df["JLZE020"] + bg_df["JLZE021"] + bg_df["JLZE022"] + bg_df["JLZE023"] + bg_df["JLZE024"] + bg_df["JLZE025"] + bg_df["JLZE044"] + bg_df["JLZE045"] + bg_df["JLZE046"] + bg_df["JLZE047"] + bg_df["JLZE048"] + bg_df["JLZE049"]
bg_df["R_White"] = bg_df["JMBE002"]
bg_df["R_Black"] = bg_df["JMBE003"]
bg_df["R_Asian"] = bg_df["JMBE005"]
bg_df["R_Other"] = bg_df["JMBE004"] + bg_df["JMBE006"] + bg_df["JMBE007"] + bg_df["JMBE008"] + bg_df["JMBE009"] + bg_df["JMBE010"]
bg_df["Eth_Hispanic"] = bg_df["JMKE003"]
bg_df["Eth_NotHispanic"] = bg_df["JMKE002"]
bg_df["E_L9"] = bg_df["JN9E003"] + bg_df["JN9E004"] + bg_df["JN9E005"] + bg_df["JN9E006"] +  bg_df["JN9E020"] + bg_df["JN9E021"] + bg_df["JN9E022"] + bg_df["JN9E023"]
bg_df["E_9To12"] = bg_df["JN9E007"] + bg_df["JN9E008"] + bg_df["JN9E009"] + bg_df["JN9E010"] +  bg_df["JN9E024"] + bg_df["JN9E025"] + bg_df["JN9E026"] + bg_df["JN9E027"]
bg_df["E_HSGrad"] = bg_df["JN9E011"] + bg_df["JN9E028"]
bg_df["E_AS"] = bg_df["JN9E012"] +bg_df["JN9E013"] +bg_df["JN9E014"] + bg_df["JN9E029"] +bg_df["JN9E030"] +bg_df["JN9E031"]
bg_df["E_BA"] = bg_df["JN9E015"] + bg_df["JN9E028"]
bg_df["E_AD"] = bg_df["JN9E016"] +bg_df["JN9E017"] +bg_df["JN9E018"] + bg_df["JN9E033"] +bg_df["JN9E034"] +bg_df["JN9E035"]
bg_df["TotalIncome"] = bg_df["JQLE001"]

bg_dataCols = ["BG_Population", "S_Male", "S_Female"
               , "A_Under18", "A_18To24", "A_25To44", "A_45To64", "A_65AndOver"
               , "R_White", "R_Black", "R_Asian", "R_Other"
               , "Eth_Hispanic", "Eth_NotHispanic"
               , "E_L9", "E_9To12", "E_HSGrad", "E_AS", "E_BA", "E_AD"
               ]

bg_dataTable = bg_df[["GISJOIN"] + bg_dataCols + ["TotalIncome"]]
print (bg_dataTable.head())

print("Loading block group shapefile")
blk_grp_geo = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
print(blk_grp_geo.head())
print(blk_grp_geo.crs)

print("Merging block group data into block group shapefile")
blk_grp_geo = blk_grp_geo.merge(bg_dataTable, on='GISJOIN')
print(blk_grp_geo.head())



print("Loading tract data tables")
tract_pop_df = pandas.read_csv("input_data/NHGIS/US_2010_tract_csv/nhgis0005_ds176_20105_2010_tract.csv")
tract_cit_df = pandas.read_csv("input_data/NHGIS/US_2010_tract_csv/nhgis0005_ds177_20105_2010_tract.csv")

print("Re-labelling/simplifying tract data")
tract_pop_df["T_Population"] = tract_pop_df["JMAE001"]
tract_pop_df = tract_pop_df[["GISJOIN","TRACTA","T_Population"]]
tract_cit_df["C_U18"] = tract_cit_df["JWBE004"] + tract_cit_df["JWBE006"] + tract_cit_df["JWBE015"] + tract_cit_df["JWBE017"]
tract_cit_df["C_O18"] = tract_cit_df["JWBE009"] + tract_cit_df["JWBE011"] + tract_cit_df["JWBE020"] + tract_cit_df["JWBE022"]
tract_cit_df = tract_cit_df[["GISJOIN","C_U18","C_O18"]]
tract_dataCols = ["T_Population","C_U18","C_O18"]

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
cd_bg_interp = tobler.area_weighted.area_interpolate(blk_grp_geo, cd_geo, extensive_variables=bg_dataCols + ["TotalIncome"], n_jobs=-1)
cd_bg_interp["PerCapitaIncome"] = cd_bg_interp["TotalIncome"] / cd_bg_interp["BG_Population"]
cd_bg_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP','SqMiles']], cd_bg_interp],axis=1) # put the keys + areas back

print("Aggregating tract data into districts (via areal interpolation)")
cd_tract_interp = tobler.area_weighted.area_interpolate(tract_geo, cd_geo, extensive_variables=tract_dataCols, n_jobs=-1)
#cd_tract_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP'], tract_interp],axis=1) # put the keys + areas back

cd_interp = cd_bg_interp_rekeyed.join(cd_tract_interp[["T_Population","C_U18","C_O18"]])

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

print ("Computing average densities")
cd_interp["PopPerSqMile"] = cd_interp["BG_Population"]/cd_interp["SqMiles"]
#cd_interp = cd_interp[]

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
outCols = ['StateFIPS','CongressionalDistrict'] + bg_dataCols + tract_dataCols + ['SqMiles', 'PopPerSqMile', 'PerCapitaIncome']
cd_interp[outCols].to_csv("output_data/US_2010_cd116/cd116.csv", index=False, float_format="%.1f")
#cd_interp.to_csv("output_data/US_2010_bg_cd116/cd116.csv", index=False, float_format="%.1f")

#print("dissolving block groups to districts")
#cds_with_data_geo = blk_grps_with_cd.dissolve(by=["GEOID"], as_index=False, aggfunc='sum')

#print("Writing merged/dissolved shapefile")
#cds_with_data_geo.to_file("output_data/US_2010_districts/US_2010_districts.shp")
