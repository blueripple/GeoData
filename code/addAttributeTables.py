import geopandas
import pandas
import tobler
geopandas.options.use_pygeos = True

print("Loading block group data tables")
tdf = pandas.read_csv("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0002_ds176_20105_2010_blck_grp.csv", encoding='latin-1')
print(tdf.head())

print("Simpifying block group data")
tdf["StateFIPS"] = tdf["STATEA"]
tdf["StateName"] = tdf["STATE"]
tdf["Population"] = tdf["JLZE001"]
tdf["S_Male"] = tdf["JLZE002"]
tdf["S_Female"] = tdf["JLZE026"]
tdf["A_Under18"] = tdf["JLZE003"] + tdf["JLZE004"] + tdf["JLZE005"] + tdf["JLZE006"] + tdf["JLZE027"] + tdf["JLZE028"] + tdf["JLZE029"] + tdf["JLZE030"]
tdf["A_18To24"] = tdf["JLZE007"] + tdf["JLZE008"] + tdf["JLZE009"] + tdf["JLZE010"] + tdf["JLZE031"] + tdf["JLZE032"] + tdf["JLZE033"] + tdf["JLZE034"]
tdf["A_25To44"] = tdf["JLZE011"] + tdf["JLZE012"] + tdf["JLZE013"] + tdf["JLZE014"] + tdf["JLZE035"] + tdf["JLZE036"] + tdf["JLZE037"]+ tdf["JLZE038"]
tdf["A_45To64"] = tdf["JLZE015"] + tdf["JLZE016"] + tdf["JLZE017"] + tdf["JLZE018"] + tdf["JLZE019"] + tdf["JLZE039"] + tdf["JLZE040"] + tdf["JLZE041"] + tdf["JLZE042"] + tdf["JLZE043"]
tdf["A_65AndOver"] = tdf["JLZE020"] + tdf["JLZE021"] + tdf["JLZE022"] + tdf["JLZE023"] + tdf["JLZE024"] + tdf["JLZE025"] + tdf["JLZE044"] + tdf["JLZE045"] + tdf["JLZE046"] + tdf["JLZE047"] + tdf["JLZE048"] + tdf["JLZE049"]
tdf["R_White"] = tdf["JMBE002"]
tdf["R_Black"] = tdf["JMBE003"]
tdf["R_Asian"] = tdf["JMBE005"]
tdf["R_Other"] = tdf["JMBE004"] + tdf["JMBE006"] + tdf["JMBE007"] + tdf["JMBE008"] + tdf["JMBE009"] + tdf["JMBE010"]
tdf["Eth_Hispanic"] = tdf["JMKE003"]
tdf["Eth_NotHispanic"] = tdf["JMKE002"]
tdf["E_L9"] = tdf["JN9E003"] + tdf["JN9E004"] + tdf["JN9E005"] + tdf["JN9E006"] +  tdf["JN9E020"] + tdf["JN9E021"] + tdf["JN9E022"] + tdf["JN9E023"]
tdf["E_9To12"] = tdf["JN9E007"] + tdf["JN9E008"] + tdf["JN9E009"] + tdf["JN9E010"] +  tdf["JN9E024"] + tdf["JN9E025"] + tdf["JN9E026"] + tdf["JN9E027"]
tdf["E_HSGrad"] = tdf["JN9E011"] + tdf["JN9E028"]
tdf["E_AS"] = tdf["JN9E012"] +tdf["JN9E013"] +tdf["JN9E014"] + tdf["JN9E029"] +tdf["JN9E030"] +tdf["JN9E031"]
tdf["E_BA"] = tdf["JN9E015"] + tdf["JN9E028"]
tdf["E_AD"] = tdf["JN9E016"] +tdf["JN9E017"] +tdf["JN9E018"] + tdf["JN9E033"] +tdf["JN9E034"] +tdf["JN9E035"]
tdf["TotalIncome"] = tdf["JQLE001"]

dataCols = ["Population", "S_Male", "S_Female"
            , "A_Under18", "A_18To24", "A_25To44", "A_45To64", "A_65AndOver"
            , "R_White", "R_Black", "R_Asian", "R_Other"
            , "Eth_Hispanic", "Eth_NotHispanic"
            , "E_L9", "E_9To12", "E_HSGrad", "E_AS", "E_BA", "E_AD"
            ]

dataTable = tdf[["GISJOIN"] + dataCols + ["TotalIncome"]]
print (dataTable.head())


print("Loading block group shapefile")
blk_grp_geo = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
print(blk_grp_geo.head())
print(blk_grp_geo.crs)

print("Merging block group data into block group shapefile")
blk_grp_geo = blk_grp_geo.merge(dataTable, on='GISJOIN')
print(blk_grp_geo.head())

print("Loading congressional district shapefile")
cd_geo = geopandas.read_file("input_data/CongressionalDistricts/cd116/tl_2018_us_cd116.shp")
print(cd_geo.head())
print("Removing ZZ entries from district geoData")
cd_geo = cd_geo[(cd_geo.CD116FP != "ZZ")]

print ("Adding areas to districts")
print ("Re-projecting to CEA")
cd_geo = cd_geo.to_crs({'proj':'cea'})
print ("Computing areas")
sq_meters_per_sq_mile = 1609.34 * 1609.34
cd_geo["SqMiles"] = cd_geo['geometry'].area/ sq_meters_per_sq_mile


crs = "EPSG:3857"
print("Projecting block groups to ", crs)
blk_grp_geo = blk_grp_geo.to_crs(crs)
print("Projecting districts to ",crs)
cd_geo = cd_geo.to_crs(crs)

print("Aggregating block group data into districts (via areal interpolation)")
cd_interp = tobler.area_weighted.area_interpolate(blk_grp_geo, cd_geo, extensive_variables=dataCols + ["TotalIncome"], n_jobs=-1)
cd_interp["PerCapitaIncome"] = cd_interp["TotalIncome"] / cd_interp["Population"]
cd_interp_rekeyed = pandas.concat([cd_geo[['STATEFP','CD116FP','SqMiles']], cd_interp],axis=1) # put the keys + areas back
print(cd_interp_rekeyed.head())

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
cd_interp_rekeyed = cd_interp_rekeyed.rename(columns={"STATEFP": "StateFIPS", "CD116FP": "CongressionalDistrict"})

print ("Computing average densities")
cd_interp_rekeyed["PopPerSqMile"] = cd_interp_rekeyed["Population"]/cd_interp_rekeyed["SqMiles"]

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

cd_interp_rekeyed[['StateFIPS','CongressionalDistrict'] + dataCols + ["SqMiles", "PopPerSqMile", "PerCapitaIncome"]].to_csv("output_data/US_2010_bg_cd116/cd116.csv", index=False, float_format="%.1f")

#print("dissolving block groups to districts")
#cds_with_data_geo = blk_grps_with_cd.dissolve(by=["GEOID"], as_index=False, aggfunc='sum')

#print("Writing merged/dissolved shapefile")
#cds_with_data_geo.to_file("output_data/US_2010_districts/US_2010_districts.shp")
