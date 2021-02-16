import geopandas
import pandas
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

dataTable = tdf[["GISJOIN", "BLKGRPA", "StateName", "StateFIPS"
                 , "Population", "S_Male", "S_Female"
                , "A_Under18", "A_18To24", "A_25To44", "A_45To64", "A_65AndOver"
                , "R_White", "R_Black", "R_Asian", "R_Other"
                , "Eth_Hispanic", "Eth_NotHispanic"
                , "E_L9", "E_9To12", "E_HSGrad", "E_AS", "E_BA", "E_AD"
                , "TotalIncome"
                 ]]
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

cd_geo["StateFIPS"] = cd_geo["STATEFP"]
cd_geo["CongressionalDistrict"] = cd_geo["CD116FP"]
print(cd_geo.head())
print(cd_geo.crs)

print("Re-projecting block groups to match coordinate system of districts")
blk_grp_geo = blk_grp_geo.to_crs(cd_geo.crs)
print (blk_grp_geo.crs)

print("Intersecting block groups with districts")
blk_grps_with_cd = geopandas.sjoin(blk_grp_geo, cd_geo, how="inner", op="intersects")
print(blk_grps_with_cd.head())

print("dissolving block groups to districts")
cds_with_data_geo = blk_grps_with_cd.dissolve(by=["STATEFP10","CongressionalDistrict"], as_index=False, aggfunc='sum')

print("Writing merged/dissolved shapefile")
cds_with_data_geo.to_file("output_data/US_2010_districts/US_2010_districts.shp")
