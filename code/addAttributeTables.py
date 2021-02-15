import geopandas
import pandas
geopandas.options.use_pygeos = True

print("Loading associated data tables")
tdf = pandas.read_csv("input_data/NHGIS/US_2010_blk_grp_csv/nhgis0002_ds176_20105_2010_blck_grp.csv", encoding='latin-1')
print(tdf.head())

print("Simpifying data")
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
tdf["E_Hispanic"] = tdf["JMKE003"]
tdf["E_NotHispanic"] = tdf["JMKE002"]



print (tdf.head())


#print("Loading shapefile")
#gdf = geopandas.read_file("input_data/NHGIS/US_2010_blk_grp_shapefile/US_blck_grp_2010.shp")
#print(gdf.head())

#print("Merging")
#df = gdf.merge(tdf, on='GISJOIN')
#rint(gdf.head())

#print("Writing merged shapefile")
#gdf.to_file("output_data/US_2010_blk_grp_merged/US_2010_blk_grp.shp")
