import psycopg2
import psycopg2.sql as sql
import re

import areal
import geoFunctions

dbname = "tracts_and_nlcd"
db_user = "postgres"
db_password = "postgres"
schema_name = "public"
tract_and_lcd = areal.acs2022_and_lcd_params
cd_shape_dir = "/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024"
cd_csv_dir = 'test/cd2024_ACS2022'
sld_shape_dir = "/Users/adam/BlueRipple/GeoData/input_data/StateLegDistricts/2024"
sldl_csv_dir = 'test/sldl2024_ACS2022'
sldu_csv_dir = 'test/sldu2024_ACS2022'
sld_overlap_dir = 'test/overlaps'

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")

def fix_df(stateFIPS, chamber, df):
    df['DistrictType'] = chamber
    df['StateFIPS'] = str(stateFIPS)
    df['SqKmPop'] = df['SqKm'] * df['TotalPopulation']
    df['SqMiles'] = df['SqKm'] / 2.58999
    df['PopPerSqMile'] = df['PopPerSqKm'] * 2.58999
    df['pwPopPerSqMile'] = df['pwPopPerSqKm'] * 2.58999
    census_data_pat = re.compile(r'A...E\d\d\d')
    census_data_cols = list(filter(census_data_pat.fullmatch, df.columns))
    new_cols =['StateFIPS','DistrictType','DistrictName','TotalPopulation','PerCapitaIncome','SqKm','SqMiles','PopPerSqMile','pwPopPerSqMile','SqKmPop'] + census_data_cols
    fixed_df = df[new_cols]
    return fixed_df

def make_cd_csv(stateFIPS, stateAB):
    shapefile = "{}/{}.geojson".format(cd_shape_dir, stateAB)
    csv_file = (cd_csv_dir + '/{}.csv').format(stateAB)
    if geoFunctions.resultIsOlderOrMissing(csv_file, [shapefile]):
        print("Doing areal interpolation for congressional districts in {} from file: {}".format(stateAB, shapefile))
        df = areal.dasymmetric_interpolation_from_file(conn, shapefile, tract_and_lcd)
        df = fix_df(stateFIPS, 'Congressional', df)
        print("Saving in {}".format(csv_file))
        df.to_csv(csv_file, header=True, index=False, float_format="%.2f")
    else:
        print("{} is up to date!".format(csv_file))

def make_sld_csvs(stateFIPS, stateAB, upperOnly):
    sldu_shapefile = "{}/{}_sldu.geojson".format(sld_shape_dir, stateAB)
    csv_file_u = (sldu_csv_dir + '/{}.csv').format(stateAB)
    if geoFunctions.resultIsOlderOrMissing(csv_file_u, [sldu_shapefile]):
        print("Doing areal interpolation for upper house districts in {} from file: {}".format(stateAB, sldu_shapefile))
        dfu = areal.dasymmetric_interpolation_from_file(conn, sldu_shapefile, tract_and_lcd)
        dfu = fix_df(stateFIPS, 'StateUpper', dfu)
        print("Saving in {}".format(csv_file_u))
        dfu.to_csv(csv_file_u, header=True, index=False, float_format="%.2f")
    else:
        print("{} is up to date!".format(csv_file_u))
    if not(stateAB in upperOnly):
         sldl_shapefile = "{}/{}_sldl.geojson".format(sld_shape_dir, stateAB)
         csv_file_l = (sldl_csv_dir + '/{}.csv').format(stateAB)
         if geoFunctions.resultIsOlderOrMissing(csv_file_l, [sldl_shapefile]):
             print("Doing areal interpolation for lower house districts in {} from file: {}".format(stateAB, sldl_shapefile))
             dfl = areal.dasymmetric_interpolation_from_file(conn, sldl_shapefile, tract_and_lcd)
             dfl = fix_df(stateFIPS, 'StateLower', dfl)
             print("Saving in {}".format(csv_file_l))
             dfl.to_csv(csv_file_l, header=True, index=False, float_format="%.2f")
         else:
             print("{} is up to date!".format(csv_file_l))
    else:
        print("{} has no lower house.".format(stateAB))

def make_sld_cd_overlaps(stateFIPS, stateAB, upperOnly):
    cd_shapefile = "{}/{}.geojson".format(cd_shape_dir, stateAB)
    sldu_shapefile = "{}/{}_sldu.geojson".format(sld_shape_dir, stateAB)
    overlap_file_u = (sld_overlap_dir + '/{}_SLDU_CD.csv').format(stateAB)
    if geoFunctions.resultIsOlderOrMissing(overlap_file_u, [sldu_shapefile, cd_shapefile]):
        print("(Re)building overlaps between upper house of state and legislature and congressional districts for {}".format(stateAB))
        dfo = areal.dasymmetric_overlap_from_files(conn, sldu_shapefile, cd_shapefile, tract_and_lcd)
        print("saving in {}".format(overlap_file_u))
        dfo.to_csv(overlap_file_u, header=True, index=False)
    else:
        print("{} is up to date!".format(overlap_file_u))
    if not(stateAB in upperOnly):
        sldl_shapefile = "{}/{}_sldl.geojson".format(sld_shape_dir, stateAB)
        overlap_file_l = (sld_overlap_dir + '/{}_SLDL_CD.csv').format(stateAB)
        if geoFunctions.resultIsOlderOrMissing(overlap_file_l, [sldl_shapefile, cd_shapefile]):
            print("(Re)building overlaps between lower house of state and legislature and congressional districts for {}".format(stateAB))
            dfo = areal.dasymmetric_overlap_from_files(conn, sldl_shapefile, cd_shapefile, tract_and_lcd)
            print("saving in {}".format(overlap_file_l))
            dfo.to_csv(overlap_file_l, header=True, index=False)
        else:
            print("{} is up to date!".format(overlap_file_l))

#areal.load_shapes_from_file(conn,"/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024/CO.geojson","CO_test")
#make_cd_csv(2,'CO','test')

#exit(0)

si = geoFunctions.loadStatesInfo()

cdStatesAndFIPS = si.fipsFromAbbr.copy()
[cdStatesAndFIPS.pop(key) for key in si.oneDistrict.copy().union(si.noMaps)]
#list(map(lambda t:make_cd_csv(t[1], t[0]), cdStatesAndFIPS.items()))

sldStatesAndFIPS = si.fipsFromAbbr.copy()
[sldStatesAndFIPS.pop(key) for key in si.noMaps]
sldStatesAndFIPS.pop("DC")
#list(map(lambda t:make_sld_csvs(t[1], t[0], si.sldUpperOnly), sldStatesAndFIPS.items()))

list(map(lambda t:make_sld_cd_overlaps(t[1], t[0], si.sldUpperOnly), sldStatesAndFIPS.items()))
