import psycopg2
import re

import areal
import geoFunctions

dbname = "tracts_and_nlcd"
db_user = "postgres"
db_password = "postgres"
schema_name = "public"

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")

def fix_df(stateFIPS, chamber, df):
    df['DistrictType'] = chamber
    df['StateFIPS'] = str(stateFIPS)
    df['SqKm'] = df['SqMiles'] * 2.58999
    df['SqKmPop'] = df['SqKm'] * df['TotalPopulation']
#    df.drop(['ID','DevSqMiles','PopPerDevSqMile','pwPopPerDevSqMile'], axis=1)
    census_data_pat = re.compile(r'A...E\d\d\d')
    census_data_cols = list(filter(census_data_pat.fullmatch, df.columns))
    new_cols =['StateFIPS','DistrictType','DistrictName','TotalPopulation','PerCapitaIncome','SqKm','SqMiles','PopPerSqMile','pwPopPerSqMile','SqKmPop'] + census_data_cols
    fixed_df = df[new_cols]
    return fixed_df


def make_cd_csv(stateFIPS, stateAB, outDir):
    shapefile = "/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024/{}.geojson".format(stateAB)
    print("Doing areal interpolation for congressional districts in {} from file: {}".format(stateAB, shapefile))
    df = areal.dasymmetric_from_file(conn, shapefile, areal.acs2022_and_lcd_params)
    df = fix_df(stateFIPS, 'Congressional', df)
    csv_file = (outDir + '/{}.csv').format(stateAB)
    print("Saving in {}".format(csv_file))
    df.to_csv(csv_file, header=True, index=False, float_format="%.2f")

areal.load_shapes_from_file(conn,"/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024/CO.geojson","CO_test")
#make_cd_csv(2,'CO','test')

exit(0)

si = geoFunctions.loadStatesInfo()

cdStatesAndFIPS = si.fipsFromAbbr.copy()
[cdStatesAndFIPS.pop(key) for key in si.oneDistrict.copy().union(si.noMaps)]
list(map(lambda t:aggCongressional(t[0], t[1]), cdStatesAndFIPS.items()))

sldStatesAndFIPS = si.fipsFromAbbr.copy()
[sldStatesAndFIPS.pop(key) for key in si.noMaps]
sldStatesAndFIPS.pop("DC")
list(map(lambda t:aggSLD(t[0], t[1],si.sldUpperOnly), sldStatesAndFIPS.items()))
