import psycopg2
import psycopg2.sql as sql
import re
import pandas as pd
import areal
import geoFunctions

dbname = "tracts_and_nlcd"
db_user = "postgres"
db_password = "postgres"
schema_name = "public"
tract_and_lcd = areal.acs2022_and_lcd_params

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


def fix_tract_df(df):
    df['SqKmPop'] = df['SqKm'] * df['TotalPopulation']
    df['SqMiles'] = df['SqKm'] / 2.58999
    df['PopPerSqMile'] =  2.58999 * df['TotalPopulation'] / df['SqKm']
    df['pwPopPerSqMile'] = df['PopPerSqMile']
    census_data_pat = re.compile(r'A...E\d\d\d')
    census_data_cols = list(filter(census_data_pat.fullmatch, df.columns))
    new_cols =['StateAbbreviation','TractId','TotalPopulation','PerCapitaIncome','SqKm','SqMiles','PopPerSqMile','pwPopPerSqMile','SqKmPop'] + census_data_cols
    fixed_df = df[new_cols]
    return fixed_df

def tract_sql(tract_and_lcd_parameters, db_cursor):
    parms = dict()
    parms["data_geom_table"] = sql.Identifier(tract_and_lcd_parameters["data_geom_table"])
    parms["data_geom_id_col"] = sql.Identifier(tract_and_lcd_parameters["data_geom_id_col"])
    parms["data_geom_col"] = sql.Identifier(tract_and_lcd_parameters["data_geom_col"])
    parms["pop_col"] = sql.Identifier(tract_and_lcd_parameters["pop_col"])
    ext_cols = areal.extensive_cols(tract_and_lcd_parameters, db_cursor)
    parms["ext_cols"] = sql.SQL(', ').join(map(lambda x: sql.SQL('{ev}').format(ev=sql.Identifier(x)), ext_cols))
    parms["ext_colsj"] = sql.SQL(', ').join(map(lambda x: sql.SQL('{ev}').format(ev=sql.Identifier(x)), ext_cols))
    parms["int_cols"] = sql.SQL(', ').join(map(lambda x: sql.SQL('coalesce({iv},0) as {ivName}').format(iv=sql.Identifier(areal.inTuple(0,x)), ivName=sql.Identifier(areal.inTuple(1,x))), tract_and_lcd_parameters["intensive_cols"]))
    parms["int_colsO"] = sql.SQL(', ').join(map(lambda x: sql.SQL('{ivName}').format(iv=sql.Identifier(areal.inTuple(0,x)), ivName=sql.Identifier(areal.inTuple(1,x))), tract_and_lcd_parameters["intensive_cols"]))
    sql_str = sql.SQL('''
select "StateAbbreviation", {data_geom_id_col} as "TractId", {pop_col} as "TotalPopulation", "SqKm", {int_colsO}, {ext_cols}
from (
    select "StateAbbreviation", {data_geom_id_col}, {pop_col}, ST_area({data_geom_col} :: geography) * 1e-6 as "SqKm", {int_cols}, {ext_cols}
    from {data_geom_table}
    inner join "states" on {data_geom_table}."statefp" :: integer = "states"."StateFIPS"
    where {data_geom_table}."statefp" :: integer < 60
    )
where {pop_col} is not null AND {pop_col} > 0
''').format(**parms)
    return sql_str

#print(tract_sql(areal.acs2022_and_lcd_params, conn.cursor()).as_string(conn))
#exit(0)

def make_tract_csv(tract_and_lcd_parameters, csv_name, db_conn):
    cur = db_conn.cursor()
    sql_str = tract_sql(tract_and_lcd_parameters, cur)
    print(sql_str.as_string(conn))
    cur.execute(sql_str)
    sql_result = cur.fetchall()
    ext_cols = areal.extensive_cols(tract_and_lcd_parameters, cur)
    int_cols = list(map(lambda x:areal.inTuple(1,x), tract_and_lcd_parameters["intensive_cols"]))
    cols = ["StateAbbreviation", "TractId", "TotalPopulation", "SqKm"] + int_cols + ext_cols
    df = pd.DataFrame(sql_result, columns=cols) #, columns=cols)
    df = fix_tract_df(df)
    df.to_csv(csv_name, header=True, index=False, float_format="%.2f")

make_tract_csv(areal.acs2022_and_lcd_params, "test_tract.csv", conn)

exit(0)


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
        print("(Re)building overlaps between upper house of state legislature and congressional districts for {}".format(stateAB))
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

overlapSLDStatesAndFIPS = si.fipsFromAbbr.copy()
[overlapSLDStatesAndFIPS.pop(key) for key in si.oneDistrict.copy().union(si.noMaps)]
list(map(lambda t:make_sld_cd_overlaps(t[1], t[0], si.sldUpperOnly), overlapSLDStatesAndFIPS.items()))
