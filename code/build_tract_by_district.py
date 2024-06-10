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
cd_shape_dir = "/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024"
sld_shape_dir = "/Users/adam/BlueRipple/GeoData/input_data/StateLegDistricts/2024"
tract_shape_table = "tracts2022_shapes"

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")

def tracts_by_district(state_fips, state_abbreviation, d_type, d_shp_file, d_name_col="id_0", d_geom_col="geom", tract_shapes=tract_shape_table, tract_id_col = "geoid", tract_geom_col="geom"):
    tmp_table_name = "shapes_tmp"
    areal.load_shapes_from_file(conn, d_shp_file, tmp_table_name)
    conn.commit()
    sql_str = sql.SQL('''
SELECT {d_name}, {t_id}
FROM {tract_table} as t
inner JOIN {d_table} as d ON (ST_OVERLAPS({t_geom}, {d_geom}) OR ST_CONTAINS({d_geom}, {t_geom}))
WHERE ST_AREA(ST_INTERSECTION({t_geom}, {d_geom})) > 0.01 * ST_AREA({t_geom})
    ''').format(d_name = sql.Identifier("d", d_name_col)
                , t_id = sql.Identifier("t", tract_id_col)
                , tract_table = sql.Identifier(tract_shapes)
                , d_table = sql.Identifier(tmp_table_name)
                , t_geom = sql.Identifier("t", tract_geom_col)
                , d_geom = sql.Identifier("d", d_geom_col)
                )
    try:
#        print(sql_str.as_string(conn))
        cur = conn.cursor()
        cur.execute(sql_str)
        df = pd.DataFrame(cur.fetchall(), columns=["DistrictName","TractGeoId"])
        df.insert(0, "DistrictType",d_type)
        df.insert(0, "StateFIPS", state_fips)
        df.insert(0, "StateAbbreviation", state_abbreviation)
        return df
    except Exception as e:
        print("Error in tracts_by_district: {}".format(e))
    else:
        print("Done with tracts_by_district for " + d_shp_file)
#        return res
    finally:
        print("Dropping temp shapes table (" + tmp_table_name +")")
        cur = conn.cursor()
        cur.execute("drop table " + tmp_table_name)
        conn.commit()

def at_large_tracts(state_fips, state_abbreviation, tract_shapes=tract_shape_table, tract_id_col = "geoid", tract_geom_col="geom"):
    sql_str = sql.SQL('''
SELECT {t_id}
FROM {tract_table} as t
WHERE starts_with({t_id},{st_fips})
    ''').format(t_id = sql.Identifier("t", tract_id_col)
                , tract_table = sql.Identifier(tract_shapes)
                , st_fips = sql.Literal(state_fips)
                )
#    print(sql_str.as_string(conn))
    cur = conn.cursor()
    cur.execute(sql_str)
    df = pd.DataFrame(cur.fetchall(), columns=["TractGeoId"])
    df.insert(0, "DistrictName", "1")
    df.insert(0, "DistrictType","Congressional")
    df.insert(0, "StateFIPS", state_fips)
    df.insert(0, "StateAbbreviation", state_abbreviation)
    return df

#print(at_large_tracts("01","AL"))
#print(tracts_by_district("AL", "Congressional", cd_shape_dir + "/AL.geojson", "id_0", "geometry"))

def string_fips(fips):
    if fips < 10:
        return "0" + str(fips)
    else:
        return str(fips)

si = geoFunctions.loadStatesInfo()

cdStatesAndFIPS = si.fipsFromAbbr.copy()
single_CD = {k: cdStatesAndFIPS[k] for k in si.oneDistrict} #cdStatesAndFIPS(si.oneDistrict) #[cdStatesAndFIPS.pop(key) for key in si.oneDistrict.copy()]
multi_CD = cdStatesAndFIPS.copy()
[multi_CD.pop(key) for key in si.oneDistrict]
print("Working on at large districts...")
single_CD_TBD = pd.concat(list(map(lambda x: at_large_tracts(string_fips(x[1]),x[0]), single_CD.items())), ignore_index=True)
print("Working on congressional districts...")
multi_CD_TBD = pd.concat(list(map(lambda x: tracts_by_district(string_fips(x[1]), x[0], "Congressional", cd_shape_dir + "/" + x[0] + ".geojson", "id_0", "geometry")
                                  , multi_CD.items()))
                         , ignore_index=True)
#print(multi_CD_TBD)
#print(single_CD)
#print(multi_CD)

sldStatesAndFIPS = si.fipsFromAbbr.copy()
hasUpper = cdStatesAndFIPS.copy()
hasUpper.pop("DC")
hasLower = cdStatesAndFIPS.copy()
hasLower.pop("DC")
{k: hasLower.pop(k) for k in si.sldUpperOnly}
print("Working on state upper houses...")
upperSLD_TBD = pd.concat(list(map(lambda x: tracts_by_district(string_fips(x[1]), x[0], "StateUpper", sld_shape_dir + "/" + x[0] + "_sldu.geojson", "id_0", "geometry")
                                  ,hasUpper.items()))
                         , ignore_index = True)

print("Working on state lower houses...")
lowerSLD_TBD = pd.concat(list(map(lambda x: tracts_by_district(string_fips(x[1]), x[0], "StateLower", sld_shape_dir + "/" + x[0] + "_sldl.geojson", "id_0", "geometry")
                                  ,hasLower.items()))
                         , ignore_index = True)

print("Writing all to CSV")
all_df = pd.concat([single_CD_TBD, multi_CD_TBD, upperSLD_TBD, lowerSLD_TBD], ignore_index=True)
all_df.to_csv("/Users/adam/BlueRipple/bigData/tractsByDistrict.csv", index = False)
print("done!")


#list(map(lambda t:make_sld_csvs(t[1], t[0], si.sldUpperOnly), sldStatesAndFIPS.items()))
