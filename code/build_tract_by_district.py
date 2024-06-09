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

def tracts_by_district(state_abbreviation, d_type, d_shp_file, d_name_col="id_0", d_geom_col="geom", tract_shapes=tract_shape_table, tract_id_col = "geoid", tract_geom_col="geom"):
    tmp_table_name = "shapes_tmp"
    areal.load_shapes_from_file(conn, d_shp_file, tmp_table_name)
    conn.commit()
    sql_str = sql.SQL('''
SELECT {d_name}, {t_id}
FROM {tract_table} as t
inner JOIN {d_table} as d ON {t_geom} && {d_geom}
    ''').format(d_name = sql.Identifier("d", d_name_col)
                , t_id = sql.Identifier("t", tract_id_col)
                , tract_table = sql.Identifier(tract_shapes)
                , d_table = sql.Identifier(tmp_table_name)
                , t_geom = sql.Identifier("t", tract_geom_col)
                , d_geom = sql.Identifier("d", d_geom_col)
                )
    try:
        print(sql_str.as_string(conn))
        cur = conn.cursor()
        cur.execute(sql_str)
        df = pd.DataFrame(cur.fetchall(), columns=["dName","TractGeoId"])
        df.insert(0, "DistrictType",d_type)
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

print(tracts_by_district("AL", "Congressional", cd_shape_dir + "/AL.geojson", "id_0", "geometry"))
