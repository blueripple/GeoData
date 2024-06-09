import psycopg2
from psycopg2 import sql
import re
import itertools
import pandas as pd
import geoFunctions
import topojson as tp
import json
import subprocess

dbname = "tracts_and_nlcd"
schema_name = "public"

shape_table = "tracts2022_shapes"
state_fips_col = "statefp"

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
cur = conn.cursor()

def inTuple(n,t):
    x = t[n]
    return x

def make_feature_collection(sql_str_coll):
    sql_str_pre = sql.SQL('''
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(feature.jsonb_build_object)
    )
    FROM (
    ''')
    sql_str_post = sql.SQL('''
) AS feature''')
    return sql_str_pre + sql_str_coll + sql_str_post

def geoms_to_geojson(shape_table, geom_col_name="geom", id_col_name="id", feature_collection=True, **kwargs):
    sql_str1  = sql.SQL('''
SELECT jsonb_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(ST_Transform(ST_SimplifyPreserveTopology(ST_Transform(t.geom, 54032), {simpl}), 4326), {md})::json,
    'properties', to_jsonb(t.*) - 'geom'
    )
FROM {shp_table} t ''').format(shp_table = sql.Identifier(shape_table)
                              , simpl = sql.Literal(kwargs.get("simplify",10))
                              , md = sql.Literal(kwargs.get("maxdec",6))
                              )
    where_col = kwargs.get("where_col")
    where_val = kwargs.get("where_val")
    limit_sql = sql.SQL("") #sql.SQL("LIMIT 20")
    if where_col and where_val:
        sql_str2 = sql.SQL('''WHERE {wc} = {wv} ''').format(wc=sql.Identifier(where_col), wv=sql.Literal(where_val))
    else:
        sql_str2 = sql.SQL('')
    if feature_collection:
        sql_str = make_feature_collection(sql_str1 + sql_str2 + limit_sql)
    else:
        sql_str = sql_str1 + sql_str2 + limit_sql
#    print(sql_str.as_string(conn))
    cur.execute(sql_str)
    if feature_collection:
        return inTuple(0,cur.fetchone())
    else:
        return list(map(lambda x: inTuple(0,x), cur.fetchall()))


def geoms_to_topojson(shape_table, geom_col_name="geom", id_col_name="id", **kwargs):
    print("Building GeoJson...")
    gj = geoms_to_geojson(shape_table, geom_col_name, id_col_name, True, **kwargs) #[:1]
#    print(gj)
    print("Converting to topojson...")
    topo = tp.Topology(gj, prequantize=200)
    return topo

def convert_and_write(geojson, out_dir, fname, tmp_name="tracts"):
    fpath = out_dir + "/" + fname
    print("writing geojson feature collection to \"" + fpath + "\"...")
    with open(tmp_name, "w") as f:
        json.dump(geojson, f) #f.write(geojson.to_json())
    subprocess.run(["geo2topo","-o", fpath, tmp_name])
    subprocess.run(["rm",tmp_name])

def geojson_list_to_featurecollection(gjl):
    geojson = {'type': "FeatureCollection"}
    geojson["features"] = gjl
    return geojson

def write_extracted_state(state_fips, state_abbr, out_dir, tmp_name="tracts"):
    print("Extracting geojson for " + state_abbr + "...")
    fname = state_abbr + "_2022_tracts_topo.json"
    geojson = {'type': "FeatureCollection"}
    geojson = geojson_list_to_featurecollection(geoms_to_geojson(shape_table,"goem","id", where_col="statefp", feature_collection=False, where_val=str(state_fips), simplify=10, maxdec=6))
    convert_and_write(geojson, out_dir, fname, tmp_name)


all_geojson = geojson_list_to_featurecollection(geoms_to_geojson(shape_table,"goem","id", simplify=10, feature_collection=False, maxdec=6))
convert_and_write(all_geojson, "/Users/adam/BlueRipple/bigData/GeoJSON", "US_tracts_2022_topo.json")
#qexit(0)

si = geoFunctions.loadStatesInfo()
statesAndFIPS = si.fipsFromAbbr.copy()
list(map(lambda t:write_extracted_state(t[1], t[0],"/Users/adam/BlueRipple/bigData/GeoJSON/states"), statesAndFIPS.items()))
#print(extract_state(12))
