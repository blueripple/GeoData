import psycopg2
from psycopg2 import sql
import re
import itertools
import pandas as pd
import geoFunctions

dbname = "tracts_and_nlcd"
schema_name = "public"

shape_table = "tracts2022_shapes"
state_fips_col = "statefp"

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
cur = conn.cursor()

def inTuple(n,t):
    x = t[n]
    return x

def extract_state(state_fips, simplify=10, maxdec=6):
    sql_str  = sql.SQL('''
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(feature)
    )
    FROM (
    SELECT jsonb_build_object(
      'type', 'Feature',
      'id', geoid,
      'geometry', ST_AsGeoJSON(ST_Transform(ST_Simplify(ST_Transform(t.geom, 54032), {simpl}), 4326), {md})::jsonb,
      'properties', to_jsonb(t.*) - 'geoid' - 'geom'
     )
    FROM {shp_table} t
    WHERE {state_fips_col_name}::integer = {given_state_fips}
    ) AS feature
    ''').format(shp_table = sql.Identifier(shape_table)
                , simpl = sql.Literal(simplify)
                , md = sql.Literal(maxdec)
                , state_fips_col_name = sql.Identifier("t",state_fips_col)
                , given_state_fips = sql.Literal(state_fips))
#    print(sql_str.as_string(conn))
    cur.execute(sql_str)
    return inTuple(0,cur.fetchone())

def write_extracted_state(state_fips, state_abbr, out_dir):
    print("Extracting geojson for " + state_abbr + "...")
    fname = out_dir + "/" + state_abbr + "_2022_tracts.geojson"
    geojson = extract_state(state_fips, 10, 6)
    print("writing geojson feature collection to \"" + fname + "\"...")
    with open(fname, "w") as f:
        f.write(str(geojson))

si = geoFunctions.loadStatesInfo()
statesAndFIPS = si.fipsFromAbbr.copy()
list(map(lambda t:write_extracted_state(t[1], t[0],"/Users/adam/BlueRipple/bigData/GeoJSON/states"), statesAndFIPS.items()))
#print(extract_state(12))
#write_extracted_state(12, "NY","/Users/adam/BlueRipple/bigData/GeoJSON/states")
exit(0)
