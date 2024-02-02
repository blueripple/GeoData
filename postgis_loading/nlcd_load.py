import psycopg2
from psycopg2 import sql
import re
import itertools
import os
import subprocess

os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGUSER'] = 'postgres'
os.environ['PGPASSWORD'] = 'postgres'
os.environ['PGDATABASE'] = 'raster_test'

raster_dir = "/Users/adam/BlueRipple/GeoData/input_data/NLCD"

#acs_dir = "/Users/adam/BlueRipple/GeoData/input_files/NHGIS"
#shape_dir = acs_dir + "/US_2022_tract_shapefile"
#data_dir = acs_dir + "/US_2022_tract_csv"
#tract_shapes = shape_dir + "/US_tract_2022.shp"
#tract_data = [data_dir + "nhgis0042_d262_20225_tract_E_.csv",
#              data_dir + "nhgis0042_d263_20225_tract_E_.csv"]

dbname = "tract_test"
schema_name = "public"

conus_nlcd = {
    "file_names": [raster_dir + "/NLCD 2021 Land Cover CONUS/nlcd_2021_land_cover_l48_20230630.img"],
    "srid": "4326",
    "block": "auto",
    "table_name": "nlcd_conus"
}

conus_nlcd_rp = {
    "file_names": [raster_dir + "/nlcd_conus_bilinear.tif"],
    "srid": "4326",
    "block": "auto",
    "table_name": "nlcd_conus2"
}

AK_nlcd = {
    "file_names": [raster_dir + "/NLCD 2016 Land Cover AK/NLCD_2016_Land_Cover_AK_20200724.img"],
    "srid": "4326",
    "block": "auto",
    "table_name": "nlcd_ak"
}

HI_nlcd = {
    "file_names": [raster_dir + "/NLCD 2010 Land Cover HI/hi_hawaii_2010_ccap_hr_land_cover20150120.img"],
    "srid": "4326",
    "block": "auto",
    "table_name": "nlcd_hi"
}

US_nlcd = {
    "file_names": [raster_dir + "/nlcd_conus_4326.tif",
                   raster_dir + "/nlcd_AK_4326.tif",
                   raster_dir + "/nlcd_HI_4326.tif"
                   ],
    "srid": "4326",
    "block": "auto",
    "table_name": "nlcd_us"
}

def load_raster(m):
    file_names = map(lambda x: "\"" + x + "\"", m["file_names"])
    cmd_p = "raster2pgsql -s {srid} -C -I -F -M -t {block}  " + ' '.join(file_names) + " " + schema_name + ".{table_name} | psql -q -d " + dbname
    cmd = cmd_p.format_map(m)
    print(cmd)
    subprocess.call(cmd, shell=True)


geom_table_info = {
    "geom_table_name": "tract_join_test",
    "geom_id_col": "geoid",
    "geom_col": "geom"
}

raster_table_info = {
    "raster_table_name": "nlcd_us",
    "rast_col": "rast"
}

def add_rasters_to_geometries(raster_info, geometry_info):
    inner_sql = sql.SQL('''\
SELECT g.{id_col} as "geom_id", ST_CLIP(r.{rast_col}, g.{geom_col},true) as "rast"
FROM {geom_table} g
INNER JOIN {rast_table} r
ON ST_INTERSECTS(r.{rast_col}, g.{geom_col})
WHERE g.{id_col} =
''').format(geom_table = sql.Identifier(geometry_info["geom_table_name"]),
            id_col = sql.Identifier(geometry_info["geom_id_col"]),
            rast_col = sql.Identifier(raster_info["rast_col"]),
            geom_col = sql.Identifier(geometry_info["geom_col"]),
            rast_table = sql.Identifier(raster_info["raster_table_name"])
            )
    sql_str = sql.SQL('''\
SELECT g.*, tr."rast"
FROM {geom_table} g
INNER JOIN (
    {inner_table}
    ) tr
ON g.{id_col} = tr."geom_id"
''').format(geom_table = sql.Identifier(geometry_info["geom_table_name"]),
            id_col = sql.Identifier(geometry_info["geom_id_col"]),
            inner_table = inner_sql
           )
    conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
    cur = conn.cursor()
    print(inner_sql.as_string(conn))
    cur.execute(inner_sql)
#load_raster(conus_nlcd_rp)
#load_raster(AK_nlcd)
#load_raster(US_nlcd)
add_rasters_to_geometries(raster_table_info, geom_table_info)

#conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
#cur = conn.cursor()
