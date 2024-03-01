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

dbname = "tracts_and_nlcd"
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

def developed_land_cover(raster_table, db_connection):
    parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), raster_table.items()))
    parms["new_table"] = sql.Identifier(raster_table["raster_table_name"] + "_dev")
    sql_str = sql.SQL('''
DROP TABLE IF EXISTS {new_table};
CREATE TABLE {new_table} as
select rid,
       ST_RECLASS(rast,1,'0-19:0, 20-29:1, 30-255:0','1BB',0) as "rast",
       filename
from {raster_table_name};
CREATE INDEX nlcd_us_dev_convexhull_idx ON {new_table} USING GIST(ST_ConvexHull("rast"));
SELECT AddRasterConstraints({new_table} :: name, "rast" :: name);
''').format(**parms)
    print("Reclassifying land cover in {raster_table_name} to developed only in {new_table}".format(**parms))
    print(sql_str.as_string(db_connection))
    cur = db_connection.cursor()
    cur.execute(sql_str)
    db_connection.commit()
    print("done!")

#load_raster(conus_nlcd_rp)
#load_raster(AK_nlcd)
#load_raster(US_nlcd)
conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
#developed_land_cover(raster_table_info, conn)
#conn.close()

def add_rasters_sql(raster_info, geometry_info):
    raster_parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), raster_info.items()))
    new_table_name = geometry_info["geom_table"] + "_rast"
    sql_str = sql.SQL(
'''
DROP TABLE IF EXISTS {new_table};
CREATE TABLE {new_table} as
select gt.*, rt."rast" as "rast"
from {geometry_table} gt
inner join (
    select g.{geometry_id_col} as "id", ST_CLIP(ST_UNION(ST_Transform(r.{raster_col}, (SELECT {raster_table}.{raster_col} from {raster_table} fetch first 1 row only))), g.{geometry_col}) as "rast"
    from {geometry_table} g
    inner join {raster_table} r
    on r.{raster_col} && g.{geometry_col}
       and g.{geometry_pop_col} > 0
       and g.{geometry_id_col} not in ('02016000100', '02185000200', '15003981200')
       and coalesce(ST_Valuecount(r.{raster_col},1,true,1),0) > 0
    group by g.{geometry_id_col}, g.{geometry_col}
) rt
on gt.{geometry_id_col} = rt."id";
CREATE INDEX {new_table_pkey} ON {new_table}({geometry_id_col});
CREATE INDEX {new_table_geom_idx} ON {new_table} USING GIST ({geometry_col});
CREATE INDEX {new_table_convexhull_idx} ON {new_table} USING GIST(ST_ConvexHull("rast"));
SELECT AddRasterConstraints({new_table_l} :: name, 'rast' :: name);
''').format(geometry_table = sql.Identifier(geometry_info["geom_table"]),
            geometry_id_col = sql.Identifier(geometry_info["geom_id_col"]),
            geometry_col = sql.Identifier(geometry_info["geom_col"]),
            geometry_pop_col = sql.Identifier(geometry_info["geom_pop_col"]),
            raster_table = sql.Identifier(raster_info["raster_table_name"]),
            raster_col = sql.Identifier(raster_info["rast_col"]),
            new_table = sql.Identifier(new_table_name),
            new_table_l = sql.Literal(new_table_name),
            new_table_pkey = sql.Identifier(new_table_name + "_pkey"),
            new_table_geom_idx = sql.Identifier(new_table_name + "_geom_idx"),
            new_table_convexhull_idx = sql.Identifier(new_table_name + "_convexhull_idx")
            )
    return sql_str

dev_raster_table = {
    "raster_table_name": "nlcd_us_dev",
    "rast_col": "rast"
}

co_params = {
    "geom_table": "co_cd",
    "geom_id_col": "id_0",
    "geom_col": "geometry"
}

acs2022_params = {
    "geom_table": "tracts2022_acs2017_2022",
    "geom_id_col": "geoid",
    "geom_col": "geom",
    "geom_pop_col": "AQNFE001"
}

ar_sql = add_rasters_sql(dev_raster_table, acs2022_params)

print(ar_sql.as_string(conn))
cur = conn.cursor()
cur.execute(ar_sql)
conn.commit()
conn.close()
#print(cur.fetchall())
exit(0)

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
#add_rasters_to_geometries(raster_table_info, geom_table_info)


#cur = conn.cursor()
