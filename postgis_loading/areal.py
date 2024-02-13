import psycopg2
from psycopg2 import sql
import re
import itertools
import pandas as pd
import subprocess

dbname = "tract_test"
db_user = "postgres"
db_password = "postgres"
schema_name = "public"
tract_table = "tract_join_test"
tract_geom_col = "geom"
tract_id_col = "geoid"
aggTo_table = "ny_sldu"
aggTo_geom_col = "geom"
aggTo_id_col = "id_0"
rast_table = "nlcd_us"

data_col_pat = re.compile('[A-Z]+E\d\d\d')

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
#cur = conn.cursor()
#cur.execute("CREATE EXTENSION postgis")

# compute areas of overlap for all census tracts in a SLD

def tupleAt(n,t):
    return t[n]

tract_pop_col = "AMPVE001"

outer_geom_params = {
    "outer_geom_table": "ny_sldu",
    "outer_id_col": "id_0",
    "outer_geom_col": "geom",
    "outer_name_col": "name",
}

tract_and_lcd_params = {
    "data_geom_table": "tract_shp_and_data",
    "data_geom_id_col": "geoid",
    "data_geom_col": "geom",
    "data_geom_schema": "public",
    "pop_col": "AMPVE001",
    "intensive_cols": ["AMTCE001"],
    "extensive_col_pat": re.compile('AN[A-Z]+E\d\d\d'),
    "lc_rast_table": "nlcd_us_dev",
    "lc_rast_col": "rast"
}


#data_col_params = {
#    "data_geom_schema": "public",
#    "pop_col": "AMPVE001",
#    "intensive_cols": ["AMTCE001"],
#    "extensive_col_pat": re.compile('AN[A-Z]+E\d\d\d')
#}

def inTuple(n,t):
    x = t[n]
    return x

def extensive_cols(tract_and_lcd_parameters, db_cursor):
    print("Getting extensive cols for data_geom_table={}".format(tract_and_lcd_parameters["data_geom_table"]))
    db_cursor.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {data_geom_schema} AND table_name = {data_geom_table}")
                      .format(data_geom_schema = sql.Literal(tract_and_lcd_parameters["data_geom_schema"]),
                              data_geom_table = sql.Literal(tract_and_lcd_parameters["data_geom_table"])
                              )
                      )
    cols = list(map(lambda x: inTuple(0,x),db_cursor.fetchall()))
    data_cols = [c for c in cols if tract_and_lcd_parameters["extensive_col_pat"].match(c)]
    return data_cols

def dasymmetric_interpolation_sql(og_parameters, tract_and_lcd_parameters, db_cursor):
    parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og_parameters.items()))
    parms["data_geom_table"] = sql.Identifier(tract_and_lcd_parameters["data_geom_table"])
    parms["data_geom_id_col"] = sql.Identifier(tract_and_lcd_parameters["data_geom_id_col"])
    parms["data_geom_col"] = sql.Identifier(tract_and_lcd_parameters["data_geom_col"])
    parms["lc_rast_table"] = sql.Identifier(tract_and_lcd_parameters["lc_rast_table"])
    parms["lc_rast_col"] = sql.Identifier(tract_and_lcd_parameters["lc_rast_col"])
    ext_cols = extensive_cols(tract_and_lcd_parameters, db_cursor)
    wgt_sql = sql.Identifier("rast_wgt")
    dev_area_sql = sql.Identifier("dev_area_m2")
    rast_area_sql = sql.Identifier("rast_area_m2")
    area_sql = sql.Identifier("overlap_area_m2")
    pop_sql = sql.Identifier(tract_and_lcd_parameters["pop_col"])
    parms["extensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('round(sum({} * {}))').format(wgt_sql,sql.Identifier(x)), ext_cols))
    parms["pop_col_sum"] = sql.SQL('sum({} * {})').format(wgt_sql, pop_sql)
    parms["density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({area}) * 2.589988e6 as "ppl_per_mi2"').format(wgt = wgt_sql, pop = pop_sql, area = rast_area_sql)
    parms["dev_density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({dev_area}) * 2.589988e6 as "ppl_per_dev_mi2"').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)
    parms["PW_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 2.589988e6 / {rast_overlap_area})
                 else 0
                 end
                )
               / sum({wgt} * {pop})) as "pw_ppl_per_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, rast_overlap_area = rast_area_sql)
    parms["PW_dev_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 2.589988e6 / {dev_area})
                 else 0
                 end
                )
                / sum({wgt} * {pop})) as "pw_ppl_per_dev_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)

    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), tract_and_lcd_parameters["intensive_cols"]))
    sql_str = sql.SQL('''
select "outer_id",
       "outer_name",
       {pop_col_sum},
       {density_sum},
       {dev_density_sum},
       {PW_ldensity_sum},
       {PW_dev_ldensity_sum},
       sum("dev_area_m2") * 3.861022e-7 as "developed_area_mi2",
       sum("rast_area_m2") * 3.681022e-7 as "area_mi2",
       {intensive_col_sums},
       {extensive_col_sums}
from {data_geom_table} "dg"
inner join (
      select "outer_id", "outer_name", "data_geom_id",
           case
		when sum("dev_in_data_geom") > 0
		then sum("dev_in_both")::float / sum("dev_in_data_geom")
		else 0
	   end as "rast_wgt",
           sum(ST_area(ST_Polygon("rast_in_both") :: geography)) as  "dev_area_m2",
           sum(ST_area(ST_Envelope("rast_in_both") :: geography)) as  "rast_area_m2",
           ST_area("data_in_outer_geom")/ST_area("data_geom") as "area_wgt",
           ST_area("data_in_outer_geom" :: geography) as "overlap_area_m2"
      from (
        select "outer".{outer_id_col} as "outer_id",
               "outer".{outer_name_col} as "outer_name",
               "outer".{outer_geom_col} as "outer_geom",
               "data_geom".{data_geom_id_col} as "data_geom_id",
               "data_geom".{data_geom_col} as "data_geom",
               case
                 when ST_WITHIN("data_geom".{data_geom_col}, "outer".{outer_geom_col})
                 then "data_geom".{data_geom_col}
                 else ST_INTERSECTION("data_geom".{data_geom_col}, "outer".{outer_geom_col})
               end as "data_in_outer_geom",
	       ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true) as "rast_in_both",
	       coalesce(ST_valuecount(ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true),1,true,1), 0)   as "dev_in_both",
  	       ST_valuecount(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true),1,true,1) as "dev_in_data_geom"
        from {data_geom_table} "data_geom"
        inner join {outer_geom_table} "outer"
        on ST_intersects("data_geom".{data_geom_col},"outer".{outer_geom_col})
        inner join {lc_rast_table}
        on ST_intersects("data_geom".{data_geom_col}, {lc_rast_table}."rast")
      )
      group by "outer_id", "outer_name", "outer_geom", "data_geom_id", "data_geom", "data_in_outer_geom"
    ) "dg_and_wgt"
on "dg".{data_geom_id_col} = "dg_and_wgt"."data_geom_id"
group by "outer_id", "outer_name"
''').format(**parms)
    return sql_str

def dasymmetric_interpolation(og_parameters, tract_and_lcd_parameters, db_connection):
    cur = db_connection.cursor()
    sql = dasymmetric_interpolation_sql(og_parameters, tract_and_lcd_parameters, cur)
    ext_cols = extensive_cols(tract_and_lcd_parameters, cur)
    int_cols = tract_and_lcd_parameters["intensive_cols"]
    cols = ["ID","DistrictName","TotalPopulation","PopPerSqMile","PopPerDevSqMile","pwPopPerSqMile","pwPopPerDevSqMile","DevArea_m2","OverlapArea_m2"] + int_cols + ext_cols
    print(sql.as_string(db_connection))
    cur.execute(sql)
    df = pd.DataFrame(cur.fetchall(),columns = cols)
    col_type_dict = dict(map(lambda x: (x, int), ["TotalPopulation"] + ext_cols))
    return df.astype(col_type_dict)

#dasymmetric_result = dasymmetric_interpolation(outer_geom_params, tract_and_lcd_params, conn)
#print(dasymmetric_result.to_csv(header=True, index=False, float_format="%.2f"))

#exit(0)

def transform_srid(table_name, geom_col, transformed_table_name, source_srid, target_srid, db_connection):
    cur = db_connection.cursor()
    cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table}").format(schema = schema_name,
                                                                                                                                            table = table_name))
    cols = list(map(lambda x: inTuple(0,x),db_cursor.fetchall()))
    if not(geom_col in cols):
        print("transform_srid: {} column is missing from table to be transformed. Exiting.".format(geom_col))
        exit(1)
    cols_no_geom = cols.remove(geom_col)
    if (source_srid == target_srid):
        print("transform_srid: srid's match. Doing nothing.")
        return table_name

    sql_str = sql.SQL('''
DROP TABLE IF EXISTS {nt};
CREATE TABLE {nt} as
SELECT {ngc}, ST_Transform({gc},{ts}) as {gc}
FROM {ot};
CREATE INDEX {nti} ON {nt} USING GIST({gc});
''').format(nt = sql.Identifier(transformed_table_name),
            ngc = sql.SQL(', ').join(map(sql.Identifier, cols_no_geom)),
            gc = sql.Identifier(geom_col),
            ts = sql.Literal(target_srid),
            nti = sql.Identifier(transformed_table_name + "_" + geom_col + "_idx")
            )
    print("transform_srid: transforming from {} to {}...".format(source_srid.as_string(),target_srid.as_string()))
    cur.execute(sql_str)
    db_connection.commit()
    print("transform_srid: done!")
    return transformed_table_name

def dasymmetric_from_file(db_connection, filename, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326"):
    print("dasymmetric_from_file: loading shapes from {} to database...".format(filename))
#    cmd_uf = "shp2pgsql -D -I -s {srid_} {filename_} {table} | psql dbname={dbname_} user={db_user_} password={db_password_}"
    if (wkt != "EPSG:4326"):
        srid_flags = "-s_SRS {} -t_srs EPSG:4326"
    else:
        srid_flags = ""
    cmd_uf = "ogr2ogr -f \"PostgreSQL\" PG:\"dbname={dbname_} user={db_user_} password={db_password_}\ host='localhost' port='5432'\" {filename_} -lco GEOMETRY_NAME=geometry -lco FID={id_col_} -progress {srid_flags_} -nln public.{table}"
    cmd = cmd_uf.format(filename_ = filename,
                        table = "shapes_tmp",
                        id_col_ = id_col,
                        dbname_ = dbname,
                        db_user_ = db_user,
                        db_password_ = db_password,
                        srid_flags_ = srid_flags)
    print("dasymmetric_from_file: running command \"{}\"".format(cmd))
    subprocess.call(cmd, shell=True)
    print("dasymmetric_from_file: done loading shapes from file into postgres table shapes_tmp.")
#    print("dasymmetric_from_file: Transforming to srid=436 if necessary.")
#    if (srid != 4326):
#        table_name = transform_srid("shapes_tmp", geom_col, "tmp_shapes_4326", srid, 4326, db_connection)
    print("Joining on shapes and rasters to interpolate census tract data.")
    ogps = {
        "outer_geom_table": "shapes_tmp",
        "outer_id_col": id_col,
        "outer_geom_col": geom_col,
        "outer_name_col": name_col
    }
    res = dasymmetric_interpolation(ogps, tract_and_lcd_params, db_connection)
    print("Done with dasymmetric interpolation. Dropping temp table...")
    cur = db_connection.cursor()
    cur.execute("drop table shapes_tmp")
    db_connection.commit()
    return res


test_from_file = dasymmetric_from_file(conn, "input_data/StateLegDistricts/2024/NY_sldu.geojson")
print(test_from_file.to_csv(header=True, index=False, float_format="%.2f"))

exit(0)

def data_and_wgt_tbl_sql2(table_parameters, data_col_parameters, db_cursor):
    parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), table_parameters.items()))
    ext_cols = extensive_cols(table_parameters, data_col_parameters, db_cursor)
    wgt_sql = sql.Identifier("rast_wgt")
    pop_sql = sql.Identifier(data_col_parameters["pop_col"])
    parms["extensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({} * {})').format(wgt_sql,sql.Identifier(x)), ext_cols))
    parms["pop_col_sum"] = sql.SQL('sum({} * {})').format(wgt_sql, pop_sql)
    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), data_col_parameters["intensive_cols"]))
    sql_str = sql.SQL('''
select distinct "a".{outer_id_col}
from (
  select ST_clip("rast_in_data_geom", "og".{outer_geom_col}, true) as "rast_in_both", "og".{outer_id_col}
  from (
    select ST_clip( "lc".{lc_rast_col}, "dg".{data_geom_col}, true) as "rast_in_data_geom"
    from {data_geom_table} "dg"
    inner join {lc_rast_table} "lc"
    on ST_intersects("dg".{data_geom_col}, "lc".{lc_rast_col})
  )
  inner join {outer_geom_table} "og"
  on ST_intersects("og".{outer_geom_col}, "rast_in_data_geom")
) "a"
group by "a".{outer_id_col}
''').format(**parms)
    return sql_str

qsql = data_and_wgt_tbl_sql2(table_params, data_col_params, cur)
print(qsql.as_string(conn))
cur.execute(qsql)
print(cur.fetchall())

exit(0)

a_wgt_sql_str = sql.SQL('''
SELECT t.{t_id} as "tract_id",
       t.{t_pop} as "tract_pop",
       at.{at_id} as "aggTo_id",
       ST_AREA((ST_Intersection(at.{at_geom}, t.{t_geom}))) / ST_AREA(t.{t_geom}) AS "tract_a_wgt"
FROM {aggTo} at
LEFT JOIN {tracts} t
ON ST_Intersects(at.{at_geom}, t.{t_geom})
''').format(tracts = sql.Identifier(tract_table),
            aggTo = sql.Identifier(aggTo_table),
            t_id = sql.Identifier(tract_id_col),
            at_id = sql.Identifier(aggTo_id_col),
            t_geom = sql.Identifier(tract_geom_col),
            t_pop = sql.Identifier(tract_pop_col),
            at_geom = sql.Identifier(aggTo_geom_col),
            st_area = sql.SQL("ST_AREA")
            )



reclass_rast_sql_str = sql.SQL('''
SELECT rr.{rid_col} as rid,
       ST_RECLASS(rr.{rast_col},1,'20-29:1','4BUI',0) as rast
FROM {rast_tbl} rr
''').format(rid_col = sql.Identifier("rid"),
            rast_col = sql.Identifier("rast"),
            rast_tbl = sql.Identifier(rast_table)
            )

nlcd_wgt_sql_str = sql.SQL('''
SELECT t.{t_id} as "tract_id",
       t.{t_pop} as "tract_pop",
       at.{at_id} as "aggTo_id",
       ST_VALUECOUNT(ST_RECLASS(ST_UNION(r.{rast_col}),{reclassarg},'1BB'),1,true,1),
       ST_AREA((ST_Intersection(at.{at_geom}, t.{t_geom}))) / ST_AREA(t.{t_geom}) AS "tract_wgt",
       r.rid
FROM {aggTo} at
LEFT JOIN {tracts} t
ON ST_Intersects(at.{at_geom}, t.{t_geom})
LEFT JOIN {rast_tbl} r
ON ST_INTERSECTS(r.{rast_col}, t.{t_geom})
WHERE at.{at_id} = {example_id}
GROUP BY t.{t_id}, t.{t_pop}
''').format(tracts = sql.Identifier(tract_table),
            aggTo = sql.Identifier(aggTo_table),
            t_id = sql.Identifier(tract_id_col),
            at_id = sql.Identifier(aggTo_id_col),
            reclassarg = sql.Literal("[0-19]:0, [20-29]:1, (29-255:0"),
            t_geom = sql.Identifier(tract_geom_col),
            t_pop = sql.Identifier(tract_pop_col),
            at_geom = sql.Identifier(aggTo_geom_col),
            st_area = sql.SQL("ST_AREA"),
            rast_tbl = sql.Identifier(rast_table),
            rast_col = sql.Identifier("rast"),
            example_id = sql.Literal("5")
            )
print(nlcd_wgt_sql_str.as_string(conn))
cur.execute(nlcd_wgt_sql_str)
print(cur.fetchall())



exit(0)


aggTo_name_col = "name"
aggTo_pop_col = "totalpop"

sql_str2 = sql.SQL('''
SELECT "aggTo_id",
       SUM("tract_a_wgt" * "tract_pop") AS "t_sum_pop"
FROM ({tract_wgt_table_sql}) j
GROUP BY "aggTo_id"
''').format(tract_wgt_table_sql = a_wgt_sql_str,
            aggTo = sql.Identifier(aggTo_table),
            at_id = sql.Identifier(aggTo_id_col),
            )
#print(sql_str2.as_string(conn))
#cur.execute(sql_str2)
#print(cur.fetchall())

sql_str3 = sql.SQL('''
SELECT a."aggTo_id", at.{at_pop} AS "aggTo_pop", a."t_sum_pop" as "t_sum_pop"
FROM ({aggregated_table_sql}) a
LEFT JOIN {aggTo} at ON a."aggTo_id" = at.{at_id}
''').format(aggregated_table_sql = sql_str2,
            aggTo = sql.Identifier(aggTo_table),
            at_id = sql.Identifier(aggTo_id_col),
            at_pop = sql.Identifier(aggTo_pop_col)
            )
print(sql_str3.as_string(conn))
cur.execute(sql_str3)
print(cur.fetchall())

conn.commit()
conn.close()
