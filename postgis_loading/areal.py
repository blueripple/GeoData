import psycopg2
from psycopg2 import sql
import re
import itertools
import pandas

dbname = "tract_test"
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
cur = conn.cursor()
#cur.execute("CREATE EXTENSION postgis")

# compute areas of overlap for all census tracts in a SLD

def tupleAt(n,t):
    return t[n]

tract_pop_col = "AMPVE001"

table_params = {
    "outer_geom_table": "ny_sldu",
    "outer_id_col": "id_0",
    "outer_geom_col": "geom",
    "data_geom_table": "tract_join_test",
    "data_geom_id_col": "geoid",
    "data_geom_col": "geom",
    "lc_rast_table": "nlcd_us",
    "lc_rast_col": "rast"
}

data_col_params = {
    "data_geom_schema": "public",
    "pop_col": "AMPVE001",
    "intensive_cols": ["AMTCE001"],
    "extensive_col_pat": re.compile('AN[A-Z]+E\d\d\d')
}

def inTuple(n,t):
    x = t[n]
    return x

def extensive_cols(table_parameters, col_parameters, db_cursor):
    print("Getting extensive cols for data_geom_table={}".format(table_parameters["data_geom_table"]))
    db_cursor.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {data_geom_schema} AND table_name = {data_geom_table}")
                      .format(data_geom_schema = sql.Literal(col_parameters["data_geom_schema"]),
                              data_geom_table = sql.Literal(table_parameters["data_geom_table"])
                              )
                      )
    cols = list(map(lambda x: inTuple(0,x),cur.fetchall()))
    data_cols = [c for c in cols if col_parameters["extensive_col_pat"].match(c)]
    return data_cols

def data_and_wgt_tbl_sql(table_parameters, data_col_parameters, db_cursor):
    parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), table_parameters.items()))
    ext_cols = extensive_cols(table_parameters, data_col_parameters, db_cursor)
    wgt_sql = sql.Identifier("rast_wgt")
    pop_sql = sql.Identifier(data_col_parameters["pop_col"])
    parms["extensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({} * {})').format(wgt_sql,sql.Identifier(x)), ext_cols))
    parms["pop_col_sum"] = sql.SQL('sum({} * {})').format(wgt_sql, pop_sql)
    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), data_col_parameters["intensive_cols"]))
    sql_str = sql.SQL('''
select "outer_id", sum("dev_area_m2"), sum("rast_area_m2"), sum("overlap_area_m2"), {pop_col_sum}, {intensive_col_sums}, {extensive_col_sums}
from {data_geom_table} "dg"
inner join (
      select "outer_id", "data_geom_id", "area_wgt",
           case
		when sum("dev_in_data_geom") > 0
		then sum("dev_in_both")::float / sum("dev_in_data_geom")
		else 0
	   end as "rast_wgt",
           sum("dev_area_m2") as "dev_area_m2",
           sum("rast_area_m2") as "rast_area_m2",
           sum("overlap_area_m2") as "overlap_area_m2"
      from (
        select "outer".{outer_id_col} as "outer_id",
               "data_geom".{data_geom_id_col} as "data_geom_id",
  	       ST_area(ST_intersection("data_geom".{data_geom_col}, "outer".{outer_geom_col}) :: geography) as "overlap_area_m2",
  	       ST_area(ST_intersection("data_geom".{data_geom_col}, "outer".{outer_geom_col}))/ST_area("data_geom".{data_geom_col}) as "area_wgt",
	       ST_CLIP("lc".rast, "data_geom".{data_geom_col}, true) as "crast",
	       coalesce(ST_valuecount(ST_reclass(ST_CLIP(ST_CLIP("lc"."rast", "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true),'[20-29]:1','1BB'),1,true,1), 0)   as "dev_in_both",
  	       ST_valuecount(ST_reclass(ST_CLIP("lc"."rast", "data_geom".{data_geom_col}, true),'[20-29]:1','1BB'),1,true,1) as "dev_in_data_geom",
               ST_Area(ST_Polygon(ST_reclass(ST_CLIP(ST_CLIP("lc"."rast", "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true),'[20-29]:1','1BB')) :: geography) as "dev_area_m2",
               ST_Area(ST_ConvexHull(ST_clip(ST_clip("lc"."rast", "data_geom".{data_geom_col},true), "outer".{outer_geom_col}, true)) :: geography) as "rast_area_m2"
      from {data_geom_table} "data_geom"
      inner join {outer_geom_table} "outer"
      on ST_intersects("data_geom".{data_geom_col},"outer".{outer_geom_col})
      inner join (
        select {lc_rast_col} as rast
	from {lc_rast_table}
      ) "lc"
      on ST_intersects("data_geom".{data_geom_col}, "lc"."rast")
      )
      group by "outer_id", "data_geom_id", "area_wgt"
    ) "dg_and_wgt"
on "dg".{data_geom_id_col} = "dg_and_wgt"."data_geom_id"
group by "outer_id"
''').format(**parms)
    return sql_str


qsql = data_and_wgt_tbl_sql(table_params, data_col_params, cur)
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
