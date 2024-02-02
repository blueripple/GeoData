import psycopg2
from psycopg2 import sql
import re
import itertools

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


tract_wgt_params = {
    "outer_geom_table": "ny_sldu",
    "outer_id_col": "id_0",
    "outer_geom_col": "geom",
    "tract_geom_table": "tract_join_test",
    "tract_id_col": "geoid",
    "tract_geom_col": "geom",
    "lc_rast_table": "nlcd_us",
    "lc_rast_col": "rast"
}

tract_wgt_tbl_sql = sql.SQL('''
select "outer_id", "tract_id", "area_wgt",
	case
		when sum("dev_in_tract") > 0
		then sum("dev_in_both")::float / sum("dev_in_tract")
		else 0
	end as "rast_wgt"
from (
select "outer".{outer_id_col} as "outer_id",
       "tract".{tract_id_col} as "tract_id",
	   ST_area(ST_intersection("tract".{tract_geom_col}, "outer".{outer_geom_col}))/ST_area("tract".{tract_geom_col}) as "area_wgt",
	   ST_CLIP("lc".rast, "tract".{tract_geom_col}, true) as "crast",
	   coalesce(ST_valuecount(ST_reclass(ST_CLIP(ST_CLIP("lc"."rast", "tract".{tract_geom_col}, true), "outer".{outer_geom_col}, true),'[20-29]:1','1BB'),1,true,1), 0) as "dev_in_both",
	   ST_valuecount(ST_reclass(ST_CLIP("lc"."rast", "tract".{tract_geom_col}, true),'[20-29]:1','1BB'),1,true,1) as "dev_in_tract"
from {tract_geom_table} "tract"
inner join {outer_geom_table} "outer"
on ST_intersects("tract".{tract_geom_col},"outer".{outer_geom_col})
inner join (
	select {lc_rast_col} as rast
	from {lc_rast_table}
) "lc"
on ST_intersects("tract".{tract_geom_col}, "lc"."rast")
)
group by "outer_id", "tract_id", "area_wgt"
''').format(outer_geom_table = sql.Identifier(tract_wgt_params["outer_geom_table"]),
            outer_id_col = sql.Identifier(tract_wgt_params["outer_id_col"]),
            outer_geom_col = sql.Identifier(tract_wgt_params["outer_geom_col"]),
            tract_geom_table = sql.Identifier(tract_wgt_params["tract_geom_table"]),
            tract_id_col = sql.Identifier(tract_wgt_params["tract_id_col"]),
            tract_geom_col = sql.Identifier(tract_wgt_params["outer_geom_col"]),
            lc_rast_table = sql.Identifier(tract_wgt_params["lc_rast_table"]),
            lc_rast_col = sql.Identifier(tract_wgt_params["lc_rast_col"]),
            )

print(tract_wgt_tbl_sql.as_string(conn))
cur.execute(tract_wgt_tbl_sql)
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
