import psycopg2
from psycopg2 import sql
import re
import itertools
import pandas as pd
import subprocess

dbname = "tracts_and_nlcd"
db_user = "postgres"
db_password = "postgres"
schema_name = "public"
tract_table = "tracts2020_acs2015_2020"
tract_geom_col = "geom"
tract_id_col = "geoid"
aggTo_table = "ny_sldu"
aggTo_geom_col = "geom"
aggTo_id_col = "id_0"
rast_table = "nlcd_us"

data_col_pat = re.compile('[A-Z]+E\d\d\d')

# compute areas of overlap for all census tracts in a SLD

def tupleAt(n,t):
    return t[n]

outer_geom_params = {
    "outer_geom_table": "co_test",
    "outer_id_col": "id_0",
    "outer_geom_col": "geometry",
    "outer_name_col": "name",
}

acs2020_and_lcd_params = {
    "data_geom_table": "tracts2020_acs2015_2020",
    "data_geom_id_col": "geoid",
    "data_geom_col": "geom",
    "data_geom_schema": "public",
    "pop_col": "AMPVE001",
    "intensive_cols": [("AMTCE001", "PerCapitaIncome")],
    "extensive_col_pat": re.compile('A[M,N]..E\d\d\d'),
    "lc_rast_table": "nlcd_us_dev",
    "lc_rast_col": "rast"
}

acs2022_and_lcd_params = {
    "data_geom_table": "tracts2022_acs2017_2022",
    "data_geom_id_col": "geoid",
    "data_geom_col": "geom",
    "data_geom_schema": "public",
    "pop_col": "AQNFE001",
    "intensive_cols": [("AQRAE001", "PerCapitaIncome")],
    "extensive_col_pat": re.compile('A[Q,R]..E\d\d\d'),
    "lc_rast_table": "nlcd_us_dev",
    "lc_rast_col": "rast"
}

def inTuple(n,t):
    x = t[n]
    return x

def extensive_cols(tract_and_lcd_parameters, db_cursor):
#    print("Getting extensive cols for data_geom_table={}".format(tract_and_lcd_parameters["data_geom_table"]))
    db_cursor.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {data_geom_schema} AND table_name = {data_geom_table}")
                      .format(data_geom_schema = sql.Literal(tract_and_lcd_parameters["data_geom_schema"]),
                              data_geom_table = sql.Literal(tract_and_lcd_parameters["data_geom_table"])
                              )
                      )
    cols = list(map(lambda x: inTuple(0,x),db_cursor.fetchall()))
    data_cols = [c for c in cols if (tract_and_lcd_parameters["extensive_col_pat"].match(c)
                                    and c != tract_and_lcd_parameters["pop_col"]
                                    and c not in tract_and_lcd_parameters["intensive_cols"])
                 ]
    return list(set(data_cols))

def dasymmetric_interpolation_sql2(og_parameters, tract_and_lcd_parameters, db_cursor):
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
    parms["density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({area}) * 1e6 as "ppl_per_km2"').format(wgt = wgt_sql, pop = pop_sql, area = rast_area_sql)
    parms["dev_density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({dev_area}) * 1e6 as "ppl_per_dev_km2"').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)
    parms["PW_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {rast_overlap_area})
                 else 0
                 end
                )
               / sum({wgt} * {pop})) as "pw_ppl_per_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, rast_overlap_area = rast_area_sql)
    parms["PW_dev_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {dev_area})
                 else 0
                 end
                )
                / sum({wgt} * {pop})) as "pw_ppl_per_dev_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)

    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), list(map(lambda x:inTuple(0,x), tract_and_lcd_parameters["intensive_cols"]))))
    sql_str = sql.SQL('''
select {outer_id_col}, {outer_name_col}, ST_area({outer_geom_col} :: geography) * 1e-6, "inner".*
from {outer_geom_table}
inner join (
    select "outer_id",
           sum("dev_area_m2") * 1e-6 as "developed_area_km2",
           {pop_col_sum},
           {density_sum},
           {dev_density_sum},
           {PW_ldensity_sum},
           {PW_dev_ldensity_sum},
           {intensive_col_sums},
           {extensive_col_sums}
    from {data_geom_table} "dg"
    inner join (
          select "outer_id", "data_geom_id",
                 case
                    when sum("dev_in_data_geom") > 0
                    then sum("dev_in_both")::float / sum("dev_in_data_geom")
                    else 0
                 end as "rast_wgt",
                 sum(ST_area(ST_Polygon("rast_in_both") :: geography)) as  "dev_area_m2",
                 sum(ST_area(ST_Envelope("rast_in_both") :: geography)) as  "rast_area_m2"
          from (
            select "outer".{outer_id_col} as "outer_id",
                   "data_geom".{data_geom_id_col} as "data_geom_id",
                   "data_geom".{data_geom_col} as "data_geom",
                   ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true) as "rast_in_both",
                   coalesce(ST_valuecount(ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true),1,true,1), 0)   as "dev_in_both",
                   ST_valuecount(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true),1,true,1) as "dev_in_data_geom"
            from {data_geom_table} "data_geom"
            inner join {outer_geom_table} "outer"
            on ST_intersects("data_geom".{data_geom_col},"outer".{outer_geom_col})
            inner join {lc_rast_table}
            on ST_intersects("data_geom".{data_geom_col}, {lc_rast_table}."rast")
          )
          group by "outer_id", "data_geom_id"
        ) "dg_and_wgt"
    on "dg".{data_geom_id_col} = "dg_and_wgt"."data_geom_id"
    group by "outer_id"
) "inner"
on "inner"."outer_id" = {outer_id_col}
''').format(**parms)
    return sql_str

def dasymmetric_interpolation(og_parameters, tract_and_lcd_parameters, db_connection):
    cur = db_connection.cursor()
    sql = dasymmetric_interpolation_sql2(og_parameters, tract_and_lcd_parameters, cur)
    ext_cols = extensive_cols(tract_and_lcd_parameters, cur)
    int_cols = list(map(lambda x:inTuple(1,x), tract_and_lcd_parameters["intensive_cols"]))
    cols = ["ID","DistrictName","SqKm", "ID_Copy","DevSqKm","TotalPopulation","PopPerSqKm","PopPerDevSqKm","pwPopPerSqKm","pwPopPerDevSqKm"] + int_cols + ext_cols
#    print(sql.as_string(db_connection))
    cur.execute(sql)
    df = pd.DataFrame(cur.fetchall(),columns = cols).drop(["ID_Copy"], axis=1)
    col_type_dict = dict(map(lambda x: (x, int), ["TotalPopulation"] + ext_cols))
    return df.astype(col_type_dict)

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
#dasymmetric_result = dasymmetric_interpolation(outer_geom_params, acs2022_and_lcd_params, conn)
#print(dasymmetric_result.to_csv(header=True, index=False, float_format="%.2f"))

#exit(0)


def load_shapes_from_file(db_connection, filename, table_name, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326"):
    print("load_shapes_from_file: loading shapes from {} to {}.{}".format(filename, dbname, table_name))
    if (wkt != "EPSG:4326"):
        srid_flags = "-s_SRS {} -t_srs EPSG:4326"
    else:
        srid_flags = ""
    cmd_uf = "ogr2ogr -f \"PostgreSQL\" PG:\"dbname={dbname_} user={db_user_} password={db_password_}\ host='localhost' port='5432'\" {filename_} -lco GEOMETRY_NAME={geom_col_} -lco FID={id_col_} -progress {srid_flags_} -nln public.{table}"
    cmd = cmd_uf.format(filename_ = filename,
                        table = table_name,
                        id_col_ = id_col,
                        geom_col_ = geom_col,
                        dbname_ = dbname,
                        db_user_ = db_user,
                        db_password_ = db_password,
                        srid_flags_ = srid_flags)
    print("load_shapes_from_file: running command \"{}\"".format(cmd))
    subprocess.call(cmd, shell=True)

def dasymmetric_interpolation_from_file(db_connection, filename, tract_data_parameters, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326"):
    load_shapes_from_file(db_connection, filename, "shapes_tmp", id_col, name_col, geom_col, wkt)
    print("Joining on census shapes and nlcd raster to perform dasymmetric interpolation.")
    ogps = {
        "outer_geom_table": "shapes_tmp",
        "outer_id_col": id_col,
        "outer_geom_col": geom_col,
        "outer_name_col": name_col
    }
    try:
        res = dasymmetric_interpolation(ogps, tract_data_parameters, db_connection)
    except Exception as e:
        print("Error in dasymmetric interpolation: {}".format(e))
    else:
        print("Done with dasymmetric interpolation.")
        return res
    finally:
        print("Dropping temp shapes table.")
        cur = db_connection.cursor()
        cur.execute("drop table shapes_tmp")
        db_connection.commit()

#def dasymmetric_sld_from_shapefile(state, chamber, db_connection, shapefile, tract_data_parameters, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326"):
#    df = dasymmetric_from_file(db_connection, shapefile, tract_data_parameters, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326")
#    df["StateFIPS"]

#test_from_file = dasymmetric_from_file(conn, "/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024/CO.geojson", acs2022_and_lcd_params)
#print(test_from_file.to_csv(header=True, index=False, float_format="%.2f"))

#conn.close(
#exit(0)

def dasymmetric_overlaps_sql3(og1_parameters, og2_parameters, tract_and_lcd_parameters):
    s1p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og1_parameters.items()))
    s2p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og2_parameters.items()))
    sql_str = sql.SQL(
'''
select "name_1", "name_2", "d_pop_in_s1_s2", "a_pop_in_s1_s2", "d_pop_in_s1", "a_pop_in_s1"
from (
    select "name_1", "name_2",
           sum(case
                when "dev_in_tract" > 0 then "tract_pop" * "dev_in_tract_s1_s2" :: float / "dev_in_tract" :: float else 0
               end
              ) as "d_pop_in_s1_s2",
           sum(case
                when "tract_area" > 0 then "tract_pop" * "tract_area_in_s1_s2" / "tract_area" else 0
               end
              ) as "a_pop_in_s1_s2",
           sum(case
                when "dev_in_tract" > 0 then "tract_pop" * "dev_in_tract_s1" :: float / "dev_in_tract" :: float else 0
               end
              ) as "d_pop_in_s1",
           sum(case
                when "tract_area" > 0 then "tract_pop" * "tract_area_in_s1" / "tract_area" else 0
               end
              ) as "a_pop_in_s1"
           from (
               select "name_1", s2.{name_col_2} as "name_2", "tract_pop",
                       sum(coalesce(ST_VALUECOUNT("rast_in_tract",1,true,1), 0)) as "dev_in_tract",
                       sum(case
                            when "rast_in_tract" && "geom_1"
                            then coalesce(ST_VALUECOUNT(ST_CLIP("rast_in_tract", "geom_1"),1,true,1), 0)
                            else 0
                           end) as "dev_in_tract_s1",
                       sum(case
                            when "rast_in_tract" && "geom_1" and ST_CLIP("rast_in_tract", "geom_1") && s2.{geom_col_2}
                            then coalesce(ST_VALUECOUNT(ST_CLIP(ST_CLIP("rast_in_tract", "geom_1"), s2.{geom_col_2}),1,true,1), 0)
                            else 0
                           end) as "dev_in_tract_s1_s2",
                       ST_AREA("tract_geom" :: geography) as "tract_area",
                       ST_AREA(ST_INTERSECTION("geom_1", "tract_geom") :: geography) as "tract_area_in_s1",
                       ST_AREA(ST_INTERSECTION(ST_INTERSECTION("geom_1", "tract_geom"), s2.{geom_col_2}) :: geography) as "tract_area_in_s1_s2"
                from (
                    select s1.{name_col_1} as "name_1",
                           s1.{geom_col_1} as "geom_1",
                           t.{data_geom_id_col} as "tract_id",
                           t.{data_geom_col} as "tract_geom",
                           t.{pop_col} as "tract_pop",
                           ST_CLIP(r.{raster_col}, t.{data_geom_col}) as "rast_in_tract"
                    from {shape_table_1} s1
                    inner join {tract_table} t on  t.{data_geom_col} && s1.{geom_col_1}
                    inner join {raster_table} r on  r.{raster_col} && t.{data_geom_col}
                )
                inner join {shape_table_2} s2
                on s2.{geom_col_2} && "geom_1"
                group by "name_1", "geom_1", "name_2", s2.{geom_col_2}, "tract_id", "tract_geom", "tract_pop"
            )
    group by "name_1", "name_2"
)
where "d_pop_in_s1_s2" > 0 or "a_pop_in_s1_s2" > 0
''').format(shape_table_1 = s1p["outer_geom_table"],
            id_col_1 = s1p["outer_id_col"],
            name_col_1 = s1p["outer_name_col"],
            geom_col_1 = s1p["outer_geom_col"],
            shape_table_2 = s2p["outer_geom_table"],
            id_col_2 = s2p["outer_id_col"],
            name_col_2 = s2p["outer_name_col"],
            geom_col_2 = s2p["outer_geom_col"],
            tract_table = sql.Identifier(tract_and_lcd_parameters["data_geom_table"]),
            data_geom_id_col = sql.Identifier(tract_and_lcd_parameters["data_geom_id_col"]),
            data_geom_col = sql.Identifier(tract_and_lcd_parameters["data_geom_col"]),
            pop_col = sql.Identifier(tract_and_lcd_parameters["pop_col"]),
            raster_table = sql.Identifier(tract_and_lcd_parameters["lc_rast_table"]),
            raster_col = sql.Identifier(tract_and_lcd_parameters["lc_rast_col"])
            )
    return sql_str

def dasymmetric_overlap(og1_parameters, og2_parameters, tract_and_lcd_parameters, db_connection):
    cur = db_connection.cursor()
    names1_sql = sql.SQL("select {col} from {tbl}").format(col=sql.Identifier(og1_parameters["outer_name_col"]), tbl=sql.Identifier(og1_parameters["outer_geom_table"]))
    names2_sql = sql.SQL("select {col} from {tbl}").format(col=sql.Identifier(og2_parameters["outer_name_col"]), tbl=sql.Identifier(og2_parameters["outer_geom_table"]))
    cur.execute(names1_sql)
    names1 = list(map(lambda x: inTuple(0,x), cur.fetchall()))
    cur.execute(names2_sql)
    names2 = list(map(lambda x: inTuple(0,x), cur.fetchall()))
    overlap_sql = dasymmetric_overlaps_sql3(og1_parameters, og2_parameters, tract_and_lcd_parameters)
    nz = {}
    for i in names1:
        nz[i] = 0
    overlapMap = {'NAME': {}, 'TotalPopulation': {}}
    for i in names2:
        overlapMap[i] = nz.copy()
    print("running overlap query...")
    cur.execute(overlap_sql)
    for x in cur.fetchall():
        overlapMap['NAME'][x[0]] = x[0]
        overlapMap['TotalPopulation'][x[0]] = round(x[4])
        overlapMap[x[1]][x[0]] = round(x[2])
#    print(overlapMap)
    df = pd.DataFrame.from_dict(overlapMap, orient='columns')
    return df

def dasymmetric_overlap_from_files(db_connection, filename1, filename2, tract_data_parameters, id_col="id_0", name_col="name", geom_col="geometry", wkt="EPSG:4326"):
    cur = db_connection.cursor()
    cur.execute("drop table if exists shapes1_tmp")
    cur.execute("drop table if exists shapes2_tmp")
    db_connection.commit()
    load_shapes_from_file(db_connection, filename1, "shapes1_tmp", id_col, name_col, geom_col, wkt)
    load_shapes_from_file(db_connection, filename2, "shapes2_tmp", id_col, name_col, geom_col, wkt)
    og1ps = {
        "outer_geom_table": "shapes1_tmp",
        "outer_id_col": id_col,
        "outer_geom_col": geom_col,
        "outer_name_col": name_col
    }
    og2ps = {
        "outer_geom_table": "shapes2_tmp",
        "outer_id_col": id_col,
        "outer_geom_col": geom_col,
        "outer_name_col": name_col
    }
    try:
        print("Joining on shapes and nlcd raster to perform dasymmetric calculation of overlaps.")
        res = dasymmetric_overlap(og1ps, og2ps, tract_data_parameters, db_connection)
    except Exception as e:
        print("Error in dasymmetric interpolation: {}".format(e))
    else:
        print("Done with dasymmetric interpolation.")
        return res
    finally:
        print("Dropping temp shapes tables.")
        cur.execute("drop table shapes1_tmp")
        cur.execute("drop table shapes2_tmp")
        db_connection.commit()

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")

#load_shapes_from_file(conn,"/Users/adam/BlueRipple/GeoData/input_data/CongressionalDistricts/cd2024/CO.geojson","CO_cd")
#load_shapes_from_file(conn,"/Users/adam/BlueRipple/GeoData/input_data/StateLegDistricts/2024/CO_sldu.geojson","CO_sldu")
#og1p = {"outer_geom_table": "co_sldu", "outer_id_col": "id_0", "outer_geom_col": "geometry", "outer_name_col": "name"}
#og2p = {"outer_geom_table": "co_cd", "outer_id_col": "id_0", "outer_geom_col": "geometry", "outer_name_col": "name"}
#df = dasymmetric_overlap(og1p, og2p, acs2022_and_lcd_params, conn)
#print(df)
#overlap_sql = dasymmetric_overlaps_sql3(og1p, og2p, acs2022_and_lcd_params)
#print(overlap_sql.as_string(conn))
#cur = conn.cursor()
#cur.execute(overlap_sql)
#print(cur.fetchall())

#exit(0)

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
    parms["density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({area}) * 1e6 as "ppl_per_km2"').format(wgt = wgt_sql, pop = pop_sql, area = rast_area_sql)
    parms["dev_density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({dev_area}) * 1e6 as "ppl_per_dev_km2"').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)
    parms["PW_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {rast_overlap_area})
                 else 0
                 end
                )
               / sum({wgt} * {pop})) as "pw_ppl_per_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, rast_overlap_area = rast_area_sql)
    parms["PW_dev_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {dev_area})
                 else 0
                 end
                )
                / sum({wgt} * {pop})) as "pw_ppl_per_dev_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)

    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), list(map(lambda x:inTuple(0,x), tract_and_lcd_parameters["intensive_cols"]))))
    sql_str = sql.SQL('''
select "outer_id",
       "outer_name",
       {pop_col_sum},
       sum("dev_area_m2") * 1e-6 as "developed_area_km2",
       sum("rast_area_m2") * 1e-6 as "area_km2",
       {density_sum},
       {dev_density_sum},
       {PW_ldensity_sum},
       {PW_dev_ldensity_sum},
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
             sum(ST_area(ST_Envelope("rast_in_both") :: geography)) as  "rast_area_m2"
      from (
        select "outer".{outer_id_col} as "outer_id",
               "outer".{outer_name_col} as "outer_name",
               "data_geom".{data_geom_id_col} as "data_geom_id",
               "data_geom".{data_geom_col} as "data_geom",
	       ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true) as "rast_in_both",
	       coalesce(ST_valuecount(ST_CLIP(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true), "outer".{outer_geom_col}, true),1,true,1), 0)   as "dev_in_both",
  	       ST_valuecount(ST_CLIP({lc_rast_table}.{lc_rast_col}, "data_geom".{data_geom_col}, true),1,true,1) as "dev_in_data_geom"
        from {data_geom_table} "data_geom"
        inner join {outer_geom_table} "outer"
        on ST_intersects("data_geom".{data_geom_col},"outer".{outer_geom_col})
        inner join {lc_rast_table}
        on ST_intersects("data_geom".{data_geom_col}, {lc_rast_table}."rast")
      )
      group by "outer_id", "outer_name", "data_geom_id"
    ) "dg_and_wgt"
on "dg".{data_geom_id_col} = "dg_and_wgt"."data_geom_id"
group by "outer_id", "outer_name"
''').format(**parms)
    return sql_str

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

def dasymmetric_overlaps_sql2(og1_parameters, og2_parameters, tract_and_lcd_parameters):
    s1p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og1_parameters.items()))
    s2p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og2_parameters.items()))
    sql_str = sql.SQL(
'''
select "name_1", "name_2", "areal_overlap_pop", "areal_pop", "dasymmetric_overlap_pop", "dasymmetric_pop"
from (
    select "name_1", "name_2",
           sum("areal_overlap_wgt" * "tract_pop") as "areal_overlap_pop",
           sum("areal_wgt" * "tract_pop") as "areal_pop",
           sum("dasymmetric_overlap_wgt" * "tract_pop") as "dasymmetric_overlap_pop",
           sum("dasymmetric_wgt" * "tract_pop") as "dasymmetric_pop"
    from (
        select "name_1", "name_2", "tract_pop",
                   case
                    when "tract_area" > 0
                    then "tract_area_in_both" / "tract_area"
                    else 0
                   end as "areal_overlap_wgt",
                   case
                    when "tract_area" > 0
                    then "tract_area_in_s1" / "tract_area"
                    else 0
                   end as "areal_wgt",
                   case
                    when "dev_in_tract" > 0
                    then "dev_in_tract_s1_s2" / "dev_in_tract"
                    else 0
                   end as "dasymmetric_overlap_wgt",
                   case
                    when "dev_in_tract" > 0
                    then "dev_in_tract_s1" / "dev_in_tract"
                    else 0
                   end as "dasymmetric_wgt"
        from (
            select  "name_1", "name_2", "tract_pop", "dev_in_tract",
                    ST_AREA("tract_geom" :: geography) as "tract_area",
                    ST_AREA(ST_INTERSECTION("geom_1", "tract_geom") :: geography) as "tract_area_in_s1",
                    ST_AREA("geom_1_2" :: geography) as "tract_area_in_both",
                    coalesce(ST_VALUECOUNT("rast_in_1",1,true,1), 0) as "dev_in_tract_s1",
                    case
                     when "rast_in_1" && "geom_2"
                     then coalesce(ST_VALUECOUNT(ST_CLIP("rast_in_1", "geom_2"),1,true,1), 0)
                     else 0
                    end as "dev_in_tract_s1_s2"
            from (
                select s1.{name_col_1} as "name_1",
                       s1.{geom_col_1} as "geom_1",
                       s2.{name_col_2} as "name_2",
                       s2.{geom_col_2} as "geom_2",
                       t."tract_pop" as "tract_pop",
                       t."tract_geom" as "tract_geom",
                       ST_INTERSECTION(s1.{geom_col_1}, s2.{geom_col_2}) as "geom_1_2",
                       coalesce(ST_VALUECOUNT(t."tract_raster",1,true,1), 0)   as "dev_in_tract",
                       ST_CLIP(t."tract_raster", s1.{geom_col_1}) as "rast_in_1"
                from {shape_table_1} as s1
                inner join {shape_table_2} s2 on s1.{geom_col_1} && s2.{geom_col_2}
                inner join (
                    select t.{pop_col} as "tract_pop", t.{data_geom_col} as "tract_geom",
                           ST_CLIP(ST_Union(r.{raster_col}), t.{data_geom_col}) as "tract_raster"
                    from {tract_table} as t
                    inner join {raster_table} r
                    on r.{raster_col} && t.{data_geom_col}
                    where ST_INTERSECTS(t.{data_geom_col}, (select ST_Union({shape_table_1}.{geom_col_1}) from {shape_table_1}))
                    group by "tract_pop", "tract_geom"
                ) t on s1.{geom_col_1} && t."tract_geom"

            )
        )
    )
    group by "name_1", "name_2"
)
where "areal_overlap_pop" > 1 or "dasymmetric_overlap_pop" > 1
order by "name_1" asc
''').format(shape_table_1 = s1p["outer_geom_table"],
            id_col_1 = s1p["outer_id_col"],
            name_col_1 = s1p["outer_name_col"],
            geom_col_1 = s1p["outer_geom_col"],
            shape_table_2 = s2p["outer_geom_table"],
            id_col_2 = s2p["outer_id_col"],
            name_col_2 = s2p["outer_name_col"],
            geom_col_2 = s2p["outer_geom_col"],
            tract_table = sql.Identifier(tract_and_lcd_parameters["data_geom_table"]),
            data_geom_col = sql.Identifier(tract_and_lcd_parameters["data_geom_col"]),
            pop_col = sql.Identifier(tract_and_lcd_parameters["pop_col"]),
            raster_table = sql.Identifier(tract_and_lcd_parameters["lc_rast_table"]),
            raster_col = sql.Identifier(tract_and_lcd_parameters["lc_rast_col"])
            )
    return sql_str

def dasymmetric_overlaps_sql(og1_parameters, og2_parameters, tract_and_lcd_parameters):
    s1p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og1_parameters.items()))
    s2p =  parms = dict(map(lambda k_v: (k_v[0], sql.Identifier(k_v[1])), og2_parameters.items()))
    sql_str = sql.SQL(
'''
select "name1", "name2",
       sum("areal_overlap_wgt" * "tract_pop") as "areal_overlap_pop",
       sum("dasymmetric_overlap_wgt" * "tract_pop") as "dasymmetric_overlap_pop",
       sum("dasymmetric_wgt" * "tract_pop") as "dasymmetric_pop"
from (
    select "name1", "name2", "tract_pop",
           case
            when "tract_area" > 0
            then "tract_area_in_both" / "tract_area"
            else 0
           end as "areal_overlap_wgt",
           case
            when "dev_in_tract" > 0
            then "dev_in_tract_s1_s2" / "dev_in_tract"
            else 0
           end as "dasymmetric_overlap_wgt",
           case
            when "dev_in_tract" > 0
            then "dev_in_tract_s1" / "dev_in_tract"
            else 0
           end as "dasymmetric_wgt"
    from (
        select "name1", "name2", "tract_pop",
               ST_AREA("tract_geom" :: geography) as "tract_area",
               ST_AREA(ST_INTERSECTION(ST_INTERSECTION("geom1", "tract_geom"), "geom2") :: geography) as "tract_area_in_both",
               coalesce(ST_VALUECOUNT("rast_in_tract",1,true,1), 0)   as "dev_in_tract",
               case
                when "rast_in_tract" && "geom1"
                then coalesce(ST_VALUECOUNT(ST_CLIP("rast_in_tract", "geom1"),1,true,1), 0)
                else 0
               end as "dev_in_tract_s1",
               case
                when "rast_in_tract" && "geom1" and ST_CLIP("rast_in_tract", "geom1") && "geom2"
                then coalesce(ST_VALUECOUNT(ST_CLIP(ST_CLIP("rast_in_tract", "geom1"), "geom2"),1,true,1), 0)
                else 0
               end as "dev_in_tract_s1_s2"
        from (
            select s1.{name_col_1} as "name1",
                   s1.{geom_col_1} as "geom1",
                   s2.{name_col_2} as "name2",
                   s2.{geom_col_2} as "geom2",
                   t.{data_geom_col} as "tract_geom",
                   t.{pop_col} as "tract_pop",
                   ST_CLIP(ST_Union(r.{raster_col}), t.{data_geom_col}) as "rast_in_tract"
            from {shape_table_1} s1
            inner join {shape_table_2} s2 on s1.{geom_col_1} && s2.{geom_col_2}
            inner join {tract_table} t on s1.{geom_col_1} && t.{data_geom_col}
            inner join {raster_table} r on t.{data_geom_col} && r.{raster_col}
            group by "name1", "geom1", "name2", "geom2", "tract_pop", "tract_geom"
        )

    )
)
group by "name1", "name2"

''').format(shape_table_1 = s1p["outer_geom_table"],
            id_col_1 = s1p["outer_id_col"],
            name_col_1 = s1p["outer_name_col"],
            geom_col_1 = s1p["outer_geom_col"],
            shape_table_2 = s2p["outer_geom_table"],
            id_col_2 = s2p["outer_id_col"],
            name_col_2 = s2p["outer_name_col"],
            geom_col_2 = s2p["outer_geom_col"],
            tract_table = sql.Identifier(tract_and_lcd_parameters["data_geom_table"]),
            data_geom_col = sql.Identifier(tract_and_lcd_parameters["data_geom_col"]),
            pop_col = sql.Identifier(tract_and_lcd_parameters["pop_col"]),
            raster_table = sql.Identifier(tract_and_lcd_parameters["lc_rast_table"]),
            raster_col = sql.Identifier(tract_and_lcd_parameters["lc_rast_col"])
            )
    return sql_str

def dasymmetric_interpolation_sql3(og_parameters, tract_and_lcd_parameters, db_cursor):
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
    parms["density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({area}) * 1e6 as "ppl_per_km2"').format(wgt = wgt_sql, pop = pop_sql, area = rast_area_sql)
    parms["dev_density_sum"] = sql.SQL('sum({wgt} * {pop})/sum({dev_area}) * 1e6 as "ppl_per_dev_km2"').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)
    parms["PW_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {rast_overlap_area})
                 else 0
                 end
                )
               / sum({wgt} * {pop})) as "pw_ppl_per_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, rast_overlap_area = rast_area_sql)
    parms["PW_dev_ldensity_sum"] = sql.SQL(
        '''exp(sum(
                case
                 when ({wgt} * {pop}) > 0
                 then {wgt} * {pop} * ln({wgt} * {pop} * 1e6 / {dev_area})
                 else 0
                 end
                )
                / sum({wgt} * {pop})) as "pw_ppl_per_dev_mi2"
''').format(wgt = wgt_sql, pop = pop_sql, dev_area = dev_area_sql)

    parms["intensive_col_sums"] = sql.SQL(', ').join(map(lambda x: sql.SQL('sum({wgt} * {pop} * {iv})/sum({wgt} * {pop})').format(wgt=wgt_sql, pop=pop_sql, iv=sql.Identifier(x)), list(map(lambda x:inTuple(0,x), tract_and_lcd_parameters["intensive_cols"]))))
    sql_str = sql.SQL('''
select {outer_id_col}, {outer_name_col}, ST_area({outer_geom_col} :: geography) * 1e-6, "inner".*
from {outer_geom_table}
inner join (
    select "outer_id",
           sum("dev_area_m2") * 1e-6 as "developed_area_km2",
           {pop_col_sum},
           {density_sum},
           {dev_density_sum},
           {PW_ldensity_sum},
           {PW_dev_ldensity_sum},
           {intensive_col_sums},
           {extensive_col_sums}
    from {data_geom_table} "dg"
    inner join (
          select "outer_id", "data_geom_id",
                 case
                    when "dev_in_data_geom" > 0
                    then "dev_in_both" ::float / "dev_in_data_geom"
                    else 0
                 end as "rast_wgt",
                 ST_area(ST_Polygon("rast_in_both") :: geography) as  "dev_area_m2",
                 ST_area(ST_Envelope("rast_in_both") :: geography) as  "rast_area_m2"
          from (
            select "outer".{outer_id_col} as "outer_id",
                   "data_geom_rast".{data_geom_id_col} as "data_geom_id",
                   "data_geom_rast".{data_geom_col} as "data_geom",
                   ST_CLIP("data_geom_rast".{lc_rast_col},"outer".{outer_geom_col}, true) as "rast_in_both",
                   coalesce(ST_valuecount(ST_CLIP("data_geom_rast".{lc_rast_col}, "outer".{outer_geom_col}, true),1,true,1), 0)   as "dev_in_both",
                   ST_valuecount("data_geom_rast".{lc_rast_col},1,true,1) as "dev_in_data_geom"
            from {data_geom_table} "data_geom_rast"
            inner join {outer_geom_table} "outer"
            on "data_geom_rast".{data_geom_col} && "outer".{outer_geom_col}
          )
        ) "dg_and_wgt"
    on "dg".{data_geom_id_col} = "dg_and_wgt"."data_geom_id"
    group by "outer_id"
) "inner"
on "inner"."outer_id" = {outer_id_col}
''').format(**parms)
    return sql_str
