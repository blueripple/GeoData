import psycopg2
from psycopg2 import sql
import re
import itertools
#acs_dir = "/Users/adam/BlueRipple/GeoData/input_files/NHGIS"
#shape_dir = acs_dir + "/US_2022_tract_shapefile"
#data_dir = acs_dir + "/US_2022_tract_csv"
#tract_shapes = shape_dir + "/US_tract_2022.shp"
#tract_data = [data_dir + "nhgis0042_d262_20225_tract_E_.csv",
#              data_dir + "nhgis0042_d263_20225_tract_E_.csv"]

dbname = "tracts_and_nlcd"
schema_name = "public"
shape_cols = ["gisjoin", "geoid", "geom", "statefp", "countyfp"]

shape_table = "tracts2022_shapes"
#data_tables = ["nhgis0038_ds249", "nhgis0038_ds250"]
#data_tables = ["nhgis0040_ds254", "nhgis0040_ds255"]
data_tables = ["nhgis0042_ds262", "nhgis0042_ds263"]

join_result_table = "tracts2022_acs2017_2022"

data_common_cols = ["GISJOIN", "STUSAB", "COUNTY", "COUNTYA", "GEO_ID"]
set_dcc = set(data_common_cols)
data_col_pat = re.compile('[A-Z\d]+E\d\d\d')

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")
cur = conn.cursor()

# create view with only columns we want in each data table

def inTuple(n,t):
    x = t[n]
    return x

def create_data_view(tbl_name):
    cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table}")
                .format(schema = sql.Literal(schema_name),
                        table = sql.Literal(tbl_name)
                        ))
    cols = list(map(lambda x: inTuple(0,x),cur.fetchall()))

    if (set(cols).intersection(set_dcc) != set_dcc):
        raise Exception("Data table " + tbl_name + " does not have all common cols!")
    data_cols = [c for c in cols if data_col_pat.match(c)]
    view_cols = data_common_cols + data_cols
    view_name = tbl_name + "_v"
    view_sql = sql.SQL("CREATE OR REPLACE TEMP VIEW {view} AS SELECT {cols} FROM {table}").format(view = sql.Identifier(view_name),
                                                                                                  cols = sql.SQL(', ').join(map(sql.Identifier,view_cols)),
                                                                                                  table = sql.Identifier(tbl_name))

    print(view_sql.as_string(conn))
    cur.execute(view_sql)
    return (view_name, data_cols)

view_info = list(map(create_data_view, data_tables))
numbered_view_info = zip([1, 2], view_info)
view_names = map(lambda x: inTuple(0, inTuple(1, x)), numbered_view_info)
view_cols = map(lambda x: inTuple(1, inTuple(1, x)), numbered_view_info)

def left_join(vi):
    view_number = inTuple(0, vi)
    view_name = inTuple(0, inTuple(1, vi))
    view_data_cols = inTuple(1, inTuple(1, vi))
    vjSql = sql.Identifier("v" + str(view_number), "GISJOIN")
    return sql.SQL('LEFT JOIN {v} v{vn} ON {tj} = {x}').format(vn = sql.SQL(str(view_number)), v = sql.Identifier(view_name), x=vjSql, tj=sql.Identifier("t", "gisjoin"))

def add_table_alias(t, c):
    return t + "." + c

def join_columns(vi):
    view_number = inTuple(0, vi)
    view_name = inTuple(0, inTuple(1, vi))
    view_data_cols = inTuple(1, inTuple(1, vi))
    table_alias = "v" + str(view_number)
    return sql.SQL(', ').join(map(lambda x: sql.Identifier(table_alias, x),view_data_cols))


join_sql = sql.SQL('''
CREATE TABLE {nt}
AS SELECT {shape_cols}, {joined_cols}
FROM {shapes} t {joins}
''').format(nt = sql.Identifier(join_result_table),
            shapes = sql.Identifier(shape_table),
            shape_cols = sql.SQL(', ').join(map(lambda x: sql.Identifier("t",x), shape_cols)),
            joined_cols = sql.SQL(', ').join(map(join_columns, zip([1,2], view_info))),
            joins = sql.SQL(' ').join(map(left_join, zip([1,2], view_info))))

print(join_sql.as_string(conn))
cur.execute(join_sql)

add_index_sql = sql.SQL('''
CREATE INDEX {idx_name}
 ON {table_name}
 USING GIST ({geom_col})
''').format(idx_name = sql.Identifier(join_result_table + "_geom_idx"),
            table_name = sql.Identifier(join_result_table),
            geom_col = sql.Identifier("geom")
            )
print(add_index_sql.as_string(conn))
cur.execute(add_index_sql)

conn.commit()
conn.close()
