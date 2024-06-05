import psycopg2
from psycopg2 import sql
import pandas as pd
import re
import os
import io


#os.environ['PGHOST'] = 'localhost'
#os.environ['PGPORT'] = '5432'
#os.environ['PGUSER'] = 'postgres'
#os.environ['PGPASSWORD'] = 'postgres'

db_user = "postgres"
db_password = "postgres"
schema_name = "public"
dbname = "tracts_and_nlcd"

conn = psycopg2.connect("dbname=" + dbname + " user=postgres")

parties = 'dem|rep|oth'
parties_to_keep_pat = '^party_(' + parties + ')'
parties_to_drop_pat = '^party_(?!' + parties + ')'
id_cols = ["geoid20","StateFIPS","CountyFIPS","TractId","BlockId"]

colToDropPat = '^(\w*2020|\w*2021|cg|[p,s]*\d.*|\w*pct|languages|commercial|absentee|eth2|' + parties_to_drop_pat + ')'

def inTuple(n,t):
    x = t[n]
    return x

def cols_from_file(file_name):
    df = pd.read_csv(file_name)
    to_drop = list(df.filter(regex=colToDropPat))
    df.drop(to_drop,inplace=True, axis=1)
    return df.columns

def create_sql(table_name,key_col_name, data_cols):
    parms = dict()
    parms["table"] = sql.Identifier(table_name)
    parms["key"] = sql.Identifier(key_col_name)
    parms["count_cols"] = sql.SQL(', ').join(map(lambda x: sql.SQL('{col_name} INTEGER').format(col_name=sql.Identifier(x)), data_cols))
    sql_str=sql.SQL('''
CREATE TABLE IF NOT EXISTS {table} (
    {key} TEXT PRIMARY KEY, "StateFIPS" TEXT, "CountyFIPS" TEXT, "TractId" TEXT, "BlockId" TEXT, {count_cols}
    )
    ''').format(**parms)
    return sql_str

def process_file(file_name, data_cols):
    df = pd.read_csv(file_name, low_memory=False)
    print("csv file has " + str(len(df.index)) + " rows.")
    df2 = simplify_party_data(df)
    to_drop = list(df2.filter(regex=colToDropPat))
    df2.drop(to_drop,inplace=True, axis=1)
    row_is_assigned_block = df2["geoid20"].str.contains('^\d+\w?$')
#    print(row_is_assigned_block)
#    good_rows = [f for f in row_is_assigned_block if f == True]
#    print("len(good_rows)=" + str(len(good_rows)))
    df3 = df2[row_is_assigned_block]
    print("after dropping unassigned rows dataframe has " + str(len(df3.index)) + " rows.")
    geoidPat = re.compile('(?P<StateFIPS>\d{2})(?P<CountyFIPS>\d{3})(?P<TractId>\d{6})(?P<BlockId>\d{4}\w?)')
    new_cols = df3["geoid20"].str.extract(geoidPat)
    new_df = pd.concat([new_cols, df3], axis=1)
    cols = ["geoid20", "StateFIPS", "CountyFIPS", "TractId", "BlockId"] + data_cols
    return new_df[cols]

def simplify_party_data(df):
    all_party_cols = list(df.filter(regex='^party_.*'))
    usual_party_cols = list(df.filter(regex=parties_to_keep_pat))
    other_party_cols = list(set(all_party_cols) - set(usual_party_cols))
    sum_cols = ["party_oth"] + other_party_cols
#    print("all present: " + str(all_party_cols))
#    print("usual present: " + str(usual_party_cols))
#    print("other present: " + str(other_party_cols))
    new_oth = df[sum_cols].sum(axis=1)
    df["party_oth"] = new_oth
    return df.drop(other_party_cols, axis=1)

def add_file_to_db(file_name, data_cols, conn, cur):
    print("Adding " + file_name)
    df = process_file(file_name, data_cols)
    print("processed file has " + str(len(df.index)) + " rows.")
    output = io.StringIO() # For Python3 use StringIO
    df.to_csv(output, sep='\t', header=True, index=False)
    output.seek(0) # Required for rewinding the String object
    copy_query = "COPY voterfile_blocks FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "  # Replace your table name in place of mem_info
    cur.copy_expert(copy_query, output)
    conn.commit()

def build_voterfile_blocks():
    csvPat = re.compile('.*\.csv')
    csv_dir =  "/Users/adam/BlueRipple/GeoData/input_data/VoterFile_CensusBlocks/"
    all_files = os.listdir(csv_dir)
    all_csv_files = [f for f in all_files if csvPat.match(f)]
    all_csv_test = [all_csv_files[1]]
    example_csv_file = all_csv_files[1]
    columns = cols_from_file(csv_dir + example_csv_file)
    key_col = "geoid20"
    data_cols = list(columns.drop(key_col))

    # create the table
    cur = conn.cursor()
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier("voterfile_blocks")))
    create_table_sql = create_sql("voterfile_blocks", key_col, data_cols)
    print(create_table_sql.as_string(conn))
    cur.execute(create_table_sql)
    [add_file_to_db(csv_dir + f, data_cols, conn, cur) for f in all_csv_files]
    conn.commit()
    return data_cols

def build_voterfile_tracts():
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table}")
                .format(schema = sql.Literal(schema_name),
                        table = sql.Literal('voterfile_blocks')
                        ))
    cols = list(map(lambda x: inTuple(0,x),cur.fetchall()))
    data_cols = list(set(cols) - set(id_cols))
    agg_sql_parms = dict()
    agg_sql_parms["state_fips"] = sql.Identifier("StateFIPS")
    agg_sql_parms["county_fips"] = sql.Identifier("CountyFIPS")
    agg_sql_parms["tract_id"] = sql.Identifier("TractId")
    agg_sql_parms["geoid"] = sql.Identifier("geoid")
    agg_sql_parms["id_col"] = sql.SQL('CONCAT({s}, {c}, {t}) as {g}').format(s=agg_sql_parms["state_fips"]
                                                                             , c=agg_sql_parms["county_fips"]
                                                                             , t=agg_sql_parms["tract_id"]
                                                                             , g=agg_sql_parms["geoid"])
    agg_sql_parms["voterfile_tracts_table_name"] = sql.Identifier("voterfile_tracts")
    agg_sql_parms["voterfile_blocks_table_name"] = sql.Identifier("voterfile_blocks")
    agg_sql_parms["agg_cols"] = sql.SQL(', ').join(map(lambda x: sql.SQL('SUM({y}) as {y}').format(y=sql.Identifier(x)), data_cols))
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {voterfile_tracts_table_name}").format(**agg_sql_parms))
    agg_sql = sql.SQL('''
CREATE TABLE IF NOT EXISTS {voterfile_tracts_table_name} AS
  (SELECT {id_col}, {state_fips}, {county_fips}, {tract_id}, {agg_cols} FROM {voterfile_blocks_table_name}
    GROUP BY {state_fips}, {county_fips}, {tract_id}
  )
    ''').format(**agg_sql_parms)
    print(agg_sql.as_string(conn))
    cur.execute(agg_sql)
    agg_sql_parms["geoid_idx"] = sql.Identifier("geoid_idx")
    cur.execute(sql.SQL("CREATE UNIQUE INDEX {geoid_idx} ON {voterfile_tracts_table_name} ({geoid})").format(**agg_sql_parms))
    conn.commit()

#build_voterfile_blocks()
build_voterfile_tracts()


conn.close()
#process_file("~/BlueRipple/GeoData/input_data/VoterFile_CensusBlocks/AK_l2_2022stats_2020block.csv")

#cr_sql = create_sql("test", key_col, data_cols)
#print(cr_sql.as_string(conn))
