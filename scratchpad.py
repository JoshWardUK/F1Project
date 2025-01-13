import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp
import time


from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import duckdb as db
import polars as pl

"""

api_url = "https://api.jolpi.ca/ergast/f1"

# Create API Client Object
api_client = ac.APIClient(base_url=api_url)

# Call endpoint with the year - this is so we can get the total
endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=None)
endpoint = endpoint_location.get_seasons_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

# Get total from JSON API
total = hp.get_total_from_json(data)

# Use total to pass through to the limit for the API Call. 
endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=total)
endpoint = endpoint_location.get_seasons_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

# Get seasons as dataframe
data =parser.JSONPolarsParser(data)
seasons_df = data.get_season_dataframe()

# Get driver standings

endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=100)
endpoint = endpoint_location.get_driverstandings_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

parser_data = parser.JSONPolarsParser(data)
drivers_df = parser_data.get_driverstandings_dataframe()

#Read delta tables using duckdb

delta_table_path = 'deltaTable/'

write_deltalake('./my_delta_table', seasons_df, mode='append')

#delta_table = DeltaTable("deltaTable")

#print("Delta table version:", delta_table.version())

#result = db.execute("SELECT * FROM delta_scan('./my_delta_table')").fetchdf()

#print(result)

"""

#df3 = db.query("""SELECT * FROM delta_scan('./my_delta_table/') """).df()

#print(df3)

"""
db = dc.DatabaseConnection("example.db")
# Connect to the database
db.connect()

db.create_table_from_dataframe("seasons", seasons_df)

season_dates = db.execute_query("SELECT DISTINCT Season FROM seasons")

season_dates_list = season_dates.values.tolist()

# Stores all driver standings into a table for every season
for x in season_dates_list:
    x = x[0]
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100)
    endpoint = endpoint_location.get_driverstandings_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    parser_data = parser.JSONPolarsParser(data)
    drivers_df = parser_data.get_driverstandings_dataframe()

    db.register_dataframe('drivers_df', drivers_df)
    
    db.execute_query("INSERT INTO drivers SELECT * FROM drivers_df")


    time.sleep(5)

    
    """


"""
prep_df = parser.JSONPolarsParser(data)
df = prep_df.get_constructorstandings_dataframe()

# Access the data from memory
if data:
    print(df)
    

# Clear the data from memory
api_client.clear_data()


json_data = data

root = dm.Root(**json_data)

# Access data
print(root.MRData.series)  # Output: f1
for season in root.MRData.SeasonTable.Seasons:
    print(f"Season: {season.season}, URL: {season.url}")

"""

# Create Iceberg table and read using duckdb
import duckdb
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import os
import shutil
import sqlite3

name_space = 'demo_db'

warehouse_path = "./icehouse"
catalog = SqlCatalog(
    name_space,
    **{
        "uri": f"sqlite:///{warehouse_path}/icyhot.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

# create a namespace for Iceberg
catalog.create_namespace(name_space)

pytable = duckdb.sql("SELECT * FROM test.json").arrow()

duckdb_conn = duckdb.connect('test.db',read_only=False)

table = catalog.create_table("demo_db.air_quality", schema=pytable.schema,)

table.append(pytable)

table.scan().to_duckdb('air',connection=duckdb_conn)

x = duckdb_conn.execute("CREATE TABLE tbl_air AS SELECT * FROM air").fetchall()
x = duckdb_conn.execute("SELECT * FROM tbl_air").fetchall()


print(x)

duckdb_conn.close()



duckdb_conn1 = duckdb.connect('test.db',read_only=False)
x = duckdb_conn1.execute("SELECT * FROM tbl_air").fetchall()
print(x)