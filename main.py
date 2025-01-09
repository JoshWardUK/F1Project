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

api_url = "https://api.jolpi.ca/ergast/f1"

# Step 1 - Get Season Data

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

# Step 2 - Get Driver Data

# Setup connection to persistent database
db = dc.DatabaseConnection("F1Data.db")

# Connect to the database
db.connect()

# Create DuckDB table
db.create_table_from_dataframe("seasons", seasons_df)

 # Get all of seasons and save as a list
season_dates = db.execute_query("SELECT DISTINCT Season FROM seasons")
season_dates_list = season_dates.values.tolist()

# This chunck of code will Create a Drivers table and then Truncate the Drivers table

endpoint_location = ap.APIEndpoints(base_url=api_url, year=2024, limit=100)
endpoint = endpoint_location.get_driverstandings_endpoint()
data = api_client.fetch_data(endpoint=endpoint)
data =parser.JSONPolarsParser(data)
drivers_df = data.get_driverstandings_dataframe()
db.create_table_from_dataframe("drivers", drivers_df)
db.execute_query("TRUNCATE DRIVERS")


# Stores all driver standings into a table for every season
for x in season_dates_list:
    # Get just the raw dates
    x = x[0]
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100)
    endpoint = endpoint_location.get_driverstandings_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    # Function will return polars dataframe
    parser_data = parser.JSONPolarsParser(data)
    drivers_df = parser_data.get_driverstandings_dataframe()

    #Register dataframe with duckdb
    db.register_dataframe('drivers_df', drivers_df)
    
    # Access Dataframe from DuckDB
    db.execute_query("INSERT INTO drivers SELECT * FROM drivers_df")

    time.sleep(5)

    


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
