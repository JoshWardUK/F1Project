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
first_name = "Lewis"
family_name = "Hamilton"

# Create API Client Object
api_client = ac.APIClient(base_url=api_url)

# Setup connection to persistent database
db = dc.DatabaseConnection("F1Data.db")

# Connect to the database
db.connect()


def get_season_data():
    # Step 1 - Get Season Data

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

    # Create DuckDB table for seasons
    # TODO: - only create table if one does not exist
    db.create_table_from_dataframe("seasons", seasons_df)

    # Get all of seasons and save as a list
    season_dates = db.execute_query("SELECT DISTINCT Season FROM seasons")
    season_dates_list = season_dates.values.tolist()

    return season_dates_list

def get_driver_data():

    # Step 2 - Get Driver Data

    # This chunck of code will Create a Drivers table and then Truncate the Drivers table
    # TODO: Only run this code if the drivers tabe does not exist - if it does exist truncate the table
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=2024, limit=100)
    endpoint = endpoint_location.get_driverstandings_endpoint()
    data = api_client.fetch_data(endpoint=endpoint)
    data =parser.JSONPolarsParser(data)
    drivers_df = data.get_driverstandings_dataframe()
    db.create_table_from_dataframe("drivers", drivers_df)
    db.execute_query("TRUNCATE DRIVERS")

    season_dates_list = get_season_data()

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

# Get all seasons for the driver
#get_season_data()
#get_driver_data()

# Get all seasons for the given driver
driver_seaons_df = db.execute_query(f"SELECT DISTINCT Season FROM DRIVERS WHERE givenName = '{first_name}' AND familyName = '{family_name}' ORDER BY SEASON ASC")

driver_id_df = db.execute_query(f"SELECT DISTINCT driverId FROM DRIVERS WHERE givenName = '{first_name}' AND familyName = '{family_name}' ORDER BY SEASON ASC")

driver_id = [x for x in driver_id_df['driverId']]

endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=100)
endpoint = endpoint_location.get_races_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

data =parser.JSONPolarsParser(data)
races_df = data.get_races_dataframe()

print(races_df)