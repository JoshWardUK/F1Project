import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp
import time

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
