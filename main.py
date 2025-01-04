import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp

db = dc.DatabaseConnection("example.db")
# Connect to the database
db.connect()

api_url = "https://api.jolpi.ca/ergast/f1"

api_client = ac.APIClient(base_url=api_url)

endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=None)
endpoint = endpoint_location.get_seasons_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

total = hp.get_total_from_json(data)

endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=total)
endpoint = endpoint_location.get_seasons_endpoint()

# Fetch data from the API
data = api_client.fetch_data(endpoint=endpoint)

print(data)


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
