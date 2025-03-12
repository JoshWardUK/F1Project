import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp
import time
import duckdb as db
import polars as pl


api_url = "https://api.jolpi.ca/ergast/f1"
first_name = "Lewis"
family_name = "Hamilton"
driverid = 'hamilton'

# Create API Client Object
api_client = ac.APIClient(base_url=api_url)

# Setup connection to persistent database
db = dc.DatabaseConnection("F1Data.db")

# Connect to the database
db.connect()


def get_season_data():
    
    """
    Get Season Data and load into Delta Table
    """
    # Step 1 - Get Season Data

    # Call endpoint with the year - this is so we can get the total
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=None, round=0,driverid='hamilton')
    endpoint = endpoint_location.get_seasons_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    # Get total from JSON API
    total = hp.get_total_from_json(data)

    # Use total to pass through to the limit for the API Call. 
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=total,round=0,driverid='hamilton')
    endpoint = endpoint_location.get_seasons_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    # Get seasons as dataframe
    data =parser.JSONPolarsParser(data)
    seasons_df = data.get_season_dataframe()

    #Write data to a delta lake table
    seasons_df.write_delta('./landing_zone/seasons/', mode='append')

def get_driver_data():

    """
    Driver Data is a snapshot at the end of the season. Load into Delta Table
    TODO: Get standings from each round
    TODO: If seasons has started but no races - handle this scenario. Currently the pipeline will fail.
    """

    # Step 2 - Get Driver Data

    # Get all of seasons and save as a list
    season_dates = db.execute_query("SELECT DISTINCT season FROM delta_scan('./landing_zone/seasons/') WHERE SEASON != 2025")
    season_dates_list = season_dates.values.tolist()

    # Stores all driver standings into a table for every season
    for x in season_dates_list:
        # Get just the raw dates
        x = x[0]
        print(f"Downloading Driver data for Year {x}")
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100, round=0,driverid='hamilton')
        endpoint = endpoint_location.get_driverstandings_endpoint()

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        # Function will return polars dataframe
        parser_data = parser.JSONPolarsParser(data)
        drivers_df = parser_data.get_driverstandings_dataframe()

        #Write data to a delta lake table
        drivers_df.write_delta('./landing_zone/drivers/', mode='append')

        # Sleep so the API doesnt block our request
        time.sleep(2)


def get_races_data():

    """
    Save race data into a Delta table. DuckDB is not suitable given the schema can change. 
    Delta Tables are much more flexible for this use case.
    Pull Race data only for the driver we want to analyse.
    """

    # Step 3 - Get Races Data

    # Get all seasons for the given driver
    driver_seaons_df = db.execute_query(f"SELECT DISTINCT Season FROM delta_scan('./landing_zone/drivers/') WHERE givenName = '{first_name}' AND familyName = '{family_name}' ORDER BY SEASON ASC")
    season_dates_list = driver_seaons_df.values.tolist()

    # Stores all races for a given driver for all seasons they particpated in
    # Data is written to a Delta Table
    for x in season_dates_list:
        # Get just the raw dates
        x = x[0]
        print(f"Downloading Race Data for Driver: {first_name} {family_name} & Year: {x}")
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100, round=1,driverid='hamilton')
        endpoint = endpoint_location.get_races_endpoint()

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        # Function will return polars dataframe
        parser_data = parser.JSONPolarsParser(data)
        races_df = parser_data.get_races_dataframe()

        #Write data to a delta lake table
        races_df.write_delta('./landing_zone/races/', mode='append')

        # Sleep so the API doesnt block our request
        time.sleep(2)


def get_results_data():

    # Step 4 - Get Races Data
    
    # Get all seasons for the given driver
    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                         delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=400, round=f1_round,driverid='hamilton')
        endpoint = endpoint_location.get_results_endpoint()
        print(f"Downloading Results Data for Driver: {first_name} {family_name} for Year: {f1_year} & Round: {f1_round}")

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        # Function will return polars dataframe
        parser_data = parser.JSONPolarsParser(data)
        results_df = parser_data.get_results_dataframe()

        #Write data to a delta lake table
        results_df.write_delta('./landing_zone/results/', mode='append')

        # Sleep so the API doesnt block our request
        time.sleep(2)

def get_lap_data():
    
    # Step 5 - Get Lap Data

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                        delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}' and b.season != 2025") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=400, round=f1_round,driverid='hamilton')
        endpoint = endpoint_location.get_laps_endpoint()
        print(f"Downloading Lap Data for Driver: {first_name} {family_name} for Year: {f1_year} & Round: {f1_round}")

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        # Function will return polars dataframe
        parser_data = parser.JSONPolarsParser(data)
        results_df = parser_data.get_lap_times_dataframe()

        if results_df.shape == (0, 0):  
            print("The DataFrame is empty!")
        else:
            #Write data to a delta lake table
            print("Writing to delta lake table")
            results_df.write_delta('./landing_zone/laps/', mode='append')

        # Sleep so the API doesnt block our request
        time.sleep(2)

def get_pitstop_data():

    # Step 6 - Get Pitstops Data

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                        delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}' and b.season != 2025") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=400, round=f1_round,driverid='hamilton')
        endpoint = endpoint_location.get_pitstops_endpoint()
        print(f"Downloading Lap Data for Driver: {first_name} {family_name} for Year: {f1_year} & Round: {f1_round}")

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:

            # Function will return polars dataframe
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_pitstops_dataframe()

            if results_df.shape == (0, 0):  
                print("The DataFrame is empty!")
            else:
            #Write data to a delta lake table
                print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/pitstops/', mode='append')
        else:
            print(f"Data not avaliable via the API for Driver: {first_name} {family_name} for Year: {f1_year} & Round: {f1_round}")

        # Sleep so the API doesnt block our request
        time.sleep(2)

def get_driverstandings_data():

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}' and b.season != 2025") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=400, round=f1_round,driverid='hamilton')
        endpoint = endpoint_location.get_driverstandings_endpoint()
        print(f"Downloading Driver Standings for Year: {f1_year} & Round: {f1_round}")

        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:
        
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_driver_standings_dataframe()
            
            if results_df.shape == (0, 0):  
                    print("The DataFrame is empty!")
            else:
                #Write data to a delta lake table
                #print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/driverstandings/', mode="append")
        else:
            print(f"Data not avaliable via the API for Driver Standings: Year: {f1_year} & Round: {f1_round}")

            # Sleep so the API doesnt block our request
        time.sleep(2)

#Remove delta table files
#hp.cleanup()

# Get all seasons for the driver
#get_season_data()
#get_driver_data()
#get_races_data()
#get_results_data()
#get_lap_data()
#get_pitstop_data()
get_driverstandings_data()


