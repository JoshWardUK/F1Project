import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp
import time
import duckdb as db
import polars as pl
import logging
import os
import sys

# API Endpoint
api_url = "https://api.jolpi.ca/ergast/f1"

# Params passed through
first_name = sys.argv[1]
family_name = sys.argv[2]
season = sys.argv[3]

# Lower family name
driverid = family_name.lower()

print(f"Downloading data for {first_name} {family_name} for season: {season}")

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

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_season_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/seasons/')

    # Call endpoint with the year - this is so we can get the total
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=season, limit=None, round=0,driverid=driverid, offset=0)
    endpoint = endpoint_location.get_seasons_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    # Get total from JSON API
    total = hp.get_total_from_json(data)

    # Use total to pass through to the limit for the API Call. 
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=season, limit=total,round=0,driverid=driverid, offset=0)
    endpoint = endpoint_location.get_seasons_endpoint()

    # Fetch data from the API
    data = api_client.fetch_data(endpoint=endpoint)

    # Get seasons as dataframe
    data =parser.JSONPolarsParser(data)
    seasons_df = data.get_season_dataframe()

    # Write data to a delta lake table
    seasons_df.write_delta('./landing_zone/seasons/', mode='append')

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_driver_data():

    """
    Driver Data is a snapshot at the end of the season. Load into Delta Table
    TODO: Get standings from each round
    TODO: If seasons has started but no races - handle this scenario. Currently the pipeline will fail.
    """

    # Step 2 - Get Driver Data

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_driver_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/drivers/')

    # Get all of seasons and save as a list
    # Only get greater than what the user input is
    season_dates = db.execute_query(f"SELECT DISTINCT season FROM delta_scan('./landing_zone/seasons/') WHERE SEASON = {season}")
    season_dates_list = season_dates.values.tolist()

    # Stores all driver standings into a table for every season
    for x in season_dates_list:
        # Get just the raw dates
        x = x[0]
        print(f"Downloading Driver data for Year {x}")
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100, round=0,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_driverstandings_year_endpoint()

        # Fetch data from the API
        data = api_client.fetch_data(endpoint=endpoint)

        # Function will return polars dataframe
        parser_data = parser.JSONPolarsParser(data)
        drivers_df = parser_data.get_driverstandings_dataframe()

        if drivers_df.shape == (0, 0):  
            print("The DataFrame is empty!")
        else:
            #Write data to a delta lake table
            print("Writing to delta lake table")
            drivers_df.write_delta('./landing_zone/drivers/', mode='append')

        # Sleep so the API doesnt block our request
        time.sleep(5)
    
    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_races_data():

    """
    Save race data into a Delta table. DuckDB is not suitable given the schema can change. 
    Delta Tables are much more flexible for this use case.
    Pull Race data only for the driver we want to analyse.
    """

    # Step 3 - Get Races Data

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_race_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/races/')

    # Get all seasons for the given driver
    driver_seaons_df = db.execute_query(f"SELECT DISTINCT Season FROM delta_scan('./landing_zone/drivers/') WHERE givenName = '{first_name}' AND familyName = '{family_name}' ORDER BY SEASON ASC")
    season_dates_list = driver_seaons_df.values.tolist()

    # Stores all races for a given driver for all seasons they particpated in
    # Data is written to a Delta Table
    for x in season_dates_list:
        # Get just the raw dates
        x = x[0]
        print(f"Downloading Race Data for Driver: {first_name} {family_name} & Year: {x}")
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=x, limit=100, round=1,driverid=driverid, offset=0)
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
    
    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_results_data():

    # Step 4 - Get Races Data

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_results_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/results/')
    
    # Get all seasons for the given driver
    result = db.execute_query(f"SELECT distinct b.season FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                            delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    # Pagination parameters
    limit = 30  # Number of results per request
    offset = 0  # Starting position
    all_results = []  # Store all results
    total = None

    for f1_year in season_dates_list:
        season_year = f1_year[0]  # Extract season year

        # Reset offset for each season
        offset = 0
        total = None  # Reset total count for the new season

        while total is None or offset < total:
            endpoint_location = ap.APIEndpoints(
                base_url=api_url, 
                year=season_year, 
                limit=100, 
                round=1, 
                driverid=driverid, 
                offset=offset
            )
            endpoint = endpoint_location.get_results_endpoint()
            print(f"Downloading Results Data for Driver: {first_name} {family_name} for Year: {season_year}, Offset: {offset}")

            # Fetch data from the API
            data = api_client.fetch_data(endpoint=endpoint)

            if "MRData" in data:
                total = int(data["MRData"].get("total", 0))  # Update total safely
                print(f"Total results for {season_year}: {total}")

                # Parse data using Polars
                parser_data = parser.JSONPolarsParser(data)
                results_df = parser_data.get_results_dataframe()

                # Write data to a Delta Lake table
                results_df.write_delta('./landing_zone/results/', mode='append')

            # Move to the next offset page
            offset += 100
            print(f"Processed {season_year} with offset {offset}")

            time.sleep(2)  # Respect API rate limits
    
    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_lap_data():
    
    # Step 5 - Get Lap Data

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_lap_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/laps/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                        delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
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
    
    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_pitstop_data():

    # Step 6 - Get Pitstops Data

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_pitstop_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/pitstops/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                        delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_pitstops_endpoint()
        print(f"Downloading Pitstop Data for Driver: {first_name} {family_name} for Year: {f1_year} & Round: {f1_round}")

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
    
    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_driverstandings_data():

    fn_name = 'get_driverstandings_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/driverstandings/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
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

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_constructorstandings_data():

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_constructorstandings_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/constructorstandings/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_constructorstandings_endpoint()
        print(f"Downloading Constructor Standings for Year: {f1_year} & Round: {f1_round}")

        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:
        
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_constructor_standings_dataframe()
            
            if results_df.shape == (0, 0):  
                    print("The DataFrame is empty!")
            else:
                #Write data to a delta lake table
                #print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/constructorstandings/', mode="append")
        else:
            print(f"Data not avaliable via the API for Constructor Standings: Year: {f1_year} & Round: {f1_round}")

            # Sleep so the API doesnt block our request
        time.sleep(2)

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_circuits_data():

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_circuits_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/circuits/')

    result = db.execute_query(f"SELECT distinct b.season FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year in season_dates_list:
        f1_year = f1_year[0]
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=0,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_circuits_endpoint()
        print(f"Downloading Circuit data for Year: {f1_year}")

        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:
        
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_circuits_dataframe()
            
            if results_df.shape == (0, 0):  
                    print("The DataFrame is empty!")
            else:
                #Write data to a delta lake table
                #print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/circuits/', mode="append")
        else:
            print(f"Data not avaliable via the API for Circuits: Year: {f1_year}")

            # Sleep so the API doesnt block our request
        time.sleep(2)

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_qualifying_data():

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_qualifying_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/qualifying/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_qualifying_endpoint()
        print(f"Downloading Qualifying Data for Year: {f1_year} & Round: {f1_round}")

        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:
        
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_qualifying_dataframe()
            
            if results_df.shape == (0, 0):  
                    print("The DataFrame is empty!")
            else:
                #Write data to a delta lake table
                #print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/qualifying/', mode="append")
        else:
            print(f"Data not avaliable via the API for Qualifying Data: Year: {f1_year} & Round: {f1_round}")

            # Sleep so the API doesnt block our request
        time.sleep(2)

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

def get_sprint_data():

    # Check the checkpoint file to see if this function completed
    fn_name = 'get_sprint_data'
    checkpoint = hp.load_function_checkpoint()
    if checkpoint.get(fn_name):
        print(f"{fn_name} already completed. Skipping.")
        return
    # If its not completed then remove directory from landing zone
    hp.clear_directory('./landing_zone/sprint/')

    result = db.execute_query(f"SELECT distinct b.season,b.round FROM delta_scan('./landing_zone/drivers/') a INNER JOIN\
                    delta_scan('./landing_zone/races') b on a.season = b.season WHERE a.driverid = '{driverid}'") 
    season_dates_list = result.values.tolist()

    for f1_year, f1_round in season_dates_list:
        endpoint_location = ap.APIEndpoints(base_url=api_url, year=f1_year, limit=300, round=f1_round,driverid=driverid, offset=0)
        endpoint = endpoint_location.get_sprint_endpoint()
        print(f"Downloading Sprint Data for Year: {f1_year} & Round: {f1_round}")

        data = api_client.fetch_data(endpoint=endpoint)

        if data is not None:
        
            parser_data = parser.JSONPolarsParser(data)
            results_df = parser_data.get_sprint_results_dataframe()
            
            if results_df.shape == (0, 0):  
                    print("The DataFrame is empty!")
            else:
                #Write data to a delta lake table
                #print("Writing to delta lake table")
                results_df.write_delta('./landing_zone/sprint/', mode="append")
        else:
            print(f"Data not avaliable via the API for Sprint Data: Year: {f1_year} & Round: {f1_round}")

            # Sleep so the API doesnt block our request
        time.sleep(2)

    # Save to checkpoint file
    hp.save_function_checkpoint(fn_name)
    print(f"{fn_name} completed.")

logging.info("Starting F1 analysis...")

checkpoint_path = 'checkpoints/function_checkpoint.json'
if os.path.exists(checkpoint_path):
    print('Using checkpoint file to restart from failure...')
else:
    print('Removing landing_zone directory...')
    hp.cleanup()

# Get all seasons for the driver
get_season_data()
get_driver_data()
get_races_data()
get_results_data()
get_lap_data()
get_pitstop_data()
get_driverstandings_data()
get_constructorstandings_data()
get_circuits_data()
get_qualifying_data()
get_sprint_data()

# Remove checkpoint file if script completes
checkpoint_path = 'checkpoints/function_checkpoint.json'
if os.path.exists(checkpoint_path):
    os.remove(checkpoint_path)
    print("Checkpoint file removed — script completed successfully.")