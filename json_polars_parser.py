import polars as pl
from json_shredder import CSVShredder


class JSONPolarsParser:

    """
    A class to shred a response in JSON and return a neatly formatted polars dataframe
    """

    def __init__(self, data, config_dir="configs"):        
        """
        Initialize the Parser
        """
        self.data = data
        self.shredder = CSVShredder(config_dir)

    def get_season_dataframe(self):
        
        df = pl.DataFrame(self.data["MRData"]["SeasonTable"]["Seasons"])
        return df
    
    def get_constructorstandings_dataframe(self):
        
        standings_lists = self.data["MRData"]["StandingsTable"]['StandingsLists']
        constructor_standings = []
        for standings_list in standings_lists:
            for standing in standings_list["ConstructorStandings"]:
                constructor_standings.append({
                    "season": standings_list["season"],
                    "round": standings_list["round"],
                    "position": standing.get("position"),
                    "positionText": standing["positionText"],
                    "points": standing["points"],
                    "wins": standing["wins"],
                    "constructorId": standing["Constructor"]["constructorId"],
                    "constructorName": standing["Constructor"]["name"],
                    "constructorNationality": standing["Constructor"]["nationality"],
                    "constructorUrl": standing["Constructor"]["url"],
                })

        df = pl.DataFrame(constructor_standings)
        return df
    
    def get_constructor_standings_dataframes(self):
        return self.shredder.shred(self.data, "constructor_standings")
    
    def get_driverstandings_dataframe(self):

        # Step 1: Extract Driver Standings from all StandingsLists
        standings_lists = self.data["MRData"]["StandingsTable"]["StandingsLists"]
        data = []

        for standings_list in standings_lists:
            for entry in standings_list["DriverStandings"]:
                driver = entry["Driver"]
                constructors = entry["Constructors"]
                
                # For multiple constructors, create one row per constructor
                for constructor in constructors:
                    data.append({
                        "season": standings_list["season"],
                        "round": standings_list["round"],
                        "position": entry.get("position", None),
                        "positionText": entry["positionText"],
                        "points": entry["points"],
                        "wins": entry["wins"],
                        "driverId": driver["driverId"],
                        "givenName": driver["givenName"],
                        "familyName": driver["familyName"],
                        "dateOfBirth": driver["dateOfBirth"],
                        "driverNationality": driver["nationality"],
                        "constructorId": constructor["constructorId"],
                        "constructorName": constructor["name"],
                        "constructorNationality": constructor["nationality"]
                    })

        # Step 2: Convert to Polars DataFrame
        df = pl.DataFrame(data)

        # Step 3: Display the DataFrame
        return df

    def get_races_dataframe(self):

        """
        This extracts the values from the JSON feed. 
        We convert None/NULL values to N/A for the Delta Table
        """

        races = self.data["MRData"]["RaceTable"]["Races"]
        race_data = []

        for race in races:
            race_data.append({
                "season": race.get("season", "N/A"),
                "round": race.get("round", "N/A"),
                "raceName": race.get("raceName", "N/A"),
                "circuitId": race["Circuit"].get("circuitId", "N/A"),
                "circuitName": race["Circuit"].get("circuitName", "N/A"),
                "lat": race["Circuit"]["Location"].get("lat", "N/A"),
                "long": race["Circuit"]["Location"].get("long", "N/A"),
                "locality": race["Circuit"]["Location"].get("locality", "N/A"),
                "country": race["Circuit"]["Location"].get("country", "N/A"),
                "raceDate": race.get("date", "N/A"),
                "raceTime": race.get("time", "N/A"),
                "fp1Date": race.get("FirstPractice", {}).get("date", "N/A"),
                "fp1Time": race.get("FirstPractice", {}).get("time", "N/A"),
                "fp2Date": race.get("SecondPractice", {}).get("date", "N/A"),
                "fp2Time": race.get("SecondPractice", {}).get("time", "N/A"),
                "fp3Date": race.get("ThirdPractice", {}).get("date", "N/A"),
                "fp3Time": race.get("ThirdPractice", {}).get("time", "N/A"),
                "qualifyingDate": race.get("Qualifying", {}).get("date", "N/A"),
                "qualifyingTime": race.get("Qualifying", {}).get("time", "N/A"),
                "sprintDate": race.get("Sprint", {}).get("date", "N/A"),
                "sprintTime": race.get("Sprint", {}).get("time", "N/A"),
            })

        df = pl.DataFrame(race_data)
        return df

    def get_results_dataframe(self):
       
        races = self.data["MRData"]["RaceTable"]["Races"]
        results_data = []

        for race in races:
            for result in race.get("Results", []):
                results_data.append({
                    "season": race.get("season", "N/A"),
                    "round": race.get("round", "N/A"),
                    "raceName": race.get("raceName", "N/A"),
                    "circuitId": race["Circuit"].get("circuitId", "N/A"),
                    "circuitName": race["Circuit"].get("circuitName", "N/A"),
                    "date": race.get("date", "N/A"),
                    "time": race.get("time", "N/A"),
                    "driverId": result["Driver"].get("driverId", "N/A"),
                    "driverName": f'{result["Driver"].get("givenName", "N/A")} {result["Driver"].get("familyName", "N/A")}',
                    "constructorId": result["Constructor"].get("constructorId", "N/A"),
                    "constructorName": result["Constructor"].get("name", "N/A"),
                    "grid": result.get("grid", "N/A"),
                    "position": result.get("position", "N/A"),
                    "points": result.get("points", "N/A"),
                    "laps": result.get("laps", "N/A"),
                    "status": result.get("status", "N/A"),
                    "time": result.get("Time", {}).get("time", "N/A"),
                    "fastestLap": result.get("FastestLap", {}).get("lap", "N/A"),
                    "fastestLapTime": result.get("FastestLap", {}).get("Time", {}).get("time", "N/A"),
                    "averageSpeed": result.get("FastestLap", {}).get("AverageSpeed", {}).get("speed", "N/A"),
                })

        df = pl.DataFrame(results_data)
        return df
    
    def get_lap_times_dataframe(self):
        """
        Convert lap timing JSON data into a Polars DataFrame.
        Ensures that missing values are replaced with "N/A".
        """
        races = self.data["MRData"]["RaceTable"]["Races"]
        lap_data = []

        for race in races:
            race_info = {
                "season": race.get("season", "N/A"),
                "round": race.get("round", "N/A"),
                "raceName": race.get("raceName", "N/A"),
                "circuitId": race["Circuit"].get("circuitId", "N/A"),
                "circuitName": race["Circuit"].get("circuitName", "N/A"),
                "raceDate": race.get("date", "N/A"),
            }
            
            for lap in race.get("Laps", []):
                lap_number = lap.get("number", "N/A")
                for timing in lap.get("Timings", []):
                    lap_data.append({
                        **race_info,  # Merge race-level data
                        "lapNumber": lap_number,
                        "driverId": timing.get("driverId", "N/A"),
                        "position": timing.get("position", "N/A"),
                        "lapTime": timing.get("time", "N/A"),
                    })

        df = pl.DataFrame(lap_data)
        return df
        
    def get_pitstops_dataframe(self):
        """Convert JSON data into a Polars DataFrame with pit stop details."""
        
        races = self.data["MRData"]["RaceTable"]["Races"]
        pitstop_data = []

        for race in races:
            for pitstop in race.get("PitStops", []):  # Extract pit stops or empty list
                pitstop_data.append({
                    "season": race.get("season", "N/A"),
                    "round": race.get("round", "N/A"),
                    "raceName": race.get("raceName", "N/A"),
                    "circuitId": race["Circuit"].get("circuitId", "N/A"),
                    "circuitName": race["Circuit"].get("circuitName", "N/A"),
                    "date": race.get("date", "N/A"),
                    "time": race.get("time", "N/A"),
                    "driverId": pitstop.get("driverId", "N/A"),
                    "lap": pitstop.get("lap", "N/A"),
                    "stop": pitstop.get("stop", "N/A"),
                    "pitTime": pitstop.get("time", "N/A"),
                    "duration": pitstop.get("duration", "N/A"),
                })

        # Convert to Polars DataFrame
        df = pl.DataFrame(pitstop_data)
        
        return df
    
    def get_driver_standings_dataframe(self):
        """Convert JSON driver standings data to a Polars DataFrame, handling missing values."""
        standings_lists = self.data["MRData"]["StandingsTable"].get("StandingsLists", [])
        driver_standings = []

        for standings_list in standings_lists:
            for standing in standings_list.get("DriverStandings", []):
                driver = standing.get("Driver", {})
                constructor = standing.get("Constructors", [{}])[0]  # Take the first constructor
                
                driver_standings.append({
                    "season": standings_list.get("season", "N/A"),
                    "round": standings_list.get("round", "N/A"),
                    "position": standing.get("position", "N/A"),
                    "positionText": standing.get("positionText", "N/A"),
                    "points": standing.get("points", "N/A"),
                    "wins": standing.get("wins", "N/A"),
                    "driverId": driver.get("driverId", "N/A"),
                    "driverName": f"{driver.get('givenName', 'N/A')} {driver.get('familyName', 'N/A')}",
                    "driverNationality": driver.get("nationality", "N/A"),
                    "driverDOB": driver.get("dateOfBirth", "N/A"),
                    "driverCode": driver.get("code", "N/A"),
                    "driverUrl": driver.get("url", "N/A"),
                    "constructorId": constructor.get("constructorId", "N/A"),
                    "constructorName": constructor.get("name", "N/A"),
                    "constructorNationality": constructor.get("nationality", "N/A"),
                    "constructorUrl": constructor.get("url", "N/A"),
                })

        # Convert list to Polars DataFrame
        df = pl.DataFrame(driver_standings)
        return df

    def get_constructor_standings_dataframe(self):
        """Converts JSON constructor standings into a Polars DataFrame with 'N/A' for null values."""
    
        standings_data = []
        
        # Navigate JSON structure to get Standings Lists
        standings_lists = self.data["MRData"]["StandingsTable"].get("StandingsLists", [])

        for standings in standings_lists:
            season = standings.get("season", "N/A")
            round_number = standings.get("round", "N/A")
            
            for constructor in standings.get("ConstructorStandings", []):
                standings_data.append({
                    "season": season,
                    "round": round_number,
                    "position": constructor.get("position", "N/A"),
                    "positionText": constructor.get("positionText", "N/A"),
                    "points": constructor.get("points", "N/A"),
                    "wins": constructor.get("wins", "N/A"),
                    "constructorId": constructor.get("Constructor", {}).get("constructorId", "N/A"),
                    "constructorName": constructor.get("Constructor", {}).get("name", "N/A"),
                    "constructorNationality": constructor.get("Constructor", {}).get("nationality", "N/A"),
                    "constructorUrl": constructor.get("Constructor", {}).get("url", "N/A"),
                })

        # Convert to Polars DataFrame
        df = pl.DataFrame(standings_data)

        return df
    
    def get_circuits_dataframe(self):
        """
        Convert circuit JSON data into a Polars DataFrame.
        Ensures that missing values are replaced with "N/A".
        """
        circuits = self.data["MRData"]["CircuitTable"]["Circuits"]
        circuit_data = []

        for circuit in circuits:
            location = circuit.get("Location", {})
            circuit_data.append({
                "circuitId": circuit.get("circuitId", "N/A"),
                "circuitName": circuit.get("circuitName", "N/A"),
                "url": circuit.get("url", "N/A"),
                "locality": location.get("locality", "N/A"),
                "country": location.get("country", "N/A"),
                "latitude": location.get("lat", "N/A"),
                "longitude": location.get("long", "N/A"),
            })

        df = pl.DataFrame(circuit_data)
        return df
    
    def get_qualifying_dataframe(self):
        """
        Convert qualifying JSON data into a Polars DataFrame.
        Ensures that missing values are replaced with "N/A".
        """
        races = self.data["MRData"]["RaceTable"]["Races"]
        qualifying_data = []

        for race in races:
            race_info = {
                "season": race.get("season", "N/A"),
                "round": race.get("round", "N/A"),
                "raceName": race.get("raceName", "N/A"),
                "circuitId": race["Circuit"].get("circuitId", "N/A"),
                "circuitName": race["Circuit"].get("circuitName", "N/A"),
                "raceDate": race.get("date", "N/A"),
            }

            for result in race.get("QualifyingResults", []):
                qualifying_data.append({
                    **race_info,
                    "driverId": result.get("Driver", {}).get("driverId", "N/A"),
                    "code": result.get("Driver", {}).get("code", "N/A"),
                    "givenName": result.get("Driver", {}).get("givenName", "N/A"),
                    "familyName": result.get("Driver", {}).get("familyName", "N/A"),
                    "constructor": result.get("Constructor", {}).get("name", "N/A"),
                    "constructorId": result.get("Constructor", {}).get("constructorId", "N/A"),
                    "position": result.get("position", "N/A"),
                    "Q1": result.get("Q1", "N/A"),
                    "Q2": result.get("Q2", "N/A"),
                    "Q3": result.get("Q3", "N/A"),
                })

        return pl.DataFrame(qualifying_data)

    def get_sprint_results_dataframe(self):
        """
        Convert sprint race JSON data into a Polars DataFrame.
        Ensures that missing values are replaced with "N/A".
        """
        races = self.data["MRData"]["RaceTable"]["Races"]
        sprint_data = []

        for race in races:
            race_info = {
                "season": race.get("season", "N/A"),
                "round": race.get("round", "N/A"),
                "raceName": race.get("raceName", "N/A"),
                "circuitId": race.get("Circuit", {}).get("circuitId", "N/A"),
                "circuitName": race.get("Circuit", {}).get("circuitName", "N/A"),
                "raceDate": race.get("date", "N/A"),
                "raceTime": race.get("time", "N/A"),
            }

            for result in race.get("SprintResults", []):
                driver = result.get("Driver", {})
                constructor = result.get("Constructor", {})
                time_info = result.get("Time", {})
                fastest_lap = result.get("FastestLap", {})

                sprint_data.append({
                    **race_info,
                    "driverId": driver.get("driverId", "N/A"),
                    "code": driver.get("code", "N/A"),
                    "givenName": driver.get("givenName", "N/A"),
                    "familyName": driver.get("familyName", "N/A"),
                    "constructor": constructor.get("name", "N/A"),
                    "constructorId": constructor.get("constructorId", "N/A"),
                    "grid": result.get("grid", "N/A"),
                    "position": result.get("position", "N/A"),
                    "positionText": result.get("positionText", "N/A"),
                    "points": result.get("points", "N/A"),
                    "laps": result.get("laps", "N/A"),
                    "status": result.get("status", "N/A"),
                    "sprintTime": time_info.get("time", "N/A"),
                    "sprintTimeMillis": time_info.get("millis", "N/A"),
                    "fastestLap": fastest_lap.get("lap", "N/A"),
                    "fastestLapTime": fastest_lap.get("Time", {}).get("time", "N/A"),
                })

        return pl.DataFrame(sprint_data)