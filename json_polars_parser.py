import polars as pl

class JSONPolarsParser:

    """
    A class to shred a response in JSON and return a neatly formatted polars dataframe
    """

    def __init__(self, data):
        """
        Initialize the Parser
        """
        self.data = data

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
