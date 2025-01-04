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