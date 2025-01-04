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