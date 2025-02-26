class APIEndpoints:
    
    """
    A class to retrieve the api endpoints for jolpi
    """

    def __init__(self, base_url, year, limit, round):
        self.base_url = base_url
        self.year = year
        self.limit = limit
        self.round = round

    def get_seasons_endpoint(self):
        if self.limit:
            return f"seasons/?limit={self.limit}"
        else:
            return f"seasons/?limit=1"

    def get_constructorstandings_endpoint(self):
        if self.limit:
            return f"{self.year}/constructorstandings/?limit={self.limit}"
        else:
            return f"{self.year}/constructorstandings/?limit=1"

    def get_driverstandings_endpoint(self):
        if self.limit:
            return f"{self.year}/driverstandings/?limit={self.limit}"
        else:
            return f"{self.year}/driverstandings/?limit=1"
    
    def get_races_endpoint(self):
        if self.limit:
            return f"{self.year}/races/?limit={self.limit}"
        else:
            return f"{self.year}/races/?limit=1"
        
    def get_results_endpoint(self):
        if self.limit:
            return f"{self.year}/{self.round}/results/?limit={self.limit}"
        else:
            return f"{self.year}/{self.round}/results/?limit=1"
