class APIEndpoints:
    
    """
    A class to retrieve the api endpoints for jolpi
    """

    def __init__(self, base_url, year, limit, round, driverid, offset):
        self.base_url = base_url
        self.year = year
        self.limit = limit
        self.round = round
        self.driverid = driverid
        self.offset = offset

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

    def get_driverstandings_year_endpoint(self):
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
            #return f"{self.year}/{self.round}/results/?limit={self.limit}"
            return f"{self.year}/results/?limit={self.limit}&offset={self.offset}"
        else:
            return f"{self.year}/results/?limit=1"
        
    def get_laps_endpoint(self):
        if self.limit:
            return f"{self.year}/{self.round}/drivers/{self.driverid}/laps/?limit={self.limit}"
        else:
            return f"{self.year}/{self.round}/drivers/{self.driverid}/laps/?limit=1"

    def get_pitstops_endpoint(self):
        if self.limit:
            return f"{self.year}/{self.round}/drivers/{self.driverid}/pitstops/?limit={self.limit}"
        else:
            return f"{self.year}/{self.round}/drivers/{self.driverid}/pitstops/?limit=1"
        
    def get_driverstandings_endpoint(self):
        if self.limit:
            return f"{self.year}/{self.round}/driverstandings/?limit={self.limit}"
        else:
            return f"{self.year}/{self.round}/driverstandings/?limit=1"
    
    def get_constructorstandings_endpoint(self):
        if self.limit:
            return f"{self.year}/{self.round}/constructorstandings/?limit={self.limit}"
        else:
            return f"{self.year}/{self.round}/constructorstandings/?limit=1"