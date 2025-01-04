class APIEndpoints:
    
    """
    A class to retrieve the api endpoints for jolpi
    """

    def __init__(self, base_url, year):
        self.base_url = base_url
        self.year = year

    def get_seasons_endpoint(self):
        return "seasons/?limit=100"
    
    def get_constructorstandings_endpoint(self):
        return f"{self.year}/constructorstandings/?limit=100"