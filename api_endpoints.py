class APIEndpoints:
    
    """
    A class to retrieve the api endpoints for jolpi
    """

    def __init__(self, base_url):
        self.base_url = base_url

    def get_seasons_endpoint(self):
        return "seasons/?limit=100"