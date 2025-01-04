import requests

class APIClient:
    """
    A class to interact with an API, download data, and store it in memory.
    """

    def __init__(self, base_url):
        """
        Initialize the API client.
        :param base_url: The base URL of the API
        """
        self.base_url = base_url
        self.data = None

    def fetch_data(self, endpoint, params=None, headers=None):
        """
        Fetch data from the API.
        :param endpoint: The specific API endpoint to call
        :param params: Optional dictionary of query parameters
        :param headers: Optional dictionary of HTTP headers
        :return: The data fetched from the API (also stored in memory)
        """
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            self.data = response.json()  # Store data in memory
            print("Data successfully fetched and stored in memory.")
            return self.data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None

    def get_data(self):
        """
        Retrieve the data stored in memory.
        :return: The data in memory
        """
        if self.data is None:
            print("No data available. Fetch data from the API first.")
        return self.data

    def clear_data(self):
        """
        Clear the data stored in memory.
        """
        self.data = None
        print("Data in memory has been cleared.")