import duckdb

class DatabaseConnection:
    """Class to manage database connection and queries."""

    def __init__(self, db_name):
        """
        Initialize the DatabaseConnection object.
        :param db_name: Name of the database file
        """
        self.db_name = db_name
        self.connection = None
        self.cursor = None

    def connect(self):
        """Connect to the database."""
        try:
            self.connection = duckdb.connect(database = self.db_name, read_only = False)
            self.cursor = self.connection.cursor()
            print(f"Connected to the database: {self.db_name}")
        except duckdb.Error as e:
            print(f"Error connecting to the database: {e}")

    def execute_query(self, query, parameters=None):
        """
        Execute a SQL query.
        :param query: SQL query string
        :param parameters: Optional tuple of parameters for the query
        :return: Query result or None
        """
        if not self.connection:
            print("No active database connection. Call connect() first.")
            return None
        try:
            if parameters:
                self.cursor.execute(query, parameters)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            return self.cursor.fetchall()
        except duckdb.Error as e:
            print(f"Error executing query: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print(f"Connection to {self.db_name} closed.")
        else:
            print("No active connection to close.")