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

    def connect(self):
        """Connect to the database."""
        try:
            self.connection = duckdb.connect(database=self.db_name, read_only=False)
            print(f"Connected to the database: {self.db_name}")
        except duckdb.Error as e:
            print(f"Error connecting to the database: {e}")

    def register_dataframe(self, table_name, df):
        """
        Register a Polars DataFrame as a temporary table in DuckDB.
        :param table_name: Name of the temporary table
        :param df: Polars DataFrame to register
        """
        if not self.connection:
            print("No active database connection. Call connect() first.")
            return
        try:
            self.connection.register(table_name, df)
            print(f"Registered DataFrame as table: {table_name}")
        except duckdb.Error as e:
            print(f"Error registering DataFrame: {e}")

    def create_table_from_dataframe(self, table_name, df):
        """
        Create a permanent table in DuckDB from a Polars DataFrame.
        :param table_name: Name of the DuckDB table to create
        :param df: Polars DataFrame to insert into the table
        """
        if not self.connection:
            print("No active database connection. Call connect() first.")
            return
        try:
            # Register the DataFrame as a temporary table
            temp_table = "temp_table"
            self.register_dataframe(temp_table, df)

            # Create a permanent table from the temporary table
            self.connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {temp_table}")
            print(f"Permanent table '{table_name}' created successfully.")
        except duckdb.Error as e:
            print(f"Error creating table: {e}")

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
                result = self.connection.execute(query, parameters).fetch_df()
            else:
                result = self.connection.execute(query).fetch_df()
            return result
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