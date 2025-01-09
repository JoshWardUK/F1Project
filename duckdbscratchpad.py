import database_connection as dc

db = dc.DatabaseConnection("F1Data.db")

# Connect to the database
db.connect()

df = db.execute_query(f"SELECT DISTINCT Season FROM DRIVERS WHERE givenName = 'Lewis' AND familyName = 'Hamilton' ORDER BY SEASON ASC")

print(df.to_string())  # Full display