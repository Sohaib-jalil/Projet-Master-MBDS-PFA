import pandas as pd
import pyodbc

# Establish connection to SQL Server database
server = '192.168.11.122'
database = 'AUTOMOBILE'
username = 'sa'
password = 'sa'

# Create a connection string
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Establish a connection to SQL Server
conn = pyodbc.connect(conn_str)

# Create a cursor object
cursor = conn.cursor()

sql_query = """
    SELECT * FROM newClientPrediction
"""

cursor.execute(sql_query)

# Fetch data into a list of lists
data = [list(row) for row in cursor.fetchall()]

# Get column names
columns = [column[0] for column in cursor.description]

# Create DataFrame
df = pd.DataFrame(data, columns=columns)

# Save DataFrame to CSV
df.to_csv('newClientPredictionCategory.csv', index=False)
