import pandas as pd
from sqlalchemy import create_engine
import jaydebeapi
import psycopg2
from psycopg2 import sql
import numpy as np
import datacompy

# Define the connection parameters for both databases
db1_config = {
    'host': '',
    'port': '5439',
    'dbname': '',
    'user': '',
    'password': ''
}

db2_config = {
    'host': '',
    'port': '5439',
    'dbname': '',
    'user': '',
    'password': ''
}

# Function to connect to Redshift and fetch data into a DataFrame
def fetch_data_from_redshift(config, query):
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        dbname=config['dbname'],
        user=config['user'],
        password=config['password']
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Example query to fetch data from both databases
query = """

"""

query1 = """

"""

# Fetch data from both Redshift databases
df1 = fetch_data_from_redshift(db1_config, query)
df2 = fetch_data_from_redshift(db2_config, query1)

df1=df1.astype(str)
df2=df2.astype(str)

# Create a datacompy comparison object
comparison = datacompy.Compare(
    df2, df1, 
    join_columns=['member_id']   # Specify the columns to compare
)


print(comparison.report())
with open('Report_comparison_1.txt', 'w') as f:
    f.write(comparison.report())