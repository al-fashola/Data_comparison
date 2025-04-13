import pandas as pd
from sqlalchemy import create_engine
import jaydebeapi
import psycopg2
from psycopg2 import sql
import numpy as np
import datacompy




# Define MSSQL connection details
url2 = ""
username2 = ""
password2 = ""

# Redshift credentials and details
username_redshift = ''
password_redshift = ''
redshift_host = ''
redshift_port = '5439'
redshift_database = ''

# Construct the Redshift connection string and establish a connection
redshift_connection_string = (
    f"dbname={redshift_database} "
    f"user={username_redshift} "
    f"password={password_redshift} "
    f"host={redshift_host} "
    f"port={redshift_port}"
)

redshift_connection = psycopg2.connect(redshift_connection_string)

# Establish a connection
conn2 = jaydebeapi.connect(
    "net.sourceforge.jtds.jdbc.Driver",
    url2,
    [username2, password2],
    "",  # Replace with the actual path to the jTDS JAR file
)



# Create a cursor to execute SQL queries

cursor2 = conn2.cursor()


try:

    # Execute SQL queries using the Redshift connection and insert into a second DataFrame
    query_redshift = """
                        """
    
    # Convert results to a DataFrame
    AWS_Redshift = pd.read_sql(query_redshift, redshift_connection, index_col=None)
    
    
    # Example: Execute a query
    cursor2.execute(
        """
            """
    )

    # Fetch results
    results2 = cursor2.fetchall()

    
    # Convert results to a DataFrame
    mssql_df = pd.DataFrame(results2, columns=[desc[0] for desc in cursor2.description])     

    
    # Process the DataFrame as needed
    #print("MSSQL Records:")
    #print(mssql_df)


    #print("ActiDB Records:")
    #print(ActiDB_df)
    

finally:
    # Close the cursor and connection when done
    cursor2.close()
    conn2.close()
  
    
#Convert the data type of a single column
mssql_df = mssql_df.astype(str)
AWS_Redshift_str_df = AWS_Redshift.astype(str)


compare = datacompy.Compare(mssql_df,AWS_Redshift_str_df,join_columns=['id'])
print(compare.report())
with open('report_MSSQL_REDSHIFT_comp.txt', 'w') as f:
    f.write(compare.report())




