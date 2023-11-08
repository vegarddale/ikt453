import pandas as pd
from sqlalchemy import create_engine
import pymysql

data = pd.read_excel("globalterrorismdb_0522dist.xlsx", nrows=100)

# Create a SQLAlchemy engine with pymysql as the driver
engine = create_engine("mysql+pymysql://user:Password123.@mysql-db/odb")

# Define the table name
table_name = "terrorist_table"

# Insert data into the table using the to_sql method
data.to_sql(table_name, con=engine, index=False)

# Dispose of the engine to close the database connection
engine.dispose()
