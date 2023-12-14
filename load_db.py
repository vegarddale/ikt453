import pandas as pd
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
import os


print("loading mysql odb...")

load_dotenv()

db_username = os.getenv("MYSQL_DB_USERNAME")
db_password = os.getenv("MYSQL_DB_PASSWORD")
db_name = os.getenv("MYSQL_DB_NAME")

data = pd.read_excel("globalterrorismdb_0522dist.xlsx", nrows=1000)

engine = create_engine(
    "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
)

table_name = "terrorist_table"

data.to_sql(table_name, con=engine, index=False, if_exists='replace')

engine.dispose()
print("mysql odb loaded")
