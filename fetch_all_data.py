import os
from dotenv import load_dotenv
from flask import render_template
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")


def fetch_data():
    engine = create_engine(
        f"mssql+pyodbc://{db_username}:{db_password}@{db_host}/{db_name}?"
        "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        "&authentication=SqlPassword"
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = "SELECT * FROM Country"  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
        df = df.replace("\t", "", regex=True)
    table = df.to_html(header=True)
    return render_template("data.html", table=table)
