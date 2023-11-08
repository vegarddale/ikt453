import os
from dotenv import load_dotenv
from flask import render_template, request
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def fetch_yearly_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = "SELECT * FROM terrorist_table"  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    years = sorted(df["iyear"].unique())

    # Default table is empty
    table = ""

    if request.method == "POST":
        selected_year = request.form.get("year")
        with engine.connect() as conn:
            query = text("SELECT * FROM terrorist_table WHERE iyear=:year")
            filtered_df = pd.read_sql_query(
                sql=query, con=conn, params={"year": selected_year}
            )
        table = filtered_df.to_html()
    return render_template("year_data.html", years=years, table=table)
