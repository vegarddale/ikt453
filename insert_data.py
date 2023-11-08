import os
from dotenv import load_dotenv
from flask import render_template
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def insert_data_func():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = "SELECT * FROM terrorist_table"  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    table = df.to_html()
    return render_template("insert_data.html")
