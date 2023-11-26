import os
from dotenv import load_dotenv
from flask import render_template, request
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


'''def fetch_region_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = "SELECT * FROM terrorist_table"  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    regions = sorted(df["region_txt"].unique())
    target_types = sorted(df["targtype1_txt"].unique())
    years = sorted(df["iyear"].unique())

    # Default table is empty
    table = ""

    if request.method == "POST":
        selected_region = request.form.get("region")
        selected_target_type = request.form.get("target")
        selected_start_year = request.form.get("start_year")
        selected_end_year = request.form.get("end_year")
        with engine.connect() as conn:
            query = text(
                """
                SELECT * 
                FROM terrorist_table 
                WHERE region_txt=:region 
                AND targtype1_txt=:target 
                AND iyear BETWEEN :start_year AND :end_year
                """
            )

            filtered_df = pd.read_sql_query(
                sql=query,
                con=conn,
                params={
                    "region": selected_region,
                    "target": selected_target_type,
                    "start_year": selected_start_year,
                    "end_year": selected_end_year,
                },
            )
        table = filtered_df.to_html()
    return render_template(
        "region_data.html",
        regions=regions,
        target_types=target_types,
        start_years=years,
        end_years=years,
        table=table,
    )'''
