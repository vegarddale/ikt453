import os
from dotenv import load_dotenv
from flask import render_template, request
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_name_2 = os.getenv("DB_NAME_2")
db_host = os.getenv("DB_HOST_LOCAL")


def relational_sql_queries():
    engine = create_engine(
        f"mssql+pyodbc://{db_username}:{db_password}@{db_host}/{db_name}?"
        "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        "&authentication=SqlPassword"
    )
    engine_2 = create_engine(
        f"mssql+pyodbc://{db_username}:{db_password}@{db_host}/{db_name_2}?"
        "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        "&authentication=SqlPassword"
    )
    with engine.connect() as conn:
        # execute a sql query
        region_query = "SELECT TOP (1000) region, region_txt FROM FactTab001"
        region_df = pd.read_sql_query(sql=region_query, con=conn)

        target_query = "SELECT TOP (1000) targtype1, targtype1_txt FROM FactTab002"
        target_df = pd.read_sql_query(sql=target_query, con=conn)

        year_query = "SELECT TOP (1000) iyear FROM FactTab001"
        year_df = pd.read_sql_query(sql=year_query, con=conn)

        attack_query = "SELECT TOP (1000) attacktype1, attacktype1_txt FROM FactTab002"
        attack_df = pd.read_sql_query(sql=attack_query, con=conn)

        target_sub_query = (
            "SELECT TOP (1000) targsubtype1, targsubtype1_txt FROM FactTab002"
        )
        target_sub_df = pd.read_sql_query(sql=target_sub_query, con=conn)
        target_sub_df.dropna(how="any", inplace=True)  # Remove null values

        group_query = "SELECT TOP (1000) gname FROM FactTab002"
        group_df = pd.read_sql_query(sql=group_query, con=conn)

        weap_query = "SELECT TOP (1000) weapdetail FROM FactTab002"
        weap_df = pd.read_sql_query(sql=weap_query, con=conn)
        weap_df.dropna(how="any", inplace=True)  # Remove null values

        country_query = "SELECT TOP (1000) country, country_txt FROM FactTab001"
        country_df = pd.read_sql_query(sql=country_query, con=conn)

        fact3_query = "SELECT TOP (1000) propextent_txt, dbsource FROM FactTab003"
        fact3_df = pd.read_sql_query(sql=fact3_query, con=conn)
        fact3_df.dropna(how="any", inplace=True)  # Remove null values

    attack_types = sorted(attack_df["attacktype1_txt"].unique())
    regions = sorted(region_df["region_txt"].unique())
    target_types = sorted(target_df["targtype1_txt"].unique())
    years = sorted(year_df["iyear"].unique())
    target_sub_types = sorted(target_sub_df["targsubtype1_txt"].unique())
    groups = sorted(group_df["gname"].unique())
    countries = sorted(country_df["country_txt"].unique())
    damage_dones = sorted(fact3_df["propextent_txt"].unique())
    db_sources = sorted(fact3_df["dbsource"].unique())
    weap_details = sorted(weap_df["weapdetail"].unique())

    # Default table is empty
    table1 = ""
    table2 = ""
    table3 = ""
    table4 = ""
    table5 = ""
    table6 = ""
    table7 = ""
    table8 = ""
    table9 = ""
    table10 = ""
    query1_df = pd.DataFrame()
    query2_df = pd.DataFrame()
    query3_df = pd.DataFrame()
    query4_df = pd.DataFrame()
    query5_df = pd.DataFrame()
    query6_df = pd.DataFrame()
    query7_df = pd.DataFrame()
    query8_df = pd.DataFrame()
    query9_df = pd.DataFrame()
    query10_df = pd.DataFrame()

    if request.method == "POST":
        # Query 1
        selected_region = request.form.get("region")
        selected_target_type = request.form.get("target")
        selected_start_year = request.form.get("start_year")
        selected_end_year = request.form.get("end_year")

        if (
            selected_region
            and selected_target_type
            and selected_start_year
            and selected_end_year
        ):
            selected_region_id = region_df.loc[
                region_df["region_txt"] == selected_region, "region"
            ].values[0]

            selected_target_type_id = target_df.loc[
                target_df["targtype1_txt"] == selected_target_type, "targtype1"
            ].values[0]
            with engine.connect() as conn:
                query1_df = pd.read_sql_query(
                    text(
                        "EXEC SP_Region_YearDiff_TargetType_1 :ARegion_Id, :PYear_1, :Year_2, :Target_Type_Id"
                    ),
                    conn,
                    params={
                        "ARegion_Id": int(selected_region_id),
                        "PYear_1": int(selected_start_year),
                        "Year_2": int(selected_end_year),
                        "Target_Type_Id": int(selected_target_type_id),
                    },
                )

        # Query 2
        selected_region_q2 = request.form.get("region_q2")
        selected_years = request.form.get("years")
        selected_attack_type = request.form.get("attack")

        if selected_region_q2 and selected_years and selected_attack_type:
            selected_region_q2_id = region_df.loc[
                region_df["region_txt"] == selected_region_q2, "region"
            ].values[0]

            selected_attack_type_id = attack_df.loc[
                attack_df["attacktype1_txt"] == selected_attack_type, "attacktype1"
            ].values[0]
            with engine.connect() as conn:
                query2_df = pd.read_sql_query(
                    text(
                        "EXEC SP_GetCity_Area_AttackType_last_x_Years_2 :ARegion_Id, :HowManyYear, :Attack_Type_Id"
                    ),
                    conn,
                    params={
                        "ARegion_Id": int(selected_region_q2_id),
                        "HowManyYear": int(selected_years),
                        "Attack_Type_Id": int(selected_attack_type_id),
                    },
                )

        # Query 3
        selected_region_q3 = request.form.get("region_q3")
        selected_target_sub = request.form.get("target_sub_type")
        selected_group = request.form.get("group")

        if selected_region_q3 and selected_target_sub and selected_group:
            selected_region_q3_id = region_df.loc[
                region_df["region_txt"] == selected_region_q3, "region"
            ].values[0]

            selected_target_sub_id = target_sub_df.loc[
                target_sub_df["targsubtype1_txt"] == selected_target_sub,
                "targsubtype1",
            ].values[0]

            with engine.connect() as conn:
                query3_df = pd.read_sql_query(
                    text(
                        "EXEC SP_GetRegion_TargetType_BYTerrorGroup_3 :Region_Id, :Trgsubtype_Id, :Ganagname"
                    ),
                    conn,
                    params={
                        "Region_Id": int(selected_region_q3_id),
                        "Trgsubtype_Id": int(selected_target_sub_id),
                        "Ganagname": str(selected_group),
                    },
                )
        # Query 4
        selected_country = request.form.get("country")
        selected_weap_detail = request.form.get("weap")
        selected_start_year_q4 = request.form.get("start_year_q4")
        selected_end_year_q4 = request.form.get("start_end_q4")

        if (
            selected_country
            and selected_weap_detail
            and selected_start_year_q4
            and selected_end_year_q4
        ):
            selected_country_id = country_df.loc[
                country_df["country_txt"] == selected_country, "country"
            ].values[0]

            with engine.connect() as conn:
                query4_df = pd.read_sql_query(
                    text(
                        "EXEC SP_Get_Country_year_weapon_4 :Country_Id, :StartYear, :EndYear, :weapdetail"
                    ),
                    conn,
                    params={
                        "Country_Id": int(selected_country_id),
                        "StartYear": int(selected_start_year_q4),
                        "EndYear": int(selected_end_year_q4),
                        "weapdetail": str(selected_weap_detail),
                    },
                )
        # Query 5
        selected_db_source = request.form.get("db_source")
        selected_damage_done = request.form.get("damage_done")

        if selected_damage_done and selected_db_source:
            with engine.connect() as conn:
                query5_df = pd.read_sql_query(
                    text("EXEC SP_propextent_DBsource_5 :DBsource, :propextent_txt"),
                    conn,
                    params={
                        "DBsource": str(selected_db_source),
                        "propextent_txt": str(selected_damage_done),
                    },
                )
        # Query 6
        with engine_2.connect() as conn:
            query6_df = pd.read_sql_query(text("EXEC SP_Most_Active_gang"), conn)
            query6_df = query6_df.head(10)

        # Query 7
        with engine_2.connect() as conn:
            query7_df = pd.read_sql_query(text("SP_Most_dangerous_city"), conn)
            query7_df = query7_df.head(10)

        # Query 8
        with engine_2.connect() as conn:
            query8_df = pd.read_sql_query(text("SP_Most_dangerous_Year"), conn)
            query8_df = query8_df.head(10)

        # Query 9
        with engine_2.connect() as conn:
            query9_df = pd.read_sql_query(text("SP_Most_trageted_Units"), conn)
            query9_df = query9_df.head(10)

        # Query 10
        with engine_2.connect() as conn:
            query10_df = pd.read_sql_query(text("SP_Most_used_weapon"), conn)
            query10_df = query10_df.head(10)

        table1 = query1_df.to_html()
        table2 = query2_df.to_html()
        table3 = query3_df.to_html()
        table4 = query4_df.to_html()
        table5 = query5_df.to_html()
        table6 = query6_df.to_html()
        table7 = query7_df.to_html()
        table8 = query8_df.to_html()
        table9 = query9_df.to_html()
        table10 = query10_df.to_html()
    return render_template(
        "sql_queries.html",
        regions=regions,
        target_types=target_types,
        start_years=years,
        end_years=years,
        table1=table1,
        regions_q2=regions,
        attack_types=attack_types,
        table2=table2,
        regions_q3=regions,
        target_sub_types=target_sub_types,
        groups=groups,
        table3=table3,
        countries=countries,
        weaps=weap_details,
        start_years_q4=years,
        end_years_q4=years,
        table4=table4,
        damage_dones=damage_dones,
        db_sources=db_sources,
        table5=table5,
        table6=table6,
        table7=table7,
        table8=table8,
        table9=table9,
        table10=table10,
    )
