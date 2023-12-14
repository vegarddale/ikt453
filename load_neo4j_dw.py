import os
from dotenv import load_dotenv
from flask import render_template
from sqlalchemy import create_engine
from neo4j import GraphDatabase
import pandas as pd

print("loading neo4j dw ...")

load_dotenv()

db_username = os.getenv("MYSQL_DB_USERNAME")
db_password = os.getenv("MYSQL_DB_PASSWORD")
db_name = os.getenv("MYSQL_DB_NAME")

row_limit = 500


def fetch_incidents_fact_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            attacktype1 as attack_id,
            city as city_id,
            iday, 
            imonth, 
            iyear,
            COUNT(*) AS nof_incidents,
            targtype1 as target_id
        FROM
            terrorist_table
        GROUP BY
            attacktype1, city, iday, imonth, iyear, targtype1
        ORDER BY
            nof_incidents DESC
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

#modify the query to select and aggregate instead of using pandas?
uri = 'neo4j://192.168.10.163:7687'  # replace with your Neo4j server URI
user = 'neo4j'
password = 'Password123.' # os env

def create_incidents_fact(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                # Extract data from the row
                # incident_id = row.Index  # You mentioned you don't want to include this
                attacktype_id = row['attack_id']
                city_id = row['city_id']
                day = row["iday"],
                month = row["imonth"],
                year = row["iyear"],
                nof_incidents = row['nof_incidents']
                target_id = row['target_id']

                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (incident:incident_fact {{"
                    f"attacktype_id: {attacktype_id}, "
                    f'city_id: "{city_id}", '
                    f"day: {day[0]}, "
                    f"month: {month[0]}, "
                    f"year: {year[0]}, "
                    f"nof_incidents: {nof_incidents}, "
                    f"target_id: {target_id}"
                    f"}})"
                )
                session.run(query)

# Fetch data from MySQL
df = fetch_incidents_fact_data()

# Create incidents in Neo4j without relationships
create_incidents_fact(df)
print("successfully loaded incidents_fact")

def fetch_time_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT DISTINCT 
            iday,
            imonth, 
            iyear
        FROM
            terrorist_table
        LIMIT {row_limit};
        
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_time_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                day = row["iday"],
                month = row["imonth"],
                year = row["iyear"],
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (time:time_dim {{"
                    f"day: {day[0]}, "
                    f"month: {month[0]}, "
                    f"year: {year[0]} "
                    f"}})"
                )
                session.run(query)                

df = fetch_time_dim_data()
create_time_dim(df)
print("successfully loaded time dimension")

def fetch_target_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            targtype1,
            targtype1_txt
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_target_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                target_id = row["targtype1"],
                targtype1_txt = row["targtype1_txt"]
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (targettype:target_dim {{"
                    f"target_id: {target_id[0]}, "
                    f'targtype1_txt: "{targtype1_txt}" '
                    f"}})"
                )
                session.run(query)  

df = fetch_target_dim_data()
create_target_dim(df)
print("successfully loaded target dimension")

def fetch_region_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            region,
            region_txt
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_region_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                region_id = row["region"],
                region_txt = row["region_txt"]
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (region:region_dim {{"
                    f"region_id: {region_id[0]}, "
                    f'region_txt: "{region_txt}" '
                    f"}})"
                )
                session.run(query)  

df = fetch_region_dim_data()
create_region_dim(df)
print("successfully loaded region dimension")

def fetch_country_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            country,
            region,
            country_txt
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_country_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                country_id = row["country"],
                region_id = row["region"],
                country_txt = row["country_txt"]
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (country:country_dim {{"
                    f"country_id: {country_id[0]}, "
                    f"region_id: {region_id[0]}, "
                    f'country_txt: "{country_txt}" '
                    f"}})"
                )
                session.run(query)

df = fetch_country_dim_data()
create_country_dim(df)
print("successfully loaded country dimension")


def fetch_attacktype_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            attacktype1,
            attacktype1_txt
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_attacktype_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                attacktype_id = row["attacktype1"],
                attacktype1_txt = row["attacktype1_txt"]
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (attacktype:attack_type_dim {{"
                    f"attacktype_id: {attacktype_id[0]}, "
                    f'attacktype1_txt: "{attacktype1_txt}" '
                    f"}})"
                )
                session.run(query)

df = fetch_attacktype_dim_data()
create_attacktype_dim(df)
print("successfully loaded attacktype dimension")


def fetch_city_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT DISTINCT
            city,
            country
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_city_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                city_id = row["city"],
                country_id = row["country"]
                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (city:city_dim {{"
                    f'city_id: "{city_id[0]}", '
                    f"country_id: {country_id} " 
                    f"}})"
                )
                session.run(query)  

df = fetch_city_dim_data()

create_city_dim(df)

def create_relationships_incidents():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (incident:incident_fact) "
                "MATCH (time:time_dim) "
                "MATCH (targettype:target_dim) "
                "MATCH (region:region_dim) "
                "MATCH (city:city_dim) "
                "MATCH (attacktype:attack_type_dim) "
                "MATCH (country:country_dim) "
                "WHERE incident.day = time.day AND incident.month = time.month AND incident.year = time.year "
                "AND incident.target_id = targettype.target_id "
                "AND incident.city_id = city.city_id "
                "AND city.country_id = country.country_id "
                "AND country.region_id = region.region_id "
                "AND incident.attacktype_id = attacktype.attacktype_id "
                "MERGE (incident)-[:OCCURRED_IN_TIME]->(time) "
                "MERGE (incident)-[:TARGET]->(targettype) "
                "MERGE (country)-[:BELONGS_TO_REGION]->(region) "
                "MERGE (city)-[:BELONGS_TO_COUNTRY]->(country) "
                "MERGE (incident)-[:OCCURRED_IN_CITY]->(city) "
                "MERGE (incident)-[:INCIDENT_ATTACKTYPE]->(attacktype)"
            )
            session.run(query)


create_relationships_incidents()


            
def create_summary_table_q1():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (incident:incident_fact)-[:OCCURRED_IN_CITY]->(city:city_dim) "
                "MATCH (city)-[:BELONGS_TO_COUNTRY]->(country:country_dim)-[:BELONGS_TO_REGION]->(region:region_dim) "
                "WITH incident.target_id AS target_id, region.region_id AS region_id, incident.year AS year, SUM(incident.nof_incidents) AS total_nof_incidents "
                "CREATE (:summary_table_q1 {target_id: target_id, region_id: region_id, year: year, total_nof_incidents: total_nof_incidents})"
            )
            session.run(query)
            print("successfully loaded summary_table_q1")

create_summary_table_q1()

def create_relationships_summary_table_q1():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (q1:summary_table_q1) "
                "MATCH (targettype:target_dim) "
                "WHERE q1.target_id = targettype.target_id "
                "MERGE (q1)-[:Q1_TARGET]->(targettype)"
            )
            session.run(query)


create_relationships_summary_table_q1()

def create_summary_table_q2():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (incident:incident_fact)-[:OCCURRED_IN_CITY]->(city:city_dim) "
                "WITH incident.attacktype_id AS attacktype_id, city.city_id AS city_id, incident.year AS year, SUM(incident.nof_incidents) AS total_nof_incidents "
                "CREATE (:summary_table_q2 {attacktype_id: attacktype_id, city_id: city_id, year: year, total_nof_incidents: total_nof_incidents})"
            )
            session.run(query)
            print("successfully loaded summary_table_q2")

create_summary_table_q2()



def create_relationships_summary_table_q2():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (q2:summary_table_q2) "
                "MATCH (attacktype:attack_type_dim) "
                "WHERE q2.attacktype_id = attacktype.attacktype_id "
                "MERGE (q2)-[:Q2_ATTACKTYPE]->(attacktype)"
            )
            session.run(query)


create_relationships_summary_table_q2()


def create_summary_table_q4():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (incident:incident_fact), "
                "(city:city_dim)-[:BELONGS_TO_COUNTRY]->(country:country_dim) "
                "WITH incident.attacktype_id AS attacktype_id, country.country_id AS country_id, incident.year AS year, SUM(incident.nof_incidents) AS total_nof_incidents  "
                "CREATE (:summary_table_q4 {attacktype_id: attacktype_id, country_id: country_id, year: year, total_nof_incidents: total_nof_incidents})"
            )
            session.run(query)
            print("successfully loaded summary_table_q4")
            
create_summary_table_q4()


def fetch_kills_fact_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT
            gname,
            city,
            propextent,
            COUNT(*) AS nof_incidents,
            SUM(nkill) as nof_kills
        FROM
            terrorist_table
        GROUP BY
            gname, city, propextent
        ORDER BY
            nof_incidents DESC
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_kills_fact(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                # Extract data from the row
                # incident_id = row.Index  # You mentioned you don't want to include this
                gname_id = row['gname']
                city_id = row['city']
                propextent_id = row["propextent"],
                nof_incidents = row['nof_incidents']
                nof_kills = row['nof_kills']

                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (kills:kills_fact {{"
                    f'gname_id: "{gname_id}", '
                    f'city_id: "{city_id}", '
                    f"propextent_id: {propextent_id[0]}, "
                    f"nof_incidents: {nof_incidents}, "
                    f"nof_kills: {nof_kills} "
                    f"}})"
                )
                session.run(query)


df = fetch_kills_fact_data()
df = df.fillna(9999999)
create_kills_fact(df)
print("successfully loaded kills fact table")


def fetch_propextent_dim_data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = f'''
        SELECT DISTINCT
            propextent,
            propextent_txt
        FROM
            terrorist_table
        LIMIT {row_limit};
        '''  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
    return df

def create_propextent_dim(df):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                propextent_id = row["propextent"],
                propextent_txt = row["propextent_txt"],

                # Create nodes for incidents in Neo4j
                query = (
                    f"MERGE (propextent:propextent_dim {{"
                    f"propextent_id: {propextent_id[0]}, "
                    f'propextent_txt: "{propextent_txt[0]}" '
                    f"}})"
                )
                session.run(query)  

df = fetch_propextent_dim_data()
df = df.fillna(9999999)
create_propextent_dim(df)

def create_relationships_kills():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (kills:kills_fact) "
                "MATCH (city:city_dim) "
                "MATCH (country:country_dim) "
                "MATCH (propextent:propextent_dim) "
                "WHERE kills.city_id = city.city_id "
                "AND city.country_id = country.country_id "
                "AND kills.propextent_id = propextent.propextent_id "
                "MERGE (kills)-[:KILLS_IN_CITY]->(city) "
                "MERGE (kills)-[:KILLS_DAMAGE_INFO]->(propextent) "
                "MERGE (city)-[:BELONGS_TO_COUNTRY]->(country)"
            )
            session.run(query)


create_relationships_kills()

def create_summary_table_q5():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (kills:kills_fact) "
                "WITH kills.gname_id AS gname_id, kills.propextent_id AS propextent_id, SUM(kills.nof_incidents) AS total_nof_incidents "
                "CREATE (:summary_table_q5 {gname_id: gname_id, propextent_id: propextent_id, total_nof_incidents: total_nof_incidents})"
            )
            session.run(query)
            print("successfully loaded summary_table_q5")
            
create_summary_table_q5()

def create_relationships_summary_table_q5():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (q5:summary_table_q5) "
                "MATCH (propextent:propextent_dim) "
                "WHERE q5.propextent_id = propextent.propextent_id "
                "MERGE (q5)-[:Q5_KILLS_DAMAGE_INFO]->(propextent) "
            )
            session.run(query)


create_relationships_summary_table_q5()


def create_summary_table_q7():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (kills:kills_fact)-[:KILLS_IN_CITY]->(city:city_dim) "
                "WITH kills.gname_id AS gname_id, city.country_id AS country_id, SUM(kills.nof_incidents) AS total_nof_incidents, SUM(kills.nof_kills) AS total_nof_kills "
                "CREATE (:summary_table_q7 {gname_id: gname_id, country_id: country_id, total_nof_incidents: total_nof_incidents, total_nof_kills: total_nof_kills})"
            )
            session.run(query)
            print("successfully loaded summary_table_q7")
            
create_summary_table_q7()

def create_relationships_summary_table_q7():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (q7:summary_table_q7) "
                "MATCH (country:country_dim) "
                "WHERE q7.country_id = country.country_id "
                "MERGE (q7)-[:Q7_OCURRED_IN_COUNTRY]->(country) "
            )
            session.run(query)


create_relationships_summary_table_q7()


# MATCH (kills:kills_fact)-[:KILLS_IN_CITY]->(city:city_dim)-[:BELONGS_TO_COUNTRY]->(country:country_dim)-[:BELONGS_TO_REGION]->(region:region_dim)
# WITH kills.gname_id AS gname_id, region.region_id AS region_id, SUM(kills.nof_incidents) AS total_nof_incidents, SUM(kills.nof_kills) AS total_nof_kills
# CREATE (:summary_table_test_5 {gname_id: gname_id, region_id: region_id, total_nof_incidents: total_nof_incidents, total_nof_kills: total_nof_kills})
def create_summary_table_q8():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            query = (
                "MATCH (kills:kills_fact)-[:KILLS_IN_CITY]->(city:city_dim)-[:BELONGS_TO_COUNTRY]->(country:country_dim)-[:BELONGS_TO_REGION]->(region:region_dim) "
                "WITH kills.gname_id AS gname_id, region.region_id AS region_id, SUM(kills.nof_incidents) AS total_nof_incidents, SUM(kills.nof_kills) AS total_nof_kills "
                "CREATE (:summary_table_q8 {gname_id: gname_id, region_id: region_id, total_nof_incidents: total_nof_incidents, total_nof_kills: total_nof_kills})"
            )
            session.run(query)
            print("successfully loaded summary_table_q8")
            
create_summary_table_q8()

def create_relationships_summary_table_q8():
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # Match incidents and dimensions, and create relationships
            query = (
                "MATCH (q8:summary_table_q8)  "
                "MATCH (region:region_dim)  "
                "WHERE q8.region_id = region.region_id  "
                "MERGE (q8)-[:Q8_OCURRED_IN_REGION]->(region)  "
            )
            session.run(query)


create_relationships_summary_table_q8()


