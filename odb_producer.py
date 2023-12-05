from mysql.connector import Error
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import re

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host_local = os.getenv("DB_HOST_LOCAL")


def odb_producer():
    odb_aggregate_query = text("SELECT TOP (10) * FROM FactTab001")

    producer = KafkaProducer(bootstrap_servers="localhost:29092", api_version=(2, 0, 2))

    try:
        engine = create_engine(
            f"mssql+pyodbc://{db_username}:{db_password}@{db_host_local}/{db_name}?"
            "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
            "&authentication=SqlPassword"
        )

        with engine.connect() as connection:
            result = connection.execute(odb_aggregate_query)
            aggr_tuples = result.fetchall()
            connection.close()

        for tuple in aggr_tuples:
            in_string = "".join(str(tuple)).strip("()")
            in_string = re.sub("'", "", in_string)
            producer.send("AggrData", in_string.encode())
            print("\nProduced aggregated tuple: {}".format(tuple))

    except Error as e:
        print(e)

    """finally:
        if odb_conn is not None and odb_conn.is_connected():
            odb_cursor.close()
            odb_conn.close()"""


if __name__ == "__main__":
    odb_producer()
