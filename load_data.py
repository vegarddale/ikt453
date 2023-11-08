import os
from dotenv import load_dotenv
from flask import Flask
from sqlalchemy import create_engine, inspect

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def data():
    engine = create_engine(
        "mysql+pymysql://" + db_username + ":" + db_password + "@mysql-db/" + db_name
    )
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    return str(table_names)
