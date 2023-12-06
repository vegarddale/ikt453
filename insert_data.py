import os
from dotenv import load_dotenv
from flask import render_template
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def render_insert_data():
    return render_template("insert_data.html")
