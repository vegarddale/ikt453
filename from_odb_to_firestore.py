import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host_local = os.getenv("DB_HOST_LOCAL")


# Use a service account.
cred = credentials.Certificate(
    "./creds/intelligent-data-mngt-firebase-adminsdk-aw9xp-91e3a22110.json"
)

app = firebase_admin.initialize_app(cred)

db = firestore.client()


def transfer_data():
    engine = create_engine(
        f"mssql+pyodbc://{db_username}:{db_password}@{db_host_local}/{db_name}?"
        "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        "&authentication=SqlPassword"
    )
    # establish a database connection and fetch all fact tables
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text("EXEC SP_GET_all_FactTable"),
            conn,
        )

    # Retrieve all documents in the collection
    docs = db.collection("fact_table").stream()

    # Delete each document
    for doc in docs:
        doc.reference.delete()

    fact_df = df.head(50)  # Use only the 50 first rows
    fact_dicts = fact_df.to_dict("records")
    for fact_dict in fact_dicts:
        db.collection("fact_table").add(fact_dict)


if __name__ == "__main__":
    transfer_data()
