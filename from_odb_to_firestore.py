import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import re

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


def upload_df_to_firestore(df, collection, id_index, is_grouped=False, is_nested=False):
    if is_grouped:
        indices = df.index.tolist()

    headers = df.columns.tolist()

    value_list = df.values.tolist()

    data = {}
    for i, values in enumerate(value_list):
        if is_grouped:
            id = re.sub("[^a-zA-Z0-9-()\s]", "", str(indices[i]))
            if is_nested:
                db.collection(collection).document(str(indices[i][0])).set({})
                inner_data = {}
                for header, value in zip(headers, values):
                    inner_data[header] = value
                data[str(indices[i][1])] = inner_data
            else:
                db.collection(collection).document(id).set({})
                for header, value in zip(headers, values):
                    data = {header: value}
                    db.collection(collection).document(id).update(data)
        else:
            db.collection(collection).document(str(values[id_index])).set({})
            for header, value in zip(headers, values):
                data = {header: value}
                db.collection(collection).document(str(values[id_index])).update(data)
        if is_grouped and is_nested:
            # Setting nested data. If it is at the end of a country's years start with an empty dict
            db.collection(collection).document(str(indices[i][0])).update(data)
            if i < len(value_list) - 1 and str(indices[i][0]) != str(indices[i + 1][0]):
                data = {}


def transfer_data():
    engine = create_engine(
        f"mssql+pyodbc://{db_username}:{db_password}@{db_host_local}/{db_name}?"
        "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        "&authentication=SqlPassword"
    )
    # establish a database connection
    with engine.connect() as conn:
        # execute a sql query
        query = "SELECT * FROM Country"  # replace 'table_name' with your table's name
        df = pd.read_sql_query(sql=query, con=conn)
        df = df.replace("\t", "", regex=True)

    upload_df_to_firestore(df, "countries2", 0)

    """doc_ref = db.collection("countries").document("Albania")

    doc = doc_ref.get()
    if doc.exists:
        print(f"Document data: {doc.to_dict()}")
    else:
        print("No such document!")

    city_dict = doc.to_dict()
    return render_template("firebase_data.html", dict=city_dict)"""


if __name__ == "__main__":
    transfer_data()
