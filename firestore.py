from flask import render_template
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Use a service account.
cred = credentials.Certificate(
    "./creds/intelligent-data-mngt-firebase-adminsdk-aw9xp-91e3a22110.json"
)

app = firebase_admin.initialize_app(cred)

db = firestore.client()


def firestore_queries():
    doc_ref = db.collection("countries").document("Albania")

    doc = doc_ref.get()
    if doc.exists:
        print(f"Document data: {doc.to_dict()}")
    else:
        print("No such document!")

    city_dict = doc.to_dict()
    return render_template("firebase_data.html", dict=city_dict)
