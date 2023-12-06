from flask import Flask, render_template
from firestore import firestore_queries
from relational_sql import relational_sql_queries
from transfer_data import render_transfer_data
from insert_data import render_insert_data

app = Flask("GTDB")


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/relational_sql", methods=["GET", "POST"])
def relational_sql_route():
    return relational_sql_queries()


@app.route("/firestore", methods=["GET", "POST"])
def firestore_route():
    return firestore_queries()


@app.route("/transfer_data", methods=["GET", "POST"])
def transfer_data_route():
    return render_transfer_data()


@app.route("/insert_data")
def insert_data_route():
    return render_insert_data()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
