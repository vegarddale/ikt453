from flask import Flask, render_template
from firestore import firestore_queries
from relational_sql import relational_sql_queries

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
