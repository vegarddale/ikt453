from flask import Flask, render_template
from fetch_all_data import fetch_data
from fetch_data_by_year import fetch_yearly_data
from flask import request

app = Flask("Terrorist Database")


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/all_data")
def data_route():
    return fetch_data()


@app.route("/yearly_data", methods=["GET", "POST"])
def yearly_data_route():
    return fetch_yearly_data()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
