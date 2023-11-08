from flask import Flask
from load_data import data

app = Flask("Terrorist Database")


@app.route("/")
def hello():
    return "Hello, Flask!"


@app.route("/data")
def data_route():
    return data()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
