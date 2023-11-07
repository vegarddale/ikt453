from flask import Flask
from sqlalchemy import create_engine, inspect

app = Flask("Terrorist Database")


@app.route("/")
def hello():
    return "Hello, Flask!"


@app.route("/data")
def data():
    engine = create_engine("mysql+pymysql://user:Password123.@mysql-db/odb")
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    return str(table_names)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
