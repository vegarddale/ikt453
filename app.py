
from flask import Flask

app = Flask("test_app")

@app.route('/')
def hello():
    return "Hello, Flask!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
