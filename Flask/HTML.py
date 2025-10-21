import sys, os
from flask import Flask, render_template, url_for, redirect, request

# Add the path to the parent directory to the sys.path list
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))


app = Flask(__name__)

# Route for the home page
@app.route('/')
def index():
    return render_template('index.html')

# Route to the attributes page
@app.route('/attributes')
def attributes():
    return render_template('attributes.html')


if __name__ == '__main__':
    app.run(debug=True)