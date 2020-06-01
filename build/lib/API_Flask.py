import requests
import json
import csv
import urllib
from json import dumps, loads, load
from requests import post
from requests import get
import pandas as pd
import numpy as np
import time
import io
from datetime import datetime
from pandas import DataFrame
from flask import Flask, jsonify

# from API_Flask import f_steps

df = pd.read_csv('C:\\Users\\jainni1\\OneDrive - Ecolab\\Documents\\My_Projects\\GT_Nexus_API\\POD_Report.csv')

df = df.to_json(orient='records')

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def test():
    return jsonify(df)


app.run()
