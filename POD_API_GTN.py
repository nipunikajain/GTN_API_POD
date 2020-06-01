"""
POD Data pull for Shipments from GTNexus
------------------------------------------

Pulling the POD data from GTNexus with the help of App Express Rest API calling

@author:Nipunika Jain
"""

import requests
import json
import csv
import urllib
from json import dumps, loads, load
from requests import post
from requests import get
import pandas as pd
import time
import io
from datetime import datetime
from pandas import DataFrame
from flask import Flask, jsonify

# from API_Flask import f_steps


# Part1: Fetching the ShipmentID data from API
# Getting the Auth Token
url = "https://network.infornexus.com/rest/3.1?dataKey=86fe89ad5a82876c208e1133166b8c0e4f67c7fd"
payload = {}
headers = {
    'Authorization': 'Basic bmlwdW5pa2EuamFpbkBlY29sYWIuY29tOlNha3RsYWRraUA5OTk=',
    'Content-Type': 'application/json'
}
response = requests.request("GET", url, headers=headers, data=payload)
var = response.headers['Authorization']

# Getting the Future ID
url1 = "https://network.infornexus.com/rest/3.1/Report/124854786579/execute?dataKey=86fe89ad5a82876c208e1133166b8c0e4f67c7fd"
headers1 = {
    'Authorization': var,
    'Content-Type': 'application/json'
}
response1 = requests.request('POST', url1, headers=headers1, data=payload)
response1 = json.loads(response1.text)
futureID = response1['execute']['futureId']

# Getting the actual Report Data
url2 = "https://network.infornexus.com/rest/3.1/Report/124854786579/poll"
headers2 = {
    'Authorization': var,
    'Content-Encoding': 'gzip',
    'Content-Type': 'application/json'
}

param = {
    'dataKey': '86fe89ad5a82876c208e1133166b8c0e4f67c7fd',
    'futureId': futureID,
    'format': 'csv'
}
response2 = requests.request('GET', url2, params=param, headers=headers2)
print(response2.status_code)

rawData = pd.DataFrame()
if response2.status_code == 202:
    message = "Error: Status Code:" + str(response2.status_code) + " Message:" + response2.text
    print("Get Status from " + str(url2))
    _retryInterval = 10

    # pooling loop to check request status every 10 sec.
    while True:
        resp = requests.request('GET', url2, params=param, headers=headers2)
        _pollstatus = int(resp.status_code)
        if _pollstatus == 200:
            print("Status:" + str(resp.status_code))
            # print(resp.text.encode('utf8'))
            break
        else:
            print("Status:" + str(resp.status_code))
        time.sleep(_retryInterval)

    if _pollstatus == 200:
        urlData = resp.content
        rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
        # print(rawData)
        rawData = rawData.fillna(value=0)

if response2.status_code == 200:
    urlData = response2.content
    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
    '''
    Extracting ShipmentIDs from rawData dataframe
    '''
# print(rawData.head())
Ship_IDs = rawData['Shipment ID']

Ship_IDs = Ship_IDs.drop_duplicates()
rawData = rawData.drop_duplicates()

# Creating a CSV file in local directory with today's date
df = pd.DataFrame(data=None, columns=['Shipment ID', 'DocumentType', 'CreateTimestamp',
                                      'Modifytimestamp', 'Carrier', 'AttachUID', 'Attach_Name',
                                      'Attach_Link']
                  )
df.to_csv('C:\\Users\\jainni1\\OneDrive - Ecolab\\Documents\\My_Projects\\GT_Nexus_API\\API_Data.csv', mode='w')


def iteration(x):
    GTN_url = "https://network.infornexus.com/rest/3.1/ASNType/query"
    GTN_payload = {}
    GTN_headers = {'Authorization': 'Basic bmlwdW5pa2EuamFpbkBlY29sYWIuY29tOlNha3RsYWRraUA5OTk=',
                   'Content-Type': 'application/json'
                   }
    GTN_param = {'dataKey': '86fe89ad5a82876c208e1133166b8c0e4f67c7fd',
                 'oql': "ShipmentID=" + "'{}'".format(x)
                 }
    GTN_response = requests.request("GET", GTN_url, headers=GTN_headers, params=GTN_param, data=GTN_payload)
    # print(response.text.encode('utf8'))
    GTN_response = json.loads(GTN_response.text)
    var1 = GTN_response['result']
    uid = var1[0]['__metadata']['uid']
    ShipmentID = var1[0]['ShipmentID']
    Carrier = var1[0]['Carrier']['Name']

    GTN_url2 = "https://network.infornexus.com/rest/3.1/ASNType/" + uid + "/attachment"
    param2 = {'dataKey': '86fe89ad5a82876c208e1133166b8c0e4f67c7fd'
              }

    GTN_response2 = requests.request("GET", GTN_url2, headers=headers, params=param2, data=payload)
    # print(response2.text.encode('utf8'))
    GTN_response2 = json.loads(GTN_response2.text)
    # print(response2)
    if "result" in GTN_response2:
        result = GTN_response2['result']
        resultInfo = GTN_response2['resultInfo']
        doc_count = resultInfo['count']

        for i in range(0, doc_count):
            DocumentType = result[i]['documentType']
            Attachment = result[i]['attachment']
            CreateTimestamp = result[i]['__metadata']['createTimestamp']
            Modifytimestamp = result[i]['__metadata']['modifyTimestamp']
            AttachUID = Attachment[0]['attachmentUid']
            Attach_Name = Attachment[0]['name']
            Attach_Link = Attachment[0]['self']
            df2 = pd.DataFrame(
                [{'Shipment ID': ShipmentID, 'DocumentType': DocumentType, 'CreateTimestamp': CreateTimestamp,
                  'Modifytimestamp': Modifytimestamp, 'Carrier': Carrier, 'AttachUID': AttachUID,
                  'Attach_Name': Attach_Name,
                  'Attach_Link': Attach_Link
                  }])
            df2.to_csv('C:\\Users\\jainni1\\OneDrive - Ecolab\\Documents\\My_Projects\\GT_Nexus_API\\API_Data.csv',
                       mode='a',
                       header=False)

        else:
            print('No Documents for the Shipment', ShipmentID)


Ship_IDs.apply(iteration)

df1 = pd.read_csv('C:\\Users\\jainni1\\OneDrive - Ecolab\\Documents\\My_Projects\\GT_Nexus_API\\API_Data.csv')
Final_data = rawData.merge(df1, how='left', on='Shipment ID')

Final_data.to_csv('C:\\Users\\jainni1\\OneDrive - Ecolab\\Documents\\My_Projects\\GT_Nexus_API\\POD_Report.csv',
                  mode='w')

Final_data = Final_data.to_json(orient='records')
app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def test():
    return jsonify(Final_data)


app.run()
