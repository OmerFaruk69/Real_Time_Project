import csv 
import json 
import requests, json, os
from elasticsearch import Elasticsearch
from datetime import datetime
import argparse



parser = argparse.ArgumentParser(description='Example of conversion csv file into json file and load in elasticsearch')

parser.add_argument("-c", "--csvpath", action="store", type=str, dest="csvpath", default="dataset.csv",
                      help="csv file path")
parser.add_argument("-j", "--jsonpath", action="store", type=str, dest="jsonpath",
                      default="dataset.json", help="json file path")
parser.add_argument("-n", "--indice_name", action="store", type=str, dest="indice_name", default="project_final",
                      help="root_url to upload photos")
#parser.add_argument("-d", "--dir", action="store", type=str, dest="directory",
#                      default="")
parser.add_argument("-p", "--port", action="store", type=str, dest="port",
                      default="9200")
parser.add_argument("-k", "--key", action="store", type=str, dest="key",
                      default="Index")
x = parser.parse_args()



#if x.directory == "":
#    x.directory = input("Provide directory path (example:/home/username/folder/):")

    
def make_json(csvFilePath, jsonFilePath):     
    data = {} 
    with open(csvFilePath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf) 
        for rows in csvReader: 
            key = rows[x.key] 
            data[key] = rows 
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonf.write(json.dumps(data, indent=4)) 
          

# We create json file 
make_json(x.csvpath, x.jsonpath)


# load it in elastic search
res = requests.get('http://localhost:'+x.port)
print (res.content)
es = Elasticsearch([{'host': 'localhost', 'port':x.port}])
f = open(x.jsonpath)
docket_content = f.read()
# Send the data into es
es.index(index=x.indice_name, 
id=0, body=json.loads(docket_content))
