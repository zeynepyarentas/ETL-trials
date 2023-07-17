# pylint: disable=unspecified-encoding
"""
This module contains code to insert data into the name_age.json in json format

Author: Zeynep Yaren Tas
Date: 2023-07-16
"""
import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


clusterName = os.getenv("cluster_name")
client = MongoClient("localhost", 27017)
cluster = MongoClient(clusterName)
db = cluster["database"]
collection = db["musteri"]
all_data = collection.find({}, {"_id": 0})

get_data = []

for info in all_data:
    info["name"] = info["name"].capitalize()
    json_data = json.dumps(info, default=str)
    get_data.append(json_data)


with open("name_age.json", "w") as file:
    for i in get_data:
        file.write(i + "\n")
