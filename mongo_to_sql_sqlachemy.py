"""
This module contains code to insert data into the 'musteri' table in the database from 
MongoDB to Postgresql via sqlalchemy.

Author: Zeynep Yaren Tas
Date: 2023-07-16
"""
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# Şifrelere erişim
clusterName = os.getenv("cluster_name")
host = os.getenv("host")
port = os.getenv("port")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")

client = MongoClient("localhost", 27017)
cluster = MongoClient(clusterName)
db = cluster["database"]
collection = db["musteri"]
all_data = collection.find({}, {"_id": 0})

data_list = []

for info in all_data:
    info["name"] = info["name"].capitalize()
    data_list.append(info)
    data = pd.DataFrame(data_list)

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database}", echo=False
)
data.to_sql("musteri", engine, if_exists="append", index=False)


client.close()
