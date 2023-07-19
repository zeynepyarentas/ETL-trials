"""
This module contains code to insert data into the 'musteri' table in the database from 
MongoDB to Postgresql.

Author: Zeynep Yaren Tas
Date: 2023-07-16
"""
import os
import pandas as pd
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


clusterName = os.getenv("cluster_name")
hostt = os.getenv("host")
portt = os.getenv("port")
databasee = os.getenv("database")
userr = os.getenv("user")
passwordd = os.getenv("password")

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

connection = psycopg2.connect(
    host=hostt, port=portt, database=databasee, user=userr, password=passwordd
)
cur = connection.cursor()

try:
    for index, row in data.iterrows():
        values = (row["name"], int(row["age"]))
        QUERY = "INSERT INTO musteri (namee, age) VALUES (%s, %s)"
        cur.execute(QUERY, values)
        connection.commit()
finally:
    connection.close()
