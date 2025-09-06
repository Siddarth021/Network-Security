import os
import sys
import json
import certifi
import pandas as pd
import pymongo

from dotenv import load_dotenv
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Load environment variables
load_dotenv()

# Make sure your .env file has:
# MONGO_DB_URL=mongodb+srv://Sidd:sidd123@cluster0.ln0uvvt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print("MongoDB URL:", MONGO_DB_URL)

# Certificate for TLS connection (needed for Atlas)
ca = certifi.where()


class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        """Read CSV file and convert to list of JSON records"""
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        """Insert records into MongoDB collection"""
        try:
            mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            db = mongo_client[database]
            col = db[collection]

            result = col.insert_many(records)
            print("Inserted IDs:", result.inserted_ids)
            return len(result.inserted_ids)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == '__main__':
    FILE_PATH = r"Network_Data\dataset.csv"   # use raw string for Windows paths
    DATABASE = "Sidd"
    COLLECTION = "Network_Data"               # match Compass exactly

    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    print("Sample records:", records[:2])     # preview first 2 rows

    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
    print("Number of records inserted:", no_of_records)