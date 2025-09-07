import sys
import os
import pandas as pd
import numpy as np

import pymongo # type: ignore
from sklearn.model_selection import train_test_split
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

class DataIngestion:
    def __init__(self,data_ingestion_config: DataIngestionConfig):
        
        try:
            self.data_ingestion_config= data_ingestion_config
            
        except Exception as e:
            NetworkSecurityException(e,sys)

    def export_collection_as_dataframe(self):
        
        """
        Read Data from MongoDB
        """
        
        
        try:
            # getting the raw data from MongoDB
            database_name= self.data_ingestion_config.database_name
            collection_name= self.data_ingestion_config.collection_name
            self.mongo_client= pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]
            
            
            df= pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df.drop(columns=["_id"],axis=1)
            df.replace({"na": np.nan},inplace=True)
            
            return df
        except Exception as e:
            NetworkSecurityException(e,sys)
    
    def export_data_into_feature_store(self,dataframe: pd.DataFrame):
        try:
            pass
            feature_store_file_path= self.data_ingestion_config.feature_store_file_path
            # creating folder
            dir_path= os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe

        except Exception as e:
            NetworkSecurityException(e,sys)
            
    def split_data_as_train_test(self,dataframe: pd.DataFrame):
        try:
            train_set,test_set= train_test_split(
                dataframe,train_size=self.data_ingestion_config.train_test_split_ratio
            )

            logging.info("Performed train test split on the dataframe")

            logging.info("Exited split_data_as_train_test method of Data_Ingestion class")

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)

            os.makedirs(dir_path,exist_ok=True)   
            
            logging.info(f"Exporting train and test file path")
            
            train_set.to_csv(self.data_ingestion_config.training_file_path, index = False, header= True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index = False, header= True)
            
            logging.info(f"Exported train and test file path")
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def initiate_data_ingestion(self):
        
        try:
            dataframe= self.export_collection_as_dataframe() # from mongoDB as json and modified by removing object id's and converted to pd dataframe
            dataframe= self.export_data_into_feature_store(dataframe) # converted to .csv file
            self.split_data_as_train_test(dataframe)
            data_ingestion_artifact= DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                           test_file_path=self.data_ingestion_config.testing_file_path)
            return data_ingestion_artifact
            
        except Exception as e:
            NetworkSecurityException(e,sys)

"""
1. What form was the data in before to_csv?

When you call collection.find() from MongoDB, it returns a cursor of BSON (Binary JSON) documents.
You then convert this cursor into a pandas DataFrame using:

df = pd.DataFrame(list(collection.find()))

At this point, your data is in pandas DataFrame format, which is an in-memory tabular structure (rows & columns), not a file.
So, before saving, the data just exists in RAM as a structured DataFrame object.

A DataFrame exists only in memory while the program is running. Once your script stops, the data is gone unless you save it somewhere.
to_csv writes it to disk so it can be reused later.
"""