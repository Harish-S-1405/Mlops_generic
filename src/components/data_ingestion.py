import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
import argparse
from src.utils import read_yaml

@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts',"train.csv")
    test_data_path: str=os.path.join('artifacts',"test.csv")
    raw_data_path: str=os.path.join('artifacts',"data.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config=DataIngestionConfig()

    def initiate_data_ingestion(self,params_path):

        params=read_yaml(params_path)


        logging.info('Enter the data ingestion method or component')
        try:
            df=pd.read_csv('notebook\data\stud.csv')
            logging.info('read the dataset as dataframe')

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path),exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path,index=False,header=True)

            logging.info('Train test split initiated')

            split_ratio=params['base']['test_split_ratio']
            random_state=params['base']['random_state']

            train_set,test_set=train_test_split(df,test_size=split_ratio,random_state=random_state)

            train_set.to_csv(self.ingestion_config.train_data_path,index=False,header=True)

            test_set.to_csv(self.ingestion_config.test_data_path,index=False,header=True)

            logging.info('ingestion of the data is completed')

            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )

        except Exception as e:
            raise CustomException(e,sys)

if __name__=='__main__':

    args=argparse.ArgumentParser()
    args.add_argument("--params","-p",default="params.yaml")

    parsed_args=args.parse_args()

    
    obj=DataIngestion()
    obj.initiate_data_ingestion(params_path=parsed_args.params)    