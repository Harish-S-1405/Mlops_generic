import sys
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder,StandardScaler
from src.utils import read_yaml,save_object
from src.exception import CustomException
from src.logger import logging
import os
import argparse

@dataclass
class DataTransformationConfig:
    preprocessor_obj_file_path=os.path.join('artifacts','preprocessor.pkl')

class DataTransformation:
    def __init__(self):
        self.data_transformation_config= DataTransformationConfig()
    
    def  get_data_transformer_object(self,params_path):

        params=read_yaml(params_path)

        try:
            

            data_path=params['Data_upload']['upload_from_local']['path']
            df=pd.read_csv(data_path)
            df.drop('math_score',axis=1,inplace=True)

            numerical_columns = [feature for feature in df.columns if df[feature].dtype != 'O']
            categorical_columns = [feature for feature in df.columns if df[feature].dtype == 'O']

            num_pipeline=Pipeline(
                steps=[
                ('imputer',SimpleImputer(strategy='median')),
                ('scaler',StandardScaler())
                ]

            )

            cat_pipeline=Pipeline(
                steps=[
                
                ('imputer',SimpleImputer(strategy='most_frequent')),
                ('one_hot_encoder',OneHotEncoder()),
                ('scaler',StandardScaler(with_mean=False))

                ]
            )

            logging.info("numerical columns standard scaling completed")
            logging.info("Categorical Columns encoding completed")

            preprocessor=ColumnTransformer(
                [
                
                ('num_pipeline',num_pipeline,numerical_columns),
                ('cat_pipeline',cat_pipeline,categorical_columns)

                ]
            )

            return preprocessor    
            
        except Exception as e:
            raise CustomException(e,sys) 


    def initiate_data_transformation(self,train_path,test_path,params_path):

        try:
            train_df=pd.read_csv(train_path)
            test_df=pd.read_csv(test_path)
            logging.info('read train and test data completed')

            logging.info('obtaining preprocessing object')
            

            preprocessing_obj=self.get_data_transformer_object(params_path)
        
            target_column_name='math_score'

            numerical_columns= ['reading_score','writing_score']

            input_feature_train_df=train_df.drop(target_column_name,axis=1)
            target_feature_train_df=train_df[target_column_name]

            input_feature_test_df=test_df.drop(target_column_name,axis=1)
            target_feature_test_df=test_df[target_column_name]

            logging.info(
                f"apply preprocessing object on train and testing dataframe"  
            )
        
            input_feature_train_arr=preprocessing_obj.fit_transform(input_feature_train_df)
            input_feature_test_arr=preprocessing_obj.transform(input_feature_test_df)

            train_arr=np.c_[input_feature_train_arr,np.array(target_feature_train_df)]
            test_arr=np.c_[input_feature_test_arr,np.array(target_feature_test_df)]

            logging.info(f"saved preprocessing object")


            save_object(

                file_path=self.data_transformation_config.preprocessor_obj_file_path,
                obj=preprocessing_obj
            )

            return (
                train_arr,
                test_arr,
                self.data_transformation_config.preprocessor_obj_file_path,
            )
        
        except Exception as e:
            raise CustomException(e, sys)