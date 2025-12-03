from networksecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file,write_yaml_file

# for data-drift
from scipy.stats import ks_2samp
import pandas as pd
import os,sys


class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config=read_yaml_file(SCHEMA_FILE_PATH)
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
        
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns=len(self._schema_config)
            logging.info(f"required no of columns:{number_of_columns}")
            logging.info(f"Dataframe has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    # function to validate that all numerical columns from schema exist in the dataframe
    def is_numerical_column_exist(self, dataframe: pd.DataFrame) -> bool:
        try:
            # assuming schema.yaml has a key "numerical_columns" which is a list of column names
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            missing_numerical_columns = [col for col in numerical_columns if col not in dataframe_columns]

            logging.info(f"Required numerical columns: {numerical_columns}")
            logging.info(f"Dataframe columns: {list(dataframe_columns)}")

            if len(missing_numerical_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")
                return False

            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_same_dist=ks_2samp(d1,d2)
                if threshold<=is_same_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                    "p_value":float(is_same_dist.pvalue),
                    "drift_status":is_found
                    
                    }}) 
            drif_report_file_path=self.data_validation_config.drift_report_file_path
            
            # create directory
            dir_path=os.path.dirname(drif_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drif_report_file_path,content=report)         
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def initiate_data_validation(self,)->DataValidationArtifact:
        try:
            train_file_path=self.data_ingestion_artifact.trained_file_path
            test_file_path=self.data_ingestion_artifact.test_file_path
            
            # read the data from train and test
            train_dataframe=DataValidation.read_data(train_file_path)
            test_dataframe=DataValidation.read_data(test_file_path)
            
            # validate number of columns
            status=self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message=f"train dataframe does not contain all columns. \n"
            status=self.validate_number_of_columns(dataframe=test_dataframe)
            if not status:
                error_message=f"test dataframe does not contain all columns. \n" 
               
            # validate numerical columns exist in train and test
            status = self.is_numerical_column_exist(dataframe=train_dataframe)
            if not status:
                error_message += "Train dataframe does not contain all required numerical columns.\n"
            status = self.is_numerical_column_exist(dataframe=test_dataframe)
            if not status:
                error_message += "Test dataframe does not contain all required numerical columns.\n"
                
            # Lets check datadrift
            status=self.detect_dataset_drift(base_df=train_dataframe,current_df=test_dataframe)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            
            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path, index=False, header=True
            )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )
            data_validation_artifact=DataValidationArtifact(
                validation_Status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drif_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)
