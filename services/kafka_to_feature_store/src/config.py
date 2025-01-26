import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load my .env variables as environment variables so i can access them with os.environ[] statements
load_dotenv(find_dotenv())


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']
    feature_group_name : str = 'ohlc_feature_group'
    feature_group_version : int = 1
    kafka_topic : str = 'ohlc'

config = Config()