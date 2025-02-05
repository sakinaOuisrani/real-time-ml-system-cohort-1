import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load my .env variables as environment variables so i can access them with os.environ[] statements
load_dotenv(find_dotenv())


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']
    feature_group_name : str = os.environ['FEATURE_GROUP_NAME']
    feature_group_version : int = os.environ['FEATURE_GROUP_VERSION']
    kafka_topic : str = os.environ['KAFKA_TOPIC']
    buffer_size : int = os.environ['BUFFER_SIZE']

config = Config()