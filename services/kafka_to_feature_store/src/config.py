import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load my .env variables as environment variables so i can access them with os.environ[] statements
load_dotenv(find_dotenv())


class Config(BaseSettings):
    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']

config = Config()