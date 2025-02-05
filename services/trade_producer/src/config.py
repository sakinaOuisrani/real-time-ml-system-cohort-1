import os
from dotenv import load_dotenv, find_dotenv
from typing import Dict, List
from pydantic_settings import BaseSettings


# load my .env variables as environment variables so i can access them with os.environ[] statements
load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic_name: str = 'trade'
    product_ids: List[str] = ['BTC/USD']
    live_or_historical: str = os.environ['LIVE_OR_HISTORICAL']
    last_n_days: int = 1

config = Config()
