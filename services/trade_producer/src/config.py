import os
from dotenv import load_dotenv, find_dotenv

# load my .env variables as environment variables so i can access them with os.environ[] statements

load_dotenv(find_dotenv())

kafka_broker_address=os.environ['KAFKA_BROKER_ADDRESS']
kafka_topic_name='trade'
product_id='BTC/USD'