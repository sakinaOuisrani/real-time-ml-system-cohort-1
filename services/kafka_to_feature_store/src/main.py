from loguru import logger
import json
from quixstreams import Application

from src.hopsworks_api import push_data_to_feature_store

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int,
) -> None :
    """Reads ohlc data from the Kafka topic and writes it to the feature store.
    Specifically, it writes the data to the specified feature group.

    Args:
        kafka_topic (str): The name of the Kafka topic to read ohlc data from.
        kafka_broker_address (str): The address of the Kafka broker.
        feature_store_url (str): The URL of the feature store.
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to .
    """

    app = Application(broker_address=kafka_broker_address, 
                      consumer_group='kafka_to_feature_store')

    # Create Consumer instance
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                continue
            elif msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            else :
                ohlc = json.loads(msg.value().decode('utf-8'))
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=ohlc
                )
            
            # Store offsets means that the consumer will commit the offset of the 
            # message it just processed to Kafka so that it won't be processed again.
            consumer.store_offsets(message=msg)


if __name__ == '__main__':
    kafka_to_feature_store(
        kafka_topic='ohlc',
        kafka_broker_address='localhost:19092',
        feature_group_name='ohlc_feature_group',
        feature_group_version=1,
    )