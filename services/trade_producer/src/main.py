from typing import Dict, List

from datetime import datetime, timezone
from loguru import logger
from quixstreams import Application
import time
from time import sleep

from src.kraken_api.websocket import KrakenWebsocketTradeAPI
from src.kraken_api.rest import KrakenRestAPI
from src.config import config

def produce_trades(
    kafka_broker_address: str,
    kafka_topic_name: str,
    product_ids: List[str],
    live_or_historical: str,
    last_n_days: int
) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic to save the trades to.
        product_ids (List[str]): The list of product ids to get trades for.

    Returns:
        None
    """

    assert live_or_historical in ['live', 'historical'], 'live_or_historical must be either "live" or "historical"'

    app = Application(broker_address=kafka_broker_address)

    # Create a Kafka topic to save the trades to
    topic = app.topic(kafka_topic_name, value_deserializer='json')

    if live_or_historical == 'historical':
        logger.info('Creating the Kraken REST API...')
        # Create Kraken REST API instance
        to_ms = int(time.time() * 1000)
        from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
        
        # Format time in miliseconds to readable date ISO 8601 with 'Z' for UTC
        from_timestamp = datetime.fromtimestamp(from_ms // 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        to_timestamp = datetime.fromtimestamp(to_ms // 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        logger.info(f"Fetching data from {from_timestamp} to {to_timestamp}")

        kraken_api = KrakenRestAPI(product_ids=product_ids, from_ms=from_ms, to_ms=to_ms)
    else :
        # Create Kraken Websocket API instance
        logger.info('Creating the Kraken websocket API...')
        kraken_api = KrakenWebsocketTradeAPI(product_ids=product_ids)

    # Create Producer instance
    logger.info('Creating the producer...')
    with app.get_producer() as producer:
        while True:

            if kraken_api.is_done():
                logger.info(f'Done fetching historical data from {from_timestamp} to {to_timestamp}')
                break

            # Get trades from Kraken API
            trades: List[Dict] = kraken_api.get_trades()
            if len(trades) > 0:
                logger.info(f"Number of trades fetched: {len(trades)}")

            for trade in trades:
                # Serialize the event into a message using the defined topic
                logger.info(trade)
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce the message to the Kafka topic
                producer.produce(topic=topic.name, key=message.key, value=message.value)
                # logger.info('Trade sent')



if __name__ == '__main__':
    try :
        produce_trades(
            kafka_broker_address=config.kafka_broker_address,
            kafka_topic_name=config.kafka_topic_name,
            product_ids=config.product_ids,
            live_or_historical=config.live_or_historical,
            last_n_days=config.last_n_days
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
