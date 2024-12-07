from quixstreams import Application
from src.kraken_api import KrakenWebsocketTradeAPI
from typing import List, Dict

def produce_trades(
        kafka_broker_address: str,
        kafka_topic_name: str,
) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic to save the trades to.

    Returns:
        None
    """

    app = Application(broker_address=kafka_broker_address)

    # Create a Kafka topic to save the trades to
    topic = app.topic(kafka_topic_name, value_deserializer="json")

    kraken_api = KrakenWebsocketTradeAPI(product_id="BTC/USD")

    # Create Producer instance
    with app.get_producer() as producer:
            
        while True :

            # Get trades from Kraken Websocket API
            trades : List[Dict] = kraken_api.get_trades()

            for trade in trades :
                # Serialize the event into a message using the defined topic
                print(trade)
                message = topic.serialize(key=trade["symbol"], 
                                          value=trade)
                
                # Produce the message to the Kafka topic
                producer.produce(topic=topic.name, 
                                 key=message.key, 
                                 value=message.value)
                print("Trade sent")

            from time import sleep
            sleep(1)

if __name__ == "__main__":

    produce_trades(
        kafka_broker_address="localhost:19092",
        kafka_topic_name="trade"
    )