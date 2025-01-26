from datetime import timedelta
from loguru import logger
from quixstreams import Application
from src.config import config


def trade_to_ohlc(
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafta_broker_address: str,
    ohlc_window_seconds: int,
) -> None:
    """Reads trades from a Kafka topic
    Aggregates trades into an OHLC (Open, High, Low, Close) candlestick using the specified time window
    Saves the OHLC candlestick into a Kafka topic

    Args:
        kafka_input_topic (str): Kafka topic to read the trades from.
        kafka_output_topic (str): Kafka topic to save the OHLC to.
        kafta_broker_address (str): The address of the Kafka broker.
        ohlc_window_seconds (int): The time window (in seconds).
    Returns:
        None
    """

    app = Application(
        broker_address=kafta_broker_address,
        consumer_group="trade_to_ohlc",
        auto_offset_reset="earliest",
    )

    # specify the input and output topics
    input_topic = app.topic(kafka_input_topic, value_deserializer="json")
    output_topic = app.topic(kafka_output_topic, value_deserializer="json")

    # create a streaming dataframe and apply transformations to the coming data
    sdf = app.dataframe(input_topic)

    def initialize_ohlc_candle(value: dict) -> dict:
        """Initialize the OHLC candle with the first trade in the window"""
        return {
            "open": value["price"],
            "high": value["price"],
            "low": value["price"],
            "close": value["price"],
            "product_id": value["symbol"],
        }

    def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        return {
            "open": ohlc_candle["open"],
            "high": max(ohlc_candle["high"], trade["price"]),
            "low": min(ohlc_candle["low"], trade["price"]),
            "close": trade["price"],
            "product_id": trade["symbol"],
        }

    sdf = (
        sdf.tumbling_window(timedelta(seconds=ohlc_window_seconds))
        .reduce(reducer=update_ohlc_candle, initializer=initialize_ohlc_candle)
        .final()
    )

    sdf["open"] = sdf["value"]["open"]
    sdf["high"] = sdf["value"]["high"]
    sdf["low"] = sdf["value"]["low"]
    sdf["close"] = sdf["value"]["close"]
    sdf["product_id"] = sdf["value"]["product_id"]

    sdf["timestamp"] = sdf["end"]

    sdf = sdf[["timestamp", "product_id", "open", "high", "low", "close"]]

    sdf = sdf.update(logger.info)
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)


if __name__ == "__main__":
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafta_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_window_seconds,
    )