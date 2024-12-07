from typing import List, Dict
import json

from websocket import create_connection

class KrakenWebsocketTradeAPI:

    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str) -> None:
        """Establishes a connection to the Kraken Websocket API and subscribes to the trade channel."""

        self.product_id = product_id

        self._ws = create_connection(self.URL)
        print("Successfully connected to Kraken Websocket API")

        print(f"Subscribing to the trades fro {product_id}")
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_id
                ],
                "snapshot": False
            }
        }

        self._ws.send(json.dumps(msg))
        print("Successfully subscribed")

        # First 2 messages from websocket are not trades but just confirmation of the subscription, so we discard them
        _ = self._ws.recv()
        _ = self._ws.recv()



    def get_trades(self) -> List[Dict]:
        """Gets the trades from the Kraken Websocket API."""

        message = self._ws.recv()
        print("Message received", message)

        message = json.loads(message)

        if message["channel"]  == "heartbeat":
            return []
        else :
            return message["data"]