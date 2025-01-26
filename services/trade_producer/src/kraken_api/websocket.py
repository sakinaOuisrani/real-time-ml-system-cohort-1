import json
from typing import Dict, List

from loguru import logger
from websocket import create_connection


class KrakenWebsocketTradeAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_ids: List[str]) -> None:
        """Establishes a connection to the Kraken Websocket API and subscribes to the trade channel."""

        self.product_ids = product_ids

        self._ws = create_connection(self.URL)
        logger.info('Successfully connected to Kraken Websocket API')

        # Subscribe to the trades for the specified product_id
        self._subscribe(product_ids)

    def _subscribe(self, product_ids: str):
        """Establishes a connection to the Kraken Websocket API and subscribes to the trade channel."""

        logger.info(f'Subscribing to the trades for {product_ids}')
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': product_ids, 'snapshot': False},
        }

        self._ws.send(json.dumps(msg))
        logger.info('Successfully subscribed')

        # First 2 messages from websocket are not trades but just confirmation of the subscription, so we discard them
        for product_id in product_ids:
            _ = self._ws.recv()
            _ = self._ws.recv()


    def get_trades(self) -> List[Dict]:
        """Gets the trades from the Kraken Websocket API."""

        message = self._ws.recv()
        logger.info('Message received', message)

        message = json.loads(message)

        if message['channel'] == 'heartbeat':
            return []
        else:
            trades = []
            for trade in message['data'] :
                trades.append({
                    'product_id': trade['symbol'],
                    'price': trade['price'],
                    'volume': trade['qty'],
                    'timestamp': trade['timestamp']
                })
            return trades
        

    def is_done(self) -> bool:
        """The websocket nevers stops getting live trades"""
        return False
