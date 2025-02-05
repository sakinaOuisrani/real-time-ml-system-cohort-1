import json
from typing import Dict, List
import requests
import time
from time import sleep
from loguru import logger
from datetime import datetime, timezone

class KrakenRestAPI:

    def __init__(self, product_ids: List[str], 
                    from_ms,
                    to_ms
                 ) -> None:
        """Initializes the Kraken REST API client."""
        self.product_ids = product_ids
        self.from_ms = from_ms
        self.to_ms = to_ms
        self.isdone = False

    def get_trades(self) -> List[Dict]:
        """Gets the trades from the Kraken REST API."""

        payload = {}
        headers = {'Accept': 'application/json'}

        since_sec = self.from_ms // 1000
        
        url = 'https://api.kraken.com/0/public/Trades?pair={}&since={}'.format(self.product_ids[0], since_sec) 

        # since_date = datetime.fromtimestamp(since_sec, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        # logger.info(f"Fetching trades since : {since_date}")

        response = requests.request("GET", url, headers=headers, data=payload)
        data = json.loads(response.text)


        if data['error']:
            if data['error'][0]=="EGeneral:Too many requests":
                logger.info(data['error'][0] + ' Sleep for 10 seconds...')
                sleep(10)
                return [] 
            else:
                raise Exception(data['error'])
        
        trades = []
        for trade in data['result'][self.product_ids[0]] :
                trades.append({
                    'price': float(trade[0]),
                    'volume': float(trade[1]),
                    'timestamp': int(trade[2]),
                    'product_id': self.product_ids[0]
                })

        last_ts_ns = int(data['result']['last'])
        last_ts_ms = last_ts_ns // 1_000_000
        
        last_timestamp = datetime.fromtimestamp(last_ts_ms // 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        logger.info(f"Last timestamp in ms: {last_ts_ms}, equivalent to {last_timestamp}; Fetched {len(trades)} trades")

        if last_ts_ms >= self.to_ms:
            self.isdone = True
        else : 
             self.from_ms = last_ts_ms

        return trades
    
    def is_done(self) -> bool:
        """Returns True if all trades have been fetched."""
        return self.isdone