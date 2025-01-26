import json
from typing import Dict, List
import requests

class KrakenRestAPI:

    URL = 'https://api.kraken.com/0/public/Trades'

    def __init__(self, product_ids: List[str], 
                    from_ms,
                    to_ms
                 ) -> None:
        """Initializes the Kraken REST API client."""
        self.product_ids = product_ids
        self.from_ms = from_ms
        self.to_ms = to_ms
        self.is_done = False

    def get_trades(self) -> List[Dict]:
        """Gets the trades from the Kraken REST API."""

        payload = {}
        headers = {'Accept': 'application/json'}

        since_sec = self.from_ms // 1000
        url = self.URL + '?pair=' + self.product_ids[0] + '&since=' + str()
        
        response = requests.request("GET", url, headers=headers, data=payload)

        data = json.loads(response.text)

        if data['error']:
            raise Exception(data['error'])
        
        trades = []
        for trade in data['result'][self.product_ids[0]] :
                trades.append({
                    'product_id': self.product_ids[0],
                    'price': trade[0],
                    'volume': trade[1],
                    'timestamp': trade[2]
                })

        last_ts_ns = int(data['result']['last'])
        last_ts_ms = last_ts_ns // 1000000
        
        if last_ts_ms >= self.to_ms:
            self.is_done = True

        return trades
    
    def is_done(self) -> bool:
        """Returns True if all trades have been fetched."""
        return self.is_done