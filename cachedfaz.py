import logging
from cachedApi import CachedApi
import requests
import json


logger = logging.getLogger()


class CachedFrankfurter(CachedApi):
    def __init__(self, file: str):
        super().__init__(file)
        CachedApi.open_db(self)
        logger.debug(f"frankfurter.app setup done")
        
    def __del__(self):
        logger.debug(f"Instance {self} destroyed.")
        CachedApi.close(self)    
        
    def convert(self, what: str, inwhat: str):
        k = f"convert8{what}{inwhat}"
        r = self.cache_get(k, 24*3600)
        if r is None:
            try:
                url = f"https://api.frankfurter.app/latest"
                params = {
                    "amount": 10000,
                    "from": what.upper(),
                    "to": inwhat.upper()
                }
                response = requests.get(url, params=params)
                response.raise_for_status()
                r = response.text
                self.cache_set(k, 3600, r)
            except BaseException as ee:
                logger.error(f"cannot connect {ee}")
                r = -2.0
                pass
        if r is None or not isinstance(r, str):
            r = -5.0
        else:
            try:
                data = json.loads(r)
                r = data["rates"][inwhat.upper()]
                if r and isinstance(r, str) or isinstance(r, int) or isinstance(r, float):
                    r = float(r) / 10000.0
                else:
                    logger.error(f"unknown reply {type(r)} ")
                    r = -3.0
            except BaseException as ee:
                logger.error(f"cache error {ee}")
                r = -4.0
        logger.debug(f"convert currency {what} in {inwhat} > {r}")
        return r
