"""
Yahoo Finance API client with caching.
"""
 
import os
import logging
import pandas as pd
import polars as pl

# from DictObj import DictObj
from cachedApi import CachedApi
import yfinance as yf
from utils import cleanse

logger = logging.getLogger()


class CachedYahooApi(CachedApi):
    __session = None
    __quotecast_api = None

    def __init__(self, file: str):
        super().__init__(file)
        '''
        try:
            if self.__session is not None:
                pass
        except BaseException:
        '''
        logger.debug(f'set yahoo cache location "{os.path.dirname(file)}" <- {file}')
        yf.set_tz_cache_location(os.path.dirname(file))
        
        """
        yf.enable_debug_mode()
        self.__session = CachedLimiterSession(
            #limiter=Limiter(RequestRate(10, Duration.SECOND*5)),
            #bucket_class=MemoryQueueBucket,
            #backend=SQLiteCache(file2),

            per_second=1,
            cache_name=file2,
            bucket_class=SQLiteBucket,
            bucket_kwargs={
            "path": file2,
            'isolation_level': "EXCLUSIVE",
            'check_same_thread': True,
            },
        )

        self.__session.request = functools.partial(self.__session.request, timeout=(15.0,15.0))
        yf.base._requests = self.__session.request
        yf.utils._requests = self.__session.request
        yf.ticker._requests = self.__session.request
        self.__session.headers['User-agent'] = (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
        )
        """
        super().open_db()
        logger.debug(f"Yahoo setup done")

    def __del__(self):
        super().__del__()
        logger.debug(f"Instance {self} destroyed.")

    """
    def get_chart(self, **kwargs):
        k = "get_chart" + str(kwargs)
        r = self.cache_get(k, 3600)
        logger.debug(k)
        if r is None:
            try:
                r = self.__quotecast_api.get_chart(**kwargs)
            except BaseException:
                self.cache_set(k, 3600, None)
                print("!! k")
            # print("get_chart cache miss", r)
            self.cache_set(k, 3600, r)
        return r
    """
    
    
    def ysearch(self, txt, max_results, news_count, enable_fuzzy_query):
        k = f"ysearch2 {txt} {max_results} {news_count} {enable_fuzzy_query}"
        quotes = self.cache_get(k, 3600 * 23 * 7)
        if quotes is None:
            try:
                quotes = yf.Search(txt, max_results=max_results, news_count=news_count, enable_fuzzy_query=enable_fuzzy_query)
                if quotes is not None:
                    self.cache_set(k, 3600 * 23 * 7, quotes)
            except:
                logger.debug(f"ysearch error - {k}")
                pass
        return quotes
            
            
    def product_search(self, isin: str, symbol: str, name: str, exchanges: list[str]):
        def cleanList(lq, quotes, txt, symb):
            cname = cleanse(txt)
            for q in list(quotes):
                if q["quoteType"] == "EQUITY" and "longname" in q and len(q["longname"]) > 0 and (cname.startswith(cleanse(q["longname"])) or cleanse(q["longname"]).startswith(cname)):
                    continue
                elif q["quoteType"] == "EQUITY" and "shortname" in q and len(q["shortname"]) > 0 and (cname.startswith(cleanse(q["shortname"])) or cleanse(q["shortname"]).startswith(cname)):
                    continue
                elif q["quoteType"] == "EQUITY" and "prevName" in q and len(q["prevName"]) > 0 and (cname.startswith(cleanse(q["prevName"])) or cleanse(q["prevName"]).startswith(cname)):
                    continue
                elif q["quoteType"] == "EQUITY" and len(exchanges) > 0 and (q["exchDisp"] in exchanges or q["exchange"] in exchanges) and q["symbol"].startswith(symb):
                    continue
                else:
                    logger.debug(f"product_search cleanList {txt} '{cname}' remove-> °{cleanse(q.get('longname', ''))}°{cleanse(q.get('shortname',''))}°{cleanse(q.get('prevName',''))}°")
                    quotes.remove(q)
                    lq -= 1             
            return lq, quotes      
         
        k = f"yproduct_search2 {isin} {symbol} {name}"
        quotes = self.cache_get(k, 3600 * 23 * 7)
        if quotes is None:
            logger.debug(f"product_search yahoo {isin}/{symbol}/{name}")  
            try:
                quotes = self.ysearch(isin, max_results=3, news_count=0, enable_fuzzy_query=False) 
                quotes = quotes.quotes
            except Exception:
                logger.debug(f"Error searching {isin} on Yahoo!")
                quotes = []

            lq = len(quotes)
            if lq:
                lq, quotes = cleanList(lq, quotes, name, symbol)
            if lq < 1: 
                try:
                    quotes = self.ysearch(symbol, max_results=3, news_count=0, enable_fuzzy_query=False)
                    quotes = quotes.quotes
                except Exception:
                    logger.debug(f"Error searching {symbol} on Yahoo!")
                    quotes = []
                    
                lq = len(quotes)
                if lq:
                    lq, quotes = cleanList(lq, quotes, name, symbol)
                if lq < 1:
                    try:
                        quotes = self.ysearch(cleanse(name), max_results=8, news_count=0, enable_fuzzy_query=True) 
                        quotes = quotes.quotes
                    except Exception:
                        logger.debug(f"Error searching {cleanse(name)} on Yahoo!")
                        quotes = []
                        
                    lq = len(quotes)
                    if lq:
                        lq, quotes = cleanList(lq, quotes, name, symbol)

            if len(quotes) >= 1:
                # save findings
                self.cache_set(k, 3600 * 23 * 7, quotes)
                label = quotes[0].get("symbol", '')
                yname = quotes[0].get("longname", quotes[0].get("shortname", ""))
                logger.debug(f"product_search yahoo {isin}/{symbol}/{name}, FOUND ! -> {cleanse(name)} -> '{label}' / '{yname}'")                

        if quotes is not None and len(quotes) >= 1:
            return quotes[0].get("symbol",'')
        else:
            logger.debug(f"product_search yahoo {isin}/{symbol}/{name}, NOT FOUND ! -> '{cleanse(name)}'")
            return None


    def get_longtermprice(self, label: str,  period: str, resolution: str) -> pl.DataFrame | None:
        # Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max Either Use period parameter or use start and end
        # Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo Intraday data cannot extend last 60 days

        k = f"yahoo get_longtermprice2 {label} {period} {resolution}"
        r = self.cache_get(k, 3600 * 23 * 7)
        logger.debug(k)
        if r is None or not isinstance(r, pd.DataFrame):
            logger.debug(k)
            try:
                handle = yf.Ticker(label)
                r = handle.history(period=period, interval=resolution, auto_adjust=False, back_adjust=False)
                # print('set',type(r).__name__)
            except BaseException:
                self.cache_set(k, 3600*2, None)
                r = None
                # print(f"!! {k}")
            # print("get_chart cache miss", r)
            self.cache_set(k, 3600 * 23 * 7, r)
            
        if isinstance(r, pd.DataFrame):
            r = pl.DataFrame(r)
            
        return r

    def get_realTimePrice(self, vwdId: list):
        return None
 