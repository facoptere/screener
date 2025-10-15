"""
Yahoo Finance API client with caching.
"""

# import pandas as pd
# import numpy as np
import os
import logging

# from DictObj import DictObj
from cachedApi import CachedApi
import yfinance as yf

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
        CachedApi.open_db(self)
        logger.debug(f"Yahoo setup done")

    def __del__(self):
        logger.debug(f"Instance {self} destroyed.")
        CachedApi.close(self)

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

    def product_search(self, what: str):
        logger.debug(f"Yahoo product_search {what}")
        print(what)
        k = f"product_search{what}"
        r = self.cache_get(k, 3600)
        if r is None:
            try:
                r = yf.Search(what).quotes  # , session=self.__sessionmax_results=3, news_count=0, enable_fuzzy_query=False, , timeout=7)
                self.cache_set(k, 3600, r)
            except BaseException:
                pass

        return r

    def get_longtermprice(self, isin: str, symbol: str, name: str, period: str, resolution: str):
        # Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max Either Use period parameter or use start and end
        # Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo Intraday data cannot extend last 60 days

        k = f"yget_longtermprice search {isin} {symbol} {name}"
        quotes = self.cache_get(k, 3600 * 23 * 7)
        if quotes is None:
            logger.debug(k)
            try:
                quotes = yf.Search(isin, max_results=3, news_count=0, enable_fuzzy_query=False)  # ,session=self.__session
                logger.debug(quotes)
                quotes = quotes.quotes
            except BaseException:
                logger.debug("Error!")
                quotes = []
            r = None
            label = None
            lq = len(quotes)
            if len(quotes):
                for q in list(quotes):
                    if (q["quoteType"] != "EQUITY") or (not (symbol in q["symbol"])):
                        quotes.remove(q)
            if len(quotes) != 1:
                logger.debug(f"get_longtermprice yahoo isin:{isin} nb quotes:{lq} then {len(quotes)}, using symbol")
                try:
                    quotes = yf.Search(symbol, max_results=3, news_count=0, enable_fuzzy_query=False)  # ,session=self.__session
                    logger.debug(quotes)
                    quotes = quotes.quotes
                except BaseException:
                    logger.debug("Error!")
                    quotes = []
                lq = len(quotes)
                if len(quotes):
                    for q in list(quotes):
                        if q["quoteType"] != "EQUITY":
                            quotes.remove(q)
                if len(quotes) != 1:
                    logger.debug(
                        f"get_longtermprice yahoo isin:{isin} > label:{symbol} nb quotes:{lq} then {len(quotes)}, using name and fuzzy logic"
                    )
                    try:
                        quotes = yf.Search(name, max_results=3, news_count=0, enable_fuzzy_query=True)  # ,session=self.__session
                        logger.debug(quotes)
                        quotes = quotes.quotes
                    except BaseException:
                        logger.debug("Error!")
                        quotes = []
                    lq = len(quotes)
                    if len(quotes):
                        for q in list(quotes):
                            if q["quoteType"] != "EQUITY" or q["score"] > 20099:
                                quotes.remove(q)
                    if len(quotes) != 1:
                        logger.debug(f"get_longtermprice yahoo isin:{isin} > label:{symbol} > name:{name} nb quotes:{lq} then {len(quotes)}, stuck")
                    else:
                        logger.debug(
                            f"get_longtermprice yahoo isin:{isin} > label:{symbol} > name:{name} "
                            f"found 1 with real name {quotes[0].get('longname')} and score {quotes[0]['score']}"
                        )
            # print(f"!! {k}")
        # else:
        #    print(f"!! found {k}  {quotes}")

        if len(quotes) == 1:
            self.cache_set(k, 3600 * 23 * 7, quotes)
            label = quotes[0]["symbol"]
            # display(label)
            k = f"yahoo get_longtermprice {label} {period} {resolution}"
            r = self.cache_get(k, 3600 * 23 * 7)
            logger.debug(k)
            if r is None or type(r).__name__ != "DataFrame":
                logger.debug(k)
                try:
                    handle = yf.Ticker(label)
                    r = handle.history(period=period, interval=resolution, auto_adjust=False, back_adjust=False)
                    # print('set',type(r).__name__)
                except BaseException:
                    self.cache_set(k, 3600, None)
                    r = None
                    # print(f"!! {k}")
                # print("get_chart cache miss", r)
                self.cache_set(k, 3600 * 23 * 7, r)
            # else:
            #    print('get',type(r).__name__)
            # else:
            #    print(f"get_longtermprice yahoo {isin} {symbol} {len(quotes)}")
            """
            if df.shape[0] != 0:
                val2 = 1+(df.iloc[-1]["Close"] - df.iloc[0]["Close"])/df.iloc[0]["Close"]
                val = (pow(val2, 1/(df.shape[0]/12))-1)*100
            display(df, val2, val)
            """
        else:
            r = None
            label = None

        return r, label

    def get_realTimePrice(self, vwdId: list):
        return None


"""
    def computeIndex(self, name, **kwargs):
        k = str(kwargs)
        ret = name + re.sub("(?:\\s+|(?<=ntAccount=|sessionId=).*?(?:&|$))", "", k)
        # print("computeIndex ",k,ret)
        return ret

    def logout(self):
        self.__trading_api.logout()

    def get_config(self):
        return self.__trading_api.credentials

    def get_config(self, **kwargs):
        k = self.computeIndex("get_config", **kwargs)
        r = self.cache_get(k)
        if r is None:
            r = self.__trading_api.get_config(**kwargs)
            self.cache_set(k, r)
        # print(r)
        self.__user_token = r["clientId"]
        # print(f"token:{self.__user_token}")
        return r

    def get_client_details(self, **kwargs):
        k = self.computeIndex("get_client_details", **kwargs)

        r = self.cache_get(k)
        if r is None:
            r = self.__trading_api.get_client_details(**kwargs)
            self.cache_set(k, r)
        # print(r)
        self.__trading_api.credentials.int_account = r["data"]["intAccount"]
        # print(f"intAccount:{self.__trading_api.credentials.int_account}")
        return r

    def connect(self):
        self.__trading_api.connect()
        if not self.__user_token:
            self.get_config()
        if self.__user_token:
            self.__quotecast_api = QuotecastAPI(user_token=self.__user_token)
        #session_id = self.__trading_api.connection_storage.session_id
        # print("You are now connected, with the session id :", session_id)

    def get_portfolio(self):
        request_list = Update.RequestList()
        request_list.values.extend(
            [
                Update.Request(option=Update.Option.PORTFOLIO, last_updated=0),
            ]
        )
        return self.__trading_api.get_update(request_list=request_list)

    def get_list_list(self):
        return self.__trading_api.get_favourites_list(raw=True)

    def create_favourite_list(self, **kwargs):
        return self.__trading_api.create_favourite_list(**kwargs)

    def delete_favourite_list(self, **kwargs):
        return self.__trading_api.delete_favourite_list(**kwargs)

    def put_favourite_list_product(self, **kwargs):
        return self.__trading_api.put_favourite_list_product(**kwargs)

    def get_products_config(self, **kwargs):
        k = self.computeIndex("get_products_config", **kwargs)
        r = self.cache_get(k)
        #logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_products_config(**kwargs)
            self.cache_set(k, r)
        self.indices = {}
        for li in r["indices"]:
            self.indices[li["id"]] = DictObj(li)
        self.countries = {}
        for li in r["countries"]:
            self.countries[li["id"]] = DictObj(li)
        self.exchanges = {}
        for li in r["exchanges"]:
            self.exchanges[li["id"]] = DictObj(li)
        self.stockCountries = r["stockCountries"]
        return r

    def get_company_ratios(self, **kwargs):
        k = self.computeIndex("get_company_ratios", **kwargs)
        r = self.cache_get(k)
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_company_ratios(**kwargs)
            self.cache_set(k, r)
        try:
            codes = {}
            # if 'data' in r:
            #    print(r['data'].keys())
            if (
                "data" in r
                and "currentRatios" in r["data"]
                and "ratiosGroups" in r["data"]["currentRatios"]
            ):
                for an in r["data"]["currentRatios"]["ratiosGroups"]:
                    for i in an["items"]:
                        v = i.get("value") or np.NaN  # value
                        t = i.get("type") or None  # type of parameter
                        k = i.get("id") or None  # name of parameter
                        m = i.get("name") or ""  # meaning
                        if t == "N" and not pd.isna(v):
                            v = float(v)
                        # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                        if not m.__contains__(" per "):
                            v = v * 1  # 000000
                        if k:
                            codes[k] = {"meaning": m, "value": v}

            if (
                "data" in r
                and "forecastData" in r["data"]
                and "ratios" in r["data"]["forecastData"]
            ):
                for i in r["data"]["forecastData"]["ratios"]:
                    # print(i)
                    v = i.get("value") or np.NaN  # value
                    t = i.get("type") or None  # type of parameter
                    k = i.get("id") or None  # name of parameter
                    m = i.get("name") or ""  # meaning
                    if t == "N" and not pd.isna(v):
                        v = float(v)
                    # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                    if not m.__contains__(" per "):
                        v = v * 1  # 000000
                    if k:
                        codes[k] = {"meaning": m, "value": v}

            if (
                "data" in r
                and "consRecommendationTrend" in r["data"]
                and "ratings" in r["data"]["consRecommendationTrend"]
            ):
                for i in r["data"]["consRecommendationTrend"]["ratings"]:
                    # print(i)
                    v = i.get("value") or np.NaN  # value
                    k = ("ratings_" + i.get("periodType")) or None  # name of parameter
                    if t == "N" and not pd.isna(v):
                        v = float(v)
                    # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                    if not m.__contains__(" per "):
                        v = v * 1  # 000000
                    if k:
                        codes[k] = {"meaning": "", "value": v}

            codes["priceCurrency"] = {
                "meaning": "",
                "value": r["data"]["currentRatios"]["priceCurrency"],
            }
            if len(codes["priceCurrency"]) <= 1:
                codes["priceCurrency"] = {
                    "meaning": "",
                    "value": r["data"]["currentRatios"]["currency"],
                }
        except:
            None
        return codes

    def get_financial_statements(self, **kwargs):
        k = self.computeIndex("get_financial_statements", **kwargs)
        r = self.cache_get(k)
        #logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_financial_statements(**kwargs)
            self.cache_set(k, r)
        codes_array = []
        if r:
            try:
                for t in ("annual", "interim"):
                    if t in r["data"]:
                        for an in r["data"][t]:

                            codes = {}
                            for st in an["statements"]:
                                #periodLength = st.get("periodLength")
                                #periodType = st.get("periodType")
                                for i in st["items"]:
                                    v = i.get("value") or np.NaN
                                    if not pd.isna(v):
                                        v = float(v)
                                    if not i.get("meaning").__contains__(" per "):
                                        v = v * 1  # 000000
                                    codes[i.get("code")] = {
                                        "meaning": i.get("meaning"),
                                        "value": v,
                                    }
                            codes_array += [codes]
            except:
                # print(k)
                # traceback.print_exc()
                # del self.cache_get(k)
                None
        return codes_array

    def get_estimates_summaries(self, **kwargs):
        k = self.computeIndex("get_estimates_summaries_", **kwargs)
        r = self.cache_get(k)
        # print("get_estimates_summaries cache hit", type(r))
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_estimates_summaries(**kwargs)
            # print("get_estimates_summaries cache miss", type(r))
            self.cache_set(k, r)
        return r

    def get_products_info(self, **kwargs):
        k = self.computeIndex("get_products_info", **kwargs)
        r = self.cache_get(k)
        # print("get_products_info cache hit", r)
        if r is None:
            r = self.__trading_api.get_products_info(**kwargs)
            # print("get_products_info cache miss", r)
            self.cache_set(k, r)
        return r


    def get_company_profile(self, **kwargs):
        k = self.computeIndex("get_company_profile", **kwargs)
        r = self.cache_get(k)
        #logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            # searching on Degiro
            r = self.__trading_api.get_company_profile(
                product_isin=kwargs["product_isin"], raw=kwargs["raw"]
            )
            self.cache_set(k, r)

        codes = {}
        if r is not None and "data" in r:
            r_data = r["data"]
            # print(f"\n{r_data}")
            try:
                codes["sector"] = r_data["sector"]
            except:
                None
            try:
                codes["industry"] = r_data["industry"]
            except:
                None
            try:
                codes["country"] = r_data["contacts"]["COUNTRY"]
            except:
                None
            try:
                codes["shrOutstanding"] = float(r_data["shrOutstanding"]) / 10**6
            except:
                None
            try:
                codes["businessSummary"] = r_data["businessSummary"]
            except:
                None

            try:
                if "ratios" in r_data and "ratiosGroups" in r_data["ratios"]:
                    for an in r_data["ratios"]["ratiosGroups"]:
                        for i in an["items"]:
                            v = i.get("value") or np.NaN  # value
                            t = i.get("type") or None  # type of parameter
                            k = i.get("id") or None  # name of parameter
                            m = i.get("name") or ""  # meaning
                            if t == "N" and not pd.isna(v):
                                v = float(v)
                            # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                            if not m.__contains__(" per "):
                                v = v * 1  # 000000
                            if k:
                                codes[k] = {"meaning": m, "value": v}
                if "forecastData" in r_data and "ratios" in r_data["forecastData"]:
                    for i in r_data["forecastData"]["ratios"]:
                        # print(i)
                        v = i.get("value") or np.NaN  # value
                        t = i.get("type") or None  # type of parameter
                        k = i.get("id") or None  # name of parameter
                        m = i.get("name") or ""  # meaning
                        if t == "N" and not pd.isna(v):
                            v = float(v)
                        # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                        if not m.__contains__(" per "):
                            v = v * 1  # 000000
                        if k:
                            codes[k] = {"meaning": m, "value": v}
            except:
                None
        else:
            # searching on Yahoo! finance
            try:
                r = self.cache_get("Y_" + k)
            except:
                sym = None  # yf.Ticker(kwargs['product_isin'])
                r = sym.info
                try:
                    r["marketCap"] /= 1000.0
                except:
                    pass
                self.cache_set("Y_" + k, r)
                logger.debug(f"OK from Yahoo {kwargs['product_isin']}")
            codes = r
        return codes
"""
