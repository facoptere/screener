from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.quotecast.api import API as QuotecastAPI
from degiro_connector.quotecast.tools.chart_fetcher import ChartFetcher
from degiro_connector.quotecast.models.chart import ChartRequest
import pandas as pd
import numpy as np
from DictObj import DictObj
from cachedApi import CachedApi
import re
import logging
import time
from degiro_connector.trading.models.credentials import Credentials

logger = logging.getLogger()


class cachedDegiroApi(CachedApi):
    __quotecast_api: QuotecastAPI
    __trading_api: TradingAPI

    def __init__(self, file: str, credentials: Credentials):
        self.__credentials = credentials
        self.__file = file
        self.__user_token = None
        self.__trading_api = None
        self.__quotecast_api = None

        super().__init__(file)

    def __del__(self):
        logger.debug(f"Instance {self} destroyed.")
        CachedApi.close(self)

    def computeIndex(self, name, **kwargs):
        k = str(kwargs)
        ret = name + re.sub("(?:\\s+|(?<=ntAccount=|sessionId=).*?(?:&|$))", "", k)
        # print("computeIndex ",k,ret)
        return ret

    def connect(self, cookies=None, headers=None):
        if not self.__trading_api:
            self.__trading_api = TradingAPI(credentials=self.__credentials)
            self.__session = self.__trading_api.session_storage.session
            if cookies:
                for item in cookies:
                    if item["domain"].endswith(".degiro.nl"):
                        # print(f"{item['name']}, {item['value']}")
                        self.__session.cookies.set(item["name"], item["value"])
            if headers:
                self.__session.headers.update(headers)
            CachedApi.open_db(self)
        self.__trading_api.connect()
        if not self.__user_token:
            time.sleep(3)
            r = self.get_config()
            logger.debug(f"{str(r)} -> user token = {self.__user_token}")
        if self.__user_token:
            self.__quotecast_api = QuotecastAPI(user_token=self.__user_token)
            session_id = self.__trading_api.connection_storage.session_id
            logger.warning(f"You are now connected, with the session id:{session_id[:5]}*********{session_id[len(session_id)-6:]}")
            time.sleep(3)
        else:
            logger.fatal("Logging failed : no user token")

    def logout(self):
        self.__trading_api.logout()
        CachedApi.close(self)

    # def get_config(self):
    #    return self.__trading_api.credentials

    def get_config(self, **kwargs):
        k = "get_config" + str(kwargs)
        logger.debug(f"{k}")
        r = None  # self.cache_get(k,3600)
        if r is None:
            r = self.__trading_api.get_config()
            self.cache_set(k, 10, r)
        # print(r)
        self.__user_token = r["clientId"]
        # print(f"token:{self.__user_token}")
        return r

    def get_client_details(self, **kwargs):
        k = "get_client_details" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 10)
        if r is None:
            r = self.__trading_api.get_client_details(**kwargs)
            self.cache_set(k, 10, r)
        # print(r)
        self.__trading_api.credentials.int_account = r["data"]["intAccount"]
        # print(f"intAccount:{self.__trading_api.credentials.int_account}")
        return r

    """
    def get_portfolio(self):
        logger.debug("get_portfolio")
        request_list = Update.RequestList()
        request_list.values.extend(
            [
                Update.Request(option=Update.Option.PORTFOLIO, last_updated=0),
            ]
        )
        return self.__trading_api.get_update(request_list=request_list)
    """

    def get_list_list(self):
        return self.__trading_api.get_favourites_list(raw=True)

    def create_favourite_list(self, **kwargs):
        return self.__trading_api.create_favourite_list(**kwargs)

    def delete_favourite_list(self, **kwargs):
        return self.__trading_api.delete_favourite_list(**kwargs)

    def put_favourite_list_product(self, **kwargs):
        return self.__trading_api.put_favourite_list_product(**kwargs)

    def get_products_config(self, **kwargs):
        k = "get_products_config" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24)
        # logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_products_config(**kwargs)
            self.cache_set(k, 3600 * 24, r)
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
        k = "get_company_ratios" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24 * 7)
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_company_ratios(**kwargs)
            self.cache_set(k, 3600 * 24 * 7, r)
        try:
            codes = {}
            # if 'data' in r:
            #    print(r['data'].keys())
            if "data" in r and "currentRatios" in r["data"] and "ratiosGroups" in r["data"]["currentRatios"]:
                for an in r["data"]["currentRatios"]["ratiosGroups"]:
                    for i in an["items"]:
                        v = i.get("value") or np.nan  # value
                        t = i.get("type") or None  # type of parameter
                        k = i.get("id") or None  # name of parameter
                        m = i.get("name") or ""  # meaning
                        if t == "N" and not pd.isna(v):
                            v = float(v)
                        # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                        # if not m.__contains__(" per "):
                        #    v = v * 1  # 000000
                        if k:
                            codes[k] = {"meaning": m, "value": v}

            if "data" in r and "forecastData" in r["data"] and "ratios" in r["data"]["forecastData"]:
                for i in r["data"]["forecastData"]["ratios"]:
                    # print(i)
                    v = i.get("value") or np.nan  # value
                    t = i.get("type") or None  # type of parameter
                    k = i.get("id") or None  # name of parameter
                    m = i.get("name") or ""  # meaning
                    if t == "N" and not pd.isna(v):
                        v = float(v)
                    # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                    # if not m.__contains__(" per "):
                    #    v = v * 1  # 000000
                    if k:
                        codes[k] = {"meaning": m, "value": v}

            if "data" in r and "consRecommendationTrend" in r["data"] and "ratings" in r["data"]["consRecommendationTrend"]:
                for i in r["data"]["consRecommendationTrend"]["ratings"]:
                    # print(i)
                    v = i.get("value") or np.nan  # value
                    k = ("ratings_" + i.get("periodType")) or None  # name of parameter
                    if t == "N" and not pd.isna(v):
                        v = float(v)
                    # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                    # if not m.__contains__(" per "):
                    #    v = v * 1  # 000000
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
        except BaseException:
            pass
        return codes

    def get_financial_statements(self, **kwargs):
        k = "get_financial_statements" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24 * 7)
        # logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_financial_statements(**kwargs)
            self.cache_set(k, 3600 * 24 * 7, r)
        codes_array = []
        if r:
            try:
                for t in ("annual", "interim"):
                    if t in r["data"]:
                        for an in r["data"][t]:
                            """
                            endDate = datetime.strptime(
                                an.get("endDate"), "%Y-%m-%d"
                            )  # T%H:%M:%S')
                            fiscalYear = an.get("fiscalYear")
                            periodNumber = an.get("periodNumber") or "Y"
                            """
                            codes = {}
                            for st in an["statements"]:
                                # periodLength = st.get("periodLength")
                                # periodType = st.get("periodType")
                                for i in st["items"]:
                                    v = i.get("value") or np.nan
                                    if not pd.isna(v):
                                        v = float(v)
                                    # if not i.get("meaning").__contains__(" per "):
                                    #    v = v * 1  # 000000
                                    codes[i.get("code")] = {
                                        "meaning": i.get("meaning"),
                                        "value": v,
                                    }
                            codes_array += [codes]
            except BaseException:
                # print(k)
                # traceback.print_exc()
                # del self.cache_get(k)
                pass
        return codes_array

    def get_estimates_summaries(self, **kwargs):
        k = "get_estimates_summaries_" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24 * 7)
        # print("get_estimates_summaries cache hit", type(r))
        if r is None or isinstance(r, str):
            r = self.__trading_api.get_estimates_summaries(**kwargs)
            # print("get_estimates_summaries cache miss", type(r))
            self.cache_set(k, 3600 * 24 * 7, r)
        return r

    def get_products_info(self, **kwargs):
        k = "get_products_info" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24 * 7)
        # print("get_products_info cache hit", r)
        if r is None:
            r = self.__trading_api.get_products_info(**kwargs)
            # print("get_products_info cache miss", r)
            self.cache_set(k, 3600 * 24 * 7, r)
        return r

    """
    def get_chart(self, **kwargs):
        k = self.computeIndex("get_chart", **kwargs)
        r = self.cache_get(k)
        # print("get_chart cache hit", r)
        if r is None:
            try:
                r = self.__quotecast_api.get_chart(**kwargs)
            except:
                self.cache_set(k, None)
                print("!! k")
            # print("get_chart cache miss", r)
            self.cache_set(k, r)
        return r
    """

    def product_search(self, **kwargs):
        k = "product_search" + str(kwargs)
        logger.debug(k)
        r = self.cache_get(k, 3600 * 24 )
        if r is None or isinstance(r, str) or (not hasattr(r, "products")) or r.products is None or len(r.products) == 0:
            logger.warning(f"{k} --> connecting since empty or unknown")
            r = self.__trading_api.product_search(**kwargs)
            logger.debug(str(r))
            #        if hasattr(r, 'products'):
            self.cache_set(k, 3600 * 24 , r)
        else:
            logger.debug(f"{k} (from cache)")

        #        else:
        #            r = None
        return r

    def get_longtermprice(self, vwdIdSecondary: str, period, resolution):
        k = f"get_longtermprice7{vwdIdSecondary}{period}{resolution}"
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24*3)
        if r is None:
            try:
                user_token = self.__user_token
                if user_token is None:
                    return None
                chart_fetcher = ChartFetcher(user_token=user_token)
                chart_request = ChartRequest(
                    culture="fr-FR",
                    period=period,
                    requestid="1",
                    resolution=resolution,
                    series=[
                        "ohlc:" + vwdIdSecondary,
                    ],
                    tz="UTC",
                )
                r = chart_fetcher.get_chart(
                    chart_request=chart_request,
                    raw=True,
                )
                if isinstance(r, dict):
                    self.cache_set(k, 3600 * 24*3, r)
                else:
                    logger.warning(f"{k} NO requestid {r}")
            except Exception as ee:
                # print(f"get_longtermprice Error chart \"{vwdIdSecondary}\" {len(vwdIdSecondary)}")
                logger.debug(ee)
                # print(repr(ee))
                # traceback.print_exc()

        if isinstance(r, dict):
            try:
                r = pd.DataFrame(r["series"][0]["data"], columns=["timestamp", "open", "high", "low", "close"])
                if r.shape[0] == 0:
                    r = None
            except Exception as ee:
                logger.debug(repr(ee))
                r = None

        return r

    """
    def get_realTimePrice(self, vwdId: list):
        logger.debug("get_realTimePrice")
        request = QuotecastAPI.Request()
        for vid in vwdId:
            request.subscriptions[vid].extend(
                [
                    "LastDate",
                    "LastTime",
                    "LastPrice",
                    "LowPrice",
                    "HighPrice",
                ]
            )
        ticker_dict = self.__quotecast_api.fetch_metrics(
            request=request,
        )
        return ticker_dict
    """

    def get_company_profile(self, **kwargs):
        k = "get_company_profile" + str(kwargs)
        logger.debug(f"{k}")
        r = self.cache_get(k, 3600 * 24 * 7)
        # logger.debug(f"{k} {type(r)}")
        if r is None or isinstance(r, str):
            # searching on Degiro
            r = self.__trading_api.get_company_profile(product_isin=kwargs["product_isin"], raw=kwargs["raw"])
            self.cache_set(k, 3600 * 24 * 7, r)

        codes = {}
        if r is not None and "data" in r:
            r_data = r["data"]
            # print(f"\n{r_data}")
            try:
                codes["sector"] = r_data["sector"]
            except BaseException:
                pass
            try:
                codes["industry"] = r_data["industry"]
            except BaseException:
                pass
            try:
                codes["country"] = r_data["contacts"]["COUNTRY"]
            except BaseException:
                pass
            try:
                codes["shrOutstanding"] = float(r_data["shrOutstanding"]) / 10**6
            except BaseException:
                pass
            try:
                codes["businessSummary"] = r_data["businessSummary"]
            except BaseException:
                pass

            try:
                if "ratios" in r_data and "ratiosGroups" in r_data["ratios"]:
                    for an in r_data["ratios"]["ratiosGroups"]:
                        for i in an["items"]:
                            v = i.get("value") or np.nan  # value
                            t = i.get("type") or None  # type of parameter
                            k = i.get("id") or None  # name of parameter
                            m = i.get("name") or ""  # meaning
                            if t == "N" and not pd.isna(v):
                                v = float(v)
                            # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                            # if not m.__contains__(" per "):
                            #    v = v * 1  # 000000
                            if k:
                                codes[k] = {"meaning": m, "value": v}
                if "forecastData" in r_data and "ratios" in r_data["forecastData"]:
                    for i in r_data["forecastData"]["ratios"]:
                        # print(i)
                        v = i.get("value") or np.nan  # value
                        t = i.get("type") or None  # type of parameter
                        k = i.get("id") or None  # name of parameter
                        m = i.get("name") or ""  # meaning
                        if t == "N" and not pd.isna(v):
                            v = float(v)
                        # elif t == 'D': v = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S') #pd.to_datetime(v)
                        # if not m.__contains__(" per "):
                        #    v = v * 1  # 000000
                        if k:
                            codes[k] = {"meaning": m, "value": v}
            except BaseException:
                pass
        else:
            """
            # searching on Yahoo! finance
            try:
                r = self.cache_get("Y_" + k)
            except BaseException:
                sym = None  # yf.Ticker(kwargs['product_isin'])
                r = sym.info
                try:
                    r["marketCap"] /= 1000.0
                except BaseException:
                    pass
                self.cache_set("Y_" + k, r)
                logger.debug(f"OK from Yahoo {kwargs['product_isin']}")
            codes = r
            """
        return codes
