import csv
import json
import logging
import numpy as np
import os
import pandas as pd
import sys
import traceback
import warnings
from DictObj import DictObj
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from degiro_connector.quotecast.models.chart import Interval
from degiro_connector.trading.models.credentials import build_credentials
from degiro_connector.trading.models.product_search import StocksRequest
from ranking import compute_rank, ranking 
from telegram import send_doc_to_telegram
from typing import Any, Dict, List, Optional, Set, Tuple
from utils import crapy_estimates_summaries_get
from xvfb import openWindow
import locale

"""
import http.client
http.client.HTTPConnection.debuglevel = 5
"""
from cachedDegiroApi import cachedDegiroApi
from cachedYahooApi import CachedYahooApi
from cachedfaz import CachedFrankfurter

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

isinDebug = "JP3860220007"
filterCountry = None
logger = logging.getLogger()    

def assess_map(product: Dict[str, Any]) -> Dict[str, Any]:
    p = DictObj(dict(product))
    row = {}

    try:
        try:
            row["symbol"] = p.symbol
        except BaseException:
            row["symbol"] = p.isin
        row["isin"] = p.isin

        if "isinDebug" in globals() and p.isin == isinDebug:
            logger.fatal(json.dumps(product))
        row["id"] = p.id
        row["vwdId"] = f"{p.vwdIdentifierType}:{p.vwdId}" if hasattr(p, "vwdId") and hasattr(p, "vwdIdentifierType") else ""
        row["vwdIdSecondary"] = (
            f"{p.vwdIdentifierTypeSecondary}:{p.vwdIdSecondary}" if hasattr(p, "vwdIdSecondary") and hasattr(p, "vwdIdentifierTypeSecondary") else ""
        )

        row["name"] = p.name.upper()
        row["closePrice"] = p.closePrice if hasattr(p, "closePrice") else np.nan
        row["closePriceDate"] = np.nan
        if hasattr(p, "closePriceDate"): 
            da = p.closePriceDate
            if isinstance(da, str):
                aujourd_hui = datetime.now().date()
                date_cible = datetime.strptime(da, "%Y-%m-%d").date()
                difference = (aujourd_hui - date_cible).days
                row["closePriceDate"] = date_cible
                row["closePriceAgeDays"] = difference


        row["currency"] = row["quoteCurrency"] = p.currency

        company_profile = None
        try:
            company_profile = trading_api.get_company_profile(product_isin=p.isin, raw=True)
        except BaseException:
            pass
        if company_profile is None:
            company_profile = {}
        # if hasattr(p, "vwdIdSecondary") row["businessSummary"] = company_profile['businessSummary']

        est_summary = None
        try:
            est_summary = trading_api.get_estimates_summaries(product_isin=p.isin, raw=True)
        except BaseException:
            pass
        if est_summary is None:
            est_summary = {}
        else:
            est_summary = crapy_estimates_summaries_get(est_summary)  # get only current trailing month

        company_ratios = None
        try:
            company_ratios = trading_api.get_company_ratios(product_isin=p.isin, raw=True)
        except BaseException:
            pass
        if company_ratios is None:
            company_ratios = {}

        row = {**row, **company_profile, **company_ratios, **est_summary}

        if "isinDebug" in globals() and row["isin"] == isinDebug:
            logger.fatal(f"row: {str(row)}")

        row2 = {}
        for key, value in row.items():
            if type(value) is dict:
                row2[key] = value["value"]
            else:
                row2[key] = value
        row = row2
        
        if row.get("MKTCAP"):
            oldcap = row["MKTCAP"]
            newcap = -1
            oldcur = row.get("priceCurrency", "")
            if not len(oldcur):
                oldcur = row.get("reportCurrency", "")
            if not len(oldcur):
                oldcur = row.get("currency", "")
            if not len(oldcur):
                oldcur = row.get("quoteCurrency", "")
            if oldcur != "USD" and len(oldcur) > 0:
                if oldcur == "BPN":
                    oldcur2 = "GBP"
                elif oldcur == "GBX":
                    oldcur2 = "GBP"
                else:
                    oldcur2 = oldcur
                rate = forex_api.convert(oldcur2, "USD")
                if isinstance(rate, float) and rate > 0.0:
                    newcap = rate * float(oldcap)
                    if oldcur == "GBX":
                        newcap /= 100.0
                    # logger.fatal(f"{row["name"]} {row["isin"]} {oldcap} {oldcur} -> {newcap:.2f} USD")
                    row["MKTCAP.USD"] = newcap
                else:
                    logger.fatal(f"{p.isin} cannot convert {row['MKTCAP']}  priceCurrency=\"{row.get('priceCurrency', '')}\"  reportCurrency=\"{row['reportCurrency']}\" currency=\"{row.get('currency', '')}\" quoteCurrency=\"{row.get('quoteCurrency', '')}\"    ")
            elif oldcur == "USD":
                row["MKTCAP.USD"] = row["MKTCAP"]

        if row.get("businessSummary"):
            row["businessSummary"] = row["businessSummary"].replace('"', " ")

        row["%M200D"] = np.nan
        row["ChPctPrice5Y"] = np.nan
        try:
            df = None
            if row["vwdId"] and len(row["vwdId"]) > 0:
                df = trading_api.get_longtermprice(row["vwdId"], Interval.P5Y, Interval.P1M)
            if (df is None or df.shape[0] == 0) and row["vwdIdSecondary"] and len(row["vwdIdSecondary"]) > 0:
                df = trading_api.get_longtermprice(row["vwdIdSecondary"], Interval.P5Y, Interval.P1M)
            if df is not None and df.shape[0] > 1:
                LastMonthClose = df.iloc[-1]["close"]
                if "closePriceAgeDays" in row and row['closePriceAgeDays'] < 15 and "closePrice" in row and row["closePrice"] > 0:
                    if (LastMonthClose-row["closePrice"])/row["closePrice"] < .30:
                        LastMonthClose = row["closePrice"]
                    else:
                        logger.warning(f"company: \"{row['name']}\" Won't update properly 'ChPctPrice5Y' since prices are too different... LastMonthClose:{LastMonthClose}  Last close: {row['closePrice']}  last close date:{row['closePriceDate']}")
                row["ChPctPrice5Y"] = (pow(1 + (LastMonthClose - df.iloc[0]["open"]) / df.iloc[0]["open"], 1 / (df.shape[0] / 12)) - 1) * 100
                if row["isin"] == isinDebug:
                    msg = (
                        f"company: \"{row['name']}\" 5YCAGR:{row['ChPctPrice5Y']:.1f}% nbRows:{df.shape[0]} "
                        f"open:{df.iloc[0]['open']} close:{LastMonthClose} "
                        f" last close: {row['closePrice']} last close date:{row['closePriceDate']} age:{row['closePriceAgeDays']}\n"
                    )
                    print(msg, df)
            elif row["isin"] == isinDebug:
                logger.fatal(f"company: \"{row['name']}\" 5YCAGR:{row['ChPctPrice5Y']} df:{df} last close date:{row['closePriceDate']} age:{row['closePriceAgeDays']}")

        except Exception as ee:
            print(f"286 error {row['name']}")
            print(ee)
            print(repr(ee))
            traceback.print_exc()

        row["YSymbol"] = np.nan
        if np.isnan(row["ChPctPrice5Y"]):
            try:
                df, ylabel = yahoo_api.get_longtermprice(row["isin"], row["symbol"], row["name"], "5y", "1mo")
                if df is not None:
                    if df.shape[0] > 1:
                        row["ChPctPrice5Y"] = (
                            pow(1 + (df.iloc[-1]["Close"] - df.iloc[0]["Open"]) / df.iloc[0]["Open"], 1 / (df.shape[0] / 12)) - 1
                        ) * 100
                        row["YSymbol"] = ylabel
                elif row["isin"] == isinDebug:
                    logger.fatal(f"yahoo company: \"{row['name']}\" 5YCAGR:{row['ChPctPrice5Y']} df:{df} last close: {row['closePrice']} last close date:{row['closePriceDate']} age:{row['closePriceAgeDays']}")
            except Exception as ee:
                print(f"303 error {row['name']}")
                print(ee)
                print(repr(ee))
                traceback.print_exc()
        elif row["isin"] == isinDebug:
            logger.fatal(f"after yahoo company: \"{row['name']}\" 5YCAGR:{row['ChPctPrice5Y']:.1f}%")

        #if "VOL10DAVG" in row and "MKTCAP.USD" in row and "shrOutstanding" in row:
        #    if row["VOL10DAVG"] is not None and row["shrOutstanding"] is not None and row["MKTCAP.USD"] is not None:
        #        row["Vol10D.USD"] = row["VOL10DAVG"] / row["shrOutstanding"] * row["MKTCAP.USD"] / 10**6

    except Exception as e:
        print(e)
        print(repr(e))
        traceback.print_exc()

    return row


def myassess(country: str, stock_list: Any, info_df: pd.DataFrame) -> pd.DataFrame:

    try:
        if hasattr(stock_list, "products"):
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                results = executor.map(assess_map, stock_list.products)
            row_df = pd.DataFrame(results)
            if info_df.shape[0] == 0:
                info_df = row_df
            else:
                info_df = pd.concat([info_df, row_df], ignore_index=True)
        else:
            print("Stock market as no product", country)
    except Exception as e:
        print(e)
        print(repr(e))
        traceback.print_exc()
    return info_df


def access1country(li_id: int, ctry: str, df: pd.DataFrame, errCounter: int, errCtry: Set[str]) -> Tuple[int, Set[str], pd.DataFrame]:
    limit = 100
    for page in range(0, 100):
        request_stock = StocksRequest(
            stock_country_id=li_id,
            limit=limit,
            offset=page * limit,
            require_total=True,
        )
        stock_list = trading_api.product_search(product_request=request_stock, raw=False)
        if hasattr(stock_list, "products") and stock_list.products is not None:
            size = len(stock_list.products)
            logger.warning(f"country:{ctry} list:All ({size} stocks for page {page + 1})")
            # dowload data for all stocks in the list. It's multi-thread !!
            if stock_list:
                df = myassess(ctry, stock_list, df)
            if size != limit:
                break
        else:
            logger.critical(f"Empty product list for {ctry} page {page + 1}")
            errCounter = errCounter + 1
            errCtry.add(ctry)
            break
    # end of page loop
    return errCounter, errCtry, df


def compute(df: pd.DataFrame) -> pd.DataFrame:
    epsilon = 10**-9
    cap = 999

    for colname in [
        "businessSummary", "name", "sector", "industry", "country", "MKTCAP", "ratings_CURR", "ratings_1WA", "reportCurrency", "NPMTRENDGR", "PR1DAYPRC", "PR5DAYPRC", "ChPctPriceMTD", 
        "ChPctPrice5Y", "YSymbol", "AROE5YAVG", "closePriceDate", "Focf2Rev_AAvg5", "MARGIN5YR", "REVPS5YGR", "%M200D", "__nprice", "ACURRATIO", "AEBITD", "ANIAC", "APENORM", "AREV", 
        "ATANBVPS", "CAPI/TANG", "closePrice", "Dette nette / EBITDA", "Dette nette", "DivYield_CurTTM", "EBITDA", "EV", "EV2FCF_CurTTM", "Juste Prix", "Net Income", "NetDebt_A", 
        "NetDebt_I", "NHIG", "NLOW", "NPRICE", "PEINCLXOR", "PER", "priceCurrency", "ProjPE", "QCURRATIO", "QTANBVPS", "quoteCurrency", "Ratio courant", "Rendement", "shrOutstanding", 
        "TTMNIAC", "TTMREV", "VE/CA", "VE/FCF", "YLD5YAVG", "EV", "TTMFCF", "VE/FCF","YLD+PRY"
    ]:
        if colname not in df.columns:
            df[colname] = np.nan

    df.loc[df["Net Income"].isna(), "Net Income"] = df["TTMNIAC"] / 10**6
    df.loc[df["Net Income"].isna(), "Net Income"] = df["ANIAC"] / 10**6
    df.loc[:, "__nprice"] = df["NPRICE"]
    df.loc[:, "L%H"] = (df["__nprice"] - df["NLOW"]) / (df["NHIG"] - df["NLOW"])
    df.loc[:, "%M200D"] = (df["closePrice"] - df["%M200D"]) / df["%M200D"]
    df.loc[df["EBITDA"].isna(), "EBITDA"] = df["AEBITD"] / 10**6
    df.loc[:, "VE/EBITDA"] = (df["EV"].clip(lower=epsilon) / df["EBITDA"].clip(lower=epsilon) / 10**6).clip(upper=cap)
    df.loc[:, "VE/CA"] = (df["EV"].clip(lower=epsilon) / df["TTMREV"].clip(lower=epsilon)).clip(upper=cap)
    df.loc[df["VE/CA"].isna(), "VE/CA"] = (df["EV"].clip(lower=epsilon) / df["AREV"].clip(lower=epsilon)).clip(upper=cap)
    df.loc[:, "CAPI/TANG"] = (df["__nprice"] / df["QTANBVPS"].clip(lower=epsilon)).clip(upper=cap)
    df.loc[df["CAPI/TANG"].isna(), "CAPI/TANG"] = (df["__nprice"] / df["ATANBVPS"].clip(lower=epsilon)).clip(upper=cap)
    df.loc[:, "PER"] = df["PEINCLXOR"]
    df.loc[df["PER"].isna(), "PER"] = df["APENORM"]
    df.loc[df["PER"].isna(), "PER"] = df["ProjPE"]
    df.loc[:, "Rendement"] = df["YLD5YAVG"]
    df.loc[df["Rendement"].isna(), "Rendement"] = df["DivYield_CurTTM"]
    df.loc[df["Rendement"].isna(), "Rendement"] = epsilon
    df.loc[:, "Dette nette"] = df["NetDebt_I"]
    df.loc[df["Dette nette"].isna(), "Dette nette"] = df["NetDebt_A"]
    df.loc[df["EBITDA"].isna() | (df["EBITDA"] < 0), "EBITDA"] = epsilon
    df.loc[:, "Dette nette / EBITDA"] = (df["Dette nette"] / df["EBITDA"] / 10**6).clip(upper=cap, lower=epsilon)
    df.loc[
        df["Dette nette / EBITDA"].isna() & (df["Dette nette"] <= 0),
        "Dette nette / EBITDA",
    ] = epsilon
    df.loc[
        df["Dette nette / EBITDA"].isna() & (df["Dette nette"] > 0),
        "Dette nette / EBITDA",
    ] = cap
    df.loc[:, "Ratio courant"] = df["QCURRATIO"]
    df.loc[df["Ratio courant"].isna(), "Ratio courant"] = df["ACURRATIO"]
    df.loc[df["Ratio courant"].isna(), "Ratio courant"] = epsilon
    df.loc[:, "VE/FCF"] = df["EV2FCF_CurTTM"]
    df.loc[(df["VE/FCF"] <= 0) | df["VE/FCF"].isna(), "VE/FCF"] = epsilon
    df.loc[:, "VE/FCF"] = df["VE/FCF"].clip(lower=epsilon, upper=cap)
    df.loc[:, "Juste Prix"] = (df["Net Income"] * df["PER"] - df["Dette nette"] / 10**6) / df["shrOutstanding"]
    df.loc[(df["Juste Prix"] <= 0) | (df["Juste Prix"].isna()), "Juste Prix"] = epsilon
    df.loc[df["Juste Prix"] > df["__nprice"], "En Solde"] = (df["Juste Prix"] - df["__nprice"]) / df["Juste Prix"]
    df.loc[df["Juste Prix"] <= df["__nprice"], "En Solde"] = (df["Juste Prix"] - df["__nprice"]) / df["__nprice"]

    df.loc[df["Rendement"].isna(), "Rendement"] = 0.0
    df.loc[df["YLD+PRY"].isna(), "YLD+PRY"] = 0.0
    df.loc[:, "YLD+PRY"] = df["Rendement"] + df["ChPctPrice5Y"]
    
    df = df.drop("__nprice", axis=1)
    
    return df


def getAll(cookies: Any, headers: Optional[Dict[str, str]], credentials: Any, basedir: str) -> pd.DataFrame:
    global trading_api
    global yahoo_api
    global forex_api

    trading_api = cachedDegiroApi(os.path.join(basedir, "cacheDegiro.bin"), credentials)
    yahoo_api = CachedYahooApi(os.path.join(basedir, "cacheYahoo.bin"))
    forex_api = CachedFrankfurter(os.path.join(basedir, "cacheFrankfurter.bin"))

    suspectError = 0
    suspectCountries = set()

    # this is the main dataframe that will be filled up
    info_df = pd.DataFrame()

    for i in range(1, 20):
        trading_api.connect(cookies=cookies, headers=headers)
        suspectError = 0
        try:
            # get all product list, countries, marketplaces
            trading_api.get_products_config()
            # get IntAccount
            trading_api.get_client_details()
            # stocked are browsed from counties(, and not marketplaces). This is the most reliable to get all stocks
            for li_dict in trading_api.stockCountries:
                li = DictObj(li_dict)
                country = trading_api.countries[li.country].name
                if "filterCountry" in globals() and filterCountry is not None and (country not in filterCountry):
                    logger.debug(f"Skipping {country}")
                    continue
                if i > 2 and country not in suspectCountries:
                    logger.debug(f"Looping only on suspected buggy countries, skipping {country}")
                    continue
                suspectError, suspectCountries, info_df = access1country(li.id, country, info_df, suspectError, suspectCountries)
            # end of country loop
        except Exception as e:
            print(e)
            print(repr(e))
            traceback.print_exc()

        trading_api.logout()
        logger.warning(f"Got {suspectError} errors when downloading asset pages")
        if suspectError == 0:
            break
    # end of retries

    del trading_api
    del yahoo_api

    if info_df.shape[0] > 0:
        info_df.reset_index()
        info_df = info_df.sort_values(by=["name"], ascending=False).groupby(["name"]).head(1).reset_index().sort_values(by=["isin"], ascending=False).groupby(["isin"]).head(1).reset_index()
        info_df = compute(info_df)
        logger.warning(f"Number of stock entries after compute: {info_df.shape[0]}")
        
    return info_df


def Screener(cookies: Any, headers: Optional[Dict[str, str]], _isinDebug: Optional[str], _filterCountry: Optional[List[str]]) -> Optional[pd.DataFrame]:
    global isinDebug
    global filterCountry

    isinDebug = _isinDebug
    filterCountry = _filterCountry.copy() if _filterCountry is not None else None

    if headers is None:
        userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "fr,fr-FR;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://trader.degiro.nl/trader/",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": userAgent,
        }

    username = os.getenv("GT_DG_USERNAME") or ""
    password = os.getenv("GT_DG_PASSWORD") or ""
    token = os.getenv("GT_DG_TOKEN") or ""
    basedir = os.getenv("GT_DG_DIRECTORY") or ""


    credentials = build_credentials(
        override={
            "username": username,
            "password": password,
            # "int_account": NUMBER_PLACEHOLDER,  # From `get_client_details`
            "totp_secret_key": token,  # For 2FA
        },
    )

    info_df = getAll(cookies, headers, credentials, basedir)

    if info_df.shape[0] > 0:
        df = compute_rank(info_df, "score", ranking)
        cols = ["score", "MKTCAP.USD"]
        Q = df[cols].quantile(
            numeric_only=True, q=list(np.arange(0.0, 1.01, 0.01).astype(float))
        )
        for c in cols:
            QQ = Q[c].to_numpy()
            df[f"q{c}"] = df.apply(lambda x: np.argmin(QQ < x[c]).astype(int), axis=1)   
        
        cols = ["REVPS5YGR", "MARGIN5YR", "Focf2Rev_AAvg5", "score", "En Solde", "YLD+PRY"]
        qdf = df[cols].copy()
        for c in cols:
            qdf.loc[qdf[c].isna() | qdf[c].isnull(), c] = 0.0
            
        Q = qdf[cols].quantile(
            numeric_only=True, q=list(np.arange(0.0, 1.01, 0.01).astype(float))
        )

        for c in cols:
            qc = f"q{c}"
            QQ = Q[c].to_numpy()
            QQ[1] = QQ[50]  # 'qc' column will get a 1 when 'c' below percentile [], and so won't contribute to scorePerf
            QQ[0] = QQ[20]  # 'qc' column will get a 0 when 'c' below percentile [], and so the final scorePerf =0
            qdf[qc] = qdf.apply(lambda x: np.argmin(QQ < x[c]).astype(int), axis=1)    
            
        qdf["scorePerf"] = 100.0
        
        for c in cols:
            qc = f"q{c}"
            qdf.loc[qdf[qc].notna(), "scorePerf"] *= qdf[qc] ** 2.0    
        
        Q = qdf[qdf["scorePerf"] > 0][["scorePerf"]].quantile(
            numeric_only=True, q=list(np.arange(0.0, 1.01, 0.01).astype(float))
        )
        QQ = Q["scorePerf"].to_numpy()
        df["qscorePerf"] = qdf.apply(lambda x: np.argmin(QQ < x["scorePerf"]).astype(int), axis=1) 
        df["scorePerf"] = qdf["scorePerf"].pow(1.0 / (2.0 * len(cols)))

        return df
    else:
        # dataframe is empty
        return None


def main(cookies: Any, headers: Optional[Dict[str, str]], _isinDebug: Optional[str], _filterCountry: Optional[List[str]]) -> Optional[pd.DataFrame]:
    info_df = Screener(cookies, headers, _isinDebug, _filterCountry)
    if info_df is not None:
        df = info_df.copy()
        #df.reset_index()
        df = df.sort_values(by=["country", "score"], ascending=[True, False])
        # df.reindex(index=list(range(len(df))))
        # logger.warning(f"Number of stock entries after Q: {df.shape[0]}")

        locale.setlocale(locale.LC_NUMERIC, os.getenv("LANG","C"))
        
        df[
            [
                "symbol", "isin", "name", "sector", "industry", "country",  "qscore",  "qscorePerf", "MKTCAP", "REVPS5YGR", 
                "MARGIN5YR", "Focf2Rev_AAvg5", "ratings_CURR", "ratings_1WA", "VE/EBITDA", "VE/CA", "CAPI/TANG", "PER", "Rendement", "Dette nette / EBITDA", 
                "Ratio courant", "VE/FCF", "%M200D", "closePrice", "quoteCurrency", "En Solde", "Juste Prix", "NPRICE", "L%H", "priceCurrency", "reportCurrency", 
                "EV2FCF_CurTTM", "EV", "TTMFCF", "Net Income", "NPMTRENDGR", "Dette nette", "shrOutstanding", "EBITDA", "PR1DAYPRC", "PR5DAYPRC", "ChPctPriceMTD", 
                "ChPctPrice5Y", "YSymbol", "businessSummary", "AROE5YAVG", "YLD+PRY", "PDATE", "qMKTCAP.USD", "VOL10DAVG"
            ]
        ].to_csv("screener4.csv", index=False, sep="\t", decimal=locale.localeconv()["decimal_point"], encoding="utf-8-sig", float_format="%.3f", quoting=csv.QUOTE_MINIMAL)

        daat = "%y-%m-%W"
        df[
            [
                "symbol", "isin", "name", "sector", "industry", "country",  "qscore",  "qscorePerf", "MKTCAP", "REVPS5YGR", "MARGIN5YR",
                "Focf2Rev_AAvg5", "ratings_CURR", "ratings_1WA", "VE/EBITDA", "VE/CA", "CAPI/TANG", "PER", "Rendement", "Dette nette / EBITDA", "Ratio courant",
                "VE/FCF", "%M200D", "closePrice", "quoteCurrency", "En Solde", "Juste Prix", "NPRICE", "L%H", "priceCurrency", "reportCurrency", "EV2FCF_CurTTM",
                "EV", "TTMFCF", "Net Income", "NPMTRENDGR", "Dette nette", "shrOutstanding", "EBITDA", "PR1DAYPRC", "PR5DAYPRC", "ChPctPriceMTD", "ChPctPrice5Y",
                "YSymbol", "AROE5YAVG", "YLD+PRY", "PDATE", "qMKTCAP.USD", "VOL10DAVG"
            ]
        ].to_csv(
            f"screener-{datetime.now().strftime(daat)}.csv",
            index=False,
            sep="\t",
            decimal=locale.localeconv()["decimal_point"],
            encoding="utf-8-sig",
            float_format="%.3f",
            quoting=csv.QUOTE_MINIMAL,
        )

        ddf = df.copy()

        QS = 80.0  # score loic
        QSP = 95.0  # score loic + perf
        REV = 5   # croissance revenu
        MRG = 10   # marge
        SLD = .2  # en solde de x%
        LH = 1  # cours relatif entre le plus bas annuel et le plus haut [0,1]
        YLD = 17  # rendement dividende+prix
        PRX = 0   # croissance annuelle du prix de l'action, sans les dividendes
        ddf = ddf[
            (ddf["qscore"] >= QS)
            & (ddf["qscorePerf"] >= QSP)
            & (ddf["REVPS5YGR"] >= REV)
            & (ddf["MARGIN5YR"] >= MRG)
            & (ddf["En Solde"] >= SLD)
            & (ddf["L%H"] <= LH)
            & (ddf["ChPctPrice5Y"] >= PRX)
            & (ddf["YLD+PRY"] >= YLD)
        ]

        ddf = ddf[
            [
                "isin", "sector", "country", "name", "industry", "qscore", "qscorePerf", "REVPS5YGR", "MARGIN5YR", "PER", "Rendement", "En Solde", 
                "L%H", "ChPctPrice5Y", "qMKTCAP.USD", "VOL10DAVG"
            ]
        ]
        ddf.to_csv("extrait.csv", index=False, sep=";", decimal=locale.localeconv()["decimal_point"], encoding="utf-8-sig", float_format="%.3f", quoting=csv.QUOTE_MINIMAL)


        telegram_token = os.getenv("GT_TL_TOKEN") or ""
        telegram_chatid = os.getenv("GT_TL_CHAT") or ""
        if telegram_token:
            uch = "\u2571"
            daat = f"%Y{uch}%m{uch}%d"
            uch2 = "\u2001"
            msg = (
                f"Screener {datetime.now().strftime(daat)}{uch2}{ddf.shape[0]}{uch}{df.shape[0]}{uch2}"
                f"Ratios: QS={QS:.0f} QSP={QSP:.0f} REV={REV:.1f} MRG={MRG:.1f} SLD={SLD:.2f} LH={LH:.2f} YLD={YLD:.1f}"
            )
            send_doc_to_telegram(
                {"message": {"apiToken": telegram_token, "chatID": telegram_chatid}},
                msg,
                "extrait.csv",
            )

        return df



if __name__ == "__main__":
    logfile = "./outdegiro.log"
    logging_level = logging.WARNING
    logging.basicConfig(
        level=logging_level,
        handlers=[
            logging.FileHandler(logfile, mode="a"),
            logging.StreamHandler(sys.stdout),
        ],
        format="%(asctime)s - %(name)s:%(filename)s:%(funcName)s:%(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()    
    cookies, headers = openWindow()
    main(cookies, headers, None, None)
    exit(0)
