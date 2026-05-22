import csv
from errno import EBUSY
import json
import logging
import numpy as np
import os
import re
import polars as pl
import sys
import traceback
import time
from DictObj import DictObj
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from degiro_connector.quotecast.models.chart import Interval
from degiro_connector.trading.models.credentials import build_credentials
from degiro_connector.trading.models.product_search import StocksRequest
from ranking import compute_rank, ranking 
from telegram import send_doc_to_telegram
from typing import Any, Dict, List, Optional, Set, Tuple
from utils import crapy_estimates_summaries_get, create_text_file, convert2USD
from xvfb import openWindow
import locale
from itertools import repeat
from screenerYahoo import getAllYahoo, compute_from_chart
from cachedDegiroApi import cachedDegiroApi
from cachedYahooApi import CachedYahooApi
from cachedfaz import CachedFrankfurter


'''
import http.client
http.client.HTTPConnection.debuglevel = 5
'''

isinDebug = "JP3860220007"
filterCountry = None
logger = logging.getLogger()    


def compute_scorePerf(df):
    # Quantile calculations for score columns
    cols = ["EPSTRENDGR", "roic", "score", "EnSolde2", "YLD+PRY"]
    weight = [1] * len(cols)
    weight[cols.index("score")] = len(cols) - 1
    sumweight = np.sum(weight)
    
    # Fill nulls with 0
    for c in cols:
        df = df.with_columns(pl.col(c).fill_null(0.0))
        
    # Quantile calculations for score columns
    Q_dict = {}
    for c in cols:
        Q_dict[c] = [df[c].quantile(q) for q in np.arange(0.0, 1.01, 0.01)]
    Q = pl.DataFrame(Q_dict)

    qdf = df.select(cols).clone()
    for c in cols:
        qc = f"q{c}"
        QQ = np.array(Q[c])
        QQ[1] = QQ[50]  # 'qc' column will get a 1 when 'c' below percentile [], and so won't contribute to scorePerf
        QQ[0] = QQ[20]  # 'qc' column will get a 0 when 'c' below percentile [], and so the final scorePerf =0
        qdf = qdf.with_columns(
            pl.col(c).map_elements(lambda x: np.argmin(QQ < x)/10.0, return_dtype=pl.Float64).alias(qc)
        )
        
    qdf = qdf.with_columns(pl.lit(100.0).alias("scorePerf"))
    
    for i, c in enumerate(cols):
        qc = f"q{c}"
        qdf = qdf.with_columns(
            (pl.col("scorePerf") * (pl.col(qc).fill_null(1.0) ** (2.0 * weight[i]))).alias("scorePerf")
        )
    
    # Quantile for scorePerf
    qdf_filtered = qdf.filter(pl.col("scorePerf") > 0)
    if qdf_filtered.shape[0] > 0:
        Q = [qdf_filtered["scorePerf"].quantile(q) for q in np.arange(0.0, 1.01, 0.01)]
    else:
        Q = [0.0] * 101
    QQ = np.array(Q)
    df = df.with_columns(
        qdf["scorePerf"].map_elements(lambda x: int(np.argmin(QQ < x)), return_dtype=pl.Int64).alias("qscorePerf"),
        qdf["scorePerf"].pow(1.0 / (2.0 * sumweight)).alias("scorePerf")
    )#.drop("scorePerf").rename({"tmpscorePerf": "scorePerf", })
    
    return df



def assess_map(product: Dict[str, Any], country:str) -> Dict[str, Any]:
    row = {}
    
    try:
        p = DictObj(dict(product))
        try:
            row["symbol"] = p.symbol
        except BaseException:
            row["symbol"] = p.isin
        row["isin"] = p.isin
        row["country"] = country
        row["name"] = p.name.upper()
        
        if "isinDebug" in globals() and p.isin == isinDebug:
            logger.fatal(json.dumps(product))
        row["id"] = p.id
        row["vwdId"] = f"{p.vwdIdentifierType}:{p.vwdId}" if hasattr(p, "vwdId") and hasattr(p, "vwdIdentifierType") else ""
        row["vwdIdSecondary"] = (
            f"{p.vwdIdentifierTypeSecondary}:{p.vwdIdSecondary}" if hasattr(p, "vwdIdSecondary") and hasattr(p, "vwdIdentifierTypeSecondary") else ""
        )

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
            if "isinDebug" in globals() and row["isin"] == isinDebug:
                logger.fatal(f"company profile: {str(company_profile)}")
        except BaseException:
            pass
        if company_profile is None:
            company_profile = {}
        # if hasattr(p, "vwdIdSecondary") row["businessSummary"] = company_profile['businessSummary']

        est_summary = None
        try:
            est_summary = trading_api.get_estimates_summaries(product_isin=p.isin, raw=True)
            if "isinDebug" in globals() and row["isin"] == isinDebug:
                logger.fatal(f"estimates summary: {str(est_summary)}")
        except BaseException:
            pass
        if est_summary is None:
            est_summary = {}
        else:
            est_summary = crapy_estimates_summaries_get(est_summary)  # get only current trailing month

        company_ratios = None
        try:
            company_ratios = trading_api.get_company_ratios(product_isin=p.isin, raw=True)
            if "isinDebug" in globals() and row["isin"] == isinDebug:
                logger.fatal(f"company ratios: {str(company_ratios)}")        
        except BaseException:
            pass
        if company_ratios is None:
            company_ratios = {}
            
        financial_statements = None
        try:
            financial_statements   = trading_api.get_financial_statements(product_isin=p.isin, raw=True)
            if "isinDebug" in globals() and row["isin"] == isinDebug:
                logger.fatal(f"financial statements: {str(financial_statements)}")        
        except Exception as eee:
            logger.debug(f"286 error {row['name']}")
            logger.debug(eee)
            logger.debug(repr(eee))
            traceback.print_exc()
        if financial_statements is None:
            financial_statements = {}

        try:
            str_version = f"Company={str(row)}\n\nCompany_profile={str(company_profile)}\n\nFinancial_statements={str(financial_statements)}\n\nCompany_ratios={str(company_ratios)}\n\nEstimate_summary={str(est_summary)}\n"
            row = {**row, **company_profile, **company_ratios, **est_summary, **financial_statements}
            # column wirh string version of all data, to produce a dedicated file per asset later on
            row['row'] = str_version
            # removing some column to avoid memory issue
            pattern = re.compile(r'^[QHY][0-9].*/')
            now = datetime.now()
            current_year = now.year
            pattern2 = re.compile(rf'^Y(?:{current_year}|{current_year - 1}|{current_year - 2}).*')
            filtered_list = [s for s in row.keys() if pattern.match(s) and not pattern2.match(s)]
            if "isinDebug" in globals() and row["isin"] == isinDebug:
                logger.fatal(pattern)           
                logger.fatal(pattern2)           
                logger.fatal(filtered_list)
            for col in filtered_list:
                del row[col]
        except BaseException:
            logger.fatal(f"row:{type(row)}, company_profile:{type(company_profile)}, company_ratios:{type(company_ratios)}, est_summary:{type(est_summary)}, ")
                        
        if "isinDebug" in globals() and row["isin"] == isinDebug:
            logger.fatal(f"row: {str(row)}")

        row2 = {}
        for key, value in row.items():
            if type(value) is dict:
                row2[key] = value["value"]
            else:
                row2[key] = value
        row = row2
        
        row["MKTCAP.USD"] = convert2USD(forex_api, row, "MKTCAP")
        if isinstance(row["MKTCAP.USD"], float) and not(row["MKTCAP.USD"] != row["MKTCAP.USD"]):
            row["MKTCAP.USD"] = int(row["MKTCAP.USD"])

        if row.get("businessSummary"):
            row["businessSummary"] = row["businessSummary"].replace('"', " ")


        if "NPRICE" not in row and 'closePrice' in row:
            row['NPRICE'] = row['closePrice']
        if "NPRICE" not in row:
            row['NPRICE'] = 0
            
        try:
            df = None
            if row["vwdId"] and len(row["vwdId"]) > 0:
                df = trading_api.get_longtermprice(row["vwdId"], Interval.P5Y, Interval.P1W)
            if isinstance(df, pl.DataFrame) and df.shape[0] > 0:
                pass
            elif row["vwdIdSecondary"] and len(row["vwdIdSecondary"]) > 0:
                df = trading_api.get_longtermprice(row["vwdIdSecondary"], Interval.P5Y, Interval.P1W)
            if isinstance(df, pl.DataFrame) and df.shape[0] > 0:
                row = {**row, **compute_from_chart(df, row["NPRICE"], row["name"])}                   
        except Exception as ee:
            logger.debug(f"286 error {row['name']}")
            logger.debug(ee)
            logger.debug(repr(ee))
            traceback.print_exc()
        
        row["YSymbol"] = ""
        ylabel = None
        try :
            relatedExchanges = []
            if country in trading_api.country2hiqAbbrs:
                relatedExchanges = list(trading_api.country2hiqAbbrs[country])
            ylabel = yahoo_api.product_search(row["isin"], row["symbol"], row["name"], relatedExchanges)
            if ylabel:
                row["YSymbol"] = ylabel
        except:
            logger.warning(f"Error yahoo_api.product_search {row['isin']}")
            traceback.print_exc()
            pass
        
        if ylabel and isinstance(ylabel, str) and "ChPctPrice5Y" not in row:
            try:
                df = yahoo_api.get_longtermprice(ylabel, "5y", "1wk")
                if isinstance(df, pl.DataFrame) and df.shape[0] > 0:
                    df = df.rename({"Close": "close", "Volume": "volume", "Open": "open"})
                    row = {**row, **compute_from_chart(df, row["NPRICE"], row["name"])}
            except Exception as ee:
                logger.debug(f"303 error {row['name']}")
                logger.debug(ee)
                logger.debug(repr(ee))
                traceback.print_exc()
        
        # removing heavy content to save RAM on my PC
        if "businessSummary" in row:
            row['businessSummary'] = ""
        if "row" in row:
            row['row'] = ""
    
    except Exception as e:
        logger.fatal(e)
        logger.debug(repr(e))
        traceback.print_exc()

    return row


def myassess(country: str, stock_list: Any) -> list:
    results = list()
    try:
        if hasattr(stock_list, "products"):
            logger.debug(f"creating threads with {len(stock_list.products)} products to search")
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                results = list(executor.map(assess_map, stock_list.products, repeat(country)))
            '''
            row_df = pl.DataFrame(results)
            if info_df.shape[0] == 0:
                info_df = row_df
            else:
                info_df = pl.concat([info_df, row_df], how="diagonal_relaxed")
            '''
        else:
            logger.debug(f"Stock market as no product, country{country}")
    except Exception as e:
        logger.debug(e)
        logger.debug(repr(e))
        traceback.print_exc()
        
    return results


def access1country(li_id: int, ctry: str, rows: list, errCounter: int, errCtry: Set[str]) -> Tuple[int, Set[str], list]:
    try:
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
                    results = myassess(ctry, stock_list)
                    if len(rows):
                        rows.extend(results)
                    else:
                        rows = results
                if size != limit:
                    break
            else:
                logger.critical(f"Empty product list for {ctry} page {page + 1}")
                errCounter = errCounter + 1
                errCtry.add(ctry)
                break
        # end of page loop
    except Exception as e:
        logger.debug(e)
        logger.debug(repr(e))
        traceback.print_exc()
        
    return errCounter, errCtry, rows


def compute(df: pl.DataFrame) -> pl.DataFrame:
    epsilon = 10**-9
    cap = 999

    # Ensure all columns exist
    for colname in [
        "businessSummary", "name", "sector", "industry", "country", "MKTCAP", "ratings_CURR", "ratings_1WA", "reportCurrency", "NPMTRENDGR", "PR1DAYPRC", "PR5DAYPRC", "ChPctPriceMTD", 
        "ChPctPrice5Y", "YSymbol", "AROE5YAVG", "closePriceDate", "Focf2Rev_AAvg5", "MARGIN5YR", "REVPS5YGR", "%M200D", "__nprice", "ACURRATIO", "AEBITD", "ANIAC", "APENORM", "AREV", 
        "ATANBVPS", "CAPI/TANG", "closePrice", "Dette nette / EBITDA", "Dette nette", "DivYield_CurTTM", "EBITDA", "EV", "EV2FCF_CurTTM", "Juste Prix", "Net Income", "NetDebt_A", 
        "NetDebt_I", "NHIG", "NLOW", "NPRICE", "PEINCLXOR", "PER", "priceCurrency", "ProjPE", "QCURRATIO", "QTANBVPS", "quoteCurrency", "Ratio courant", "Rendement", "shrOutstanding", 
        "TTMNIAC", "TTMREV", "VE/CA", "VE/FCF", "YLD5YAVG", "EV", "TTMFCF", "VE/FCF","YLD+PRY"
    ]:
        if colname not in df.columns:
            df = df.with_columns(pl.lit(np.nan).alias(colname))

    # Fill Net Income
    df = df.with_columns(
        pl.when(pl.col("Net Income").is_null() | pl.col("Net Income").is_nan())
        .then(pl.col("TTMNIAC") / 10**6)
        .otherwise(pl.col("Net Income"))
        .alias("Net Income")
    )
    df = df.with_columns(
        pl.when(pl.col("Net Income").is_null() | pl.col("Net Income").is_nan())
        .then(pl.col("ANIAC") / 10**6)
        .otherwise(pl.col("Net Income"))
        .alias("Net Income")
    )
    
    df = df.with_columns(__nprice=pl.col("NPRICE"))
    
    # L%H calculation
    df = df.with_columns(
        pl.when((pl.col("__nprice") > 0) & (pl.col("NHIG") > pl.col("NLOW")))
        .then((pl.col("__nprice") - pl.col("NLOW")) / (pl.col("NHIG") - pl.col("NLOW")) * 100.0)
        .otherwise(-1.0)
        .alias("L%H")
    )
    df = df.with_columns(pl.col("L%H").round(0))
    
    # Fill EBITDA

    df = df.with_columns(
        pl.when(pl.col("EBITDA").is_null() | pl.col("EBITDA").is_nan())
        .then(pl.col("AEBITD") / 10**6)
        .otherwise(pl.col("EBITDA"))
        .alias("EBITDA")
    )
    
    df = df.with_columns(
        pl.when((pl.col("EBITDA").is_null() | pl.col("EBITDA").is_nan()) | (pl.col("EBITDA") < 0))
        .then(pl.lit(epsilon))
        .otherwise(pl.col("EBITDA"))
        .alias("EBITDA")
    )
      
    # VE/EBITDA
    df = df.with_columns(
        (pl.col("EV").clip(lower_bound=epsilon) / pl.col("EBITDA").clip(lower_bound=epsilon) / 10**6).clip(upper_bound=cap).alias("VE/EBITDA")
    )
    
    # VE/CA
    df = df.with_columns(
        (pl.col("EV").clip(lower_bound=epsilon) / pl.col("TTMREV").clip(lower_bound=epsilon)).clip(upper_bound=cap).alias("VE/CA")
    )
    df = df.with_columns(
        pl.when(pl.col("VE/CA").is_null() | pl.col("VE/CA").is_nan())
        .then((pl.col("EV").clip(lower_bound=epsilon) / pl.col("AREV").clip(lower_bound=epsilon)).clip(upper_bound=cap))
        .otherwise(pl.col("VE/CA"))
        .alias("VE/CA")
    )
    
    # CAPI/TANG
    df = df.with_columns(
        (pl.col("__nprice") / pl.col("QTANBVPS").clip(lower_bound=epsilon)).clip(upper_bound=cap).alias("CAPI/TANG")
    )
    df = df.with_columns(
        pl.when(pl.col("CAPI/TANG").is_null() | pl.col("CAPI/TANG").is_nan())
        .then((pl.col("__nprice") / pl.col("ATANBVPS").clip(lower_bound=epsilon)).clip(upper_bound=cap))
        .otherwise(pl.col("CAPI/TANG"))
        .alias("CAPI/TANG")
    )
    
    # PER
    df = df.with_columns(pl.col("PEINCLXOR").alias("PER"))
    df = df.with_columns(
        pl.when(pl.col("PER").is_null() | pl.col("PER").is_nan())
        .then(pl.col("APENORM"))
        .otherwise(pl.col("PER"))
        .alias("PER")
    )
    df = df.with_columns(
        pl.when(pl.col("PER").is_null() | pl.col("PER").is_nan())
        .then(pl.col("ProjPE"))
        .otherwise(pl.col("PER"))
        .alias("PER")
    )
    
    # Rendement
    df = df.with_columns(pl.col("YLD5YAVG").alias("Rendement"))
    df = df.with_columns(
        pl.when(pl.col("Rendement").is_null() | pl.col("Rendement").is_nan())
        .then(pl.col("DivYield_CurTTM"))
        .otherwise(pl.col("Rendement"))
        .alias("Rendement")
    )
    df = df.with_columns(
        pl.when(pl.col("Rendement").is_null() | pl.col("Rendement").is_nan())
        .then(pl.lit(epsilon))
        .otherwise(pl.col("Rendement"))
        .alias("Rendement")
    )
    
    # Dette nette
    df = df.with_columns(pl.col("NetDebt_I").alias("Dette nette"))
    df = df.with_columns(
        pl.when(pl.col("Dette nette").is_null() | pl.col("Dette nette").is_nan())
        .then(pl.col("NetDebt_A"))
        .otherwise(pl.col("Dette nette"))
        .alias("Dette nette")
    )
    
    # Dette nette / EBITDA
    df = df.with_columns(
        (pl.col("Dette nette") / pl.col("EBITDA") / 10**6).clip(upper_bound=cap, lower_bound=epsilon).alias("Dette nette / EBITDA")
    )
    df = df.with_columns(
        pl.when((pl.col("Dette nette / EBITDA").is_null() | pl.col("Dette nette / EBITDA").is_nan()) & (pl.col("Dette nette") <= 0))
        .then(pl.lit(epsilon))
        .otherwise(pl.col("Dette nette / EBITDA"))
        .alias("Dette nette / EBITDA")
    )
    df = df.with_columns(
        pl.when((pl.col("Dette nette / EBITDA").is_null() | pl.col("Dette nette / EBITDA").is_nan()) & (pl.col("Dette nette") > 0))
        .then(pl.lit(cap))
        .otherwise(pl.col("Dette nette / EBITDA"))
        .alias("Dette nette / EBITDA")
    )
    
    # Ratio courant
    df = df.with_columns(pl.col("QCURRATIO").alias("Ratio courant"))
    df = df.with_columns(
        pl.when(pl.col("Ratio courant").is_null() | pl.col("Ratio courant").is_nan())
        .then(pl.col("ACURRATIO"))
        .otherwise(pl.col("Ratio courant"))
        .alias("Ratio courant")
    )
    df = df.with_columns(
        pl.when(pl.col("Ratio courant").is_null() | pl.col("Ratio courant").is_nan())
        .then(pl.lit(epsilon))
        .otherwise(pl.col("Ratio courant"))
        .alias("Ratio courant")
    )
    
    # VE/FCF
    df = df.with_columns(pl.col("EV2FCF_CurTTM").alias("VE/FCF"))
    df = df.with_columns(
        pl.when((pl.col("VE/FCF") <= 0) | (pl.col("VE/FCF").is_null() | pl.col("VE/FCF").is_nan()))
        .then(pl.lit(epsilon))
        .otherwise(pl.col("VE/FCF"))
        .alias("VE/FCF")
    )
    df = df.with_columns(
        pl.col("VE/FCF").clip(lower_bound=epsilon, upper_bound=cap).alias("VE/FCF")
    )
    
    # Juste Prix
    df = df.with_columns(
        ((pl.col("Net Income") * pl.col("PER") - pl.col("Dette nette") / 10**6) / pl.col("shrOutstanding")).alias("Juste Prix")
    )
    df = df.with_columns(
        pl.when((pl.col("Juste Prix") <= 0) | (pl.col("Juste Prix").is_null() | pl.col("Juste Prix").is_nan()))
        .then(pl.lit(epsilon))
        .otherwise(pl.col("Juste Prix"))
        .alias("Juste Prix")
    )
    
    # En Solde
    df = df.with_columns(
        pl.when((pl.col("Juste Prix") != 0) & (pl.col("Juste Prix") > pl.col("__nprice")))
        .then((pl.col("Juste Prix") - pl.col("__nprice")) / pl.col("Juste Prix") * 100.0)
        .when((pl.col("__nprice") != 0) & (pl.col("Juste Prix") <= pl.col("__nprice")))
        .then((pl.col("Juste Prix") - pl.col("__nprice")) / pl.col("__nprice") * 100.0)
        .otherwise(pl.lit(-100.0))
        .alias("En Solde")
    )
    df = df.with_columns(pl.col("En Solde").round(0))
    
    # YLD+PRY
    df = df.with_columns(
        pl.when(pl.col("Rendement").is_null() | pl.col("Rendement").is_nan())
        .then(pl.lit(0.0))
        .otherwise(pl.col("Rendement"))
        .alias("Rendement")
    )
    df = df.with_columns(
        pl.when(pl.col("YLD+PRY").is_null() | pl.col("YLD+PRY").is_nan())
        .then(pl.lit(0.0))
        .otherwise(pl.col("YLD+PRY"))
        .alias("YLD+PRY")
    )
    df = df.with_columns(
        (pl.col("Rendement") + pl.col("ChPctPrice5Y")).alias("YLD+PRY")
    )
    
    # VOL10DUSD
    df = df.with_columns(
        pl.when(
            pl.col("MKTCAP.USD").is_not_null() & pl.col("MKTCAP.USD").is_not_nan() &
            pl.col("shrOutstanding").is_not_null() & pl.col("shrOutstanding").is_not_nan() &
            pl.col("VOL10DAVG").is_not_null() & pl.col("VOL10DAVG").is_not_nan() &
            (pl.col("shrOutstanding") > 0)
        )
        .then((pl.col("MKTCAP.USD") / (pl.col("shrOutstanding") * 10**6) * pl.col("VOL10DAVG")).round(0))
        .otherwise(pl.lit(0.0))
        .alias("VOL10DUSD")
    )
  
    return df


# DCF FCFF 
def compute_dcf(ddf: pl.DataFrame, DCFstr: str, SalesStr: str) -> pl.DataFrame:
    wacc_map = {
        # Tes 12 originaux
        "oilgas":  0.085,     # Energy/Oil
        "finance": 0.095,     # PE/Asset mgmt
        "retail":  0.105,     # Supermarkets
        "IT":      0.120,     # Software/IT
        "telecom": 0.115,     # Networking/Semi
        "biotech": 0.140,     # Healthcare R&D
        "REIT":    0.090,     # Retail Real Estate (bas risque locatif)
        "utilities": 0.075,    # Stable, reglementé
        "bank":    0.088,     # Bank-specific (bas β)
        "Default": 0.105,     # Moyenne
    }

    g_map = {  # percent, like FOCF_AYr5CAGR or EPSTRENDGR
        "oilgas":    10.0, 
        "finance": 6.0, 
        "retail": 5.0, 
        "IT": 13.0, 
        "telecom": 10.0,
        "biotech":  18.0, 
        "REIT":     3.5,   # Croissance loyers faible
        "utilities": 3.0,    # Reglementé
        "bank":     4.5,   # Croissance actifs
        "Default": 6.0,
        }

    t_map = {
        "oilgas": 0.020, 
        "finance": 0.020, 
        "retail": 0.020, 
        "IT": 0.025, 
        "telecom": 0.025,
        "biotech": 0.025, 
        "REIT": 0.018,     # Inflation loyers
        "utilities": 0.015,  # Très stable
        "bank": 0.020,
        "Default": 0.020,
        }
    
    if "SctRoic" not in ddf.columns:
        ddf = ddf.with_columns(pl.lit("Default").alias("SctRoic"))
                               
    ddf = ddf.with_columns([
        pl.col("SctRoic").replace(wacc_map, default=wacc_map["Default"], return_dtype=pl.Float64).alias("wacc"),  # Par ligne
        pl.col("SctRoic").replace(g_map, default=g_map["Default"], return_dtype=pl.Float64).alias("g"),
        pl.col("SctRoic").replace(t_map, default=t_map["Default"], return_dtype=pl.Float64).alias("t")
    ])

    # NetDebtShr
    ddf = ddf.with_columns(
        (pl.col("NetDebt_I").fill_null(pl.col("NetDebt_A")).fill_nan(pl.col("NetDebt_A")).fill_nan(0.0) / pl.col("shrOutstanding") / 1e6).alias("NetDebtShr")
    )

    # FOCF5Y
    ddf = ddf.with_columns(
        pl.col("FOCF_AYr5CAGR").fill_null(pl.col("EPSTRENDGR")).fill_nan(pl.col("EPSTRENDGR")).fill_null(0.0).fill_nan(0.0).alias("FOCF5Y")
    )


    ddf = ddf.with_columns(
        pl.when(pl.col.FOCF5Y > pl.col.g)
        .then(pl.col.g)
        .otherwise(pl.col.FOCF5Y)
        .alias("FOCF5Yend")
    )
    ddf = ddf.with_columns(
        (pl.lit(1.0) + pl.col("FOCF5Y") / 100.0).alias("FOCF5Y"),
        (pl.lit(1.0) + pl.col("FOCF5Yend") / 100.0).alias("FOCF5Yend")
    )

    # Fill null values for financial columns
    for c in ['TTMFCFSHR', 'A1FCF', 'TTMDIVSHR', 'ADIVSHR']:
        ddf = ddf.with_columns(pl.col(c).fill_null(0.0).fill_nan(0.0))
        
    # fcff0 calculation
    ddf = ddf.with_columns(
        ((pl.col("TTMFCFSHR") + (pl.col("A1FCF") / pl.col("shrOutstanding") / 1e6) + pl.col("TTMDIVSHR") + pl.col("ADIVSHR")) / 2.0).clip(0).alias("fcff0")
    )

    for y in range(1, 6):
        ddf = ddf.with_columns(
            (pl.col.fcff0 * ((pl.col.FOCF5Y*(5-y)+pl.col.FOCF5Yend*(y-1))/4).pow(y) / (pl.lit(1.0)+pl.col.wacc).pow(y)).alias(f"pv{y}")
        )
    # TV année 5 (FCF6 / (wacc-t)), PV à t=0
    ddf = ddf.with_columns(
        (pl.col.fcff0 * pl.col.FOCF5Yend.pow(5) * (1 + pl.col.t) / (pl.col.wacc - pl.col.t) / (pl.lit(1.0) + pl.col.wacc).pow(5)).alias("pv6")
    )
    # Enterprise Value / shr - NetDebt = Equity Value
    ddf = ddf.with_columns(
        (pl.sum_horizontal([f"pv{i}" for i in range(1, 7)]) - pl.col("NetDebtShr")).alias(DCFstr)
    ).with_columns(pl.when(pl.col(DCFstr) < 0).then(0).otherwise(pl.col(DCFstr)).alias(DCFstr))

    
    # En Solde calculation
    ddf = ddf.with_columns(
        pl.when((pl.col(DCFstr) != 0) & (pl.col(DCFstr) > pl.col("__nprice")))
        .then((pl.col(DCFstr) - pl.col("__nprice")) / pl.col(DCFstr) * 100.0)
        .otherwise(pl.lit(-100.0))
        .alias(SalesStr)
    )
    
    ddf = ddf.with_columns(   
        pl.when((pl.col("__nprice") != 0) & (pl.col(DCFstr) <= pl.col("__nprice")))
        .then((pl.col(DCFstr) - pl.col("__nprice")) / pl.col("__nprice") * 100.0)
        .otherwise(SalesStr)
        .alias(SalesStr)
    )
    
    ddf = ddf.with_columns(
        pl.when(pl.col(SalesStr) < -100.0)
        .then(pl.lit(-100.0))
        .otherwise(pl.col(SalesStr))
        .alias(SalesStr)
    )
    ddf = ddf.with_columns(pl.col(SalesStr).round(0)).drop(["FOCF5Y", "FOCF5Yend", "fcff0", "pv1", "pv2", "pv3", "pv4", "pv5", "pv6"], strict=False)
    
    return ddf


# DCF FCFF 
def compute_dcfold(ddf: pl.DataFrame, g: float, t: float, DCFstr: str, SalesStr: str) -> pl.DataFrame:
    # NetDebtShr
    ddf = ddf.with_columns(
        (pl.col("NetDebt_I").fill_null(pl.col("NetDebt_A")).fill_nan(pl.col("NetDebt_A")) / pl.col("shrOutstanding") / 1e6).alias("NetDebtShr")
    )

    # FOCF5Y
    ddf = ddf.with_columns(
        pl.col("FOCF_AYr5CAGR").fill_null(pl.col("EPSTRENDGR")).fill_nan(pl.col("EPSTRENDGR")).fill_null(0.0).fill_nan(0.0).alias("FOCF5Y")
    )
    ddf = ddf.with_columns(
        pl.when(pl.col("FOCF5Y") > g)
        .then(pl.lit(g))
        .otherwise(pl.col("FOCF5Y"))
        .alias("FOCF5Y")
    )
    ddf = ddf.with_columns(
        (pl.lit(1.0) + pl.col("FOCF5Y") / 100.0).alias("FOCF5Y")
    )
    
    # Fill null values for financial columns
    for c in ['TTMFCFSHR', 'A1FCF', 'TTMDIVSHR', 'ADIVSHR']:
        ddf = ddf.with_columns(pl.col(c).fill_null(0.0).fill_nan(0.0))
        
    # tmp calculation
    ddf = ddf.with_columns(
        ((pl.col("TTMFCFSHR") + (pl.col("A1FCF") / pl.col("shrOutstanding") / 1e6) + pl.col("TTMDIVSHR") + pl.col("ADIVSHR")) / 2.0).alias("tmp")
    )
    
    # DCF calculation
    ddf = ddf.with_columns(pl.col("tmp").alias(DCFstr))
    ddf = ddf.with_columns(
        (
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(1) +
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(2) +
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(3) +
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(4) +
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(5) +
            pl.col(DCFstr) * (pl.col("FOCF5Y") / (1 + g)).pow(5) * (1 + t) / (g - t) -
            pl.col("NetDebtShr")
        ).alias(DCFstr)
    )
    ddf = ddf.with_columns(
        pl.when(pl.col(DCFstr) < 0.0)
        .then(pl.lit(0.0))
        .otherwise(pl.col(DCFstr))
        .alias(DCFstr)
    )
    
    # En Solde calculation
    ddf = ddf.with_columns(
        pl.when((pl.col(DCFstr) != 0) & (pl.col(DCFstr) > pl.col("__nprice")))
        .then((pl.col(DCFstr) - pl.col("__nprice")) / pl.col(DCFstr) * 100.0)
        .otherwise(pl.lit(-100.0))
        .alias(SalesStr)
    )
    
    ddf = ddf.with_columns(   
        pl.when((pl.col("__nprice") != 0) & (pl.col(DCFstr) <= pl.col("__nprice")))
        .then((pl.col(DCFstr) - pl.col("__nprice")) / pl.col("__nprice") * 100.0)
        .otherwise(SalesStr)
        .alias(SalesStr)
    )
    
    ddf = ddf.with_columns(
        pl.when(pl.col(SalesStr) < -100.0)
        .then(pl.lit(-100.0))
        .otherwise(pl.col(SalesStr))
        .alias(SalesStr)
    )
    ddf = ddf.with_columns(pl.col(SalesStr).round(0)).drop('tmp')
    
    return ddf


# compute most recent ROIC according to 12 sector/industries
def compute_roic(ddf: pl.DataFrame, roicStr="roic") -> pl.DataFrame:

    # Extract the current year
    now = datetime.now()
    current_year = now.year

    # reset ROIC
    ddf = ddf.with_columns(
        pl.lit(None, dtype=pl.Float64).alias(roicStr)
    )
    
    # set to zero unknown columns
    pattern = re.compile(r'^Y.*/')
    filtered_list = [s for s in ddf.columns if pattern.match(s)]
    for col in filtered_list:
        ddf = ddf.with_columns(pl.col(col).cast(pl.Float64).fill_null(strategy="zero"))
    
    # compute most recent ROIC
    for y in [current_year, current_year - 1, current_year - 2]:
        for val in ["/INC/SOPI", "/INC/TTAX", "/INC/EIBT", "/BAL/ATOT", "/BAL/SINV", "/BAL/ATRC", "/BAL/AACR", "/BAL/AITL", "/BAL/LAPB", "/BAL/LAEX", "/BAL/LCLO", "/BAL/STLD", "/BAL/LTTD", "/BAL/SCSI", "/BAL/AGWI", "/BAL/APPN", "/BAL/AINT"]:
            ebit_str = f"Y{y}{val}"
            if ebit_str not in ddf.columns:
                ddf = ddf.with_columns(pl.lit(0.0).alias(ebit_str))
        ddf = ddf.with_columns(
            (pl.col(f"Y{y}/INC/SOPI") * (pl.lit(1.0) - (pl.col(f"Y{y}/INC/TTAX") / pl.col(f"Y{y}/INC/EIBT")))).alias('nopat'),
            (pl.col(f"Y{y}/INC/SOPI")).alias('sopi'),
            (pl.col(f"Y{y}/BAL/ATOT")).alias('atot'),
            (pl.col(f"Y{y}/BAL/SINV")).alias('sinv'),
            (pl.col(f"Y{y}/BAL/ATRC") + pl.col(f"Y{y}/BAL/AACR") + pl.col(f"Y{y}/BAL/AITL") - pl.col(f"Y{y}/BAL/LAPB") - pl.col(f"Y{y}/BAL/LAEX")).alias("NWC"),
            (pl.col(f"Y{y}/BAL/LCLO") + pl.col(f"Y{y}/BAL/STLD") + pl.col(f"Y{y}/BAL/LTTD") - pl.col(f"Y{y}/BAL/SCSI")).alias("NetDebtOp")
        )    

        ddf = ddf.with_columns(
            pl.when((pl.col.NetDebtOp < 0.0))
            .then(pl.lit(0.0))
            .otherwise(pl.col.NetDebtOp)
            .alias('NetDebtOp')
        )
                
        ddf = ddf.with_columns(
            pl.when(pl.col.industry.str.contains(r"(?i)\bbanks?\b", literal=False))
            .then(pl.lit("bank"))
            .when((pl.col.industry.str.contains(r"(?i)retail|store|Restaurants?|\bBars?\b|distribution|resorts|casino|dealership", literal=False)))
            .then(pl.lit("retail"))
            .when((pl.col.sector.str.contains(r"(?i)technology|services", literal=False)) & (pl.col.industry.str.contains(r"(?i)\bsoftwares?\b|\bIT\b|Semiconductors?|Online|Internet|Gaming|multimedia|electronic|information technology|computer", literal=False)))
            .then(pl.lit("IT"))
            .when((pl.col.sector == "Healthcare") & (pl.col.industry.str.contains(r"(?i)bio|pharma|drug|research", literal=False)))
            .then(pl.lit("biotech"))
            .when((pl.col.industry.str.contains(r"(?i)\bREITs?\b|\breal estates?\b", literal=False)))
            .then(pl.lit("REIT"))
            .when((pl.col.industry.str.contains(r"(?i)telecom", literal=False)))
            .then(pl.lit("telecom"))
            .when((pl.col.sector.str.contains(r"(?i)energy", literal=False)) & (pl.col.industry.str.contains(r"(?i)oil|gas|Petroleum Refining|coal mining|thermal coal", literal=False)))
            .then(pl.lit("oilgas"))
            .when((pl.col.sector.str.contains(r"(?i)Financial", literal=False)))
            .then(pl.lit("finance"))
            .when(pl.col.sector.str.contains("(?i)utilities|energy"))
            .then(pl.lit("utilities"))
            .otherwise(pl.lit('Default'))  # industry, commerce
            .alias('SctRoic')
        )

        ddf = ddf.with_columns(
            pl.when((pl.col.SctRoic == "REIT"))
            .then(pl.col.sinv + pl.col(f"Y{y}/BAL/AGWI") + pl.col(f"Y{y}/BAL/APPN") + pl.col.NWC + pl.col.NetDebtOp)
            .when((pl.col.SctRoic == "Financial"))
            .then(pl.col(f"Y{y}/BAL/AGWI") + pl.col(f"Y{y}/BAL/AINT") + pl.col.NWC)
            .when((pl.col.SctRoic == "bank"))
            .then(pl.lit(0))  # invert next ROIC compute for banks
            .when((pl.col.SctRoic == "biotech"))
            .then(pl.col(f"Y{y}/BAL/AGWI") + pl.col(f"Y{y}/BAL/AINT") + pl.col.NWC)
            .when((pl.col.SctRoic == "utilities"))
            .then(pl.col(f"Y{y}/BAL/APPN") + pl.col.sinv + pl.col.NetDebtOp)
            .when((pl.col.SctRoic == "IT"))
            .then(pl.col(f"Y{y}/BAL/AGWI") + pl.col.NWC + pl.col(f"Y{y}/BAL/APPN"))
            .when((pl.col.SctRoic == "telecom"))
            .then(pl.col.sinv + pl.col(f"Y{y}/BAL/AGWI") + pl.col.NetDebtOp)
            .when((pl.col.SctRoic == "retail"))
            .then(pl.col(f"Y{y}/BAL/APPN") + pl.col.NWC + pl.col(f"Y{y}/BAL/AITL"))
            .when((pl.col.SctRoic == "oilgas"))
            .then(pl.col.sinv + pl.col(f"Y{y}/BAL/APPN"))
            .otherwise(pl.col(f"Y{y}/BAL/APPN") + pl.col(f"Y{y}/BAL/AGWI") + pl.col(f"Y{y}/BAL/AINT") + pl.col.NWC + pl.col.NetDebtOp)  # Default Industrie/Commerce
            .alias('ic')
        )
        
        ddf = ddf.with_columns(
            pl.when((pl.col.ic < 1.0))
            .then(pl.lit(1.0))
            .otherwise(pl.col.ic)
            .alias('ic')
        )

        ddf = ddf.with_columns([
            (pl.col.ic / pl.col.atot).alias("ic_atot_ratio")
        ])
        
        ddf = ddf.with_columns(
            (pl.lit(100.0) * pl.col.nopat/pl.col.ic).alias(f"Y{y}/{roicStr}")
        )
        
        ddf = ddf.with_columns(
            pl.when(pl.col(roicStr).is_null() | pl.col(roicStr).is_nan() | pl.col(roicStr).is_infinite() | (pl.col(roicStr) == 0))   #  | (pl.col(roicStr) > 500
            .then(pl.col(f"Y{y}/{roicStr}"))
            .otherwise(pl.col(roicStr))
            .alias(roicStr)
        ) 
    
    #ddf = ddf.drop(['nopat', 'sopi', 'atot', 'sinv', 'NWC', 'NetDebtOp', 'ic', "ic_atot_ratio"], strict=False)  # 'SctRoic',  f"new{roicStr}" 
      
    return ddf        


def compute_momentum(ddf: pl.DataFrame, momStr="momentum") -> pl.DataFrame:
    # Prix > MM50 > MM150 > MM200.
    ddf = ddf.with_columns(((
        (ddf["NPRICE"]*.95 > ddf["MM10W"]).cast(pl.Int8) + 
        (ddf["NPRICE"] > ddf["MM20W"]).cast(pl.Int8)*2 +
        (ddf["NPRICE"] > ddf["MM40W"]).cast(pl.Int8)*3 +
        (ddf["MM10W"] > ddf["MM20W"]).cast(pl.Int8)*2 + 
        (ddf["MM10W"] > ddf["MM40W"]).cast(pl.Int8) + 
        (ddf["MM20W"] > ddf["MM40W"]).cast(pl.Int8))/10).fill_nan(0.0).alias('c1'))

    # High 52 semaines à moins de 8%.
    ddf = ddf.with_columns((ddf["L%H"]/100).fill_nan(0.0).pow(2).alias('c2'))

    # Volume moyen en baisse sur 10 à 20 séances.
    # moyenne sur 10 semaines supérieure à moyenne sur 4 semaines 
    ddf = ddf.with_columns((ddf["daily_MM10WVOL"] / ddf["daily_MM4WVOL"]).fill_nan(0.0).pow(3).clip(0, 2).alias('c3')) 

    # Volume du semaine > 3x moyenne longue
    ddf = ddf.with_columns((ddf["daily_MM1WVOL"]/ddf["daily_MM10WVOL"] / 3).fill_nan(0.0).pow(3).clip(0, 2).alias('c4'))

    # calcul de la performance relative sur une fenetre de 6 mois, normalisé avec la moyenne des performances relative du meme secteur
    sector_means = ddf.group_by("sector").agg(
        pl.mean('%RS6M').alias("%RS6M_persector")
    )
    ddf = ddf.join(sector_means[["sector", "%RS6M_persector"]], on="sector", how="left")
    ddf = ddf.with_columns((ddf["%RS6M"]/ddf["%RS6M_persector"]).alias("%RS6M")) 

    # Quantile calculations for score
    cols = ['%RS6M']
    Q_dict = {}
    for c in cols:
        Q_dict[c] = [ddf[c].quantile(q) for q in np.arange(0.0, 1.01, 0.01)]
    Q = pl.DataFrame(Q_dict)

    for c in cols:
        QQ = np.array(Q[c])
        ddf = ddf.with_columns(
            pl.col(c).map_elements(lambda x: int(np.argmin(QQ < x)), return_dtype=pl.Int64).cast(pl.Int8).alias(f"q{c}")
        )

    # RS 6 mois dans le top décile/quintile.
    ddf = ddf.with_columns((ddf["q%RS6M"]/100).pow(2).fill_nan(0.0).alias('c5'))

    # Score=0.35×RS6m+0.20×ProxHigh+0.20×VolumeSurge+0.15×TrendQuality+0.10×VolDryUp
    ddf = ddf.with_columns((100*((ddf["c5"].pow(1.1))*(ddf["c2"].pow(1.05))*(ddf["c4"].pow(1.2))*(ddf["c1"]*1.3)*(ddf["c3"]*1.05)*.25).pow(0.333)).cast(pl.Int8).alias(momStr))

    # ddf.drop(['MM40W', '%RS6M', '%RS6M_persector', 'q%RS6M', 'MM20W', 'MM10W', 'daily_MM10WVOL', 'daily_MM4WVOL', 'daily_MM1WVOL', 'c1', 'c2', 'c3', 'c4', 'c5'])
    
    return ddf


    
def percentilize(df, cols):
    Q_dict = {}
    for c in cols:
        Q_dict[c] = [df[c].quantile(q) for q in np.arange(0.0, 1.01, 0.01)]
    Q = pl.DataFrame(Q_dict)
    for c in cols:
        QQ = np.array(Q[c])
        df = df.with_columns(
            pl.col(c).map_elements(lambda x: int(np.argmin(QQ < x)), return_dtype=pl.Int64).alias(f"q{c}")
        )    
    return df    



def getAll(cookies: Any, headers: Optional[Dict[str, str]], credentials: Any, basedir: str, yahoo_api, forex_api) -> Tuple[pl.DataFrame, list]:
    global trading_api
    trading_api = cachedDegiroApi(os.path.join(basedir, "cacheDegiro.bin"), credentials)


    suspectError = 0
    suspectCountries = set()

    
    rows = list()

    for i in range(1, 6):
        trading_api.connect(cookies=cookies, headers=headers)
        suspectError = 0
        try:
            # get all product list, countries, marketplaces
            trading_api.get_products_config()
            # get IntAccount
            trading_api.get_client_details()
            # stocked are browsed from counties(, and not marketplaces). This is the most reliable to get all stocks
            for id in trading_api.countries:
                li_dict = trading_api.countries[id]
                country = li_dict['name']
                if "filterCountry" in globals() and filterCountry is not None and (country not in filterCountry):
                    logger.debug(f"Skipping {country}")
                    continue
                if country == "SB":  # fake country for non tradable assets
                    continue
                if i > 2 and country not in suspectCountries:
                    logger.debug(f"Looping only on suspected buggy countries, skipping {country}")
                    continue
                suspectError, suspectCountries, rows = access1country(id, country, rows, suspectError, suspectCountries)
            # end of country loop
        except Exception as e:
            logger.debug(e)
            logger.debug(repr(e))
            traceback.print_exc()

        trading_api.logout()
        logger.warning(f"Got {suspectError} errors when downloading asset pages")
        if suspectError == 0:
            break
    # end of retries

    del trading_api
    del yahoo_api

    # this is the main dataframe will be filled up
    logger.warning(f"Creating a polars dataframe from {len(rows)} rows... Please wait")
    info_df = pl.from_dicts(rows, infer_schema_length=None)  # , orient="row"
    
    if info_df.shape[0] > 0:
        logger.warning(f"Number of stock entries before doublons: {info_df.shape[0]} / columns: {info_df.shape[1]}")
        info_df =  info_df.lazy().unique(subset=["isin", "name"], maintain_order=False).collect(streaming=True)
        logger.warning(f"Number of stock entries before compute: {info_df.shape[0]} / columns: {info_df.shape[1]}")
        info_df = compute(info_df)
        logger.warning(f"Number of stock entries after compute: {info_df.shape[0]} / columns: {info_df.shape[1]}")
    else:
        info_df = pl.DataFrame()
        
    return info_df, rows


def Screener(cookies: Any, headers: Optional[Dict[str, str]], _isinDebug: Optional[str], _filterCountry: Optional[List[str]], _yahooList: Optional[List[str]]) -> Tuple[pl.DataFrame, list]:
    global isinDebug
    global filterCountry

    global yahoo_api
    global forex_api


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
    yahoo_api = CachedYahooApi(os.path.join(basedir, "cacheYahoo.bin"))
    forex_api = CachedFrankfurter(os.path.join(basedir, "cacheFrankfurter.bin"))

    info_df, rows = getAll(cookies, headers, credentials, basedir, yahoo_api, forex_api)
    info_df2, rows2 = getAllYahoo(forex_api, basedir, _yahooList)
    rows.extend(rows2) 
    info_df = pl.concat([info_df, info_df2], how="diagonal_relaxed")
    logger.warning(f"Number of stock entries after merging Degiro with Yahoo finance: {info_df.shape[0]} / columns: {info_df.shape[1]}")
    
    if info_df.shape[0] > 0:
        df = compute_roic(info_df, roicStr="roic")  # create also Sctroic column
        logger.debug(f"Number of stock entries after ROIC: {df.shape[0]} / columns: {df.shape[1]}")
        df = compute_dcf(df, DCFstr="DCF", SalesStr="EnSolde2")
        logger.debug(f"Number of stock entries after DCF: {df.shape[0]} / columns: {df.shape[1]}")
        df = compute_momentum(df, momStr="momentum")
        logger.debug(f"Computed momentum")
        df = compute_rank(df, "score", ranking)
        logger.debug(f"Number of stock entries after ranking: {df.shape[0]} / columns: {df.shape[1]}")
        
        # Quantile calculations for score and MKTCAP.USD
        df = percentilize(df, ["score", "MKTCAP.USD"])

        df = compute_scorePerf(df)

        return df, rows
    else:
        # dataframe is empty
        return None, None


def build_csv(df: pl.DataFrame, critMinValue, critRemoveRegex, columns, filename, separator, fformat, tformat) -> pl.DataFrame:
    ddf = df.clone()

    if critMinValue:
        for c in critMinValue:
            column = c[0]
            valuemin = c[2]
            if column in ddf.columns:
                ddf = ddf.filter(pl.col(column) >= valuemin)

    if critRemoveRegex:
        ddf = ddf.with_columns(pl.lit(1).alias("keep"))
        for c in critRemoveRegex:
            column1 = c[0]
            regex1 = c[1]
            if len(c) > 2:
                column2 = c[2]
                regex2 = c[3]
                ddf = ddf.with_columns(
                    pl.when(pl.col(column1).str.contains(regex1, literal=False) & pl.col(column2).str.contains(regex2, literal=False))
                    .then(pl.lit(0))
                    .otherwise(pl.col("keep"))
                    .alias("keep")
                )
            else:
                ddf = ddf.with_columns(
                    pl.when(pl.col(column1).str.contains(regex1, literal=False))
                    .then(pl.lit(0))
                    .otherwise(pl.col("keep"))
                    .alias("keep")
                )
        ddf = ddf.filter(pl.col("keep") == 1).sort(["country", "qscorePerf", "score"], descending=[False, True, True])

    if tformat:
        ddf = ddf.with_columns(
            pl.col("name").str.slice(0, tformat),
            pl.col("industry").str.slice(0, tformat)
        )
        
    if filename is not None:
        # Convert to pandas for CSV writing with locale support
        for c in (set(columns) - set(ddf.columns)):
            logger.debug(f"adding empty column {c} in csv")
            ddf = ddf.with_columns(pl.lit("").alias(c))
        pdf = ddf.to_pandas()[columns]
        pdf.to_csv(filename, index=False, sep=separator, decimal=locale.localeconv()["decimal_point"], encoding="utf-8-sig", float_format=fformat, quoting=csv.QUOTE_MINIMAL)
    
    return ddf

  
    
def push_telegram(token, chat, init_msg, crit, filename):
    telegram_token = os.getenv(token) or ""
    telegram_chatid = os.getenv(chat) or ""
    if telegram_token and telegram_chatid:
        msg = [init_msg, "Ratios:"]
        for c in crit:
            label = c[1]
            val = c[2]
            msg.append(f"{label}={val:.0f}")
        msg = " ".join(msg)
        send_doc_to_telegram(
            {"message": {"apiToken": telegram_token, "chatID": telegram_chatid}},
            msg,
            filename,
        )
        
        
def main(cookies: Any, headers: Optional[Dict[str, str]], _isinDebug: Optional[str], _filterCountry: Optional[List[str]], _yahooList: Optional[List[str]]) -> Optional[pl.DataFrame]:
    info_df, _ = Screener(cookies, headers, _isinDebug, _filterCountry, _yahooList)
    time.sleep(2)
    
    if info_df is not None:
        info_df = info_df.sort(["country", "qscorePerf", "score"], descending=[False, True, True])
        
        
        columns = [
            "symbol", "isin", "name", "sector", "industry", "country",  "qscore",  "qscorePerf", "MKTCAP", "REVPS5YGR", 
            "MARGIN5YR", "Focf2Rev_AAvg5", "ratings_CURR", "ratings_1WA", "VE/EBITDA", "VE/CA", "CAPI/TANG", "PER", "Rendement", "Dette nette / EBITDA", 
            "Ratio courant", "VE/FCF", "%M200D", "closePrice", "quoteCurrency", "En Solde", "Juste Prix", "NPRICE", "L%H", "priceCurrency", "reportCurrency", 
            "EV2FCF_CurTTM", "EV", "TTMFCF", "Net Income", "NPMTRENDGR", "Dette nette", "shrOutstanding", "EBITDA", "PR1DAYPRC", "PR5DAYPRC", "ChPctPriceMTD", 
            "ChPctPrice5Y", "YSymbol", "businessSummary", "AROE5YAVG", "YLD+PRY", "PDATE", "qMKTCAP.USD", "VOL10DAVG", "EPSTRENDGR", "EnSolde2", 'DCF', 'TTMFCFSHR', 
            'FOCF_AYr5CAGR', "MKTCAP.USD", "VOL10DUSD", "TTMROAPCT", "TTMROEPCT", "roic", 'SctRoic', 'momentum'
        ]
        # locale.setlocale(locale.LC_NUMERIC, os.getenv("LANG", "C"))  # force point as decimal sign since build_csv will use locale configuration
        build_csv(info_df, None, None, columns, "screener4.csv", ";", "%.1f", 40)
 
        filename = f"screener-{datetime.now().strftime('%y-%m-%W')}.csv"
        build_csv(info_df, None, None, info_df.columns, filename, "\t", "%.3f", 0)
        
        columns = [ 
            "YSymbol", "sector", "country", "name", "industry", "qscore", "qscorePerf", "EPSTRENDGR", "EnSolde2", 
            "%M200D", "ChPctPrice5Y", "Rendement", "roic", "momentum"
        ]
        crit = (
            ("qscorePerf", "QSP", 50),      # score loic + perf
            ("roic", "ROIC", 7),            # return on invested capital
            ("EnSolde2", "SLD", 20),        # en solde de x%   DCF FCFF (discounted cash flow from free cash flow to the firm)
            ("VOL10DUSD", "VOL", 1e6),      # daily traded volume in US dollar
            ("momentum", "momentum", 25),   # momentum (accélération à la hausse) acceptable
        )
        # Removing banks, freight, holdings and mines
        critRemoveRegex = (
            ("sector", r"(?i)Financial", "industry", r"(?i)bank|investment|Financial|insurance"),
            ("sector", r"(?i)Basic Materials", "industry", r"(?i)Mining"),
            ("sector", r"(?i)Transportation", "industry", r"(?i)Freight|Tankers"),
            ("name", r"(?i)holding"),
        )
        
        ddf = build_csv(info_df, crit, critRemoveRegex, columns, "extrait.csv", ",", "%.1f", 40)
        
        if ddf.shape[0] > 0:
            uch = "\u2571"
            daat = f"%Y{uch}%m{uch}%d"
            uch2 = "\u2001"
            init_msg = f"Screener {datetime.now().strftime(daat)}{uch2}{ddf.shape[0]}{uch}{info_df.shape[0]}{uch2}"
            
            push_telegram("GT_TL_TOKEN", "GT_TL_CHAT", init_msg, crit, "extrait.csv")

            for company in ddf["name"].to_list():
                row = ddf.filter(pl.col('name') == company)[["row", "YSymbol", "symbol"]].head().to_dicts()[0]
                sym = row['YSymbol']
                if len(sym) == 0:
                    sym = row['symbol']
                filename = re.sub(r"[^A-Z0-9().]", "_", f"{company} ({sym})")
                dump = row['row']
                create_text_file(folder_path="./dump/", filename=filename, content=dump)
    
        
    return info_df



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
    try:
        cookies, headers = openWindow()
        main(cookies, headers, None, None, ['KL.json', 'JK.json', 'TW.json', "KS KQ.json", "NS BO.json", "SA.json","MX.json"])  # added Korea, India, South America, Mexico, Taiwan, Indonesia, Malaysia static lists
    except Exception as e:
        logger.debug(e)
        logger.debug(repr(e))
        traceback.print_exc()
    exit(0)
