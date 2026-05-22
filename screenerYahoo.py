 
import pandas as pd
from cachedYahooApi import CachedYahooApi 
import polars as pl
from utils import create_text_file, convert2USD, to_ascii_upper
import re
import numpy as np
from scipy.stats import linregress
from collections import OrderedDict
import os
import logging
import json
from typing import Any, Dict, Tuple
import traceback

np.seterr(divide='ignore', invalid='ignore')
logger = logging.getLogger() 



def compute_from_chart(df: pl.DataFrame, price: float, name: str):
    row = {
        "%M200D": -1.0,
        "ChPctPrice5Y": -1.0,
        "%RS6M": -1.0,
        "MM40W": -1.0,
        "MM20W": -1.0,
        "MM10W": -1.0,
        "daily_MM1WVOL": -1.0,
        "daily_MM10WVOL": -1.0,
        "daily_MM4WVOL": -1.0,
    }
    
    if isinstance(df, pl.DataFrame) and 'close' in df.columns:
        # compute MM40W %M200D %RS6M MM20W MM10W using close data
        data = df["close"].to_numpy().copy()
        mask = np.isnan(data)
        data[mask] = np.interp(np.flatnonzero(mask), np.flatnonzero(~mask), data[~mask])

        # setting close price if missing
        row["NPRICE"] = data[-1] if not price else price

        if df.shape[0] >= 40 and row["NPRICE"] > 0:
            # moving average 200 days (40 weeks)
            row["MM40W"] = np.mean(data[-40:])
            if np.isnan(row["MM40W"]):
                logger.fatal(f"nan for {row['isin']} <- {data}")  # should not happen
            row["%M200D"] = ((row["NPRICE"] - row["MM40W"]) / row["MM40W"] * 100.0) if row["MM40W"] > 0.0 else -100.0
            row["%M200D"] = round(row["%M200D"])
        if df.shape[0] >= 26 and row["NPRICE"] > 0:
            # relative strengh 6 months (26 weeks)
            rs6 = data[-26]
            row['%RS6M'] = (row["NPRICE"] - rs6) / rs6
            row["%RS6M"] = round(row["%RS6M"]*100)
        if df.shape[0] >= 20:        
            # moving average 100 days (20 weeks)
            row["MM20W"] = np.mean(data[-20:])
        if df.shape[0] >= 10:        
            # moving average 50 days (10 weeks)
            row["MM10W"] = np.mean(data[-10:])

        # compute MM40W %M200D %RS6M MM20W MM10W using close data
        data = df["volume"].to_numpy().copy()
        row["daily_MM1WVOL"] = data[-1] * 52 / 252
        mask = np.isnan(data)
        data[mask] = np.interp(np.flatnonzero(mask), np.flatnonzero(~mask), data[~mask])
        if df.shape[0] >= 10:
            # volume moving average 50 days (10 weeks)
            row["daily_MM10WVOL"] = np.mean(data[-10:]) * 52 / 252
        if df.shape[0] >= 4:
            # volume moving average 20 days (4 weeks)
            row["daily_MM4WVOL"] = np.mean(data[-4:]) * 52 / 252
            
        # compute ChPctPrice5Y using oldest open data
        if df.shape[0] >= 52:
            row["ChPctPrice5Y"] = (pow(1 + (row["NPRICE"] - df.head(1)["open"][0]) / df.head(1)["open"][0], 1 / (df.shape[0] / 52)) - 1) * 100
            if df.shape[0] < 52 * 5:
                logger.debug(f"Missing data to compute ChPctPrice5Y for {name}, only {df.shape[0]/52} years")  # Should not happen

    return row



def create_info_dict(inDict: Dict) -> Dict[str, float | str]:
    outDict: Dict[str, float | str] = {}
    mapping = OrderedDict([
        ("enterpriseValue", "EV"),	
        ("revenuePerShare", "TTMREVPS"),	
        ("currentPrice", "__nprice"),
        ("regularMarketPrice", "__nprice"),
        ("totalRevenue", "TTMREV"),	
        ("country", "country"),
        ("industry", "industry"),	
        ("sector", "sector"),	
        ("longName", "name"),
        ("shortName", "name"),
        ("prevName", "name"),
        ("fiftyTwoWeekLow", "NLOW"),	
        ("fiftyTwoWeekHigh", "NHIG"),	
        ("financialCurrency", "reportCurrency"),	
        ("currency", "priceCurrency"),
        ("currentRatio", "Ratio courant"),	
        ("freeCashflow", "A1FCF"),
        ("freeCashflow", "TTMFCF"),   
        ("sharesOutstanding", "shrOutstanding"),	
        ("ebitda", "EBITDA"),	
        ("marketCap", "MKTCAP"),
        ("priceToSalesTrailing12Months", "APR2REV"),
        ("averageDailyVolume10Day", "VOL10DAVG"),
        ("enterpriseToRevenue", "VE/CA"),
        ("enterpriseToEbitda", "VE/EBITDA"),
        ("returnOnAssets", "AROAPCT"),
        ("returnOnEquity", "AROEPCT"),
        ("netIncomeToCommon", "ANIAC"),
        ("lastDividendValue", "TTMDIVSHR"),
        ("dividendYield", "DivYield_CurTTM"),
        ("fiveYearAvgDividendYield", "YLD5YAVG"),
        ("priceEpsCurrentYear", "PEINCLXOR"),
        ("dividendYield", "DivYield_CurTTM"),
        ("totalDebt", "totalDebt"),
        ('averageVolume10days', 'VOL10DAVG'),
        ('averageDailyVolume10Day', 'VOL10DAVG'),
        ('averageDailyVolume3Month', 'VOL10DAVG'),
        ('longBusinessSummary', 'businessSummary'),
        ('averageAnalystRating', 'ratings_CURR')
    ])
    
    for k,v in mapping.items():
        if k in inDict and v not in outDict:
            outDict[v] = inDict[k]
    return outDict



def create_fundamentals_dict(df_income: pd.DataFrame) -> Dict[str, float]:
    result: Dict[str, float] = {}
    # Mapping pour Income Statement, balance, cash
    income_mapping = OrderedDict([
        ("OperatingIncome", "INC/SOPI"),
        ("TaxProvision", "INC/TTAX"),
        ("PretaxIncome", "INC/EIBT"),
        ("TotalAssets", "BAL/ATOT"),
        ("InvestmentinFinancialAssets", "BAL/SINV"),
        ("AccountsReceivable", "BAL/ATRC"),
        ("AccountsReceivable", "BAL/AACR"),
        ("Inventory", "BAL/AITL"),
        ("AccountsPayable", "BAL/LAPB"),
        ("CapitalLeaseObligations", "BAL/LCLO"),
        ("TotalDebt", "BAL/STLD"),
        ("LongTermDebt", "BAL/LTTD"),
        ("CashCashEquivalentsAndShortTermInvestments", "BAL/SCSI"),
        ("CashAndCashEquivalents", "BAL/SCSI"),
        ("GoodwillAndOtherIntangibleAssets", "BAL/AGWI"),
        ("Goodwill", "BAL/AGWI"),
        ("NetPPE", "BAL/APPN"),
        ("OtherIntangibleAssets", "BAL/AINT"),
        ("TotalUnusualItems", "BAL/AGWI-2"),
        ("TotalUnusualItemsExcludingGoodwill", "BAL/AGWI-1"),
        ("NetDebt", "NetDebt_I"),
        ("TangibleBookValue", "ATANBV"),
        ("FreeCashFlow", "A1FCF"),
        ("EBITDA", "EBITDA"),
        ("NetIncomeCommonStockholders", "ANIAC"),
        ("ShareIssued", "shrOutstanding")])
    year_columns = [col for col in df_income.columns if isinstance(col, pd.Timestamp)]

    for year_col in sorted(year_columns, reverse=False):
        year = year_col.year
        for row_name, degiro_code  in income_mapping.items():
            key = f"Y{year}/{degiro_code}"
            if row_name in df_income.index:
                value = df_income.loc[row_name, year_col]
                result[key] = float(value) if pd.notna(value) else None
                
        # compute goodwill in some cases
        key2 = f"Y{year}/BAL/AGWI-2"      
        key1 = f"Y{year}/BAL/AGWI-1"      
        key = f"Y{year}/BAL/AGWI"      
        if key not in result and key2 in result and key1 in result and result[key1] is not None and result[key2] is not None:
            result[key] = result[key2] - result[key1]
            
        # extract latest netdebt in some cases
        key = f"Y{year}/NetDebt_I"
        if key in result and result[key] is not None:
            result['NetDebt_I'] = result[key]
            result['NetDebt_A'] = result[key]
        else:
            totaldebt = f"Y{year}/BAL/STLD"
            cashequiv = f"Y{year}/BAL/SCSI"
            if cashequiv in result and result[cashequiv] is not None:
                result['cashequiv'] = result[cashequiv]
                if totaldebt in result and result[totaldebt] is not None:
                    result['NetDebt_I'] = result[totaldebt] - result[cashequiv]
                    result['NetDebt_A'] = result['NetDebt_I']
            
        # extract latest values
        for param in ['ATANBV', 'A1FCF', 'EBITDA', "ANIAC", 'shrOutstanding', 'NetDebt_I', 'NetDebt_A', 'cashequiv']:
            key = f"Y{year}/{param}"
            if key in result and result[key] is not None:
                result[param] = result[key]
              
    return result



def analyze_trend(df: pd.DataFrame, valueNames: list[str]):
    ret = None
    avg = None
    valueName = None

    for possible in valueNames:
        if possible in df.index:
            valueName = possible
            break
    if valueName:
        try:
            df_filtered = df.reindex(sorted(df.columns), axis=1).loc[:, df.loc[valueName].notna()]
            times = df_filtered.columns.view(np.int64) / 3600.0 / 24.0 / 365.2425 + 1970.0
            values = df_filtered.loc[valueName].tolist()
            logger.debug(f"{times} {values}")
            if len(values) > 2:
                delay_years = (times[-1] - times[0])
                avg = float(np.mean(values))
                if values[0] > 0 and values[-1] > 0:
                    # CAGR if value ends are positive
                    ret = 100.0 * ((values[-1] / values[0]) ** (1.0 / delay_years) - 1.0)
                elif avg != 0:
                    # linear regression if a value is negative
                    slope, _, _, _, _ = linregress(times, values)
                    ret = 100.0 * (float(slope) / avg)
            else:
                logger.debug(f"no enough data {valueName}: {values}")

        except Exception as e:
            logger.warning(e)
            logger.warning(repr(e))
            traceback.print_exc()
    else:
        logger.debug(f"no column {valueNames} in {df.index}")
                
    return ret, avg


 
def read_json_to_dict(file_path: str) -> Dict[str, Any]:
    """
    Lit un fichier JSON et retourne un dictionnaire Python.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.warning(f"Fichier '{file_path}' chargé avec succès !")
        return data
    except FileNotFoundError:
        logger.debug(f"Erreur : Fichier '{file_path}' non trouvé.")
        return {}
    except json.JSONDecodeError as e:
        logger.debug(f"Erreur JSON : {e}")
        return {}
    except Exception as e:
        logger.debug(f"Erreur inattendue : {e}")
        return {} 



def concat_with_trailing(income, incomeT):
    income = pd.concat([income, incomeT], axis=1)
    income = income.loc[:, ~income.columns.duplicated()]
    income = income.reindex(sorted(income.columns), axis=1)
    return income



def getAllYahoo(forex_api, basedir, _yahooList) -> Tuple[pl.DataFrame, list]:
    
    def getEventually(r: Dict, d: str, s: str):
        if d not in row and s in row:
            row[d] = row[s]
        return row
            
    yahoo_api = CachedYahooApi(os.path.join(basedir, "cacheScreenerYahoo.bin"))
    
    rowArray = [] 
    for filename in _yahooList:
        assetList = read_json_to_dict(filename)
        exchanges = os.path.splitext(filename)[0].split() 
        
        for labelStr, nameStr in assetList.items():
            try:
                label = yahoo_api.product_search(None, labelStr, nameStr, exchanges)    
                logger.info(f"{exchanges}°{labelStr}°{nameStr} -> {label}")

                info = yahoo_api.get_info(label=label) if label is not None else None
                income = yahoo_api.get_income_stmt(label=label, as_dict=False, pretty=False, freq='yearly') if label is not None else None
                balance = yahoo_api.get_balance_sheet(label=label, as_dict=False, pretty=False, freq='yearly') if label is not None else None
                cashflow = yahoo_api.get_cashflow(label=label, as_dict=False, pretty=False, freq='yearly') if label is not None else None

                if (info is None) or (income is None) or (balance is None) or (cashflow is None) or (income.shape[0] == 0) or (balance.shape[0] == 0) or (cashflow.shape[0] == 0):
                    logger.warning(f"{exchanges}°{labelStr}°{nameStr} -> YF Ticker:{label}, info:{type(info)} income:{type(income)}/{income.shape[0] if isinstance(income, pd.DataFrame) else ''} balance:{type(balance)}/{balance.shape[0] if isinstance(balance, pd.DataFrame) else ''} cashflow:{type(cashflow)}/{cashflow.shape[0] if isinstance(cashflow, pd.DataFrame) else ''}")
                    continue

                incomeT = yahoo_api.get_income_stmt(label=label, as_dict=False, pretty=False, freq='trailing')  # "quarterly" or "trailing"     
                if not ((incomeT is None) or (incomeT.shape[0] == 0)):
                    income = concat_with_trailing(income, incomeT)
                balanceT = yahoo_api.get_balance_sheet(label=label, as_dict=False, pretty=False, freq='trailing')
                if not ((balanceT is None) or (balanceT.shape[0] == 0)):
                    balance = concat_with_trailing(balance, balanceT)
                cashflowT = yahoo_api.get_cashflow(label=label, as_dict=False, pretty=False, freq='trailing')
                if not ((cashflowT is None) or (cashflowT.shape[0] == 0)):
                    cashflow = concat_with_trailing(cashflow, cashflowT)

                # print(f"{type(info)} {type(income)} {type(balance)} {type(cashflow)} ")
                # filename = re.sub(r"[^A-Z0-9().]", "_", f"{nameStr.upper()} ({label})")
                rowstr = f"{info}\n\nIncome Statement CSV:\n{income.to_csv()}\n\nBalance CSV:\n{balance.to_csv()}\n\nCashflow CSV:\n{cashflow.to_csv()}\n\n"
                # create_text_file(folder_path="./dump/", filename=f"__{filename}.txt", content=rowstr)
                
                row = { **create_fundamentals_dict(income), **create_fundamentals_dict(balance), **create_fundamentals_dict(cashflow), **create_info_dict(info) }
                row["YSymbol"] = label
                row["symbol"] = label
                row['row'] = rowstr
                row['AROAPCT'] = row.get('AROAPCT', 0) * 100.0
                row['AROEPCT'] = row.get('AROEPCT', 0) * 100.0
                row = getEventually(row, 'TTMROAPCT', 'AROAPCT')
                row = getEventually(row, 'TTMROEPCT', 'AROEPCT')
                row = getEventually(row, 'Net Income', 'ANIAC')
                row = getEventually(row, 'priceCurrency', 'reportCurrency')
                row = getEventually(row, 'quoteCurrency', 'priceCurrency')
                row = getEventually(row, 'closePrice', '__nprice')
                row = getEventually(row, 'TTMFCF', 'A1FCF')
        

                row['L%H'] = int(100.0*(row['__nprice'] - row['NLOW']) / (row['NHIG'] - row['NLOW'])) if (row['NHIG'] - row['NLOW']) != 0 else 0
                if 'PEINCLXOR' not in row and 'shrOutstanding' in row:
                    row['PEINCLXOR'] = row['__nprice'] / (row['ANIAC'] / row['shrOutstanding']) if (row['shrOutstanding'] > 0.0) and (row['ANIAC'] != 0.0) else 99.0
                row = getEventually(row, 'PER', 'PEINCLXOR')

                if 'ATANBVPS' not in row and 'shrOutstanding' in row:
                    row['ATANBVPS'] = row['ATANBV'] / row['shrOutstanding'] if row['shrOutstanding'] != 0 else -2.0     
                if 'MKTCAP' not in row and '__nprice' in row and 'shrOutstanding' in row:
                    row['MKTCAP'] = int(row['__nprice'] * row['shrOutstanding'])     
                if 'MKTCAP' in row and 'ATANBV' in row:
                    row["CAPI/TANG"] = row['MKTCAP'] / row["ATANBV"]
                if 'NetDebt_I' not in row and 'totalDebt' in row and 'cashequiv' in row:
                    row["NetDebt_I"] = row["totalDebt"] - row["cashequiv"]
                    row["NetDebt_A"] = row["NetDebt_I"]
                if 'NetDebt_I' not in row:
                    row["NetDebt_I"] = 0
                row = getEventually(row, 'Dette nette', 'NetDebt_I')
                row["Dette nette / EBITDA"] = row["NetDebt_I"] / row["EBITDA"] if 'EBITDA' in row else -1.0
                if 'EV' not in row and 'NetDebt_I' in row and 'MKTCAP' in row:
                    row["EV"] = row["NetDebt_I"] + row["MKTCAP"] 
                
                row["VE/FCF"] = row["EV"] / row["A1FCF"] if row["A1FCF"] != 0.0 else -5.0
                row = getEventually(row, 'EV2FCF_CurTTM', 'VE/FCF')
                if 'DivYield_CurTTM' not in row:
                    row['DivYield_CurTTM'] = 100.0 * row['TTMDIVSHR'] / row['__nprice'] if 'TTMDIVSHR' in row else -6.0
                #else:
                #    row['DivYield_CurTTM'] *= 100.0
                if 'Rendement' not in row:
                    row['Rendement'] = row['DivYield_CurTTM']
                # free operating cash flow, trend CAGR    
                if 'FreeCashFlow' not in cashflow and 'OperatingCashFlow' in cashflow and 'CapitalExpenditure' in cashflow:
                    cashflow.loc["FreeCashFlow"] = 100.0 * cashflow.loc["OperatingCashFlow"] + cashflow.loc["CapitalExpenditure"]  # CapitalExpenditure is negative
                row['FOCF_AYr5CAGR'], _ = analyze_trend(cashflow, ['FreeCashFlow'])
                # earnings per share, trend CAGR
                row['EPSTRENDGR'], _ = analyze_trend(income, ['DilutedEPS', 'BasicEPS'])
                # profit margin, trend CAGR  + average
                income.loc["npm"] = 100.0 * income.loc["NetIncomeCommonStockholders"] / income.loc["TotalRevenue"] if "NetIncomeCommonStockholders" in income.index and "TotalRevenue" in income.index else None
                row['NPMTRENDGR'], row['MARGIN5YR'] = analyze_trend(income, ['npm'])
                # gross sales, trend CAGR
                row['REVPS5YGR'], _ = analyze_trend(income, ['TotalRevenue'])
                # free operating cash flow to revenue, trend CAGR
                income = pd.concat([income, cashflow])
                income.loc["focf2rev"] = 100.0 * income.loc["FreeCashFlow"] / income.loc["TotalRevenue"] if "FreeCashFlow" in income.index and "TotalRevenue" in income.index else None
                _, row['Focf2Rev_AAvg5'] = analyze_trend(income, ['focf2rev'])
                
                logging.debug(f"{row['FOCF_AYr5CAGR']} {row['EPSTRENDGR']} {row['NPMTRENDGR']} {row['MARGIN5YR']} {row['REVPS5YGR']} {row['Focf2Rev_AAvg5']} ")
                
                if 'TTMFCFSHR' not in row and 'shrOutstanding' in row:
                    row['TTMFCFSHR'] = row['A1FCF'] / row['shrOutstanding'] if row['shrOutstanding'] != 0 else -7.0
                if 'ADIVSHR' not in row:
                    row['ADIVSHR'] = row['TTMDIVSHR'] if 'TTMDIVSHR' in row else 0.0
                row = getEventually(row, 'NPRICE', '__nprice')
                    
                # DEGIRO value is per million.   screener:compute_dcf() apply an inverse factor  
                row['shrOutstanding'] /= 1e6
                    
                '''
                # Affichage des résultats
                for k in sorted(row.keys()):
                    print(f"{k} = {row[k]}")
                '''
                    
                ohlc = yahoo_api.get_longtermprice(label=label, period="5y", resolution="1wk")
                if isinstance(ohlc, pd.DataFrame):
                    df = pl.DataFrame(ohlc)
                    if "Close" in df.columns and "Volume" in df.columns and "Open" in df.columns:
                        df = df.rename({"Close": "close", "Volume": "volume", "Open": "open"})
                    else:
                        logger.fatal(f"Unexpected format for Yahoo chart: {df.head()}")
                else:
                    df = None
                    
                # display(df)
                row_chart = compute_from_chart(df, row['NPRICE'], row['name']) 
                row = {**row, **row_chart}
                row["YLD+PRY"] = row['ChPctPrice5Y'] + row["Rendement"]
                row['name'] = to_ascii_upper(row['name'])

                row['MKTCAP.USD'] = convert2USD(forex_api, row, "MKTCAP")
                if isinstance(row["MKTCAP.USD"], float):
                    row['MKTCAP.USD'] = int(row['MKTCAP.USD'])

                if 'VOL10DAVG' not in row or row["VOL10DAVG"] <= 0:
                    row["VOL10DAVG"] = int(row["daily_MM4WVOL"])
                if 'VOL10DAVG' in row and 'NPRICE' in row:
                    row['VOL10DUSD'] = int(convert2USD(forex_api, row, "NPRICE") * row["VOL10DAVG"])

                # normalize sector, needed for ROIC and percentile filtering
                if 'sector' in row:
                    if row['sector'] == 'Communication Services':
                        row['sector'] = 'Services'
                    elif row['sector'] == 'Consumer Defensive':
                        row['sector'] = 'Consumer/Non-Cyclical'
                    elif row['sector'] == 'Financial Services':
                        row['sector'] = 'Financial'
                    elif row['sector'] == 'Real Estate':
                        row['sector'] = 'Capital Goods'
                rowArray.append(row)
            except:
                logger.error(f"Error geeting fundamental data from {labelStr}/{nameStr}")
            
        

    
    info_df = pl.DataFrame(rowArray, orient="row", infer_schema_length=None)   
    
    return info_df, rowArray
        
        
        
        
        
        
        
        