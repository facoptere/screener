import os
from typing import Any, Callable, Dict, List, Optional, Union
from multiprocessing import Pool

import pandas as pd
from datetime import datetime
import numpy as np
import warnings
import re
import unicodedata
from cachedfaz import CachedFrankfurter

import logging
logger = logging.getLogger() 



warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def crapy_estimates_summaries_get(fs: Dict[str, Any]) -> Dict[str, Any]:
    r = {}
    try:
        r["reportCurrency"] = ""
        # ['data']['annual'][0]['statements'][1]
        for _st in fs["data"]["interim"][0]["statements"]:
            for st in _st.get("items", []):
                try:
                    # print(st['code'])
                    r[st["name"]] = float(st["value"])
                except BaseException:
                    pass
        r["reportCurrency"] = fs["data"]["currency"]
    except BaseException:
        pass
    return r


def isna(num: float) -> bool:
    return num != num


def get(d: Optional[Dict[str, Any]], k: str) -> Any:
    r = np.nan  # sys.float_info.epsilon #float("nan")np.nan
    if d is not None and (type(d) is dict) and k in d:
        r = d[k]
        if (type(r) is dict) and ("value" in r):
            r = r["value"]
    else:
        r = np.nan
    return r


def getmin(d: Optional[Dict[str, Any]], a: List[str]) -> float:
    r = float(10**6)
    isset = False
    for p in a:
        v = get(d, p)
        if v == v:
            r = min(float(r), float(v))
            isset = True
    if isset:
        return r
    else:
        return np.nan


def yget(d: Optional[Dict[str, Any]], k: str) -> Union[float, str]:
    r = np.nan  # sys.float_info.epsilon #float("nan")np.nan
    if d is not None and k in d:
        r = d[k]
        if (type(r) is dict) and ("value" in r):
            r = r["value"]
    else:
        r = np.nan
    try:
        r = float(r)
    except BaseException:
        r = str(r)
        if r == "None" or r == "":
            r = np.nan
    return r


def write2csv(df: pd.DataFrame) -> None:
    now = datetime.now()  # current date and time
    filename = "degiro-export-" + now.strftime("%Y-%m-%d-%H-%M") + ".csv"
    filepath = "."
    fullpath = os.path.join(filepath, filename)
    print(f"Writing csv file '{fullpath}' (encoding utf-8)")
    df.to_csv(
        fullpath,
        index=True,
        sep=str(";"),
        decimal=str(","),
        encoding="utf-8",
    )


def parallelize_dataframe(df: pd.DataFrame, func: Callable, n_cores: Optional[int] = None) -> pd.DataFrame:
    n_cores = n_cores or (os.cpu_count() or 1)
    df_split = np.array_split(df, n_cores)
    with Pool(n_cores) as pool:
        return pd.concat(pool.map(func, df_split))
    '''
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
    '''

"""
def write2fav(df):
    if df.shape[0] > 0:
        username = os.getenv("GT_DG_USERNAME") or ""
        password = os.getenv("GT_DG_PASSWORD") or ""

        if username == "" or password == "":
            exit(0)

        credentials = Credentials(
            int_account=None,  # updated by get_client_details()
            username=username,
            password=password,
        )
        trading_api = cachedDegiroApi("/home/fab/Documents/simu/data/", credentials)
        trading_api.connect()
        products_config_dict = trading_api.get_products_config( )
        trading_api.get_client_details()
        now = datetime.now()
        prefix = "Screener-"
        fl = trading_api.get_list_list()
        # print(fl)
        for l in fl["data"]:
            if "name" in l and l["name"].startswith(prefix):
                trading_api.delete_favourite_list(id=l["id"])
                print(f'Deleting DEGIRO favourite list "{l["name"]}"')
        name = prefix + now.strftime("%Y-%m-%d-%H-%M")
        print(f'Creating DEGIRO favourite list "{name}"')
        favorite_list_id = trading_api.create_favourite_list(name=name)
        for p in df.index[:50].tolist():
            # list is limited to 50 entries
            trading_api.put_favourite_list_product(id=favorite_list_id, product_id=p)
            # print(f'Adding product id {p}')
        trading_api.logout()
"""


def to_ascii_upper(text: str) -> str:
    # Step 1: Normalize unicode characters (NFD decomposition)
    normalized = unicodedata.normalize('NFD', text)
    
    # Step 2: Remove combining diacritical marks (accents)
    ascii_str = ''.join(c for c in normalized if not unicodedata.combining(c))
    
    # Step 3: Custom replacements for characters not handled by NFKD
    replacements = {
        'ø': 'OE',  # Danish/Norwegian
        'Ø': 'OE',
        'œ': 'OE',  # French
        'Œ': 'OE',
        'ß': 'SS',  # German
        'æ': 'AE',  # Danish/Norwegian
        'Æ': 'AE',
        'å': 'AA',  # Danish/Norwegian
        'Å': 'AA',
        'é': 'E', 'è': 'E', 'ê': 'E', 'ë': 'E', 'É': 'E',
        'à': 'A', 'â': 'A', 'ä': 'A', 'ã': 'A', 'Á': 'A',
        'í': 'I', 'ì': 'I', 'î': 'I', 'ï': 'I', 'Í': 'I',
        'ó': 'O', 'ò': 'O', 'ô': 'O', 'ö': 'O', 'õ': 'O', 'Ó': 'O',
        'ú': 'U', 'ù': 'U', 'û': 'U', 'ü': 'U', 'Ú': 'U',
        'ç': 'C', 'Ç': 'C',
        'ñ': 'N', 'Ñ': 'N',
        'ý': 'Y', 'ÿ': 'Y',
        'ð': 'D', 'Ð': 'D',
        'þ': 'TH', 'Þ': 'TH',
        'ł': 'L', 'Ł': 'L',
        'ś': 'S', 'Ś': 'S', 'š': 'S', 'Š': 'S',
        'ž': 'Z', 'Ž': 'Z', 'ż': 'Z', 'Ź': 'Z',
        'ą': 'A', 'Ą': 'A', 'ć': 'C', 'Ć': 'C',
        'ę': 'E', 'Ę': 'E', 'ł': 'L', 'Ń': 'N', 'ń': 'N',
        'ř': 'R', 'Ř': 'R', 'ů': 'U', 'Ů': 'U',
    }
    
    result = []
    for char in ascii_str:
        result.append(replacements.get(char, char))
    
    # Step 4: Convert to uppercase and return
    return ''.join(result).upper()


def cleanse(text: str) -> str:
    pattern = r"[ ,]+(?: {2,}[RIA]|call|CO|Co\.|Company|Corp|Corp\.|Corpo|Corporat|Corporation|HOLDINGs?|Inc|Inc\.|Incorporated|Limited|Ltd\.?|N\.V\.|NV|plc|put|S\.A\.?|S\.p\.A\.?|SA|Shares|SpA|Stock|A\.S\.|SE|AS|A/S|AB|BV|PT|OYJ|ASA|Co\.,Ltd\.|Holdings|ORD SHS|FPO|\[[A-Z0-9]{3,3}\]|ORDINARY|ORDINARY S|PREFERRED|CLASS [ABCHO]|AG|\(The\)|CPI|\(PUBL\)|AO|SCA|S.?A.?B.? de C.?V.?)$"
    pattern2 = r"^(?:The |[SAGF]DR ON |COMPANHIA DE)"
    pattern3 = r"[^A-Z0-9]+"
    text = to_ascii_upper(text).strip()
    prev = None
    text = re.sub(pattern2, "", text, flags=re.IGNORECASE)
    while prev != text:
        prev = text
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r" *& *", " AND ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b([A-Z])\.", "\\1", text, flags=re.IGNORECASE)
    text = re.sub(r"'S\b", "S", text, flags=re.IGNORECASE)
    text = re.sub(pattern3, " ", text.upper(), flags=re.IGNORECASE).strip()
    return text 
    # ''.join(filter(str.isalnum, text.strip())).upper()
    
    

def create_text_file(folder_path: str, filename: str, content: str) -> None:
    """
    Creates a text file with the given filename and content in the specified folder.
    
    Args:
        folder_path (str): Path to the folder where the file will be created.
        filename (str): Name of the file (should end with .txt).
        content (str): Text content to write into the file.
    """
    try:
        # Validate filename
        if not filename.strip():
            raise ValueError("Filename cannot be empty.")
        if not filename.lower().endswith(".txt"):
            filename += ".txt"

        # Ensure the folder exists
        os.makedirs(folder_path, exist_ok=True)

        # Full file path
        file_path = os.path.join(folder_path, filename)

        # Write content to file
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(content)

        logger.debug(f"File created successfully at: {file_path}")

    except (OSError, ValueError) as e:
        logger.warning(f"Error creating file: {e}")
    
    
def convert2USD(forex_api, row: Dict, col: str) -> float: 
    ret = -1.0
    if col in row and type(row[col]) in [int, float] and not(row[col] != row[col]):
        ret = float(row[col])
        oldcap = ret
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
            rate = forex_api.convert(oldcur2, "USD")  # how much for 1 USD ?
            if isinstance(rate, float) and rate > 0.0:
                newcap = float(oldcap) / rate
                if oldcur == "GBX":
                    newcap /= 100.0
                # logger.fatal(f"{row['name']} {row['isin']} {oldcap} {oldcur} -> {newcap:.2f} USD   (rate : {rate:.3f} {oldcur2} for 1 USD)")
                ret = newcap
            else:
                logger.fatal(f"{row['name']} cannot convert {row[col]}  priceCurrency=\"{row.get('priceCurrency', '')}\"  reportCurrency=\"{row['reportCurrency']}\" currency=\"{row.get('currency', '')}\" quoteCurrency=\"{row.get('quoteCurrency', '')}\"    ")
    else:
        logger.info(f"Column is not legit: name:{col} type:{type(row[col]) if col in row else 'N/A'}")
    return ret
    
    
    
    
