import os
from typing import Any, Callable, Dict, List, Optional, Union
from multiprocessing import Pool

import pandas as pd
from datetime import datetime
import numpy as np
import warnings
import re
import unicodedata

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
    pattern = r"[ ,]+(?: {2,}[RIA]|call|CO|Co\.|Company|Corp|Corp\.|Corpo|Corporat|Corporation|HOLDINGs?|Inc|Inc\.|Incorporated|Limited|Ltd\.?|N\.V\.|NV|plc|put|S\.A\.?|S\.p\.A\.?|SA|Shares|SpA|Stock|A\.S\.|SE|AS|A/S|AB|BV|PT|OYJ|ASA|Co\.,Ltd\.|Holdings|ORD SHS|FPO|\[[A-Z0-9]{3,3}\]|ORDINARY|ORDINARY S|PREFERRED|CLASS [ABCHO]|AG|\(The\)|CPI|\(PUBL\)|AO|SCA)$"
    pattern2 = r"^(?:The |[SAGF]DR ON )"
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