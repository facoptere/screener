import logging
from cachedApi import CachedApi
import requests
from requests.exceptions import HTTPError
import json
from lxml import html

logger = logging.getLogger()

class CachedFrankfurter(CachedApi):
    iso4217 = {"AED":784, "AFN":971, "ALL":8, "AMD":51, "AOA":973, "ARS":32, "AUD":36, "AWG":533, "AZN":944, "BAM":977, "BBD":52, "BDT":50, 
           "BGN":975, "BHD":48, "BIF":108, "BMD":60, "BND":96, "BOB":68, "BOV":984, "BRL":986, "BSD":44, "BTN":64, "BWP":72, "BYN":933, 
           "BZD":84, "CAD":124, "CDF":976, "CHE":947, "CHF":756, "CHW":948, "CLF":990, "CLP":152, "CNY":156, "COP":170, "COU":970, 
           "CRC":188, "CUP":192, "CVE":132, "CZK":203, "DJF":262, "DKK":208, "DOP":214, "DZD":12, "EGP":818, "ERN":232, "ETB":230, 
           "EUR":978, "FJD":242, "FKP":238, "GBP":826, "GEL":981, "GHS":936, "GIP":292, "GMD":270, "GNF":324, "GTQ":320, "GYD":328, 
           "HKD":344, "HNL":340, "HTG":332, "HUF":348, "IDR":360, "ILS":376, "INR":356, "IQD":368, "IRR":364, "ISK":352, "JMD":388, 
           "JOD":400, "JPY":392, "KES":404, "KGS":417, "KHR":116, "KMF":174, "KPW":408, "KRW":410, "KWD":414, "KYD":136, "KZT":398, 
           "LAK":418, "LBP":422, "LKR":144, "LRD":430, "LSL":426, "LYD":434, "MAD":504, "MDL":498, "MGA":969, "MKD":807, "MMK":104, 
           "MNT":496, "MOP":446, "MRU":929, "MUR":480, "MVR":462, "MWK":454, "MXN":484, "MXV":979, "MYR":458, "MZN":943, "NAD":516, 
           "NGN":566, "NIO":558, "NOK":578, "NPR":524, "NZD":554, "OMR":512, "PAB":590, "PEN":604, "PGK":598, "PHP":608, "PKR":586, 
           "PLN":985, "PYG":600, "QAR":634, "RON":946, "RSD":941, "RUB":643, "RWF":646, "SAR":682, "SBD":90, "SCR":690, "SDG":938, 
           "SEK":752, "SGD":702, "SHP":654, "SLE":925, "SOS":706, "SRD":968, "SSP":728, "STN":930, "SVC":222, "SYP":760, "SZL":748, 
           "THB":764, "TJS":972, "TMT":934, "TND":788, "TOP":776, "TRY":949, "TTD":780, "TWD":901, "TZS":834, "UAH":980, "UGX":800, 
           "USD":840, "USN":997, "UYI":940, "UYU":858, "UYW":927, "UZS":860, "VED":926, "VES":928, "VND":704, "VUV":548, "WST":882, 
           "XAD":396, "XAF":950, "XAG":961, "XAU":959, "XBA":955, "XBB":956, "XBC":957, "XBD":958, "XCD":951, "XCG":532, "XDR":960, 
           "XOF":952, "XPD":964, "XPF":953, "XPT":962, "XSU":994, "XTS":963, "XUA":965, "XXX":999, "YER":886, "ZAR":710, "ZMW":967, "ZWG":924}
    
    def __init__(self, file: str):
        super().__init__(file)
        super().open_db()
        logger.debug(f"frankfurter.app setup done")
        
    def __del__(self):
        super().__del__()
        logger.debug(f"Instance {self} destroyed.")
    
    def convert(self, what: str, inwhat: str):
        rate = self.convert_with_api(what, inwhat)
        if rate < 0.0:
            rate = self.convert_with_www(what, inwhat)
        return rate    
    
    
    def convert_with_www(self, what: str, inwhat: str):
        sw = self.iso4217[what] if what in self.iso4217 else None
        zw = self.iso4217[inwhat] if inwhat in self.iso4217 else None
        if sw and zw:
            if sw != zw:
                k = f"convertwww{what}{inwhat}"
                html_content = self.cache_get(k, 24*3600)
                if html_content is None:
                    url = 'https://www.faz.net/aktuell/finanzen/boersen-maerkte/snippet.htn'
                    params = {
                        'betrag': 10000,
                        'swaehrung': sw,
                        'zwaehrung': zw,
                        'ajax': 6
                    }
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    html_content = response.text
                    self.cache_set(k, 24*3600, html_content)
                if html_content is not None:    
                    tree = html.fromstring(html_content)
                    span_element = tree.xpath('//span[@class="bigone"]')[0]
                    text = span_element.text_content().strip().split()[0]
                    numeric_str = text.replace('.', '').replace(',', '.')
                    r = float(numeric_str) / 10000.0
                else:
                    r = -6.0
            else:
                r = 1.0
        else:
            r = -7.0
        
        logger.debug(f"FAZ WWW convert currency {what} in {inwhat} > {r}")    
        return r
    
    
    def convert_with_api(self, what: str, inwhat: str):
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
                self.cache_set(k, 24*3600, r)
            except HTTPError as ee:
                r = -2.0
                code = ee.response.status_code
                if code == 404:
                    self.cache_set(k, 24*3600, r)  # we store in cache the 404 error since it won't change any soon
                else:
                    logger.info(f"FAZ API {ee}, unexpected code {code}")
            except BaseException as ee:
                logger.warning(f"FAZ API other error {ee}")
                r = -4.0
                
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
        logger.debug(f"FAZ API convert currency {what} in {inwhat} > {r}")
        return r
