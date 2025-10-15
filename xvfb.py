import os
import pyotp
import datetime
import time
import selenium.common.exceptions
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains

driver = None


def openWindow():

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
    totp_secret_key = os.getenv("GT_DG_TOKEN") or ""
    
    chrome_datadir = os.getenv("GT_CH_DATADIR") or ""  # full path of google profiles' folder
    chrome_profile = os.getenv("GT_CH_PROFILE") or ""  # name of user profile
    chrome_binary = os.getenv("GT_CH_BINARY") or ""    # full path of google chrome (or chromium) binary
    chrome_driver = os.getenv("GT_CH_DRIVER") or ""    # full path of selenium driver
    
    cookies = None

    if len(chrome_datadir) == 0 or len(chrome_binary) == 0 or len(chrome_driver) == 0:
        print("skipping selenium - please define env variables GT_CH_DATADIR GT_CH_BINARY GT_CH_DRIVER")
        return cookies, headers

    chrome_options = Options()
    # chrome_options.add_argument("--headless");
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1143,992")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument(f'--user-agent="{userAgent}"')

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"--user-data-dir={chrome_datadir}")
    chrome_options.add_argument(f"--profile-directory={chrome_profile}")
    chrome_options.binary_location = chrome_binary
    webdriver_service = Service(chrome_driver)
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

    try:
        driver.get("https://trader.degiro.nl/login/fr")

        # username input field
        wait = WebDriverWait(driver, 10)
        input_element = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        print("page ready, login prompt")
        input_element.send_keys(username)

        # password input field
        input_element = driver.find_element(By.NAME, "password")
        input_element.send_keys(password)

        button = driver.find_element(By.NAME, "loginButtonUniversal")
        ActionChains(driver).move_to_element(button).click(button).perform()
    except selenium.common.exceptions.TimeoutException:
        print("No Login page")

    # OTP input field (eventually)
    try:
        input_element = wait.until(EC.presence_of_element_located((By.NAME, "oneTimePassword")))
        if len(totp_secret_key) > 0:
            totp = pyotp.TOTP(totp_secret_key)
            time_remaining = totp.interval - datetime.datetime.now().timestamp() % totp.interval
            print(time_remaining)
            if time_remaining < 10:
                time.sleep(time_remaining + 1.0)
            one_time_password = str(totp.now())
            print(one_time_password)
            input_element.send_keys(one_time_password)
            checkbox = driver.find_element(By.NAME, "saveDevice")
            ActionChains(driver).move_to_element(checkbox).click(checkbox).perform()
            print("checkbox click")
            button = driver.find_element(By.XPATH, "//button[@type='submit']")
            ActionChains(driver).move_to_element(button).click(button).perform()
            print("page submit")
    except selenium.common.exceptions.TimeoutException:
        print("No OTP required")
    """
    try:
        logoff = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[aria-label='Log off']")))
        print("found log off button, we are in a valid session")
        ActionChains(driver).move_to_element(logoff).click(logoff).perform()
    except selenium.common.exceptions.TimeoutException:
        print("Log off button not found...")
    """
    try:
        cookies = None
        # input_element = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        cookies = driver.get_cookies()
        print(cookies)
        print("page ready, login prompt again")
    except selenium.common.exceptions.TimeoutException:
        print("Cannot get back to login prompt")
    """
    driver.quit()
    """
    return cookies, headers
