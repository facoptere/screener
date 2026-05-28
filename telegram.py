"""
Telegram messaging utilities.
"""

import logging
from typing import Any, Dict
import requests


def send_to_telegram(config: Dict[str, Any], message: str) -> None:
    """
    Send a text message to Telegram.

    Args:
        config: Configuration dictionary containing 'message' with 'apiToken' and 'chatID'.
        message: The message text to send.
    """
    try:
        api_token = config["message"]["apiToken"]
        chat_id = config["message"]["chatID"]
        if api_token and chat_id:
            api_url = f"https://api.telegram.org/bot{api_token}/sendMessage"
            response = requests.post(api_url, json={"chat_id": chat_id, "text": message})
            logging.debug(response.text)
        else:
            logging.critical("Telegram: missing credentials")
    except Exception as e:
        logging.error("Telegram send error: %s", e)


def send_doc_to_telegram(config: Dict[str, Any], message: str, filepath: str | None) -> None:
    """
    Send a document to Telegram.

    Args:
        config: Configuration dictionary containing 'message' with 'apiToken' and 'chatID'.
        message: The caption for the document.
        filepath: Path to the file to send. Will post an error message to Telegram if missing
    """
    try:
        api_token = config["message"]["apiToken"]
        chat_id = config["message"]["chatID"]
        if api_token and chat_id:
            api_url = f"https://api.telegram.org/bot{api_token}/sendDocument"
            try:
                file = open(filepath, "rb")
                logging.error(f"{file}")
                data = {"chat_id": chat_id, "parse_mode": "HTML", "caption": message}
                files = {"document": file}
                response = requests.post(api_url, data=data, files=files, stream=True)
            except:  # file does not exist
                api_url = f"https://api.telegram.org/bot{api_token}/sendMessage"
                data = {"chat_id": chat_id, "parse_mode": "HTML", "text": f"<b>Empty file</b> related to « <i>{message}</i> »"}
                response = requests.post(api_url, data=data, stream=True)           
            logging.debug(response.text)
        else:
            logging.critical("Telegram: missing credentials")
    except Exception as e:
        logging.error("Telegram send document error: %s", e)
