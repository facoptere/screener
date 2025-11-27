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


def send_doc_to_telegram(config: Dict[str, Any], message: str, filepath: str) -> None:
    """
    Send a document to Telegram.

    Args:
        config: Configuration dictionary containing 'message' with 'apiToken' and 'chatID'.
        message: The caption for the document.
        filepath: Path to the file to send.
    """
    try:
        api_token = config["message"]["apiToken"]
        chat_id = config["message"]["chatID"]
        if api_token and chat_id:
            api_url = f"https://api.telegram.org/bot{api_token}/sendDocument"
            data = {"chat_id": chat_id, "parse_mode": "HTML", "caption": message}
            with open(filepath, "rb") as file:
                files = {"document": file}
                response = requests.post(api_url, data=data, files=files, stream=True)
            logging.debug(response.text)
        else:
            logging.critical("Telegram: missing credentials")
    except Exception as e:
        logging.error("Telegram send document error: %s", e)
