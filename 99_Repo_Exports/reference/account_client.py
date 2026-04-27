# account_client.py
"""
Account Client - получение баланса счёта из go-gateway.
"""
from __future__ import annotations
import os
import requests
from common.log import setup_logger

log = setup_logger("account_client")

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:8090")

def get_balance(default: float = 10000.0) -> float:
    """
    Получить баланс счёта из go-gateway.
    
    Ожидается, что go-gateway отдаёт {"balance": 12345.67}
    
    Args:
        default: Значение по умолчанию если не удалось получить
    
    Returns:
        Баланс счёта в USD
    """
    url = f"{GATEWAY_URL}/account/balance"
    try:
        r = requests.get(url, timeout=2.5)
        if r.ok:
            js = r.json()
            balance = float(js.get("balance", default))
            log.debug("Balance retrieved: %.2f", balance)
            return balance
    except Exception as e:
        log.warning("Failed to get balance from gateway: %s, using default %.2f", e, default)
    return default

