# -*- coding: utf-8 -*-
"""
OrderPushDispatcher — идемпотентная отправка ордеров в go-gateway /orders/push.
"""

from typing import Dict, Any
import json
import time
import logging
import requests
import redis

from infra.config import Config


class OrderPushDispatcher:
    def __init__(self, r: redis.Redis, cfg: Config, logger: logging.Logger):
        self.r = r
        self.cfg = cfg
        self.log = logger

    def _sent_key(self, sid: str) -> str:
        return self.cfg.orders_sent_key_tpl.replace("{SID}", sid)

    def already_sent(self, sid: str) -> bool:
        return self.r.exists(self._sent_key(sid)) == 1

    def mark_sent(self, sid: str) -> None:
        self.r.setex(self._sent_key(sid), self.cfg.dedupe_ttl_sec, "1")

    def push(self, payload: Dict[str, Any], retries: int = 3, timeout: float = 1.5) -> bool:
        sid = str(payload.get("sid") or "")
        if sid and self.already_sent(sid):
            self.log.info("skip push, already sent sid=%s", sid)
            return True

        url = f"{self.cfg.gateway_url}{self.cfg.orders_push_path}"
        self.log.info("Order push disabled, skipping request to %s (sid=%s)", url, sid or "[no-sid]")
        return False


