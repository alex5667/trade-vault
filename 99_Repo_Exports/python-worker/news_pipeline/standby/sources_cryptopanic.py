# python-worker/news_pipeline/standby/sources_cryptopanic.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import json
import time
import urllib.parse
import urllib.request
from typing import Dict, Any, List, Iterable

def fetch_cryptopanic(
    *
    base_url: str
    path: str
    auth_token: str
    currencies: Iterable[str]
    filter_: str = "important"
    kind: str = "news"
    region: str = "en"
    timeout: float = 10.0
    user_agent: str = "trade-standby/1.0"
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now_ms = get_ny_time_millis()

    # CryptoPanic принимает currencies как CSV (часто)
    cur = ",".join([c.strip().upper() for c in currencies if (c or "").strip()])

    qs = urllib.parse.urlencode({
        "auth_token": auth_token
        "currencies": cur
        "filter": filter_
        "kind": kind
        "region": region
    })
    url = f"{base_url.rstrip('/')}{path}?{qs}"

    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
    data = json.loads(raw) if raw else {}

    # обычно results=[...]
    results = (data or {}).get("results") or []
    for r in results:
        if not isinstance(r, dict):
            continue

        title = str(r.get("title") or "").strip()
        link = str(r.get("url") or "").strip()
        published_at = str(r.get("published_at") or "").strip()
        provider_id = str(r.get("id") or "").strip()  # в вашем Go это добавлено в UID

        pub_ms = now_ms
        try:
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            pub_ms = int(dt.timestamp() * 1000)
        except Exception:
            pub_ms = now_ms

        # currencies: [{"code":"BTC"}...]
        syms = []
        try:
            for c in (r.get("currencies") or []):
                code = str((c or {}).get("code") or "").strip().upper()
                if code:
                    syms.append(code)
        except Exception:
            syms = []

        if not title or not link:
            continue

        out.append({
            "published_ts_ms": pub_ms
            "ingested_ts_ms": now_ms
            "source": "cryptopanic"
            "title": title
            "url": link
            "summary": ""
            "symbols": syms
            "importance": float(r.get("importance") or 0.0) if isinstance(r.get("importance"), (int, float)) else 0.0
            "payload": r
            "provider_id": provider_id or published_at or link
        })

    return out
