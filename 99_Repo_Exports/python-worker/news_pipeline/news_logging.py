# python-worker/news_pipeline/news_logging.py
from __future__ import annotations
"""
News logging helpers for signal/tick pipelines.

Hard requirements:
- NEVER log the whole ctx.
- Tick-loop must remain bounded (<= 50ms budget for the whole emit path).
- "Full debug" mode must not add Redis IO to the tick-loop.

Design:
- Mini-log: small, always safe fields.
- Full debug: enqueue ref into a background fetcher thread which does Redis GET
  with tight timeouts and logs a separate JSON line.

Integration points:
- When you emit a signal (NOT on every tick), call:
    add_news_minilog(ev, ctx)
    maybe_enqueue_news_full_debug(ctx, fetcher, symbol=..., ts_ms=...)

Environment knobs:
- NEWS_DEBUG_FULL=true|false
- NEWS_DEBUG_SAMPLE_PCT=1  (default 1%)
- NEWS_DEBUG_MAX_BYTES=65536
"""
from utils.time_utils import get_ny_time_millis

import json
import logging
import os
import queue
import threading
import time
from dataclasses import asdict
from typing import Any, Dict, Optional, Tuple

from common.stable_hash import sample_pct

log_full = logging.getLogger("news_full_debug")


def _env_bool(k: str, default: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def normalize_analysis_key(ref: str) -> str:
    """
    Accepts either:
    - "news:analysis:<uid>"  (preferred)
    - "<uid>"               (legacy)
    """
    r = (ref or "").strip()
    if not r:
        return ""
    if r.startswith("news:analysis:"):
        return r
    return f"news:analysis:{r}"


def add_news_minilog(ev: Dict[str, Any], ctx: Any) -> None:
    """
    Adds compact news fields to an existing log event dict.
    Safe to call in hot paths: O(1), no IO, no allocations beyond a few primitives.
    """
    nf = getattr(ctx, "news", None)
    if not nf:
        return

    # Keep exactly the minimal fields requested.
    ev["news_risk"] = float(getattr(nf, "news_risk", 0.0) or 0.0)
    ev["news_tminus_sec"] = int(getattr(nf, "event_tminus_sec", -1) or -1)
    ev["news_grade_id"] = int(getattr(nf, "news_grade_id", 0) or 0)
    ev["news_tags_mask"] = int(getattr(nf, "tags_mask", 0) or 0)
    # Optional but still compact/useful:
    ev["news_primary_tag_id"] = int(getattr(nf, "primary_tag_id", 0) or 0)
    ev["news_confidence"] = float(getattr(nf, "confidence", 0.0) or 0.0)
    ev["news_horizon_sec"] = int(getattr(nf, "horizon_sec", 0) or 0)
    ev["news_asof_ts_ms"] = int(getattr(nf, "asof_ts_ms", 0) or 0)
    # ref is a short string; safe to log (helps correlate)
    ev["news_ref"] = str(getattr(nf, "ref", "") or "")


class NewsFullDebugFetcher:
    """
    Background fetcher that pulls heavy JSON by ref and logs it.

    IMPORTANT:
    - Do NOT call Redis GET inside tick-loop.
    - Tick-loop should only call `enqueue(...)`, which is O(1) and non-blocking.
    """

    def __init__(
        self,
        *,
        redis,
        max_queue: int = 4096,
        sample_pct_default: int = 1,
        max_bytes_default: int = 65536,
    ) -> None:
        self.r = redis
        self.q: "queue.Queue[Tuple[str, str, int]]" = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._thr: Optional[threading.Thread] = None

        self.enabled = _env_bool("NEWS_DEBUG_FULL", False)
        self.sample_pct = _safe_int(os.getenv("NEWS_DEBUG_SAMPLE_PCT", str(sample_pct_default)), sample_pct_default)
        self.max_bytes = _safe_int(os.getenv("NEWS_DEBUG_MAX_BYTES", str(max_bytes_default)), max_bytes_default)

    def start(self) -> None:
        if not self.enabled:
            return
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._run, name="news-full-debug", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=1.0)

    def enqueue(self, *, ref: str, symbol: str, ts_ms: int) -> bool:
        """
        Non-blocking enqueue. Returns False if disabled or queue is full.
        """
        if not self.enabled:
            return False
        r = (ref or "").strip()
        if not r:
            return False

        # Sampling is deterministic by ref (+symbol for slight diversification).
        if not sample_pct(r, symbol, pct=self.sample_pct):
            return False

        try:
            self.q.put_nowait((r, symbol, int(ts_ms)))
            return True
        except queue.Full:
            return False

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                ref, symbol, ts_ms = self.q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self._process_one(ref=ref, symbol=symbol, ts_ms=ts_ms)
            except Exception:
                # fail-open: never crash background worker
                pass
            finally:
                try:
                    self.q.task_done()
                except Exception:
                    pass

    def _process_one(self, *, ref: str, symbol: str, ts_ms: int) -> None:
        key = normalize_analysis_key(ref)
        if not key:
            return
        raw = None
        try:
            raw = self.r.get(key)  # STRING JSON, TTL already managed by writer
        except Exception as e:
            log_full.info(
                json.dumps(
                    {
                        "kind": "news_full",
                        "ok": False,
                        "symbol": symbol,
                        "ts_ms": int(ts_ms),
                        "ref": ref,
                        "key": key,
                        "err": str(e)[:256],
                        "fetched_ts_ms": get_ny_time_millis(),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            return

        if not raw:
            log_full.info(
                json.dumps(
                    {
                        "kind": "news_full",
                        "ok": False,
                        "symbol": symbol,
                        "ts_ms": int(ts_ms),
                        "ref": ref,
                        "key": key,
                        "miss": True,
                        "fetched_ts_ms": get_ny_time_millis(),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            return

        s = str(raw)
        if self.max_bytes > 0 and len(s) > self.max_bytes:
            s = s[: self.max_bytes]

        # We keep raw JSON string to avoid parse overhead + parse failures in debug.
        log_full.info(
            json.dumps(
                {
                    "kind": "news_full",
                    "ok": True,
                    "symbol": symbol,
                    "ts_ms": int(ts_ms),
                    "ref": ref,
                    "key": key,
                    "json": s,
                    "json_len": len(str(raw)),
                    "fetched_ts_ms": get_ny_time_millis(),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
