# snapshot_builder.py
"""
Snapshot builder - creates market snapshot for signal context.
Loads ticks, pivots, and optionally OBI/depth images.
"""
from __future__ import annotations
import os
import base64
import json
import requests
from typing import Dict, List, Any
from common.log import setup_logger
from common.utils import get_redis, xrevrange_json

log = setup_logger("snapshot")

class SnapshotBuilder:
    """
    Builds market snapshot from Redis data and optional HTTP services.
    
    Environment variables:
        TICK_STREAM: Redis stream for ticks (default: stream:tick_XAUUSD)
        PIVOTS_KEY: Redis key for pivot levels (default: pivots:latest)
        SNAPSHOT_IMAGES: Whether to fetch images (default: false)
        OBI_SERVICE_URL: OBI service URL (default: http://py-obi-service:8088)
        SNAPSHOT_HTTP_TIMEOUT: HTTP timeout in seconds (default: 2.0)
    """
    
    def __init__(self):
        self.r = get_redis()
        self.tick_stream = os.getenv("TICK_STREAM", "stream:tick_XAUUSD")
        self.pivots_key = os.getenv("PIVOTS_KEY", "pivots:latest")
        self.include_images = os.getenv("SNAPSHOT_IMAGES", "false").lower() == "true"
        self.obi = os.getenv("OBI_SERVICE_URL", "http://py-obi-service:8088")
        self.http_timeout = float(os.getenv("SNAPSHOT_HTTP_TIMEOUT", "2.0"))

    def _load_ticks(self, count: int = 300) -> List[Dict]:
        """Load recent ticks from stream."""
        items = xrevrange_json(self.r, self.tick_stream, count=count)
        ticks = []
        for it in reversed(items):
            p = it["payload"]
            if "data" in p and isinstance(p["data"], str):
                # data уже json-строка? пробуем распарсить
                try:
                    p = json.loads(p["data"])
                except Exception:
                    pass
            ticks.append({
                "ts": int(p.get("ts") or 0),
                "bid": float(p.get("bid") or 0.0),
                "ask": float(p.get("ask") or 0.0),
                "last": float(p.get("last") or 0.0),
                "volume": float(p.get("volume") or 0.0),
                "flags": int(p.get("flags") or 0),
            })
        return ticks

    def _load_pivots(self) -> Dict[str, float]:
        """Load pivot levels from Redis."""
        raw = self.r.get(self.pivots_key)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    @staticmethod
    def ascii_dom(levels: Dict, rows: int = 10, bar_width: int = 20) -> str:
        """
        Генерация компактной ASCII визуализации DOM.
        Для простоты — ASCII-барчарт (работает без зав. от GUI).
        """
        bids: List[Tuple[float, float]] = levels.get("bids", [])[:rows]
        asks: List[Tuple[float, float]] = levels.get("asks", [])[:rows]
        out = []
        out.append("Price        | Bids           | Asks")
        out.append("-----------------------------------------")
        for i in range(rows):
            bp, bs = bids[i] if i < len(bids) else (0.0, 0.0)
            ap, asz = asks[i] if i < len(asks) else (0.0, 0.0)
            max_size = max(1.0, bs, asz)
            lb = int(min(bar_width, bs / max_size * bar_width))
            la = int(min(bar_width, asz / max_size * bar_width))
            out.append(f"{bp:10.2f} | {'#'*lb:<{bar_width}} | {'#'*la:<{bar_width}} {ap:10.2f}")
        return "\n".join(out)

    def _maybe_images(self) -> Dict[str, str]:
        """Optionally fetch OBI/depth images from service."""
        if not self.include_images:
            return {}
        imgs = {}
        try:
            u1 = f"{self.obi}/render/obi.png"
            u2 = f"{self.obi}/render/depth.png"
            r1 = requests.get(u1, timeout=self.http_timeout)
            r2 = requests.get(u2, timeout=self.http_timeout)
            if r1.status_code == 200:
                imgs["obi_png_b64"] = base64.b64encode(r1.content).decode()
            if r2.status_code == 200:
                imgs["depth_png_b64"] = base64.b64encode(r2.content).decode()
        except Exception as e:
            log.warning("image fetch failed: %s", e)
        return imgs

    def _load_dom_levels(self) -> Optional[Dict[str, Any]]:
        """Load DOM levels from Redis last key."""
        try:
            last_key = os.getenv("BOOK_LAST_KEY", f"book:levels:{os.getenv('SYMBOL', 'XAUUSD')}")
            raw = self.r.get(last_key)
            if raw:
                return json.loads(raw)
        except Exception as e:
            log.debug("DOM levels not available: %s", e)
        return None

    def build(self) -> Dict[str, Any]:
        """
        Build complete market snapshot.
        
        Returns:
            Dict with:
                - ticks: int - number of ticks loaded
                - pivots: Dict - pivot levels
                - mid_last: float - last mid price
                - mid_first: float - first mid price
                - mid_change_bp: float - price change in basis points
                - dom: Dict (optional) - DOM levels from last snapshot
                - obi_png_b64: str (optional) - OBI image base64
                - depth_png_b64: str (optional) - depth image base64
        """
        ticks = self._load_ticks()
        pivots = self._load_pivots()
        dom = self._load_dom_levels()
        
        out: Dict[str, Any] = {"ticks": len(ticks), "pivots": pivots}
        
        if dom:
            out["dom"] = dom

        # вычисление простых сводных метрик по тикам
        if ticks:
            mids = []
            for t in ticks:
                b, a = t["bid"], t["ask"]
                mids.append((b + a) / 2.0 if (b and a) else (t["last"] or b or a))
            out["mid_last"] = mids[-1]
            out["mid_first"] = mids[0]
            if mids[0] and mids[0] > 0:
                out["mid_change_bp"] = ((mids[-1] - mids[0]) / mids[0]) * 10000.0
            else:
                out["mid_change_bp"] = 0.0

        out.update(self._maybe_images())
        return out

