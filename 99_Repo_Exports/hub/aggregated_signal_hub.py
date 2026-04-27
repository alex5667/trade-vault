# -*- coding: utf-8 -*-
"""
AggregatedSignalHub — единая «надстройка» для мета-сигналов.
"""

from dataclasses import dataclass
from typing import Optional
import logging
import time
import redis

from infra.config import Config
from infra.redis_client import get_redis
from core.snapshot_builder import SnapshotBuilder
from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig
from core.smart_cluster_analyzer import SmartClusterAnalyzer
from core.filtered_signal_writer import FilteredSignalWriter
from dispatch.order_push_dispatcher import OrderPushDispatcher
from persistence.label_sink import ParquetLabelSink


@dataclass
class HubScore:
    confidence: float
    dir_up: Optional[bool]
    reason: str


class AggregatedSignalHub:
    def __init__(self, r: redis.Redis, cfg: Config, logger: logging.Logger):
        self.r = r
        self.cfg = cfg
        self.log = logger
        self.snapshot = SnapshotBuilder(r, cfg, logger)
        self.detector = MicrostructureSpikeDetector(
            SpikeConfig(
                z_delta_thr=cfg.z_delta_thr,
                z_extreme_thr=cfg.z_extreme_thr,
                speed_z_thr=cfg.speed_z_thr,
            )
        )
        dispatcher = OrderPushDispatcher(r, cfg, logger)
        self.writer = FilteredSignalWriter(r, cfg, logger, dispatcher)
        self.label_sink = ParquetLabelSink()

    def _score(self, snap: dict) -> HubScore:
        tick = snap.get("tick", {}) or {}
        bid, ask = float(tick.get("bid", 0) or 0), float(tick.get("ask", 0) or 0)
        atr = float(snap.get("atr", 0) or 0)
        dom = snap.get("dom", []) or []

        ms = self.detector.update(bid, ask, volume=1.0, delta_hint=None, ts_ms=tick.get("ts"))
        cl = SmartClusterAnalyzer.analyze_from_dom(dom)

        conf = 0.0
        reason_parts = []

        if ms["trigger"]:
            conf += 0.35
            reason_parts.append(f"zΔ={ms['z_delta']:.2f}, zSpeed={ms['z_speed']:.2f}")
            if ms["extreme"]:
                conf += 0.15
                reason_parts.append("extreme")

        if cl["imbalance_score"] != 0.0:
            conf += 0.25 * abs(cl["imbalance_score"])
            reason_parts.append(f"cluster {cl['direction']} (imb={cl['imbalance_score']:.2f})")
        if cl["absorption_score"] > 0.4:
            conf += 0.1
            reason_parts.append(f"absorption {cl['absorption_score']:.2f}")

        if atr > 0 and self._is_near_pivot(tick.get("last") or (bid+ask)/2, snap.get("pivots") or {}, atr):
            conf += 0.1
            reason_parts.append("near pivot")

        conf = max(0.0, min(1.0, conf))
        dir_up = ms["dir_up"]
        if cl["direction"] == "buy":
            dir_up = True
        elif cl["direction"] == "sell":
            dir_up = False

        return HubScore(confidence=conf, dir_up=dir_up, reason="; ".join(reason_parts))

    @staticmethod
    def _is_near_pivot(price: float, pivots: dict, atr: float, mult: float = 0.5) -> bool:
        if not (price and pivots and atr):
            return False
        thr = atr * mult
        for lvl in ("P", "R1", "S1", "cam_R3", "cam_S3"):
            v = pivots.get(lvl)
            if not v:
                continue
            if abs(price - float(v)) <= thr:
                return True
        return False

    def step(self) -> None:
        snap = self.snapshot.build(self.cfg.symbol, with_dom_depth=12)
        sc = self._score(snap)
        if sc.confidence < 0.6 or sc.dir_up is None:
            return
        side = "LONG" if sc.dir_up else "SHORT"
        tick = snap["tick"]
        entry = tick["last"] or (tick["bid"] + tick["ask"]) / 2.0
        fs = self.writer.write_and_push(
            symbol=self.cfg.symbol,
            side=side,
            entry=entry,
            atr=snap["atr"],
            confidence=sc.confidence,
            reason=sc.reason,
            source="AggregatedHub",
        )
        
        # Логируем метку события для оффлайн-валидации
        if fs:
            rec = {
                "ts": snap["ts"],
                "symbol": self.cfg.symbol,
                "source": "hub",
                "side": side,
                "price": fs.price,
                "sl": fs.sl,
                "tp_levels": fs.tp_levels,
                "lot": fs.lot,
                "confidence": sc.confidence,
                "atr": snap["atr"],
                "reason": sc.reason,
                "metrics": {
                    "confidence": sc.confidence,
                },
                "emitted": True,
            }
            try:
                self.label_sink.write(rec)
            except Exception:
                self.log.exception("label_sink write failed")

    def run_forever(self) -> None:
        self.log.info("AggregatedSignalHub started for %s", self.cfg.symbol)
        delay = max(self.cfg.poll_ms, 200) / 1000.0
        while True:
            try:
                self.step()
            except Exception as e:
                self.log.exception("hub step error: %s", e)
            time.sleep(delay)


def build_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s | %(levelname)5s | %(name)s | %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger


if __name__ == "__main__":
    from infra.config import load_config
    cfg = load_config()
    log = build_logger(cfg.logger_name, cfg.log_level)
    r = get_redis(cfg.redis_url)
    AggregatedSignalHub(r, cfg, log).run_forever()


