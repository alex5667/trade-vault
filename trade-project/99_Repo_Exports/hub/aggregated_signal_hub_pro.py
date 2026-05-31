# -*- coding: utf-8 -*-
"""
AggregatedSignalHubPro — улучшенная версия с MicrostructureSpikeDetectorPro.
Поддерживает:
- Гибридный детектор (legacy + pro)
- Интеграцию потока принтов
- Расширенные метрики в labels
- Адаптивный скоринг
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import logging
import time
import redis
import os
import sys
from pathlib import Path

# Ensure correct import paths for detectors
_hub_dir = Path(__file__).parent
_root_dir = _hub_dir.parent
if str(_root_dir) not in sys.path:
    sys.path.insert(0, str(_root_dir))

from infra.config import Config
from infra.redis_client import get_redis
from core.snapshot_builder import SnapshotBuilder
from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig
from core.microstructure_spike_detector_pro import MicrostructureSpikeDetectorPro, ProConfig
from core.smart_cluster_analyzer import SmartClusterAnalyzer
from core.filtered_signal_writer import FilteredSignalWriter
from dispatch.order_push_dispatcher import OrderPushDispatcher
from persistence.label_sink import ParquetLabelSink


@dataclass
class HubScore:
    confidence: float
    dir_up: Optional[bool]
    reason: str
    metrics: Dict[str, Any]


class AggregatedSignalHubPro:
    """
    Улучшенная версия хаба с поддержкой Pro детектора.
    
    Особенности:
    - Гибридный детектор (автоматический выбор лучшего источника)
    - Интеграция потока принтов из Redis
    - Расширенное логирование метрик
    - Адаптивный скоринг на базе реальной дельты
    """
    
    def __init__(self, r: redis.Redis, cfg: Config, logger: logging.Logger):
        self.r = r
        self.cfg = cfg
        self.log = logger
        self.snapshot = SnapshotBuilder(r, cfg, logger)
        
        # Legacy детектор (суррогат)
        try:
            self.detector_legacy = MicrostructureSpikeDetector(
                SpikeConfig(
                    z_delta_thr=cfg.z_delta_thr,
                    z_extreme_thr=cfg.z_extreme_thr,
                    speed_z_thr=cfg.speed_z_thr,
                )
            )
            # Verify it has the update method
            if not hasattr(self.detector_legacy, 'update'):
                self.log.warning("⚠️  Legacy detector doesn't have 'update' method")
                self.detector_legacy = None
        except Exception as e:
            self.log.error(f"❌ Failed to initialize legacy detector: {e}")
            self.detector_legacy = None
        
        # Pro детектор (реальная дельта)
        self.detector_pro = MicrostructureSpikeDetectorPro(
            ProConfig(
                window_seconds=60.0,
                z_delta_thr=cfg.z_delta_thr,
                z_extreme_thr=cfg.z_extreme_thr,
                z_speed_thr=cfg.speed_z_thr,
                svbp_bins=20,
                min_trades=5
            )
        )
        
        dispatcher = OrderPushDispatcher(r, cfg, logger)
        self.writer = FilteredSignalWriter(r, cfg, logger, dispatcher)
        self.label_sink = ParquetLabelSink()
        
        # Настройки
        self.min_trades_for_pro = int(os.getenv("MIN_TRADES_FOR_PRO", "5"))
        self.trades_stream = os.getenv("TRADES_STREAM", f"trades:{cfg.symbol}")
        self.last_trade_id = "0-0"
        
        # Статистика
        self.stats = {
            "signals_total": 0,
            "signals_emitted": 0,
            "trades_processed": 0,
            "pro_detector_used": 0,
            "legacy_detector_used": 0,
        }
    
    def _process_trades(self) -> int:
        """
        Обрабатывает новые принты из Redis Stream.
        Возвращает количество обработанных принтов.
        """
        try:
            # Читаем новые принты из стрима (block=5000ms < socket_timeout=10s)
            streams = self.r.xread({self.trades_stream: self.last_trade_id}, count=100, block=5000)
            
            if not streams:
                return 0
            
            count = 0
            for stream_name, messages in streams:
                for msg_id, data in messages:
                    try:
                        price = float(data.get(b"price", 0) if isinstance(data.get("price"), bytes) else data.get("price", 0))
                        qty = float(data.get(b"qty", 0) if isinstance(data.get("qty"), bytes) else data.get("qty", 0))
                        side = (data.get(b"side", b"").decode() if isinstance(data.get("side"), bytes) else data.get("side", "")).lower()
                        ts_ms = int(data.get(b"ts", 0) if isinstance(data.get("ts"), bytes) else data.get("ts", 0))
                        
                        if price > 0 and qty > 0 and side in ["buy", "sell"]:
                            self.detector_pro.on_trade(price, qty, side, ts_ms)
                            count += 1
                    except Exception as e:
                        self.log.warning(f"Error processing trade: {e}")
                    
                    self.last_trade_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            
            self.stats["trades_processed"] += count
            return count
            
        except Exception as e:
            self.log.error(f"Error reading trades stream: {e}")
            return 0
    
    def _score(self, snap: dict) -> HubScore:
        """
        Скоринг сигнала с использованием гибридного подхода.
        МОДИФИКАЦИЯ: работает без DOM (cluster analysis закомментирован)
        """
        tick = snap.get("tick", {}) or {}
        bid, ask = float(tick.get("bid", 0) or 0), float(tick.get("ask", 0) or 0)
        atr = float(snap.get("atr", 0) or 0)
        # dom = snap.get("dom", []) or []  # ЗАКОММЕНТИРОВАНО - не используется
        
        # Обновляем оба детектора
        ms_legacy = {}
        try:
            if hasattr(self.detector_legacy, 'update'):
                ms_legacy = self.detector_legacy.update(bid, ask, volume=1.0, delta_hint=None, ts_ms=tick.get("ts"))
            else:
                self.log.warning("Legacy detector missing 'update' method")
        except Exception as e:
            self.log.debug(f"Legacy detector error: {e}")
        
        self.detector_pro.update_tick(bid, ask, tick.get("ts"))
        ms_pro = self.detector_pro.metrics()
        
        # Выбор источника метрик
        trades_count = ms_pro.get("trades_in_window", 0)
        use_pro = trades_count >= self.min_trades_for_pro
        
        if use_pro:
            z_delta = ms_pro["z_delta"]
            z_speed = ms_pro["z_speed"]
            svbp_imbalance = ms_pro["svbp_imbalance"]
            trigger = ms_pro["trigger"]
            extreme = ms_pro["extreme"]
            dir_up = ms_pro["dir_up"]
            self.stats["pro_detector_used"] += 1
            source = "pro"
        else:
            z_delta = ms_legacy.get("z_delta", 0.0)
            z_speed = ms_legacy.get("z_speed", 0.0)
            svbp_imbalance = 0.0
            trigger = ms_legacy.get("trigger", False)
            extreme = ms_legacy.get("extreme", False)
            dir_up = ms_legacy.get("dir_up")
            self.stats["legacy_detector_used"] += 1
            source = "legacy"
        
        # ЗАКОММЕНТИРОВАНО - Cluster анализ (требует DOM)
        # cl = SmartClusterAnalyzer.analyze_from_dom(dom)
        # Заменяем на пустые значения
        cl = {
            "imbalance_score": 0.0,
            "absorption_score": 0.0,
            "direction": None
        }
        
        # Скоринг БЕЗ cluster analysis
        conf = 0.0
        reason_parts = []
        
        # Microstructure trigger (основной фактор)
        if trigger:
            conf += 0.45  # Увеличено с 0.35 (компенсация за отсутствие cluster)
            reason_parts.append(f"zΔ={z_delta:.2f}, zSpeed={z_speed:.2f}")
            if extreme:
                conf += 0.20  # Увеличено с 0.15
                reason_parts.append("extreme")
        
        # ЗАКОММЕНТИРОВАНО - Cluster analysis (недоступен без DOM)
        # if cl["imbalance_score"] != 0.0:
        #     conf += 0.25 * abs(cl["imbalance_score"])
        #     reason_parts.append(f"cluster {cl['direction']} (imb={cl['imbalance_score']:.2f})")
        # 
        # if cl["absorption_score"] > 0.4:
        #     conf += 0.1
        #     reason_parts.append(f"absorption {cl['absorption_score']:.2f}")
        
        # Бонус за SVbP (только для Pro) - УВЕЛИЧЕН
        if use_pro and abs(svbp_imbalance) > 0.3:
            conf += 0.20  # Увеличено с 0.15 (компенсация за отсутствие cluster)
            direction = "buy" if svbp_imbalance > 0 else "sell"
            reason_parts.append(f"SVbP {direction} (imb={svbp_imbalance:.2f})")
        
        # Бонус за использование реальных принтов
        if use_pro:
            conf += 0.05
            reason_parts.append(f"real_delta (trades={trades_count})")
        
        # Pivot analysis (опционально - может быть недоступен)
        if atr > 0 and self._is_near_pivot(tick.get("last") or (bid+ask)/2, snap.get("pivots") or {}, atr):
            conf += 0.10
            reason_parts.append("near pivot")
        
        conf = max(0.0, min(1.0, conf))
        
        # Определение направления (БЕЗ cluster override)
        # ЗАКОММЕНТИРОВАНО - cluster направление
        # if cl["direction"] == "buy":
        #     dir_up = True
        # elif cl["direction"] == "sell":
        #     dir_up = False
        
        # Используем только направление от детекторов
        if dir_up is None and svbp_imbalance != 0:
            # Если детектор не определил направление, используем SVbP
            dir_up = svbp_imbalance > 0
        
        # Расширенные метрики
        metrics = {
            "z_delta": z_delta,
            "z_speed": z_speed,
            "z_range": ms_pro.get("z_range", 0.0) if use_pro else 0.0,
            "svbp_imbalance": svbp_imbalance,
            "svbp_top_bin": ms_pro.get("svbp_top", {}).get("price_bin", 0.0) if use_pro else 0.0,
            "cluster_imbalance": 0.0,  # Недоступно без DOM
            "cluster_absorption": 0.0,  # Недоступно без DOM
            "trades_count": trades_count,
            "detector_source": source,
            "trigger": trigger,
            "extreme": extreme,
            "dom_available": False,  # Флаг отсутствия DOM
        }
        
        return HubScore(
            confidence=conf,
            dir_up=dir_up,
            reason="; ".join(reason_parts),
            metrics=metrics
        )
    
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
        """Основной шаг обработки."""
        self.stats["signals_total"] += 1
        
        # Обрабатываем новые принты
        self._process_trades()
        
        # Строим снапшот (БЕЗ DOM - with_dom_depth=0)
        snap = self.snapshot.build(self.cfg.symbol, with_dom_depth=0)  # ИЗМЕНЕНО: было 12
        sc = self._score(snap)
        
        # Снижен порог confidence для работы без DOM
        if sc.confidence < 0.5 or sc.dir_up is None:  # ИЗМЕНЕНО: было 0.6
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
            source="AggregatedHub-Pro",
        )
        
        # Логируем метку с расширенными метриками
        if fs:
            self.stats["signals_emitted"] += 1
            
            rec = {
                "ts": snap["ts"],
                "symbol": self.cfg.symbol,
                "source": "hub_pro",
                "side": side,
                "price": fs.price,
                "sl": fs.sl,
                "tp_levels": fs.tp_levels,
                "lot": fs.lot,
                "confidence": sc.confidence,
                "atr": snap["atr"],
                "reason": sc.reason,
                "metrics": sc.metrics,  # расширенные метрики
                "emitted": True,
            }
            try:
                path = self.label_sink.write(rec)
                self.log.debug(f"Label saved: {path}")
            except Exception:
                self.log.exception("label_sink write failed")
    
    def _log_stats(self) -> None:
        """Логирование статистики."""
        self.log.info(
            f"Stats: signals={self.stats['signals_total']}, "
            f"emitted={self.stats['signals_emitted']}, "
            f"trades={self.stats['trades_processed']}, "
            f"pro_used={self.stats['pro_detector_used']}, "
            f"legacy_used={self.stats['legacy_detector_used']}"
        )
    
    def run_forever(self) -> None:
        self.log.info("AggregatedSignalHubPro started for %s", self.cfg.symbol)
        self.log.info(f"Trades stream: {self.trades_stream}")
        self.log.info(f"Min trades for pro: {self.min_trades_for_pro}")
        
        delay = max(self.cfg.poll_ms, 200) / 1000.0
        last_stats_log = time.time()
        
        while True:
            try:
                self.step()
                
                # Логируем статистику каждые 60 секунд
                if time.time() - last_stats_log >= 60:
                    self._log_stats()
                    last_stats_log = time.time()
                    
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
    AggregatedSignalHubPro(r, cfg, log).run_forever()

