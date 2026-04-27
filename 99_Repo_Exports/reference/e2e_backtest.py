# e2e_backtest.py
"""
End-to-End Backtest - полный реплей исторических данных через всю пайплайн.
"""
from __future__ import annotations
import os
import json
import time
import csv
from typing import Dict, Any, List, Optional

try:
    import redis
except ImportError:
    redis = None

from common.log import setup_logger
from backtest_hooks import ReplayConfig, replay
from aggregated_signal_hub import AggregatedSignalHub

log = setup_logger("e2e_backtest")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def _emit_ta_signal(r, symbol: str, side: str, confidence_ta: float, atr: float, entry: float):
    """Эмитировать TA сигнал в Redis stream."""
    payload = {
        "sid": f"bt:{symbol}:{int(time.time()*1000)}",
        "symbol": symbol,
        "side": side,
        "confidence_ta": confidence_ta,
        "atr": atr,
        "entry": entry,
        "context": {"ta_bullish": side=="LONG", "ta_bearish": side=="SHORT"}
    }
    r.xadd(f"signals:ta:{symbol}", {"data": json.dumps(payload)}, maxlen=10000)

def _emit_of_signal(r, symbol: str, side: str, confidence_of: float, z_delta: float, entry: float):
    """Эмитировать OrderFlow сигнал в Redis stream."""
    payload = {
        "sid": f"bt:{symbol}:{int(time.time()*1000)}",
        "symbol": symbol,
        "side": side,
        "confidence_of": confidence_of,
        "z_delta": z_delta,
        "entry": entry,
        "context": {"of_extreme": True, "z_delta": z_delta}
    }
    r.xadd(f"signals:orderflow:{symbol}", {"data": json.dumps(payload)}, maxlen=10000)

def run_bt(parquet_path: str, symbol="XAUUSD", output_csv: Optional[str] = None):
    """
    Запустить end-to-end backtest.
    
    Args:
        parquet_path: Путь к Parquet файлу с историческими данными
        symbol: Торговый символ
        output_csv: Путь для сохранения результатов в CSV (опционально)
    """
    if not redis:
        raise RuntimeError("redis-py не установлен")
    
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    hub = AggregatedSignalHub(symbol=symbol)

    # запустим hub в «легком» синхронном режиме из этого же процесса (однопоточно)
    # в реале hub крутится своим контейнером.
    import threading
    t = threading.Thread(target=hub.run, daemon=True)
    t.start()

    cfg = ReplayConfig(parquet_path=parquet_path, speed=0.0, window_ticks=300)
    results = []
    
    def on_step(res):
        # простая эвристика: при экстремуме отдадим TA-сигнал для проверки пайплайна
        m = res["micro"]
        if m.get("ok") and m.get("extreme"):
            side = "LONG" if m["z_delta"] > 0 else "SHORT"
            entry = float(res["mid"])
            atr = max(1.0, 0.5 * abs(m["z_delta"]))  # суррогат ATR для BT
            
            # Эмитируем оба типа сигналов
            _emit_ta_signal(r, symbol, side, confidence_ta=65.0, atr=atr, entry=entry)
            _emit_of_signal(r, symbol, side, confidence_of=70.0, z_delta=m["z_delta"], entry=entry)
            
            results.append({
                "ts": res["ts"],
                "mid": res["mid"],
                "side": side,
                "z_delta": m["z_delta"],
                "atr": atr,
                "micro": m,
                "cluster": res["cluster"]
            })
        return False

    log.info("Starting E2E backtest with parquet: %s", parquet_path)
    replay(cfg, on_step=on_step)
    time.sleep(2.0)  # даём hub дочитать
    
    log.info("Backtest completed. Generated %d signals", len(results))
    
    if output_csv:
        with open(output_csv, 'w', newline='') as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
        log.info("Results saved to: %s", output_csv)
    
    return results

if __name__ == "__main__":
    path = os.getenv("BT_PARQUET", "/data/xau_ticks.parquet")
    output = os.getenv("BT_OUTPUT_CSV", "/data/backtest_results.csv")
    symbol = os.getenv("SYMBOL", "XAUUSD")
    run_bt(path, symbol, output)
