import asyncio
import os
import time
from services.periodic_reporter import PeriodicReporter

async def main():
    r = PeriodicReporter(
        redis_dsn="redis://127.0.0.1:6379/0",
        stats_dsn="redis://127.0.0.1:6379/4"
    )
    # simulate what _generate_and_send_report_internal does
    src = "cryptoorderflow"
    sym = "ALL"
    win = 3600
    trades = r._iter_recent_trades_window(
        strategy="crypto_orderflow",
        symbol=sym,
        tf="tick",
        source=src,
        window_seconds=win
    )
    print(f"Total trades fetched from ZSET/Stream: {len(trades)}")
    
    _min_conf = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
    
    valid_count = 0
    skipped_conf = 0
    for t in trades:
        _conf_raw = t.get("conf") or t.get("confidence")
        if _conf_raw is None:
            _inds = t.get("indicators") or {}
            if isinstance(_inds, dict):
                _conf_raw = _inds.get("confidence") or _inds.get("conf") or _inds.get("score")
        try:
            _conf_val = float(_conf_raw) * 100.0 if float(_conf_raw) <= 1.0 else float(_conf_raw)
        except (ValueError, TypeError):
            _conf_val = 100.0

        if _conf_val < _min_conf:
            skipped_conf += 1
            continue
        valid_count += 1
        
        # print some debug
        print(f"Valid trade: ID={t.get('order_id')} symbol={t.get('symbol')} conf_val={_conf_val} is_v={t.get('is_virtual')}")

    print(f"Valid: {valid_count}, Skipped conf: {skipped_conf}")

if __name__ == "__main__":
    asyncio.run(main())
