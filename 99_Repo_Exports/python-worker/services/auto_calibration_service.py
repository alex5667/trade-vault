from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras
import redis

from tools.trailing_tp1_calibration import (
    calibrate_trailing_offset,
)
from common.log import setup_logger

logger = setup_logger("AutoCalibrationService")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


@dataclass
class SymbolConfig:
    source: str
    symbol: str
    offsets: List[float]
    limit_trades: int = 300          # сколько последних сделок брать в калибровку
    min_total_trades: int = 100      # минимум сделок для старта калибровки
    min_new_trades: int = 30         # минимум новых сделок с прошлого запуска
    use_mfe_exit: bool = False       # использовать ли MFE-выход в симуляции


def _parse_float_list(s: str, default: List[float]) -> List[float]:
    try:
        out: List[float] = []
        for part in (s or "").split(","):
            part = part.strip()
            if not part:
                continue
            out.append(float(part))
        return out or list(default)
    except Exception:
        return list(default)


def _parse_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _parse_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


class AutoCalibrationService:
    def __init__(
        self,
        dsn: str,
        redis_url: str,
        symbols: List[SymbolConfig],
        use_walk_forward: bool = True,
    ) -> None:
        self._dsn = dsn
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self._symbols = symbols
        self._use_walk_forward = use_walk_forward

    # ----- Вспомогательные ключи Redis -----

    def _state_key(self, cfg: SymbolConfig) -> str:
        return f"calibration:trailing_tp1_offset:{cfg.source}:{cfg.symbol}"

    def _spec_key(self, symbol: str) -> str:
        # Canonical storage confirmed in project:
        #   GET "symbol_specs:{symbol}" -> JSON
        return f"symbol_specs:{symbol}"

    # ----- Работа с БД -----

    def _open_conn(self):
        # connect_timeout protects runner from hanging forever
        # application_name helps tracing in pg_stat_activity
        return psycopg2.connect(
            self._dsn,
            connect_timeout=_parse_int("AUTO_CALIB_PG_CONNECT_TIMEOUT_SEC", 5),
            application_name="auto_calibration_service",
        )

    def _load_trade_counters(
        self,
        conn,
        cfg: SymbolConfig,
        last_max_trade_id: int,
    ) -> Tuple[int, int, int]:
        """
        Возвращает (max_id, total_cnt, new_cnt) по сделкам с tp1_hit=TRUE.
        new_cnt — сколько сделок с id > last_max_trade_id.
        """
        sql = """
        SELECT
            max(id) AS max_id,
            count(*) AS total_cnt,
            sum(CASE WHEN id > %(last_id)s THEN 1 ELSE 0 END) AS new_cnt
        FROM trades_closed
        WHERE source = %(source)s
          AND symbol = %(symbol)s
          AND tp1_hit = TRUE;
        """
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                sql,
                {
                    "source": cfg.source,
                    "symbol": cfg.symbol,
                    "last_id": last_max_trade_id,
                },
            )
            row = cur.fetchone()

        max_id = int(row["max_id"]) if row["max_id"] is not None else 0
        total_cnt = int(row["total_cnt"] or 0)
        new_cnt = int(row["new_cnt"] or 0)
        return max_id, total_cnt, new_cnt

    # ----- Обновление symbol_spec -----

    def _update_symbol_spec_trailing_offset(
        self,
        symbol: str,
        offset_mult: float,
    ) -> None:
        """
        Обновляет trailing_tp1_offset_atr в symbol_spec.
        Здесь я предполагаю JSON-вью по ключу symbol:{symbol}:spec.
        Если у тебя HSET — просто замени реализацию.
        """
        key = self._spec_key(symbol)
        raw = self._redis.get(key)
        if raw:
            try:
                spec = json.loads(raw)
            except json.JSONDecodeError:
                spec = {}
        else:
            spec = {}

        # Обновляем trailing параметры
        if "trailing" not in spec:
            spec["trailing"] = {}

        spec["trailing"]["tp1_offset_atr"] = float(offset_mult)

        # Compact JSON, stable ordering (less diff-noise in debugging)
        self._redis.set(key, json.dumps(spec, separators=(",", ":"), sort_keys=True))

        logger.info(f"Updated trailing_tp1_offset_atr for {symbol}: {offset_mult}")

    # ----- Калибровка по одному символу -----

    def _calibrate_symbol(self, conn, cfg: SymbolConfig) -> None:
        state_key = self._state_key(cfg)
        raw_state = self._redis.get(state_key)
        if raw_state:
            try:
                state = json.loads(raw_state)
            except json.JSONDecodeError:
                state = {}
        else:
            state = {}

        last_max_trade_id = int(state.get("last_max_trade_id", 0))

        max_id, total_cnt, new_cnt = self._load_trade_counters(
            conn=conn,
            cfg=cfg,
            last_max_trade_id=last_max_trade_id,
        )

        if total_cnt < cfg.min_total_trades:
            logger.info(
                f"[{cfg.symbol}] skip: total trades {total_cnt} < "
                f"min_total_trades={cfg.min_total_trades}"
            )
            return

        # если уже запускали и новых сделок мало — пропускаем
        if last_max_trade_id > 0 and new_cnt < cfg.min_new_trades:
            logger.info(
                f"[{cfg.symbol}] skip: new trades {new_cnt} < "
                f"min_new_trades={cfg.min_new_trades}"
            )
            return

        logger.info(
            f"[{cfg.symbol}] calibrating: total={total_cnt}, "
            f"new={new_cnt}, max_id={max_id}, last_max_id={last_max_trade_id}, "
            f"mode={'walk-forward' if self._use_walk_forward else 'in-sample'}"
        )

        if self._use_walk_forward:
            self._calibrate_symbol_wf(conn, cfg, state_key, max_id, total_cnt, new_cnt)
        else:
            self._calibrate_symbol_insample(conn, cfg, state_key, max_id, total_cnt, new_cnt)

    def _calibrate_symbol_insample(
        self, conn, cfg: SymbolConfig, state_key: str,
        max_id: int, total_cnt: int, new_cnt: int,
    ) -> None:
        """Original in-sample calibration path (backward compat)."""
        best_stats, all_stats = calibrate_trailing_offset(
            conn=conn,
            source=cfg.source,
            symbol=cfg.symbol,
            offset_mult_list=cfg.offsets,
            limit=cfg.limit_trades,
            use_mfe_exit=cfg.use_mfe_exit,
        )

        if best_stats is None:
            logger.warning(f"[{cfg.symbol}] no trades loaded for calibration")
            return

        # лог для контроля
        for s in all_stats:
            from tools.trailing_tp1_calibration import score_offset
            sc = score_offset(s)
            logger.info(
                f"[{cfg.symbol}] offset={s.offset_mult:.2f} "
                f"count={s.count} "
                f"expR={s.expectancy_r:.3f} "
                f"giveback={s.avg_giveback_r:.3f} "
                f"missed={s.avg_missed_r:.3f} "
                f"fake={s.share_fake_stopout:.3f} "
                f"score={sc:.3f}"
            )

        logger.info(
            f"[{cfg.symbol}] best offset={best_stats.offset_mult:.2f} "
            f"expR={best_stats.expectancy_r:.3f} "
            f"giveback={best_stats.avg_giveback_r:.3f} "
            f"missed={best_stats.avg_missed_r:.3f} "
            f"fake={best_stats.share_fake_stopout:.3f} "
            f"count={best_stats.count}"
        )

        self._update_symbol_spec_trailing_offset(
            symbol=cfg.symbol,
            offset_mult=best_stats.offset_mult,
        )

        new_state = {
            "last_max_trade_id": max_id,
            "last_run_ts": datetime.now(timezone.utc).isoformat(),
            "last_offset_mult": float(best_stats.offset_mult),
            "total_cnt": total_cnt,
            "new_cnt": new_cnt,
            "mode": "in-sample",
        }
        self._redis.set(state_key, json.dumps(new_state))

    def _calibrate_symbol_wf(
        self, conn, cfg: SymbolConfig, state_key: str,
        max_id: int, total_cnt: int, new_cnt: int,
    ) -> None:
        """Walk-Forward calibration path — out-of-sample validated."""
        from tools.trailing_tp1_calibration import calibrate_trailing_offset_wf

        wf_min_train = _parse_int("AUTO_CALIB_WF_MIN_TRAIN", 100)
        wf_test = _parse_int("AUTO_CALIB_WF_TEST_TRADES", 30)
        wf_step = _parse_int("AUTO_CALIB_WF_STEP_TRADES", 20)
        wf_stability_thr = float(
            os.getenv("AUTO_CALIB_WF_STABILITY_THRESHOLD", "0.5") or 0.5
        )

        wf_result = calibrate_trailing_offset_wf(
            conn=conn,
            source=cfg.source,
            symbol=cfg.symbol,
            offset_mult_list=cfg.offsets,
            limit=cfg.limit_trades,
            use_mfe_exit=cfg.use_mfe_exit,
            min_train_trades=wf_min_train,
            test_trades=wf_test,
            step_trades=wf_step,
            stability_threshold=wf_stability_thr,
        )

        # Log fold details
        for f in wf_result.folds:
            logger.info(
                f"[{cfg.symbol}] WF fold {f.fold_idx}: "
                f"param={f.best_param:.3f}, "
                f"train_score={f.train_score:.3f}, "
                f"oos_sharpe={f.oos_sharpe:.3f}, "
                f"oos_pf={f.oos_profit_factor:.3f}, "
                f"oos_wr={f.oos_win_rate:.1%}"
            )

        logger.info(
            f"[{cfg.symbol}] WF result: robust_param={wf_result.robust_param:.3f}, "
            f"stability={wf_result.stability_score:.4f}, "
            f"deploy={wf_result.deploy}, "
            f"folds={wf_result.n_folds} ({wf_result.n_stable_folds} stable), "
            f"overfit_ratio={wf_result.overfit_ratio:.2f}"
        )

        # Deploy gate: only apply if OOS is stable
        if not wf_result.deploy:
            logger.warning(
                f"[{cfg.symbol}] WF calibration REJECTED: "
                f"stability={wf_result.stability_score:.4f} >= threshold "
                f"or insufficient stable folds ({wf_result.n_stable_folds}). "
                f"Keeping current offset."
            )
            # Still save state for observability, but don't apply
            new_state = {
                "last_max_trade_id": max_id,
                "last_run_ts": datetime.now(timezone.utc).isoformat(),
                "wf_rejected": True,
                "wf_stability_score": wf_result.stability_score,
                "wf_robust_param": wf_result.robust_param,
                "wf_n_folds": wf_result.n_folds,
                "wf_n_stable_folds": wf_result.n_stable_folds,
                "wf_overfit_ratio": wf_result.overfit_ratio,
                "total_cnt": total_cnt,
                "new_cnt": new_cnt,
                "mode": "walk-forward-rejected",
            }
            self._redis.set(state_key, json.dumps(new_state))
            return

        # Apply the robust parameter
        self._update_symbol_spec_trailing_offset(
            symbol=cfg.symbol,
            offset_mult=wf_result.robust_param,
        )

        new_state = {
            "last_max_trade_id": max_id,
            "last_run_ts": datetime.now(timezone.utc).isoformat(),
            "last_offset_mult": wf_result.robust_param,
            "wf_stability_score": wf_result.stability_score,
            "wf_n_folds": wf_result.n_folds,
            "wf_n_stable_folds": wf_result.n_stable_folds,
            "wf_overfit_ratio": wf_result.overfit_ratio,
            "wf_mean_oos_sharpe": wf_result.mean_oos_sharpe,
            "total_cnt": total_cnt,
            "new_cnt": new_cnt,
            "mode": "walk-forward",
        }
        self._redis.set(state_key, json.dumps(new_state))

    # ----- Public entrypoints -----

    def run_once(self) -> None:
        """Run one full pass for all configured symbols."""
        conn = self._open_conn()
        try:
            for cfg in self._symbols:
                self._calibrate_symbol(conn, cfg)
        finally:
            conn.close()

    def on_trade_closed(self, symbol: str, source: str) -> None:
        """
        Called when a trade is closed. Currently a no-op stub.
        In the future, this could trigger incremental calibration.
        """
        pass


# ----- Пример настройки и запуска -----

def _build_default_symbols() -> List[SymbolConfig]:
    """
    Пример: ETH и BTC с разными диапазонами offset_mult.
    Подправь под свои символы.
    """
    return [
        SymbolConfig(
            source="CryptoOrderFlow",
            symbol="ETHUSDT",
            offsets=[0.3, 0.4, 0.5, 0.6, 0.7],
            limit_trades=300,
            min_total_trades=150,
            min_new_trades=30,
            use_mfe_exit=False,
        ),
        SymbolConfig(
            source="CryptoOrderFlow",
            symbol="BTCUSDT",
            offsets=[0.5, 0.6, 0.8, 1.0],
            limit_trades=300,
            min_total_trades=150,
            min_new_trades=30,
            use_mfe_exit=False,
        ),
    ]


# Global service instance
_auto_calibration_service: Optional[AutoCalibrationService] = None
_auto_calibration_lock = threading.Lock()
_auto_calibration_inited: bool = False
_auto_calibration_cfg: dict = {}
_auto_calibration_lock = threading.Lock()


def _normalize_enabled_symbols(items: List[str]) -> Optional[set[str]]:
    """
    Returns:
      - None => means "all symbols enabled" (e.g. items empty or contains '*')
      - set of UPPERCASE symbols otherwise
    Accepts items like ["ETHUSDT", "BTCUSDT"] or ["ETHUSDT,BTCUSDT"].
    """
    if not items:
        return None
    out: set[str] = set()
    for it in items:
        if it is None:
            continue
        for part in str(it).split(","):
            s = part.strip()
            if not s:
                continue
            if s == "*":
                return None
            out.add(s.upper())
    return None if not out else out


def get_auto_calibration_service() -> AutoCalibrationService:
    """Get or create a singleton instance of AutoCalibrationService."""
    global _auto_calibration_service

    if _auto_calibration_service is None:
        dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")) or os.getenv("PG_DSN_CALIBRATION") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

        symbols = _build_default_symbols()
        use_wf = _env_bool("AUTO_CALIB_WALK_FORWARD", default=True)
        _auto_calibration_service = AutoCalibrationService(
            dsn=dsn, redis_url=redis_url, symbols=symbols, use_walk_forward=use_wf,
        )

    return _auto_calibration_service


def init_auto_calibration(trades_threshold: int, enabled_symbols: List[str], source: str) -> None:
    """
    Initialize auto calibration service for trade monitor runner.
    Creates/configures singleton AutoCalibrationService instance.

    - trades_threshold: applied as min_total_trades override (>=1)
    - enabled_symbols: list of symbols (["ETHUSDT","BTCUSDT"]) or comma-separated items (["ETHUSDT,BTCUSDT"])
    - source: if non-empty -> filters configs by cfg.source == source
    """
    global _auto_calibration_service

    with _auto_calibration_lock:
        thr = int(trades_threshold or 0)
        src = str(source or "").strip()
        enabled = _normalize_enabled_symbols(enabled_symbols)

        if thr <= 0:
            logger.warning(
                "Auto calibration disabled: trades_threshold <= 0 " +
                f"(threshold={trades_threshold}, source={src}, enabled_symbols={enabled_symbols})"
            )
            _auto_calibration_service = None
            return

        if not enabled_symbols:
            logger.warning(
                "Auto calibration disabled: enabled_symbols is empty " +
                f"(threshold={thr}, source={src})"
            )
            _auto_calibration_service = None
            return

        base = _build_default_symbols()
        selected: List[SymbolConfig] = []
        for cfg in base:
            if src and cfg.source != src:
                continue
            if enabled is not None and cfg.symbol.upper() not in enabled:
                continue

            # override min_total_trades by threshold (keep other defaults)
            d = asdict(cfg)
            d["min_total_trades"] = max(int(d.get("min_total_trades") or 0), thr)
            selected.append(SymbolConfig(**d))

        if not selected:
            logger.warning(
                "Auto calibration not started: no matching symbols after filtering " +
                f"(threshold={thr}, source={src or 'ANY'}, enabled_symbols={enabled_symbols})"
            )
            _auto_calibration_service = None
            return

        dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")) or os.getenv("PG_DSN_CALIBRATION") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        use_wf = _env_bool("AUTO_CALIB_WALK_FORWARD", default=True)

        _auto_calibration_service = AutoCalibrationService(
            dsn=dsn, redis_url=redis_url, symbols=selected, use_walk_forward=use_wf,
        )
        logger.info(
            "Auto calibration initialized: " +
            f"threshold={thr}, source={src or 'ANY'}, "
            f"symbols={[c.symbol for c in selected]}, "
            f"walk_forward={use_wf}"
        )


def main() -> None:
    dsn = os.getenv("PG_DSN_CALIBRATION", "postgresql://user:pass@localhost:5432/trade")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    symbols = _build_default_symbols()
    svc = AutoCalibrationService(dsn=dsn, redis_url=redis_url, symbols=symbols)
    svc.run_once()


if __name__ == "__main__":
    main()
