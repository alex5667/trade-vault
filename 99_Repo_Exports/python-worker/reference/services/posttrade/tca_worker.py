from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""tca_worker — compute TCA metrics and publish Redis rollups (Phase B2/B3).

Data flow
---------
fills_writer            → fills (Timescale)
decision_snapshot_writer→ decision_snapshot (Timescale)
bbo_ts_writer           → bbo_ts (Timescale)

tca_worker reads those tables and writes:
  - tca_fill_metrics (Timescale)
  - Redis rollups keys for online gates (B3)

Why not compute directly on streams?
-----------------------------------
Post-trade analytics requires stable, queryable joins.
DB is the source of truth. Redis is only a cache for online gating.

Safety
------
* at-least-once: idempotent upsert into tca_fill_metrics
* fail-open: missing BBO/decision rows cause skip/partial metrics, not crashes
* bounded: batch processing with cursor

ENV
---
REDIS_URL=redis://redis-worker-1:6379/0
TRADES_DB_DSN=postgresql://trading:...@postgres:5432/scanner_analytics

TCA_WORKER_BATCH_SIZE=200
TCA_WORKER_POLL_SEC=2
TCA_CURSOR_KEY=tca_worker:cursor_v1

EXEC_TCA_DELTA_SEC_LIST=1,5
EXEC_TCA_ROLLUP_WINDOW_MIN=60
EXEC_TCA_REDIS_TTL_SEC=600
TCA_ROLLUPS_ENABLE=1
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from services.posttrade.tca_math import (
    effective_spread_bps,
    realized_spread_bps,
    permanent_impact_bps,
    implementation_shortfall_bps,
)
from services.posttrade.tca_redis_state import TcaKeyDims, write_rollups


logger = logging.getLogger("tca_worker")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(_env(name, default)))
    except Exception:
        return int(float(default))


def _env_bool(name: str, default: str) -> bool:
    return str(_env(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return get_ny_time_millis()


def pick_dsn() -> str:
    return (
        os.getenv("TCA_DB_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("ANALYTICS_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or ""
    )


def _parse_delta_list(s: str) -> List[int]:
    out: List[int] = []
    for p in str(s or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue
    return out or [1, 5]


class Pg:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            import psycopg2  # type: ignore
            return psycopg2.connect(self.dsn)

    def fetch_fills_after(self, *, cursor_ts_ms: int, cursor_sid: str, limit: int) -> List[Dict[str, Any]]:
        q = (
            "SELECT ts_fill_ms, sid, sym, venue, side, fill_role, px, qty, fee_bps "
            "FROM fills WHERE (ts_fill_ms, sid) > (%s, %s) "
            "ORDER BY ts_fill_ms ASC, sid ASC LIMIT %s"
        )
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(q, (int(cursor_ts_ms), str(cursor_sid), int(limit)))
            out = []
            for row in cur.fetchall() or []:
                out.append(
                    {
                        "ts_fill_ms": int(row[0]),
                        "sid": str(row[1]),
                        "sym": str(row[2]),
                        "venue": str(row[3]),
                        "side": str(row[4]),
                        "fill_role": str(row[5]),
                        "px": float(row[6]),
                        "qty": float(row[7]),
                        "fee_bps": float(row[8]),
                    }
                )
            return out
        finally:
            conn.close()

    def fetch_decision_for_fill(self, *, sid: str, ts_fill_ms: int) -> Optional[Dict[str, Any]]:
        q = (
            "SELECT ts_decision_ms, session, tf, kind, side, venue, decision_mid "
            "FROM decision_snapshot WHERE sid=%s AND ts_decision_ms <= %s "
            "ORDER BY ts_decision_ms DESC LIMIT 1"
        )
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(q, (str(sid), int(ts_fill_ms)))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "decision_ts_ms": int(row[0]),
                "session": str(row[1]),
                "tf": str(row[2]),
                "kind": str(row[3]),
                "side": str(row[4]),
                "venue": str(row[5]),
                "decision_mid": float(row[6]) if row[6] is not None else None,
            }
        finally:
            conn.close()

    def fetch_bbo_mid(self, *, sym: str, venue: str, ts_ms: int, lookback_ms: int) -> Optional[Dict[str, float]]:
        # nearest <= ts within lookback
        q = (
            "SELECT bid, ask, mid, ts_ms FROM bbo_ts "
            "WHERE sym=%s AND venue=%s AND ts <= to_timestamp(%s/1000.0) "
            "  AND ts >= to_timestamp(%s/1000.0) "
            "ORDER BY ts DESC LIMIT 1"
        )
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(q, (str(sym).upper(), str(venue).lower(), int(ts_ms), int(ts_ms - lookback_ms)))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "bid": float(row[0]),
                "ask": float(row[1]),
                "mid": float(row[2]),
                "ts_ms": int(row[3]),
            }
        finally:
            conn.close()

    def upsert_tca_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                "INSERT INTO tca_fill_metrics (ts, ts_fill_ms, sid, sym, venue, side, fill_role, "
                "decision_ts_ms, session, tf, kind, decision_mid, "
                "mid_t, bid_t, ask_t, mid_t_1s, mid_t_5s, "
                "eff_spread_bps, realized_spread_1s_bps, realized_spread_5s_bps, perm_impact_1s_bps, perm_impact_5s_bps, is_bps, "
                "px, qty, fee_bps, ts_insert_ms) "
                "VALUES (to_timestamp(%(ts_fill_ms)s/1000.0), %(ts_fill_ms)s, %(sid)s, %(sym)s, %(venue)s, %(side)s, %(fill_role)s, "
                "%(decision_ts_ms)s, %(session)s, %(tf)s, %(kind)s, %(decision_mid)s, "
                "%(mid_t)s, %(bid_t)s, %(ask_t)s, %(mid_t_1s)s, %(mid_t_5s)s, "
                "%(eff_spread_bps)s, %(realized_spread_1s_bps)s, %(realized_spread_5s_bps)s, %(perm_impact_1s_bps)s, %(perm_impact_5s_bps)s, %(is_bps)s, "
                "%(px)s, %(qty)s, %(fee_bps)s, %(ts_insert_ms)s) "
                "ON CONFLICT (sid, ts_fill_ms, fill_role) DO UPDATE SET "
                "decision_ts_ms=excluded.decision_ts_ms, session=excluded.session, tf=excluded.tf, kind=excluded.kind, decision_mid=excluded.decision_mid, "
                "mid_t=excluded.mid_t, bid_t=excluded.bid_t, ask_t=excluded.ask_t, mid_t_1s=excluded.mid_t_1s, mid_t_5s=excluded.mid_t_5s, "
                "eff_spread_bps=excluded.eff_spread_bps, realized_spread_1s_bps=excluded.realized_spread_1s_bps, realized_spread_5s_bps=excluded.realized_spread_5s_bps, "
                "perm_impact_1s_bps=excluded.perm_impact_1s_bps, perm_impact_5s_bps=excluded.perm_impact_5s_bps, is_bps=excluded.is_bps, "
                "px=excluded.px, qty=excluded.qty, fee_bps=excluded.fee_bps, ts_insert_ms=excluded.ts_insert_ms"
            )
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)
        finally:
            conn.close()

    def compute_rollups(self, *, dims: TcaKeyDims, window_min: int, delta_sec: int) -> Dict[str, float]:
        # For P1 we compute only the minimal set used by ExecutionHealthGate.
        # NOTE: percentile_cont ignores NULL.
        col = "1s" if int(delta_sec) == 1 else "5s" if int(delta_sec) == 5 else f"{int(delta_sec)}s"
        # We only support columns 1s and 5s at P1; other deltas are ignored.
        if col not in {"1s", "5s"}:
            return {}

        # Column names are derived from a controlled allowlist (col in {1s,5s}).
        col_imp = f"perm_impact_{col}_bps"
        col_rs = f"realized_spread_{col}_bps"
        q2 = (
            "SELECT "
            "  percentile_cont(0.95) WITHIN GROUP (ORDER BY is_bps) AS is_p95, "
            "  percentile_cont(0.95) WITHIN GROUP (ORDER BY eff_spread_bps) AS eff_p95, "
            f"  percentile_cont(0.95) WITHIN GROUP (ORDER BY {col_imp}) AS imp_p95, "
            f"  percentile_cont(0.50) WITHIN GROUP (ORDER BY {col_rs}) AS rs_p50 "
            "FROM tca_fill_metrics "
            "WHERE ts > now() - (%s || ' minutes')::interval "
            "  AND sym=%s AND venue=%s AND session=%s AND tf=%s AND kind=%s AND side=%s"
        )
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                q2,
                (
                    int(window_min),
                    dims.sym.upper(),
                    dims.venue.lower(),
                    dims.session,
                    dims.tf,
                    dims.kind,
                    dims.side.upper(),
                )
            )
            row = cur.fetchone()
            if not row:
                return {}
            out: Dict[str, float] = {}
            if row[0] is not None:
                out["is_p95"] = float(row[0])
            if row[1] is not None:
                out["eff_spread_p95"] = float(row[1])
            if row[2] is not None:
                out["perm_impact_p95"] = float(row[2])
            if row[3] is not None:
                out["realized_spread_p50"] = float(row[3])
            return out
        finally:
            conn.close()


@dataclass
class Cfg:
    redis_url: str
    batch_size: int
    poll_sec: float
    cursor_key: str
    deltas: List[int]
    rollup_window_min: int
    redis_ttl_sec: int
    rollups_enable: bool
    bbo_lookback_ms: int

    @staticmethod
    def from_env() -> "Cfg":
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            batch_size=_env_int("TCA_WORKER_BATCH_SIZE", "200"),
            poll_sec=float(_env("TCA_WORKER_POLL_SEC", "2") or 2),
            cursor_key=_env("TCA_CURSOR_KEY", "tca_worker:cursor_v1"),
            deltas=_parse_delta_list(_env("EXEC_TCA_DELTA_SEC_LIST", "1,5")),
            rollup_window_min=_env_int("EXEC_TCA_ROLLUP_WINDOW_MIN", "60"),
            redis_ttl_sec=_env_int("EXEC_TCA_REDIS_TTL_SEC", "600"),
            rollups_enable=_env_bool("TCA_ROLLUPS_ENABLE", "1"),
            bbo_lookback_ms=_env_int("TCA_MAX_BBO_LOOKBACK_MS", "3000"),
        )


async def _load_cursor(r: Any, key: str) -> Tuple[int, str]:
    try:
        raw = await r.get(key)
        if raw is None:
            return 0, ""
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        obj = json.loads(str(raw))
        return int(obj.get("ts_fill_ms", 0) or 0), str(obj.get("sid", "") or "")
    except Exception:
        return 0, ""


async def _save_cursor(r: Any, key: str, ts_fill_ms: int, sid: str) -> None:
    try:
        await r.set(key, json.dumps({"ts_fill_ms": int(ts_fill_ms), "sid": str(sid)}, separators=(",", ":")))
    except Exception:
        return


async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")

    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("TRADES_DB_DSN must be set")

    cfg = Cfg.from_env()
    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    pg = Pg(dsn)

    logger.info("tca_worker started")

    while True:
        try:
            cur_ts, cur_sid = await _load_cursor(r, cfg.cursor_key)
            fills = pg.fetch_fills_after(cursor_ts_ms=cur_ts, cursor_sid=cur_sid, limit=cfg.batch_size)
            if not fills:
                await asyncio.sleep(cfg.poll_sec)
                continue

            tca_rows: List[Dict[str, Any]] = []
            touched_dims: Dict[Tuple[str, str, str, str, str, str], TcaKeyDims] = {}
            last = (cur_ts, cur_sid)

            for f in fills:
                last = (int(f["ts_fill_ms"]), str(f["sid"]))
                sid = str(f["sid"])
                sym = str(f["sym"]).upper()
                venue = str(f["venue"]).lower()
                side = str(f["side"]).upper()
                ts_fill_ms = int(f["ts_fill_ms"])

                dec = pg.fetch_decision_for_fill(sid=sid, ts_fill_ms=ts_fill_ms)
                if dec is None:
                    # No decision snapshot yet (race): skip; next iteration will retry.
                    continue

                # Trust decision snapshot for venue/side/session/tf/kind if fill lacks.
                session = str(dec.get("session") or "na")
                tf = str(dec.get("tf") or "na")
                kind = str(dec.get("kind") or "na")
                if not venue or venue == "none":
                    venue = str(dec.get("venue") or venue or "binance").lower()
                if not side or side == "NONE":
                    side = str(dec.get("side") or side or "na").upper()

                decision_mid = dec.get("decision_mid")

                bbo_t = pg.fetch_bbo_mid(sym=sym, venue=venue, ts_ms=ts_fill_ms, lookback_ms=cfg.bbo_lookback_ms)
                mid_t = bbo_t.get("mid") if bbo_t else None
                bid_t = bbo_t.get("bid") if bbo_t else None
                ask_t = bbo_t.get("ask") if bbo_t else None

                # Δ mids
                mid_1s = mid_5s = None
                for d in cfg.deltas:
                    ts_d = ts_fill_ms + int(d) * 1000
                    bbo_d = pg.fetch_bbo_mid(sym=sym, venue=venue, ts_ms=ts_d, lookback_ms=max(cfg.bbo_lookback_ms, int(d) * 1000 + 500))
                    if bbo_d is None:
                        continue
                    if int(d) == 1:
                        mid_1s = bbo_d.get("mid")
                    if int(d) == 5:
                        mid_5s = bbo_d.get("mid")

                # TCA formulas
                eff = effective_spread_bps(trade_px=float(f["px"]), mid_t=float(mid_t) if mid_t is not None else 0.0, side=side) if mid_t is not None else None
                rs_1 = realized_spread_bps(trade_px=float(f["px"]), mid_t=float(mid_t), mid_t_delta=float(mid_1s), side=side) if (mid_t is not None and mid_1s is not None) else None
                rs_5 = realized_spread_bps(trade_px=float(f["px"]), mid_t=float(mid_t), mid_t_delta=float(mid_5s), side=side) if (mid_t is not None and mid_5s is not None) else None
                imp_1 = permanent_impact_bps(mid_t=float(mid_t), mid_t_delta=float(mid_1s), side=side) if (mid_t is not None and mid_1s is not None) else None
                imp_5 = permanent_impact_bps(mid_t=float(mid_t), mid_t_delta=float(mid_5s), side=side) if (mid_t is not None and mid_5s is not None) else None

                is_bps = None
                if decision_mid is not None:
                    is_bps = implementation_shortfall_bps(
                        vwap_fill_px=float(f["px"]),
                        decision_mid=float(decision_mid),
                        side=side,
                        fee_bps=float(f["fee_bps"]),
                    )

                tca_rows.append(
                    {
                        "ts_fill_ms": ts_fill_ms,
                        "sid": sid,
                        "sym": sym,
                        "venue": venue,
                        "side": side,
                        "fill_role": str(f.get("fill_role") or "entry"),
                        "decision_ts_ms": int(dec["decision_ts_ms"]),
                        "session": session,
                        "tf": tf,
                        "kind": kind,
                        "decision_mid": float(decision_mid) if decision_mid is not None else None,
                        "mid_t": float(mid_t) if mid_t is not None else None,
                        "bid_t": float(bid_t) if bid_t is not None else None,
                        "ask_t": float(ask_t) if ask_t is not None else None,
                        "mid_t_1s": float(mid_1s) if mid_1s is not None else None,
                        "mid_t_5s": float(mid_5s) if mid_5s is not None else None,
                        "eff_spread_bps": float(eff) if eff is not None else None,
                        "realized_spread_1s_bps": float(rs_1) if rs_1 is not None else None,
                        "realized_spread_5s_bps": float(rs_5) if rs_5 is not None else None,
                        "perm_impact_1s_bps": float(imp_1) if imp_1 is not None else None,
                        "perm_impact_5s_bps": float(imp_5) if imp_5 is not None else None,
                        "is_bps": float(is_bps) if is_bps is not None else None,
                        "px": float(f["px"]),
                        "qty": float(f["qty"]),
                        "fee_bps": float(f["fee_bps"]),
                        "ts_insert_ms": _now_ms(),
                    }
                )

                dims = TcaKeyDims(sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side)
                touched_dims[(dims.sym, dims.venue, dims.session, dims.tf, dims.kind, dims.side)] = dims

            if tca_rows:
                pg.upsert_tca_rows(tca_rows)

            # Update cursor even if some rows were skipped (monotonic scan).
            await _save_cursor(r, cfg.cursor_key, last[0], last[1])

            # Redis rollups for online gates
            if cfg.rollups_enable and touched_dims:
                for dims in touched_dims.values():
                    # Compute rollups for Δ=1s only (P1 minimal). Δ=5s can be added later.
                    try:
                        roll = pg.compute_rollups(dims=dims, window_min=cfg.rollup_window_min, delta_sec=1)
                        if roll:
                            await write_rollups(
                                redis=r,
                                dims=dims,
                                rollups=roll,
                                ttl_sec=cfg.redis_ttl_sec,
                                delta_sec=1,
                            )
                    except Exception:
                        continue

        except Exception:
            logger.exception("tca_worker loop error")
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(main())
