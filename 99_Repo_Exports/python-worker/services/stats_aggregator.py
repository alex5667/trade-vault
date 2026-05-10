# services/stats_aggregator.py
from __future__ import annotations

import json
import math
import os
from typing import Any

# EV gate EMA stats (best-effort)
from common.log import setup_logger
from domain.normalizers import bucket_close_reason, canon_source, canon_strategy, canon_symbol, canon_tf
from utils.time_utils import get_ny_time_millis
import contextlib

log = setup_logger("StatsAggregator")

STATS_EPS = float(os.getenv("STATS_EPS", "1e-9"))
STATS_DEDUPE_TTL_SEC = int(os.getenv("STATS_DEDUPE_TTL_SEC", str(60 * 60 * 24 * 30)))  # 30 days
STATS_REQUIRE_EXPLICIT_FINAL = os.getenv("STATS_REQUIRE_EXPLICIT_FINAL", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Empirical levels buffers (MFE/MAE/TTD) for quantiles.
# These buffers are used by signals.empirical_levels.RedisEmpiricalStatsProvider
# to compute q60/q80/median and dynamically calibrate TP1/SL.
# Fail-open: if disabled -> no extra keys/args, no extra Redis writes.
# ---------------------------------------------------------------------------
EMP_LEVELS_BUF_ENABLED = os.getenv("LEVELS_EMPIRICAL_BUF_ENABLED", "0").strip().lower() in {"1","true","yes","on"}
EMP_LEVELS_BUF_MAX = int(os.getenv("LEVELS_EMPIRICAL_BUF_MAX", "300") or 300)
EMP_LEVELS_BUF_TTL_SEC = int(os.getenv("LEVELS_EMPIRICAL_BUF_TTL_SEC", "2592000") or 2592000)  # 30d
EMP_LEVELS_USE_REGIME_DIM = os.getenv("LEVELS_EMPIRICAL_USE_REGIME_DIM", "1").strip().lower() in {"1","true","yes","on"}



def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _parse_csv_ints(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        with contextlib.suppress(Exception):
            out.append(int(p))
    return out


def _time_buckets_ms_from_env() -> list[int]:
    mins = _parse_csv_ints(os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45"))
    ms = [m * 60_000 for m in mins if m and m > 0]
    ms.sort()
    return ms


def _parse_json_dict_strfloat(v: Any) -> dict[int, float]:
    """
    Accept:
      - dict[int,float] already
      - JSON string {"60000": 12.3, ...}
    Return:
      dict[int,float] with bucket_ms keys
    """
    if isinstance(v, dict):
        out: dict[int, float] = {}
        for k, x in v.items():
            try:
                kk = int(k)
                xx = float(x)
                # NOTE:
                #   MAE PnL is often negative for LONG (drawdown),
                #   and positive magnitude for some specs.
                #   We store raw PnL (can be negative) and later convert to bps via abs().
                if math.isfinite(xx) and abs(xx) > 0:
                    out[kk] = xx
            except Exception:
                continue
        return out
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="ignore")
        except Exception:
            return {}
    if isinstance(v, str):
        s = v.strip()
        if not s or s[0] != "{":
            return {}
        try:
            obj = json.loads(s)
        except Exception:
            return {}
        if isinstance(obj, dict):
            out2: dict[int, float] = {}
            for k, x in obj.items():
                try:
                    kk = int(k)
                    xx = float(x)
                    # NOTE:
                    #   MAE PnL is often negative for LONG (drawdown),
                    #   and positive magnitude for some specs.
                    #   We store raw PnL (can be negative) and later convert to bps via abs().
                    if math.isfinite(xx) and abs(xx) > 0:
                        out2[kk] = xx
                except Exception:
                    continue
            return out2
    return {}


def _estimate_notional(
    *, entry_price: float | None, qty: float | None, notional: float | None
) -> float | None:
    """
    Keep it consistent with _estimate_bps_from_pnl() logic:
    notional ~= |qty| * entry_price (for USDT-quoted symbols this is exact enough).
    """
    nt = None
    try:
        if notional is not None and notional > STATS_EPS:
            nt = notional
    except Exception:
        nt = None
    if nt is not None:
        return nt
    try:
        if qty is not None and entry_price is not None:
            q = abs(qty)
            e = entry_price
            if q > STATS_EPS and e > STATS_EPS:
                return q * e
    except Exception:
        pass
    return None


def _pnl_to_bps(pnl: float, *, entry_price: float | None, qty: float | None, notional: float | None) -> float | None:
    """
    Convert pnl (quote) -> bps using notional.
    bps = |pnl| / notional * 10000
    """
    try:
        pnl_f = pnl
        nt = _estimate_notional(entry_price=entry_price, qty=qty, notional=notional)
        if nt is None or nt <= STATS_EPS:
            return None
        bps = abs(pnl_f) / nt * 10_000.0
        if math.isfinite(bps) and bps > 0:
            return bps
    except Exception:
        pass
    return None


def _write_timebucket_buffers(
    redis_client: Any,
    *,
    strategy: str,
    symbol: str,
    tf: str,
    regime_key: str,
    trade_closed: dict[str, Any],
) -> None:
    """
    Writer for time-bucket empirical buffers (MFE@T / MAE@T) and survival counters.

    Keys:
      statsbuf:{strategy}:{symbol}:{tf}:{regime}:mfe_bps_t{bucket_ms}   LIST
      statsbuf:{strategy}:{symbol}:{tf}:{regime}:mae_bps_t{bucket_ms}   LIST
      statscnt:{strategy}:{symbol}:{tf}:{regime}:survival               HASH
        - total
        - alive_t{bucket_ms}

    This is invoked only AFTER the main stats Lua applied=1 (i.e. not a dedup).
    Fail-open: any issues => skip (do not break stats aggregation).
    """
    if not _env_bool("EMP_TIME_SNAPSHOTS_WRITE", True):
        return
    buckets = _time_buckets_ms_from_env()
    if not buckets:
        return
    try:
        buf_max = int(os.getenv("EMP_TIME_SNAPSHOT_BUF_MAX", "300"))
    except Exception:
        buf_max = 300
    try:
        buf_ttl = int(os.getenv("EMP_TIME_SNAPSHOT_BUF_TTL_SEC", "0"))
    except Exception:
        buf_ttl = 0

    # Inputs needed for pnl->bps
    entry_price = None
    qty = None
    notional = None
    try:
        entry_price = float(trade_closed.get("entry_price") or 0.0) if trade_closed.get("entry_price") is not None else None
    except Exception:
        entry_price = None
    try:
        qty = float(trade_closed.get("lot") or trade_closed.get("qty") or 0.0) if (trade_closed.get("lot") is not None or trade_closed.get("qty") is not None) else None
    except Exception:
        qty = None
    try:
        notional = float(trade_closed.get("notional_usd") or trade_closed.get("notional") or 0.0) if (trade_closed.get("notional_usd") is not None or trade_closed.get("notional") is not None) else None
    except Exception:
        notional = None

    mfe_pnl_t = _parse_json_dict_strfloat(trade_closed.get("mfe_pnl_t"))
    mae_pnl_t = _parse_json_dict_strfloat(trade_closed.get("mae_pnl_t"))
    if not mfe_pnl_t and not mae_pnl_t:
        return

    # Duration for survival counters (alive_to_T approximation)
    try:
        duration_ms = int(float(trade_closed.get("duration_ms") or 0))
    except Exception:
        duration_ms = 0

    pipe = redis_client.pipeline(transaction=False)
    # survival counters (cheap and extremely useful for regime-specific "survive(T) >= S_MIN")
    surv_key = f"statscnt:{strategy}:{symbol}:{tf}:{regime_key}:survival"
    try:
        pipe.hincrby(surv_key, "total", 1)
        for b in buckets:
            if duration_ms >= b and b > 0:
                pipe.hincrby(surv_key, f"alive_t{b}", 1)
        if buf_ttl and buf_ttl > 0:
            pipe.expire(surv_key, buf_ttl)
    except Exception:
        # keep going; buffers still useful
        pass

    # buffers: per-bucket MFE/MAE in bps
    for b in buckets:
        if b in mfe_pnl_t:
            bps = _pnl_to_bps(mfe_pnl_t[b], entry_price=entry_price, qty=qty, notional=notional)
            if bps is not None and bps > 0:
                k = f"statsbuf:{strategy}:{symbol}:{tf}:{regime_key}:mfe_bps_t{b}"
                pipe.lpush(k, str(bps))
                pipe.ltrim(k, 0, max(buf_max, 1) - 1)
                if buf_ttl and buf_ttl > 0:
                    pipe.expire(k, buf_ttl)
        if b in mae_pnl_t:
            bps = _pnl_to_bps(mae_pnl_t[b], entry_price=entry_price, qty=qty, notional=notional)
            if bps is not None and bps > 0:
                k = f"statsbuf:{strategy}:{symbol}:{tf}:{regime_key}:mae_bps_t{b}"
                pipe.lpush(k, str(bps))
                pipe.ltrim(k, 0, max(buf_max, 1) - 1)
                if buf_ttl and buf_ttl > 0:
                    pipe.expire(k, buf_ttl)
    with contextlib.suppress(Exception):
        pipe.execute()


def canon_regime(v: Any) -> str:
    """
    Normalize regime to a stable key segment.
    Stored as part of statsbuf key if LEVELS_EMPIRICAL_USE_REGIME_DIM=1.
    """
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
    return s if s else "na"


def _first_nonzero_float(*vals: Any) -> float | None:
    for x in vals:
        try:
            if x is None:
                continue
            f = float(x)
            if math.isfinite(f) and abs(f) > STATS_EPS:
                return f
        except Exception:
            pass
    return None


def _first_positive_float(*vals: Any) -> float | None:
    for x in vals:
        try:
            if x is None:
                continue
            f = float(x)
            if math.isfinite(f) and f > STATS_EPS:
                return f
        except Exception:
            pass
    return None


def _estimate_bps_from_pnl(pnl: float, *, entry_price: float | None, qty: float | None, notional: float | None) -> float | None:
    """
    Convert PnL (quote currency) into bps using notional ~= |qty|*entry_price.
    If notional is missing and qty/entry are missing -> returns None (fail-open).
    """
    try:
        pnl_f = pnl
        if not math.isfinite(pnl_f):
            return None
        nt = _first_positive_float(notional)
        if nt is None:
            ep = _first_positive_float(entry_price)
            q = _first_nonzero_float(qty)
            if ep is None or q is None:
                return None
            nt = abs(q) * ep
        if nt <= STATS_EPS:
            return None
        bps = abs(pnl_f) / nt * 10_000.0
        return bps if math.isfinite(bps) and bps > 0 else None
    except Exception:
        return None



def _pick_entry_regime(trade_closed: dict[str, Any]) -> str:
    """
    IMPORTANT: we segment empirical levels & EV stats by *entry* regime, not exit regime.
    Use entry_regime if present; fallback to regime.
    """
    try:
        v = trade_closed.get("entry_regime") or trade_closed.get("regime")
    except Exception:
        v = None
    return canon_regime(v)

def extract_empirical_triplet(trade_closed: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a dict with:
      regime_key, mfe_bps, mae_bps, ttd_tp1_ms

    Accepts multiple possible field names to be robust across pipelines.
    Fail-open: returns None-like values if not enough info.
    """
    # Regime (optional)
    # Regime (optional) - logic moved to _pick_entry_regime used in return


    # If pipeline already supplies bps, prefer them.
    mfe_bps = _first_positive_float(trade_closed.get("mfe_bps"), trade_closed.get("mfe_bp"))
    mae_bps = _first_positive_float(trade_closed.get("mae_bps"), trade_closed.get("mae_bp"))

    # Otherwise attempt to convert mfe_pnl/mae_pnl to bps.
    entry_price = _first_positive_float(
        trade_closed.get("entry_price"),
        trade_closed.get("entry"),
        trade_closed.get("avg_entry"),
        trade_closed.get("open_price"),
    )
    qty = _first_nonzero_float(
        trade_closed.get("qty"),
        trade_closed.get("size"),
        trade_closed.get("position_qty"),
        trade_closed.get("base_qty"),
    )
    notional = _first_positive_float(
        trade_closed.get("notional"),
        trade_closed.get("position_notional"),
        trade_closed.get("entry_notional"),
    )

    tp1_hit = _boolish(trade_closed.get("tp1_hit"))

    if mfe_bps is None:
        # Priority: snapshot at TP1 (if hit) -> global MFE
        val_pnl = None
        if tp1_hit:
            val_pnl = _first_nonzero_float(trade_closed.get("mfe_pnl_at_tp1"))
        if val_pnl is None:
            val_pnl = _safe_float(trade_closed.get("mfe_pnl") or 0.0)
        mfe_bps = _estimate_bps_from_pnl(val_pnl, entry_price=entry_price, qty=qty, notional=notional)

    if mae_bps is None:
        # Priority: snapshot before TP1 (if hit) -> global MAE
        val_pnl = None
        if tp1_hit:
            val_pnl = _first_nonzero_float(trade_closed.get("mae_pnl_before_tp1"))
        if val_pnl is None:
            val_pnl = _safe_float(trade_closed.get("mae_pnl") or 0.0)

        # MAE PnL is usually negative, we want BPS to be positive distance
        # _estimate_bps_from_pnl uses abs(pnl) inside, so sign doesn't matter much,
        # but let's be consistent.
        mae_bps = _estimate_bps_from_pnl(val_pnl, entry_price=entry_price, qty=qty, notional=notional)

    # TTD_tp1 (best-effort):
    # If tp1_hit and we have timestamps: tp1_hit_ts - entry_ts
    ttd_tp1_ms = 0
    try:
        tp1_hit = _safe_int(trade_closed.get("tp1_hit") or 0)
        if tp1_hit:
            tp1_ts = _safe_int(trade_closed.get("tp1_hit_ts_ms") or trade_closed.get("tp1_ts_ms") or 0)
            entry_ts = _safe_int(
                trade_closed.get("entry_ts_ms")
                or trade_closed.get("open_ts_ms")
                or trade_closed.get("enter_ts_ms")
                or trade_closed.get("open_time_ms")
                or 0
            )
            if tp1_ts > 0 and entry_ts > 0 and tp1_ts >= entry_ts:
                ttd_tp1_ms = tp1_ts - entry_ts
    except Exception:
        ttd_tp1_ms = 0

    return {
        # IMPORTANT: entry regime, not exit regime
        # (otherwise your online calibration learns "wrong buckets" when regime flips mid-trade)
        "regime": _pick_entry_regime(trade_closed),
        "mfe_bps": mfe_bps if mfe_bps is not None else None,
        "mae_bps": mae_bps if mae_bps is not None else None,
        "ttd_tp1_ms": ttd_tp1_ms if ttd_tp1_ms > 0 else 0,
    }


def _to_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return str(v)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _boolish(v) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


# -------- Lua: atomic dedupe + counters + indexes --------
# KEYS:
#   1 dedupe_key
#   2 stats_key
#   3 stats_src_key
#   4 idx_strategies_key                    ("stats:strategies")
#   5 idx_symbols_key                       (f"stats:symbols:{strategy}")
#   6 idx_tfs_key                           (f"stats:tfs:{strategy}:{symbol}")
#   7 idx_sources_key                       (f"stats:sources:{strategy}:{symbol}:{tf}")
#   8..10: empirical buffers (lists): mfe_bps, mae_bps, ttd_ms
#
# ARGV:
#   1  ttl_sec
#   2  win
#   3  loss
#   4  be
#   5  pnl_net
#   6  pnl_gross
#   7  fees
#   8  pnl_pct
#   9  gross_profit_inc
#   10 gross_loss_inc
#   11 tp1_hit
#   12 tp2_hit
#   13 tp3_hit
#   14 tp_then_sl1
#   15 tp_then_sl2
#   16 tp_then_sl3
#   17 trailing_stop
#   18 trailing_started
#   19 r_multiple
#   20 duration_ms
#   21 mfe_pnl
#   22 mae_pnl
#   23 giveback
#   24 missed_profit
#   25 missed_profit_trades_inc
#   26 trailing_moves
#   27 now_ms
#   28 last_trade_id
#   29 last_close_reason
#   30 last_pnl   (string)
#   31 strategy
#   32 symbol
#   33 tf
#   34 source
_STATS_LUA = r"""
local dedupe = KEYS[1]
local k1 = KEYS[2]
local k2 = KEYS[3]

local ttl = tonumber(ARGV[1])
local ok = redis.call('SET', dedupe, '1', 'NX', 'EX', ttl)
if not ok then
  return 0
end

local win = tonumber(ARGV[2]) or 0
local loss = tonumber(ARGV[3]) or 0
local be = tonumber(ARGV[4]) or 0

local pnl_net = tonumber(ARGV[5]) or 0
local pnl_gross = tonumber(ARGV[6]) or 0
local fees = tonumber(ARGV[7]) or 0
local pnl_pct = tonumber(ARGV[8]) or 0

local gross_profit_inc = tonumber(ARGV[9]) or 0
local gross_loss_inc = tonumber(ARGV[10]) or 0

local tp1_hit = tonumber(ARGV[11]) or 0
local tp2_hit = tonumber(ARGV[12]) or 0
local tp3_hit = tonumber(ARGV[13]) or 0

local tp_then_sl1 = tonumber(ARGV[14]) or 0
local tp_then_sl2 = tonumber(ARGV[15]) or 0
local tp_then_sl3 = tonumber(ARGV[16]) or 0

local trailing_stop = tonumber(ARGV[17]) or 0
local trailing_started = tonumber(ARGV[18]) or 0

local r_multiple = tonumber(ARGV[19]) or 0
local duration_ms = tonumber(ARGV[20]) or 0
local mfe_pnl = tonumber(ARGV[21]) or 0
local mae_pnl = tonumber(ARGV[22]) or 0
local giveback = tonumber(ARGV[23]) or 0
local missed_profit = tonumber(ARGV[24]) or 0
local missed_profit_trades_inc = tonumber(ARGV[25]) or 0
local trailing_moves = tonumber(ARGV[26]) or 0

local now_ms = ARGV[27]
local last_trade_id = ARGV[28]
local last_close_reason = ARGV[29]
local last_pnl = ARGV[30]

local strategy = ARGV[31]
local symbol = ARGV[32]
local tf = ARGV[33]
local source = ARGV[34]

local function apply(key, include_source)
  redis.call('HINCRBY', key, 'total_trades', 1)
  redis.call('HINCRBY', key, 'wins', win)
  redis.call('HINCRBY', key, 'losses', loss)
  redis.call('HINCRBY', key, 'breakeven', be)

  redis.call('HINCRBYFLOAT', key, 'total_pnl', pnl_net)
  redis.call('HINCRBYFLOAT', key, 'total_pnl_gross', pnl_gross)
  redis.call('HINCRBYFLOAT', key, 'total_fees', fees)
  redis.call('HINCRBYFLOAT', key, 'total_pnl_pct', pnl_pct)

  redis.call('HINCRBYFLOAT', key, 'gross_profit', gross_profit_inc)
  redis.call('HINCRBYFLOAT', key, 'gross_loss', gross_loss_inc)

  redis.call('HINCRBY', key, 'tp1_hits', tp1_hit)
  redis.call('HINCRBY', key, 'tp2_hits', tp2_hit)
  redis.call('HINCRBY', key, 'tp3_hits', tp3_hit)

  redis.call('HINCRBY', key, 'tp1_then_sl', tp_then_sl1)
  redis.call('HINCRBY', key, 'tp2_then_sl', tp_then_sl2)
  redis.call('HINCRBY', key, 'tp3_then_sl', tp_then_sl3)

  redis.call('HINCRBY', key, 'trailing_stop_hits', trailing_stop)
  redis.call('HINCRBY', key, 'trailing_started', trailing_started)

  redis.call('HINCRBYFLOAT', key, 'sum_r', r_multiple)
  redis.call('HINCRBYFLOAT', key, 'sum_duration_ms', duration_ms)
  redis.call('HINCRBYFLOAT', key, 'sum_mfe', mfe_pnl)
  redis.call('HINCRBYFLOAT', key, 'sum_mae', mae_pnl)
  redis.call('HINCRBYFLOAT', key, 'giveback_total', giveback)
  redis.call('HINCRBYFLOAT', key, 'missed_profit_total', missed_profit)
  redis.call('HINCRBY', key, 'missed_profit_trades', missed_profit_trades_inc)
  redis.call('HINCRBYFLOAT', key, 'trailing_moves_total', trailing_moves)

-- ---------------------------------------------------------------------------
-- Empirical buffers (quantiles) for dynamic levels.
-- Tail-ARGV layout (from Python):
--   ARGV[#-6]=regime_key (string)
--   ARGV[#-5]=buf_enabled (0/1)
--   ARGV[#-4]=buf_max (int)
--   ARGV[#-3]=buf_ttl_sec (int)
--   ARGV[#-2]=mfe_bps (float or "")
--   ARGV[#-1]=mae_bps (float or "")
--   ARGV[#]=ttd_tp1_ms (int)
-- Fail-open: ignore missing/invalid values.
-- ---------------------------------------------------------------------------
local buf_enabled = tonumber(ARGV[#ARGV-5]) or 0
if buf_enabled == 1 then
  local buf_max = tonumber(ARGV[#ARGV-4]) or 300
  if buf_max < 10 then buf_max = 10 end
  local buf_ttl = tonumber(ARGV[#ARGV-3]) or 0

  local mfe_bps = tonumber(ARGV[#ARGV-2])
  local mae_bps = tonumber(ARGV[#ARGV-1])
  local ttd_ms = tonumber(ARGV[#ARGV])

  if mfe_bps and mfe_bps > 0 then
    redis.call('LPUSH', KEYS[8], tostring(mfe_bps))
    redis.call('LTRIM', KEYS[8], 0, buf_max - 1)
    if buf_ttl and buf_ttl > 0 then redis.call('EXPIRE', KEYS[8], buf_ttl) end
  end
  if mae_bps and mae_bps > 0 then
    redis.call('LPUSH', KEYS[9], tostring(mae_bps))
    redis.call('LTRIM', KEYS[9], 0, buf_max - 1)
    if buf_ttl and buf_ttl > 0 then redis.call('EXPIRE', KEYS[9], buf_ttl) end
  end
  if ttd_ms and ttd_ms > 0 then
    redis.call('LPUSH', KEYS[10], tostring(ttd_ms))
    redis.call('LTRIM', KEYS[10], 0, buf_max - 1)
    if buf_ttl and buf_ttl > 0 then redis.call('EXPIRE', KEYS[10], buf_ttl) end
  end
end

  if include_source == 1 then
    redis.call('HSET', key,
      'last_update', now_ms,
      'last_trade_id', last_trade_id,
      'last_close_reason', last_close_reason,
      'last_pnl', last_pnl,
      'strategy', strategy,
      'symbol', symbol,
      'tf', tf,
      'source', source
    )
  else
    redis.call('HSET', key,
      'last_update', now_ms,
      'last_trade_id', last_trade_id,
      'last_close_reason', last_close_reason,
      'last_pnl', last_pnl,
      'strategy', strategy,
      'symbol', symbol,
      'tf', tf
    )
  end
end

apply(k1, 0)
apply(k2, 1)

redis.call('SADD', KEYS[4], strategy)
redis.call('SADD', KEYS[5], symbol)
redis.call('SADD', KEYS[6], tf)
redis.call('SADD', KEYS[7], source)

return 1
"""


class StatsAggregator:
    logger = log
    _script = None  # cached registered script (per-process)

    @staticmethod
    def _get_script(redis_client):
        # register_script кеширует SHA внутри redis-py, но мы еще кешируем ссылку на объект
        if StatsAggregator._script is None:
            StatsAggregator._script = redis_client.register_script(_STATS_LUA)
        return StatsAggregator._script

    @staticmethod
    def update_stats(redis_client, pos: dict, trade_closed: dict) -> None:
        """
        IMPORTANT:
          This method is used in production AND in tests with FakeRedis.
          Therefore:
            - core logic can be "best-effort"
            - new writers MUST be fail-open and should run even if core part returns early
        """
        try:
            if not trade_closed:
                return

            # strict final-close gate (учитывает "1"/1/true)
            if STATS_REQUIRE_EXPLICIT_FINAL:
                if not _boolish(trade_closed.get("is_final_close")):
                    return
            else:
                is_final = bool(trade_closed.get("is_final_close", True))
                if not is_final:
                    return

            strategy = canon_strategy(trade_closed.get("strategy") or (pos.get("strategy") if isinstance(pos, dict) else None))
            symbol = canon_symbol(trade_closed.get("symbol") or (pos.get("symbol") if isinstance(pos, dict) else None))
            tf = canon_tf(trade_closed.get("tf") or (pos.get("tf") if isinstance(pos, dict) else None))
            source = canon_source(trade_closed.get("source") or (pos.get("source") if isinstance(pos, dict) else None))

            stats_key = f"stats:{strategy}:{symbol}:{tf}"
            stats_src_key = f"stats:{strategy}:{symbol}:{tf}:{source}"

            order_id = _to_str(trade_closed.get("order_id") or trade_closed.get("orderId") or trade_closed.get("id") or trade_closed.get("order_id".upper()) or "")
            sid = _to_str(trade_closed.get("sid") or "")
            last_trade_id = order_id or sid or f"pseudo:{symbol}:{_safe_int(trade_closed.get('exit_ts_ms'))}"

            exit_ts = _safe_int(trade_closed.get("exit_ts_ms") or trade_closed.get("closed_time") or trade_closed.get("close_time") or trade_closed.get("ts") or 0)
            if exit_ts <= 0:
                exit_ts = get_ny_time_millis()

            pnl_net = _safe_float(trade_closed.get("pnl_net") or trade_closed.get("pnl") or 0.0)
            pnl_gross = _safe_float(trade_closed.get("pnl_gross") or pnl_net)
            fees = _safe_float(trade_closed.get("fees") or 0.0)
            pnl_pct = _safe_float(trade_closed.get("pnl_pct") or 0.0)

            close_reason_raw = _to_str(trade_closed.get("close_reason_raw") or trade_closed.get("close_reason") or "")
            close_bucket = bucket_close_reason(close_reason_raw)

            # -------- stable dedupe v2 (order_id is best) --------
            # Если order_id есть (у вас он есть всегда при save_closed) — это идеальный ключ.
            if order_id:
                dedupe_key = f"stats:dedupe:v2:close:{order_id}:{exit_ts}"
            else:
                # fallback если вдруг нет order_id
                dedupe_key = f"stats:dedupe:v2:close:{last_trade_id}:{exit_ts}"

            win = 1 if pnl_net > STATS_EPS else 0
            loss = 1 if pnl_net < -STATS_EPS else 0
            be = 1 if (win == 0 and loss == 0) else 0

            tp1_hit = _safe_int(trade_closed.get("tp1_hit") or 0)
            tp2_hit = _safe_int(trade_closed.get("tp2_hit") or 0)
            tp3_hit = _safe_int(trade_closed.get("tp3_hit") or 0)
            tp_before_sl = _safe_int(trade_closed.get("tp_before_sl") or 0)

            trailing_started = _safe_int(trade_closed.get("trailing_started") or 0)
            trailing_stop = 1 if close_bucket == "TRAILING_STOP" else 0

            tp_then_sl1 = 1 if (close_bucket == "SL" and tp_before_sl >= 1) else 0
            tp_then_sl2 = 1 if (close_bucket == "SL" and tp_before_sl >= 2) else 0
            tp_then_sl3 = 1 if (close_bucket == "SL" and tp_before_sl >= 3) else 0

            duration_ms = _safe_int(trade_closed.get("duration_ms") or 0)
            mfe_pnl = _safe_float(trade_closed.get("mfe_pnl") or 0.0)
            mae_pnl = _safe_float(trade_closed.get("mae_pnl") or 0.0)
            giveback = _safe_float(trade_closed.get("giveback") or 0.0)
            missed_profit = _safe_float(trade_closed.get("missed_profit") or 0.0)
            missed_profit_trades_inc = 1 if abs(missed_profit) > STATS_EPS else 0

            # ------------------------------------------------------------------
            # Empirical triplet for dynamic levels:
            #  - mfe_bps: for TP1 quantile
            #  - mae_bps: for SL quantile
            #  - ttd_tp1_ms: for time-to-hit TP1 median (optional)
            # Fail-open: may return None/0 if insufficient info in trade_closed.
            # ------------------------------------------------------------------
            emp = extract_empirical_triplet(trade_closed)
            regime_key = canon_regime(emp.get("regime"))
            if not EMP_LEVELS_USE_REGIME_DIM:
                regime_key = "na"
            buf_mfe_key = f"statsbuf:{strategy}:{symbol}:{tf}:{regime_key}:mfe_bps"
            buf_mae_key = f"statsbuf:{strategy}:{symbol}:{tf}:{regime_key}:mae_bps"
            buf_ttd_key = f"statsbuf:{strategy}:{symbol}:{tf}:{regime_key}:ttd_ms"

            r_multiple = _safe_float(trade_closed.get("r_multiple") or 0.0)
            trailing_moves = _safe_int(trade_closed.get("trailing_moves") or 0)

            gross_profit_inc = pnl_gross if pnl_gross > STATS_EPS else 0.0
            gross_loss_inc = abs(pnl_gross) if pnl_gross < -STATS_EPS else 0.0

            now_ms = get_ny_time_millis()

            # ---- atomic update via Lua ----
            script = StatsAggregator._get_script(redis_client)

            keys = [
                dedupe_key,
                stats_key,
                stats_src_key,
                "stats:strategies",
                f"stats:symbols:{strategy}",
                f"stats:tfs:{strategy}:{symbol}",
                f"stats:sources:{strategy}:{symbol}:{tf}",
                # Buffers for quantiles (used by EmpiricalLevels provider)
                buf_mfe_key, buf_mae_key, buf_ttd_key,
            ]

            args = [
                str(STATS_DEDUPE_TTL_SEC),
                str(win),
                str(loss),
                str(be),
                str(pnl_net),
                str(pnl_gross),
                str(fees),
                str(pnl_pct),
                str(gross_profit_inc),
                str(gross_loss_inc),
                str(tp1_hit),
                str(tp2_hit),
                str(tp3_hit),
                str(tp_then_sl1),
                str(tp_then_sl2),
                str(tp_then_sl3),
                str(trailing_stop),
                str(trailing_started),
                str(r_multiple),
                str(float(duration_ms)),
                str(mfe_pnl),
                str(mae_pnl),
                str(giveback),
                str(missed_profit),
                str(missed_profit_trades_inc),
                str(float(trailing_moves)),
                str(now_ms),
                last_trade_id,
                close_bucket,
                f"{pnl_net:.8f}",
                strategy,
                symbol,
                tf,
                source,

                # --- Tail ARGV: empirical buffers control & values ---
                # Keep these at the end and read from Lua via ARGV[#ARGV-k] to avoid index fragility.
                regime_key,
                "1" if EMP_LEVELS_BUF_ENABLED else "0",
                str(max(10, EMP_LEVELS_BUF_MAX)),
                str(max(0, EMP_LEVELS_BUF_TTL_SEC)),
                str(emp.get("mfe_bps") if emp.get("mfe_bps") is not None else ""),
                str(emp.get("mae_bps") if emp.get("mae_bps") is not None else ""),
                str(int(emp.get("ttd_tp1_ms") or 0)),
            ]

            applied = script(keys=keys, args=args)

            if applied == 0:
                return

            # ------------------------------------------------------------------
            # Post-applied hooks (TESTABLE):
            #   - reliability calibration curves (conf_pct -> hit-rate) with 4 outcomes
            #   - (optional) other derived stats writers can live here too
            #
            # IMPORTANT:
            #   - must be fail-open: never break stats aggregation
            #   - must run ONLY when Lua applied==1 (not a dedup)
            # ------------------------------------------------------------------
            with contextlib.suppress(Exception):
                _post_applied_hooks(redis_client, pos, trade_closed)

            # ------------------------------------------------------------------
            # NEW: Execution slippage/spread EMA writer for EdgeCostGate.
            #
            # Why:
            #   - EdgeCostGate now uses slippage_bps = max(default, spread/2, EMA(realized_slippage_bps@...))
            #   - realized_slippage_bps and realized_spread_bps are computed at finalize_trade()
            #   - We aggregate them here into a low-cost EMA keyed by:
            #       symbol × venue × session × tf × kind
            #
            # Notes:
            #   - TradeClosed has no 'kind' field; we use strategy as 'kind' (pipeline convention).
            #   - venue is read from PositionState.signal_payload["venue"] if present; otherwise "na".
            #   - timestamp uses TradeClosed.exit_ts_ms (epoch ms).
            # Fail-open: NEVER break stats aggregation.
            # ------------------------------------------------------------------
            try:
                from services.execution_slippage_stats import SlippageEmaConfig, update_slippage_ema

                cfg_slip = SlippageEmaConfig.from_env()
                if cfg_slip.enabled:
                    # closed dict comes from TradeClosed.__dict__ (includes dynamic fields)
                    slip_bps = _safe_float(trade_closed.get("realized_slippage_bps") or 0.0, 0.0)
                    spr_bps = _safe_float(trade_closed.get("realized_spread_bps") or 0.0, 0.0)
                    if slip_bps > 0:
                        # dims
                        sym = str(trade_closed.get("symbol") or symbol or "")
                        tfv = str(trade_closed.get("tf") or tf or "na")
                        # kind: TradeClosed has strategy; in your pipeline it represents kind dimension.
                        knd = str(trade_closed.get("strategy") or strategy or "na")

                        # venue: prefer pos.signal_payload["venue"] (CryptoOrderFlow sets it in payload)
                        ven = "na"
                        try:
                            sp = None
                            if isinstance(pos, dict):
                                sp = pos.get("signal_payload")
                            if isinstance(sp, dict):
                                ven = str(sp.get("venue") or sp.get("exchange") or "na")
                        except Exception:
                            ven = "na"

                        ts_exit = int(float(trade_closed.get("exit_ts_ms") or 0))
                        update_slippage_ema(
                            redis_client,
                            cfg=cfg_slip,
                            symbol=sym,
                            venue=ven,
                            tf=tfv,
                            kind=knd,
                            ts_ms=ts_exit,
                            realized_slippage_bps=slip_bps,
                            realized_spread_bps=spr_bps,
                        )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: Execution-cost EMA writer (slippage/spread).
            #
            # Why:
            #   Gate uses slippage_bps = max(default, spread/2, EMA(realized_slippage_bps@dims)).
            #   We must actually maintain that EMA from closed trades.
            #
            # Dims:
            #   (symbol × venue × session × tf × kind)
            # IMPORTANT:
            #   TradeClosed has no `kind` -> we use TradeClosed.strategy as kind dimension.
            #
            # Fail-open: any issue here must not break stats aggregation.
            # ------------------------------------------------------------------
            try:
                from services.execution_cost_ema import ExecCostEmaConfig, update_exec_cost_ema_from_closed
                ecfg = ExecCostEmaConfig.from_env()
                if ecfg.enabled:
                    now_ms2 = get_ny_time_millis()
                    update_exec_cost_ema_from_closed(
                        redis_client,
                        cfg=ecfg,
                        pos=pos if isinstance(pos, dict) else {},
                        trade_closed=trade_closed if isinstance(trade_closed, dict) else {},
                        now_ms=now_ms2,
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: realized spread EMA writer (baseline for entry spread-shock gate).
            #
            # Inputs:
            #   - TradeClosed.realized_spread_bps (set in finalize_trade from exit tick bid/ask)
            # Dims:
            #   - symbol
            #   - venue (best-effort: from Position.signal_payload["venue"] or "na")
            #   - session (from entry_ts_ms; strict epoch normalization is applied upstream)
            #   - tf
            #   - kind (we store as strategy for compatibility; default "na")
            #
            # Fail-open: MUST NOT affect stats aggregation.
            # ------------------------------------------------------------------
            try:
                from domain.time_utils import normalize_epoch_ms_strict, session_from_ts_ms
                from services.execution_spread_stats import SpreadEmaConfig, update_spread_ema

                sp_cfg = SpreadEmaConfig.from_env()
                if sp_cfg.enabled:
                    # venue: keep it best-effort and backward-compatible
                    venue = "na"
                    try:
                        sp = pos.get("signal_payload") if isinstance(pos, dict) else None
                        if isinstance(sp, dict):
                            venue = str(sp.get("venue") or sp.get("exchange") or "na")
                    except Exception:
                        venue = "na"

                    tf2 = "na"
                    try:
                        tf2 = str(trade_closed.get("tf") or pos.get("tf") or "na")
                    except Exception:
                        tf2 = "na"

                    # Strategy is the closest stable substitute for kind in your TradeClosed model.
                    knd = "na"
                    try:
                        knd = str(trade_closed.get("strategy") or pos.get("strategy") or "na")
                    except Exception:
                        knd = "na"

                    # entry_ts_ms is epoch ms in this pipeline; harden anyway.
                    entry_ts = 0
                    try:
                        entry_ts = normalize_epoch_ms_strict(trade_closed.get("entry_ts_ms") or pos.get("entry_ts_ms") or 0)
                    except Exception:
                        entry_ts = 0
                    sess = "na"
                    if entry_ts > 0:
                        try:
                            sess = session_from_ts_ms(entry_ts)
                        except Exception:
                            sess = "na"

                    now_ms2 = get_ny_time_millis()
                    update_spread_ema(
                        redis_client,
                        cfg=sp_cfg,
                        symbol=symbol,
                        venue=venue,
                        session=sess,
                        tf=tf2,
                        kind=(knd or "na"),
                        now_ms=now_ms2,
                        realized_spread_bps=trade_closed.get("realized_spread_bps") or trade_closed.get("realized_spread") or 0.0,
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: reliability curves for confidence calibration (conf_pct -> hit-rate)
            #
            # Why:
            #   One calibration "per symbol" is often wrong because different kinds/regimes
            #   have different true hit-rates at the same reported confidence.
            #
            # What we store:
            #   Redis HASH per (symbol × kind × regime) [optional tf]:
            #     n_total/h_total, n:<bin>/h:<bin>, last_ts_ms
            #
            # Outcome (default):
            #   hit = 1 if tp1_hit == True
            #
            # Fail-open:
            #   Any error must NOT affect stats aggregation.
            # ------------------------------------------------------------------
            try:
                from services.reliability_calibrator import (
                    RelCalConfig,
                    update_reliability_curves,
                )

                # Avoid env parsing on each aggregation tick: cache config on self.
                rcfg = getattr(StatsAggregator, "_rel_cal_cfg", None)
                if rcfg is None:
                    rcfg = RelCalConfig.from_env()
                    StatsAggregator._rel_cal_cfg = rcfg
                if rcfg.enabled:
                    update_reliability_curves(
                        redis_client,
                        cfg=rcfg,
                        pos=pos if isinstance(pos, dict) else {},
                        trade_closed=trade_closed if isinstance(trade_closed, dict) else {},
                        now_ms=None,
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: Minimal correct TP1 hit-rate writer for EV / cost gates.
            #
            # Requirements (single writer point, executed only after applied=1):
            #   - total_trades += 1
            #   - tp1_hits += 1 if tp1_hit==1
            #   - ema_tp1 updated with alpha
            #
            # Implemented via a separate small Lua (services/ev_tp1_stats.py),
            # WITHOUT touching the main stats Lua.
            #
            # Fail-open: any error here must not break aggregation.
            # ------------------------------------------------------------------
            try:
                from services.ev_tp1_stats import EvTp1StatsConfig, update_tp1_hit_ema

                ev_cfg = EvTp1StatsConfig.from_env()
                if ev_cfg.enabled:
                    rg = canon_regime(
                        trade_closed.get("entry_regime")
                        or trade_closed.get("regime")
                        or (pos.get("entry_regime") if isinstance(pos, dict) else None)
                        or (pos.get("regime") if isinstance(pos, dict) else None)
                    )
                    tp1_hit = 1 if bool(trade_closed.get("tp1_hit")) else 0
                    now_ms0 = get_ny_time_millis()
                    update_tp1_hit_ema(
                        redis_client,
                        cfg=ev_cfg,
                        kind=strategy,   # NOTE: in your pipeline strategy==kind
                        symbol=symbol,
                        tf=tf,
                        regime=rg,
                        tp1_hit=tp1_hit,
                        now_ms=now_ms0,
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: strict time-bucket snapshots writer (MFE@T / MAE@T) + survival counters.
            #
            # Invoked only after applied=1 (dedup passed).
            # This writes:
            #   - statsbuf:*:mfe_bps_t{bucket}
            #   - statsbuf:*:mae_bps_t{bucket}
            #   - statscnt:*:survival {total, alive_t{bucket}}
            #
            # Fail-open.
            # ------------------------------------------------------------------
            try:
                # regime dimension is optional; keep consistent with empirical_levels keys.
                rg = (
                    trade_closed.get("entry_regime")
                    or trade_closed.get("regime")
                    or (pos.get("entry_regime") if isinstance(pos, dict) else None)
                    or (pos.get("regime") if isinstance(pos, dict) else None)
                )
                regime_key = canon_regime(rg) if EMP_LEVELS_USE_REGIME_DIM else "na"
                _write_timebucket_buffers(
                    redis_client,
                    strategy=strategy,
                    symbol=symbol,
                    tf=tf,
                    regime_key=regime_key,
                    trade_closed=trade_closed,
                )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: Slippage-by-fact EMA writer (symbol×venue×session).
            #
            # Измеряем и накапливаем:
            #   - realized_slippage_bps (по факту исполнения)  [обязательно]
            #   - realized_spread_bps   (по факту рынка)      [опционально]
            #
            # Затем EdgeCostGate использует:
            #   slippage_bps = max(default, spread/2, EMA(realized_slippage_bps))
            #
            # Fail-open:
            #   - если полей нет -> не пишем
            #   - ошибки Redis / парсинга -> пропускаем
            # ------------------------------------------------------------------
            try:
                from services.slippage_stats import SlippageEmaConfig, session_from_ts_ms, update_slippage_ema

                cfg_s = SlippageEmaConfig.from_env()
                if cfg_s.enabled:
                    # symbol обязателен — он у вас уже есть переменной выше
                    sym = symbol

                    # venue: берём из trade_closed, либо из pos, либо na
                    venue = (
                        trade_closed.get("venue")
                        or (pos.get("venue") if isinstance(pos, dict) else None)
                        or "na"
                    )

                    # NEW: kind dimension (optional).
                    # Prefer explicit trade_closed.kind; else strategy (often equals kind in your pipeline).
                    # Back-compat: if missing -> None -> BASE key (legacy format).
                    kind_key = (
                        trade_closed.get("kind")
                        or trade_closed.get("signal_kind")
                        or trade_closed.get("strategy")
                        or strategy
                    )

                    # tf: в вашем пайплайне это ключевая ось (как в ev_tp1_stats.py)
                    tf_key = (
                        trade_closed.get("tf")
                        or trade_closed.get("timeframe")
                        or (pos.get("tf") if isinstance(pos, dict) else None)
                        or (pos.get("timeframe") if isinstance(pos, dict) else None)
                        or tf  # локальная переменная StatsAggregator (у вас уже есть выше в методе)
                        or "na"
                    )

                    # session: либо явная в событии, либо вычисляем по exit_ts
                    sess = (
                        trade_closed.get("session")
                        or (pos.get("session") if isinstance(pos, dict) else None)
                        or session_from_ts_ms(int(float(trade_closed.get("exit_ts_ms") or 0)))
                    )

                    # входные поля от finalize_trade:
                    slip_bps = _safe_float(trade_closed.get("realized_slippage_bps") or 0.0, 0.0)
                    spr_bps = _safe_float(trade_closed.get("realized_spread_bps") or 0.0, 0.0)
                    now_ms = get_ny_time_millis()

                    if slip_bps > 0:
                        update_slippage_ema(
                            redis_client,
                            cfg=cfg_s,
                            symbol=sym,
                            venue=venue,
                            session=sess,
                            tf=tf_key,
                            kind=kind_key if kind_key is not None else None,
                            now_ms=now_ms,
                            realized_slippage_bps=slip_bps,
                            realized_spread_bps=spr_bps,
                        )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: Giveback-risk EMA writer for conditional trailing.
            #
            # Why:
            #   "Trailing always after TP1" often increases noise and premature exits.
            #   A better rule is trailing only when:
            #     - momentum is strong, OR
            #     - historically high giveback risk for this kind/regime.
            #
            # This writer maintains EMA of giveback in bps per (kind,strategy)×symbol×tf×regime:
            #   key: trailgb:{kind}:{symbol}:{tf}:{regime}
            # fields:
            #   samples, ema_giveback_bps, last_ts_ms
            #
            # Fail-open: any errors must not break stats aggregation.
            # ------------------------------------------------------------------
            try:
                from services.ev_giveback_stats import GivebackEmaConfig, update_giveback_ema

                gb_cfg = GivebackEmaConfig.from_env()
                if gb_cfg.enabled:
                    # Same "regime extraction" logic as EV gate uses:
                    # prefer entry_regime (regime at entry), fallback to regime.
                    rg = canon_regime(
                        trade_closed.get("entry_regime")
                        or trade_closed.get("regime")
                        or (pos.get("entry_regime") if isinstance(pos, dict) else None)
                        or (pos.get("regime") if isinstance(pos, dict) else None)
                    )
                    now_ms2 = get_ny_time_millis()
                    update_giveback_ema(
                        redis_client,
                        cfg=gb_cfg,
                        kind=strategy,          # strategy == kind in your pipeline
                        symbol=symbol,
                        tf=tf,
                        regime=rg,
                        now_ms=now_ms2,
                        giveback_pnl=_safe_float(trade_closed.get("giveback") or 0.0),
                        entry_price=_safe_float(trade_closed.get("entry_price") or 0.0),
                        qty=_safe_float(trade_closed.get("lot") or trade_closed.get("qty") or 0.0),
                        notional=_safe_float(trade_closed.get("notional_usd") or trade_closed.get("notional") or 0.0),
                    )
            except Exception:
                pass

            # -----------------------------------------------------------------
            # NEW: trailing quality stats (giveback-risk EMA)
            #
            # Used by TrailConditionalEvaluator to decide:
            #   - allow trailing only when historical giveback is high
            #
            # Inputs:
            #   giveback_r = giveback / one_r_money (>=0)
            #   trailing_stop = 1 if close_bucket == "TRAILING_STOP" else 0
            #
            # Fail-open: any failure here must NOT affect stats aggregation.
            # -----------------------------------------------------------------
            try:
                from services.trail_giveback_stats import TrailStatsConfig, update_trail_giveback_ema
                cfg2 = TrailStatsConfig.from_env()
                if cfg2.enabled:
                    # regime dimension (consistent with your empirical buffers)
                    rg = None
                    if cfg2.use_regime_dim:
                        rg = (
                            trade_closed.get("entry_regime")
                            or trade_closed.get("regime")
                            or (pos.get("entry_regime") if isinstance(pos, dict) else None)
                            or (pos.get("regime") if isinstance(pos, dict) else None)
                        )
                    regime_key = canon_regime(rg) if cfg2.use_regime_dim else "na"

                    one_r = _safe_float(trade_closed.get("one_r_money") or 0.0, 0.0)
                    giveback = _safe_float(trade_closed.get("giveback") or 0.0, 0.0)
                    giveback_r = 0.0
                    if one_r > STATS_EPS:
                        giveback_r = max(0.0, giveback / one_r)

                    trailing_stop_flag = 1 if close_bucket == "TRAILING_STOP" else 0
                    update_trail_giveback_ema(
                        redis_client,
                        cfg=cfg2,
                        kind=strategy,
                        symbol=symbol,
                        tf=tf,
                        regime=regime_key,
                        giveback_r=giveback_r,
                        trailing_stop=trailing_stop_flag,
                        now_ms=now_ms,
                    )
            except Exception:
                pass

            # -------------------------------------------------------------------------
            # NEW: time-bucket empirical buffers writer (MFE@T / MAE@T) + survival.
            #
            # Your file already has _write_timebucket_buffers(...) and helpers:
            #   - _time_buckets_ms_from_env()
            #   - _parse_json_dict_strfloat()
            #   - _pnl_to_bps()
            # and it expects TradeClosed to carry JSON dicts:
            #   trade_closed["mfe_pnl_t"] = {"60000": 1.23, ...}
            #   trade_closed["mae_pnl_t"] = {"60000": 0.45, ...}
            #
            # We call it AFTER applied=1 so duplicates do NOT pollute buffers.
            # Fail-open by design.
            # -------------------------------------------------------------------------
            try:
                # Keep regime dimension consistent with your existing EMP_LEVELS_USE_REGIME_DIM.
                rg = None
                try:
                    rg = trade_closed.get("entry_regime") or trade_closed.get("regime")
                except Exception:
                    rg = None
                regime_key = canon_regime(rg) if EMP_LEVELS_USE_REGIME_DIM else "na"
                _write_timebucket_buffers(
                    redis_client,
                    strategy=strategy,
                    symbol=symbol,
                    tf=tf,
                    regime_key=regime_key,
                    trade_closed=trade_closed,
                )
            except Exception:
                pass

            # ---------------------------------------------------------------------
            # EV gate stats (p_hit_tp1): atomic update to Redis hash:
            #   evstats:{kind}:{symbol}:{tf}:{regime}
            #
            # Fields:
            #   total_trades, tp1_hits, ema_tp1, updated_ms
            #
            # IMPORTANT:
            #   - We use entry_regime (not exit regime) for gating at entry time.
            #   - Fail-open: any issue here must never break the main aggregation.
            # ---------------------------------------------------------------------
            try:
                ev_cfg = EvTp1StatsConfig.from_env()
                if ev_cfg.enabled:
                    # Strategy is the "kind" dimension for your crypto pipeline (obi_spike/absorption/breakout/extreme).
                    kind = canon_strategy(trade_closed.get("strategy") or (pos.get("strategy") if isinstance(pos, dict) else None))
                    symbol = canon_symbol(trade_closed.get("symbol") or (pos.get("symbol") if isinstance(pos, dict) else None))
                    tf = canon_tf(trade_closed.get("tf") or (pos.get("tf") if isinstance(pos, dict) else None))

                    # Prefer entry_regime (stamped at open) → fallback to regime.
                    regime = trade_closed.get("entry_regime") or trade_closed.get("regime")
                    if not regime and isinstance(pos, dict):
                        regime = pos.get("entry_regime") or pos.get("regime")
                    regime = canon_regime(regime)

                    tp1_hit = _safe_int(trade_closed.get("tp1_hit") or 0)

                    # Atomic updater (Lua) with safe fallbacks.
                    update_tp1_hit_ema(
                        redis_client,
                        cfg=ev_cfg,
                        kind=kind,
                        symbol=symbol,
                        tf=tf,
                        regime=regime,
                        tp1_hit=tp1_hit,
                    )
            except Exception:
                pass

            # optional: trigger reporter by trades (если у вас есть periodic_reporter)
            try:
                from services.periodic_reporter import check_and_trigger_report
                oid = trade_closed.get("order_id") or trade_closed.get("id")
                check_and_trigger_report(source, symbol, counter_type="trades", order_id=oid)
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: Execution-cost EMA writer (slippage/spread) for EdgeCostGate.
            #
            # Why:
            #   Fixed slippage assumptions (0 / spread/2 / constant) make EV/cost gates lie.
            #   We write realized execution metrics on CLOSE:
            #     - realized_slippage_bps  (|fill-mid| / mid * 1e4)
            #     - realized_spread_bps    ((ask-bid)/mid * 1e4) at close tick (if available)
            # and maintain EMA per:
            #   symbol × venue × session × tf × kind
            #
            # Fail-open:
            #   - any exception => skip silently
            #   - does not affect main stats lua path
            # ------------------------------------------------------------------
            try:
                from services.execution_cost_ema import (
                    ExecCostEmaConfig,
                    build_exec_cost_ema_key,
                    session_from_ts_ms,
                    update_exec_cost_ema,
                )
                cfg_ec = ExecCostEmaConfig.from_env()
                if cfg_ec.enabled:
                    # Inputs come from TradeClosed (saved by finalize_trade)
                    sym = (symbol or "").strip().upper() or "NA"
                    tfv = (tf or "").strip().lower() or "na"
                    # In your pipeline "strategy == kind" (per provided notes). Keep that convention.
                    knd = (strategy or "na").strip().lower() or "na"
                    # Venue: we persist it in TradeClosed (patched in domain/handlers.py).
                    ven = (trade_closed.get("venue") or "na").strip().lower() or "na"
                    # Session: prefer persisted entry_session, else derive from entry_ts_ms.
                    ses = (trade_closed.get("entry_session") or "").strip().lower()
                    if not ses:
                        try:
                            ets = int(float(trade_closed.get("entry_ts_ms") or 0))
                        except Exception:
                            ets = 0
                        ses = session_from_ts_ms(ets)
                    if not ses:
                        ses = "na"

                    # Realized execution quality
                    slip = _safe_float(trade_closed.get("realized_slippage_bps") or 0.0, 0.0)
                    spr = _safe_float(trade_closed.get("realized_spread_bps") or 0.0, 0.0)
                    if slip > 0 and ses != "na":
                        now_ms3 = get_ny_time_millis()
                        key_full = build_exec_cost_ema_key(cfg_ec, symbol=sym, venue=ven, session=ses, tf=tfv, kind=knd)
                        update_exec_cost_ema(redis_client, cfg=cfg_ec, key=key_full, now_ms=now_ms3, realized_slippage_bps=slip, realized_spread_bps=spr)
                        # Backward compatibility: also write legacy key without tf/kind if enabled.
                        if cfg_ec.write_legacy:
                            key_legacy = build_exec_cost_ema_key(cfg_ec, symbol=sym, venue=ven, session=ses, tf="na", kind="na", legacy=True)
                            update_exec_cost_ema(redis_client, cfg=cfg_ec, key=key_legacy, now_ms=now_ms3, realized_slippage_bps=slip, realized_spread_bps=spr)
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: reliability curves (confidence calibration)
            # Controlled from docker-compose:
            #   RELIABILITY_CURVES_ENABLED=1
            #   RELIABILITY_TARGET=tp1_hit|tp2_hit|win|tp1_no_sl
            #
            # Default target (as confirmed): tp1_hit.
            # ------------------------------------------------------------------
            try:
                from services.reliability_curves import update_reliability_curve
                update_reliability_curve(redis_client, pos=pos, closed=trade_closed)
            except Exception:
                pass

        except Exception as e:
            StatsAggregator.logger.error("update_stats error: %s", e, exc_info=True)
        finally:
            # ------------------------------------------------------------------
            # NEW: slippage EMA writer (for EdgeCostGate execution-cost realism)
            # Key: slipema:v2:{symbol}:{venue}:{session}:{tf}:{kind}
            # ------------------------------------------------------------------
            try:
                from services.slippage_ema_stats import update_slippage_ema
                update_slippage_ema(redis_client, closed=trade_closed, pos=pos if isinstance(pos, dict) else None)
            except Exception:
                pass

            # ------------------------------------------------------------------
            # NEW: reliability curves (confidence calibration)
            # Targets selectable via docker-compose:
            #   RELIABILITY_TARGETS=tp1|win|tp2|tp1_not_sl|all
            # Default: tp1 (вы подтвердили)
            # ------------------------------------------------------------------
            try:
                from services.reliability_curves import update_reliability_curve
                update_reliability_curve(redis_client, closed=trade_closed, pos=pos if isinstance(pos, dict) else None)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Read helpers — mirror the key schema written by update_stats / Lua.
    # ------------------------------------------------------------------

    @classmethod
    def get_all_strategies(cls, redis_client) -> list[str]:
        try:
            raw = redis_client.smembers("stats:strategies")
            return [v.decode() if isinstance(v, bytes) else str(v) for v in raw]
        except Exception:
            return []

    @classmethod
    def get_strategy_symbols(cls, redis_client, strategy: str) -> list[str]:
        try:
            raw = redis_client.smembers(f"stats:symbols:{strategy}")
            return [v.decode() if isinstance(v, bytes) else str(v) for v in raw]
        except Exception:
            return []

    @classmethod
    def get_strategy_timeframes(cls, redis_client, strategy: str, symbol: str) -> list[str]:
        try:
            raw = redis_client.smembers(f"stats:tfs:{strategy}:{symbol}")
            return [v.decode() if isinstance(v, bytes) else str(v) for v in raw]
        except Exception:
            return []

    @classmethod
    def get_strategy_sources(cls, redis_client, strategy: str, symbol: str, tf: str) -> list[str]:
        try:
            raw = redis_client.smembers(f"stats:sources:{strategy}:{symbol}:{tf}")
            return [v.decode() if isinstance(v, bytes) else str(v) for v in raw]
        except Exception:
            return []

    @classmethod
    def get_stats_by_source(cls, redis_client, strategy: str, symbol: str, tf: str, source: str) -> dict[str, Any]:
        try:
            raw = redis_client.hgetall(f"stats:{strategy}:{symbol}:{tf}:{source}")
            return {
                (k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v))
                for k, v in raw.items()
            }
        except Exception:
            return {}

    @classmethod
    def get_stats(cls, redis_client, strategy: str, symbol: str, tf: str) -> dict[str, Any]:
        try:
            raw = redis_client.hgetall(f"stats:{strategy}:{symbol}:{tf}")
            return {
                (k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v))
                for k, v in raw.items()
            }
        except Exception:
            return {}

    @classmethod
    def get_strategy_summary(cls, redis_client, strategy: str) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        try:
            for symbol in cls.get_strategy_symbols(redis_client, strategy):
                for tf in cls.get_strategy_timeframes(redis_client, strategy, symbol):
                    s = cls.get_stats(redis_client, strategy, symbol, tf)
                    if not s:
                        continue
                    for k, v in s.items():
                        if k in summary:
                            try:
                                summary[k] = str(float(summary[k]) + float(v))
                            except Exception:
                                pass
                        else:
                            summary[k] = v
        except Exception:
            pass
        return summary

    @classmethod
    def get_all_stats(cls, redis_client) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        try:
            for strategy in cls.get_all_strategies(redis_client):
                for symbol in cls.get_strategy_symbols(redis_client, strategy):
                    for tf in cls.get_strategy_timeframes(redis_client, strategy, symbol):
                        key = f"{strategy}:{symbol}:{tf}"
                        try:
                            raw = redis_client.hgetall(f"stats:{strategy}:{symbol}:{tf}")
                            result[key] = {
                                (k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v))
                                for k, v in raw.items()
                            }
                        except Exception:
                            pass
        except Exception:
            pass
        return result


def _post_applied_hooks(redis_client: Any, pos: dict[str, Any], trade_closed: dict[str, Any]) -> None:
    """
    Extracted for testability:
      - called only after the main Lua script reports applied==1
      - safe to call directly in tests with FakeRedis
    """
    # Reliability calibrator (4 outcomes, default: tp2 + nosl_after_tp1)
    try:
        from services.reliability_calibrator import RelCalConfig, update_reliability_curves
        cfg = RelCalConfig.from_env()
        if cfg.enabled:
            update_reliability_curves(
                redis_client,
                cfg=cfg,
                pos=pos if isinstance(pos, dict) else {},
                trade_closed=trade_closed if isinstance(trade_closed, dict) else {},
            )
    except Exception:
        pass

    # ------------------------------------------------------------------
    # NEW: slippage EMA writer (for EdgeCostGate execution-cost realism)
    # Key: slipema:v2:{symbol}:{venue}:{session}:{tf}:{kind}
    # ------------------------------------------------------------------
    try:
        from services.slippage_ema_stats import update_slippage_ema
        update_slippage_ema(redis_client, closed=trade_closed, pos=pos if isinstance(pos, dict) else None)
    except Exception:
        pass
