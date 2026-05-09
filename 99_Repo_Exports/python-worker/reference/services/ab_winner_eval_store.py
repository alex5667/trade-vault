from __future__ import annotations

"""
AB Winner Eval Store (Redis)
============================

"Ещё выше" слой:
  - Инкрементальный ingest из events:trades по курсору (после lock)
  - Храним rolling window per (symbol, regime, group, scenario, arm) в ZSET:
        ab:eval:r:z:v1:{symbol}:{regime}:{group}:{scenario}:{arm}
    score = event_ts_ms
    member = "{stream_id}|{r_mult}"
  - Авто-прунинг по времени (window_ms) + cap по размеру (max_items)
  - Реестр контекстов:
        ab:eval:ctx:set:v1  (set of "{symbol}|{regime}|{group}")
        ab:eval:ctx:ts:v1:{symbol}|{regime}|{group} -> last_seen_ts_ms
  - Курсор:
        ab:eval:cursor:v1:{stream} -> last_stream_id

Этот слой решает проблему:
  - events:microbar_closed maxlen маленький => не годится для long horizon
  - events:trades большой => нельзя сканировать целиком каждый час
  => инкрементальный ingest + собственное окно.
"""

from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(v: Any, d: str = "") -> str:
    try:
        return str(v if v is not None else d)
    except Exception:
        return d


def _f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _norm_symbol(v: str) -> str:
    return (v or "").strip().upper()


def _norm_regime(v: str) -> str:
    r = (v or "na").strip().lower()
    return r or "na"


def _norm_group(v: str) -> str:
    g = (v or "default").strip().lower()
    return g or "default"


def _norm_scenario(v: str) -> str:
    s = (v or "na").strip().lower()
    if s in ("continuation", "cont"):
        return "continuation"
    if s in ("reversal", "rev"):
        return "reversal"
    return "na"


def _norm_arm(v: str) -> str:
    a = (v or "A").strip().upper()
    return a if a in ("A", "B", "C") else "A"


@dataclass
class IngestResult:
    n_msgs: int
    n_closed: int
    last_id: str


class ABWinnerEvalStore:
    def __init__(
        self,
        *,
        r,
        stream: str,
        prefix: str = "ab:eval:r:z:v1",
        ctx_set_key: str = "ab:eval:ctx:set:v1",
        ctx_ts_prefix: str = "ab:eval:ctx:ts:v1",
        cursor_prefix: str = "ab:eval:cursor:v1",
    ) -> None:
        self.r = r
        self.stream = stream
        self.prefix = prefix
        self.ctx_set_key = ctx_set_key
        self.ctx_ts_prefix = ctx_ts_prefix
        self.cursor_key = f"{cursor_prefix}:{stream}"

    def get_cursor(self) -> str:
        try:
            v = self.r.get(self.cursor_key)
            return (v or "")
        except Exception:
            return ""

    def set_cursor(self, last_id: str) -> None:
        try:
            if last_id:
                self.r.set(self.cursor_key, str(last_id))
        except Exception:
            pass

    def _is_closed(self, f: dict[str, str]) -> bool:
        et = _s(f.get("event_type") or f.get("event") or "").upper()
        if et in ("POSITION_CLOSED", "CLOSE", "CLOSED"):
            return True
        t = _s(f.get("type") or "").upper()
        return t == "POSITION_CLOSED"

    def _extract(self, f: dict[str, str]) -> tuple[str, str, str, str, str, float, int] | None:
        symbol = _norm_symbol(_s(f.get("symbol") or ""))
        if not symbol:
            return None
        regime = _norm_regime(_s(f.get("regime") or "na"))
        group = _norm_group(_s(f.get("ab_group") or "default"))
        scenario = _norm_scenario(_s(f.get("scenario") or f.get("decision") or "na"))
        arm = _norm_arm(_s(f.get("ab_arm") or "A"))
        r_mult = _f(f.get("r_mult"), 0.0)
        if r_mult == 0.0:
            pnl = _f(f.get("pnl"), 0.0)
            risk = _f(f.get("risk_usd"), 0.0)
            if risk > 0:
                r_mult = pnl / risk
        ts_ms = _i(f.get("ts"), 0)
        if ts_ms <= 0:
            ts_ms = _now_ms()
        if abs(r_mult) <= 0:
            return None
        if scenario == "na":
            return None
        return symbol, regime, group, scenario, arm, float(r_mult), int(ts_ms)

    def _zkey(self, symbol: str, regime: str, group: str, scenario: str, arm: str) -> str:
        return f"{self.prefix}:{symbol}:{regime}:{group}:{scenario}:{arm}"

    def ingest_from_stream(
        self,
        *,
        end_ms: int,
        window_ms: int,
        max_items_per_zset: int,
        start_from_id: str = "",
        batch: int = 2000,
        hard_cap_msgs: int = 200000,
    ) -> IngestResult:
        """
        Incremental XREAD from cursor/start_id until end_ms (by stream id ms part).
        Store to ZSETs + prune.
        """
        start_id = start_from_id or self.get_cursor() or f"{max(0, end_ms - window_ms)}-0"
        cur = start_id
        n_msgs = 0
        n_closed = 0
        last_id = start_id
        window_start = int(end_ms) - int(window_ms)

        while True:
            try:
                res = self.r.xread({self.stream: cur}, count=int(batch), block=0)
            except Exception:
                break
            if not res:
                break
            _, msgs = res[0]
            if not msgs:
                break
            pipe = self.r.pipeline()
            touched_ctx: list[tuple[str, int]] = []
            for mid, f in msgs:
                n_msgs += 1
                last_id = mid
                cur = mid
                # stop by end_ms
                try:
                    ts0 = int(str(mid).split("-")[0])
                    if ts0 > int(end_ms):
                        pipe.execute()
                        self.set_cursor(last_id)
                        return IngestResult(n_msgs=n_msgs, n_closed=n_closed, last_id=last_id)
                except Exception:
                    pass
                if not self._is_closed(f):
                    if n_msgs >= hard_cap_msgs:
                        break
                    continue
                ex = self._extract(f)
                if ex is None:
                    if n_msgs >= hard_cap_msgs:
                        break
                    continue
                symbol, regime, group, scenario, arm, r_mult, ts_ms = ex
                # write zset
                zkey = self._zkey(symbol, regime, group, scenario, arm)
                member = f"{mid}|{r_mult:.8f}"
                pipe.zadd(zkey, {member: float(ts_ms)})
                # prune by time
                pipe.zremrangebyscore(zkey, 0, float(window_start - 1))
                # cap size (keep most recent)
                if max_items_per_zset > 0:
                    pipe.zremrangebyrank(zkey, 0, -(int(max_items_per_zset) + 1))
                # ctx registry (no scenario in ctx id)
                ctx_id = f"{symbol}|{regime}|{group}"
                touched_ctx.append((ctx_id, ts_ms))
                n_closed += 1
                if n_msgs >= hard_cap_msgs:
                    break
            # update ctx registry
            try:
                for ctx_id, ts_ms in touched_ctx:
                    pipe.sadd(self.ctx_set_key, ctx_id)
                    pipe.set(f"{self.ctx_ts_prefix}:{ctx_id}", str(int(ts_ms)))
            except Exception:
                pass
            try:
                pipe.execute()
            except Exception:
                pass
            if n_msgs >= hard_cap_msgs:
                break

        self.set_cursor(last_id)
        return IngestResult(n_msgs=n_msgs, n_closed=n_closed, last_id=last_id)

    def list_contexts(self) -> list[str]:
        try:
            xs = list(self.r.smembers(self.ctx_set_key))
            out = []
            for x in xs:
                if isinstance(x, str) and x:
                    out.append(x)
            return out
        except Exception:
            return []

    def load_r_mult_series(
        self,
        *,
        symbol: str,
        regime: str,
        group: str,
        scenario: str,
        arm: str,
        start_ms: int,
        end_ms: int,
        max_items: int = 5000,
    ) -> list[float]:
        zkey = self._zkey(symbol, regime, group, scenario, arm)
        try:
            members = self.r.zrangebyscore(zkey, float(start_ms), float(end_ms), start=0, num=int(max_items))
        except Exception:
            members = []
        out: list[float] = []
        for m in members or []:
            try:
                s = str(m)
                # member = "{mid}|{r}"
                parts = s.split("|", 1)
                if len(parts) != 2:
                    continue
                r = float(parts[1])
                out.append(r)
            except Exception:
                continue
        return out
