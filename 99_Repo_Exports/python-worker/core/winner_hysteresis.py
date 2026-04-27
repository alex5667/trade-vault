from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Union


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def _f(x, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


@dataclass
class HysteresisResult:
    changed: bool
    winner: str
    winner_lcb: float
    prev_winner: str
    prev_lcb: float
    pending: str
    pending_count: int
    reason: str


class WinnerHysteresis:
    """Winner hysteresis stored in Redis.

    Change winner only if:
      - (lcb_new - lcb_old) >= MIN_DELTA_LCB
      - and the same candidate appears CONFIRM_WINDOWS consecutive runs.
    
    Supports both sync and async Redis clients.
    """

    def __init__(self, r):
        self.r = r
        self.min_delta = float(os.getenv("LCB_MIN_DELTA_LCB", "0.05"))
        self.confirm_windows = int(os.getenv("LCB_CONFIRM_WINDOWS", "2"))
        self.ttl_sec = int(os.getenv("LCB_WINNER_STATE_TTL_SEC", "604800"))  # 7d

    def _k(self, bucket: str, suffix: str) -> str:
        return f"cfg:lcb:{suffix}:{bucket}"

    async def get_winner_async(self, bucket: str) -> str:
        v = await self.r.get(self._k(bucket, "winner"))
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore").strip()
        return str(v or "").strip()

    def get_winner(self, bucket: str) -> str:
        v = self.r.get(self._k(bucket, "winner"))
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore").strip()
        return str(v or "").strip()

    async def get_winner_lcb_async(self, bucket: str) -> float:
        v = await self.r.get(self._k(bucket, "winner_lcb"))
        return _f(v, 0.0)

    def get_winner_lcb(self, bucket: str) -> float:
        v = self.r.get(self._k(bucket, "winner_lcb"))
        return _f(v, 0.0)

    async def apply_async(self, *, bucket: str, candidate: str, candidate_lcb: float) -> HysteresisResult:
        prev = await self.get_winner_async(bucket)
        prev_lcb = await self.get_winner_lcb_async(bucket)

        if not prev:
            await self._set_winner_async(bucket, candidate, candidate_lcb)
            await self._clear_pending_async(bucket)
            return HysteresisResult(True, candidate, candidate_lcb, "", 0.0, "", 0, "init")

        if candidate == prev:
            await self._clear_pending_async(bucket)
            await self._set_winner_async(bucket, prev, prev_lcb)
            return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, "", 0, "same")

        if (candidate_lcb - prev_lcb) < self.min_delta:
            await self._clear_pending_async(bucket)
            await self._set_winner_async(bucket, prev, prev_lcb)
            return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, "", 0, "delta_too_small")

        pend = await self._get_pending_async(bucket)
        if pend == candidate:
            cnt = await self._incr_pending_async(bucket)
        else:
            await self._set_pending_async(bucket, candidate, 1)
            cnt = 1

        if cnt >= self.confirm_windows:
            await self._set_winner_async(bucket, candidate, candidate_lcb)
            await self._clear_pending_async(bucket)
            return HysteresisResult(True, candidate, candidate_lcb, prev, prev_lcb, candidate, cnt, "confirmed")

        await self._set_winner_async(bucket, prev, prev_lcb)
        return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, candidate, cnt, "pending")

    def apply(self, *, bucket: str, candidate: str, candidate_lcb: float) -> HysteresisResult:
        prev = self.get_winner(bucket)
        prev_lcb = self.get_winner_lcb(bucket)

        if not prev:
            self._set_winner(bucket, candidate, candidate_lcb)
            self._clear_pending(bucket)
            return HysteresisResult(True, candidate, candidate_lcb, "", 0.0, "", 0, "init")

        if candidate == prev:
            self._clear_pending(bucket)
            self._set_winner(bucket, prev, prev_lcb)
            return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, "", 0, "same")

        if (candidate_lcb - prev_lcb) < self.min_delta:
            self._clear_pending(bucket)
            self._set_winner(bucket, prev, prev_lcb)
            return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, "", 0, "delta_too_small")

        pend = self._get_pending(bucket)
        if pend == candidate:
            cnt = self._incr_pending(bucket)
        else:
            self._set_pending(bucket, candidate, 1)
            cnt = 1

        if cnt >= self.confirm_windows:
            self._set_winner(bucket, candidate, candidate_lcb)
            self._clear_pending(bucket)
            return HysteresisResult(True, candidate, candidate_lcb, prev, prev_lcb, candidate, cnt, "confirmed")

        self._set_winner(bucket, prev, prev_lcb)
        return HysteresisResult(False, prev, prev_lcb, prev, prev_lcb, candidate, cnt, "pending")

    async def _set_winner_async(self, bucket: str, winner: str, lcb: float) -> None:
        pipe = self.r.pipeline()
        pipe.set(self._k(bucket, "winner"), str(winner), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "winner_lcb"), float(lcb), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "winner_ts_ms"), _now_ms(), ex=self.ttl_sec)
        await pipe.execute()

    def _set_winner(self, bucket: str, winner: str, lcb: float) -> None:
        pipe = self.r.pipeline()
        pipe.set(self._k(bucket, "winner"), str(winner), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "winner_lcb"), float(lcb), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "winner_ts_ms"), _now_ms(), ex=self.ttl_sec)
        pipe.execute()

    async def _get_pending_async(self, bucket: str) -> str:
        v = await self.r.get(self._k(bucket, "pending"))
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore").strip()
        return str(v or "").strip()

    def _get_pending(self, bucket: str) -> str:
        v = self.r.get(self._k(bucket, "pending"))
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore").strip()
        return str(v or "").strip()

    async def _incr_pending_async(self, bucket: str) -> int:
        k = self._k(bucket, "pending_count")
        pipe = self.r.pipeline()
        pipe.incr(k, 1)
        pipe.expire(k, self.ttl_sec)
        res = await pipe.execute()
        return _i(res[0], 1)

    def _incr_pending(self, bucket: str) -> int:
        k = self._k(bucket, "pending_count")
        pipe = self.r.pipeline()
        pipe.incr(k, 1)
        pipe.expire(k, self.ttl_sec)
        res = pipe.execute()
        return _i(res[0], 1)

    async def _set_pending_async(self, bucket: str, cand: str, cnt: int) -> None:
        pipe = self.r.pipeline()
        pipe.set(self._k(bucket, "pending"), str(cand), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "pending_count"), int(cnt), ex=self.ttl_sec)
        await pipe.execute()

    def _set_pending(self, bucket: str, cand: str, cnt: int) -> None:
        pipe = self.r.pipeline()
        pipe.set(self._k(bucket, "pending"), str(cand), ex=self.ttl_sec)
        pipe.set(self._k(bucket, "pending_count"), int(cnt), ex=self.ttl_sec)
        pipe.execute()

    async def _clear_pending_async(self, bucket: str) -> None:
        pipe = self.r.pipeline()
        pipe.delete(self._k(bucket, "pending"))
        pipe.delete(self._k(bucket, "pending_count"))
        await pipe.execute()

    def _clear_pending(self, bucket: str) -> None:
        pipe = self.r.pipeline()
        pipe.delete(self._k(bucket, "pending"))
        pipe.delete(self._k(bucket, "pending_count"))
        pipe.execute()

