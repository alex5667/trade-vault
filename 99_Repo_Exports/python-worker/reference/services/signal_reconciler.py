from __future__ import annotations

import os
import time

import redis

from common.log import setup_logger
from core.delivery_journal import DeliveryJournal, DeliveryJournalSettings
from core.sid_lease import SidLease, SidLeaseSettings
from utils.time_utils import get_ny_time_millis

logger = setup_logger("SignalReconciler")


def _now_ms() -> int:
    return get_ny_time_millis()


class SignalReconciler:
    """
    Автоматически находит sid с неполной доставкой (по journal) и ставит их в replay-stream.
    Гарантии:
      - SETNX queued-marker на sid, чтобы не спамить replay
      - lease на sid, чтобы не конфликтовать с живым dispatcher (best-effort)
    """

    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis = redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=15,
            health_check_interval=0,
        )

        self.replay_stream = os.getenv("SIGNAL_REPLAY_STREAM", "stream:signals:replay")

        self.scan_count = int(os.getenv("SIGNAL_RECONCILER_SCAN_COUNT", "200"))
        self.poll_sec = float(os.getenv("SIGNAL_RECONCILER_POLL_SEC", "5"))
        self.stale_ms = int(os.getenv("SIGNAL_RECONCILER_STALE_MS", "60000"))  # touched older than 60s

        self.queued_prefix = os.getenv("SIGNAL_REPLAY_QUEUED_PREFIX", "sig:replay:queued")
        self.queued_ttl_sec = int(os.getenv("SIGNAL_REPLAY_QUEUED_TTL_SEC", "900"))  # 15m

        self.metrics_prefix = os.getenv("SIGNAL_METRICS_PREFIX", "sig:metrics")

        self.journal = DeliveryJournal(self.redis, settings=DeliveryJournalSettings())
        self.lease = SidLease(self.redis, settings=SidLeaseSettings())

    def _metric_incr(self, name: str, n: int = 1) -> None:
        try:
            self.redis.incrby(f"{self.metrics_prefix}:{name}", int(n))
        except Exception:
            pass

    def _queued_key(self, sid: str) -> str:
        return f"{self.queued_prefix}:{sid}"

    def _enqueue_replay_once(self, sid: str, reason: str) -> bool:
        # SETNX queued-key with TTL to avoid requeue storms
        qk = self._queued_key(sid)
        try:
            ok = self.redis.set(qk, "1", nx=True, ex=self.queued_ttl_sec)
        except Exception:
            return False
        if not ok:
            self._metric_incr("replay_skipped_total", 1)
            return False

        payload = {
            "sid": sid,
            "reason": reason,
            "ts_ms": _now_ms(),
        }
        try:
            self.redis.xadd(self.replay_stream, payload, maxlen=50000, approximate=True)
            self._metric_incr("replay_enqueued_total", 1)
            return True
        except Exception:
            # rollback queued marker so next scan can retry
            try:
                self.redis.delete(qk)
            except Exception:
                pass
            return False

    def _scan_candidates(self) -> list[str]:
        idx = self.journal.settings.index_key
        cutoff = _now_ms() - int(self.stale_ms)
        # take oldest/stale by score <= cutoff
        try:
            sids = self.redis.zrangebyscore(idx, "-inf", cutoff, start=0, num=self.scan_count) or []
        except Exception:
            return []
        out: list[str] = []
        for sid in sids:
            if not sid:
                continue
            out.append(str(sid))
        return out

    def _process_sid(self, sid: str) -> None:
        # best-effort: if dispatcher is actively working, lease will be held
        token = self.lease.try_acquire(sid)
        if not token:
            self._metric_incr("lease_contention_total", 1)
            return
        try:
            complete, desired, delivered = self.journal.is_complete(sid)
            if complete:
                # done -> remove from index (cleanup orphan journals)
                self.journal.drop_index(sid)
                self._metric_incr("journals_completed_total", 1)
                return

            missing = [t for t in desired if t not in delivered]
            reason = f"incomplete:{','.join(missing[:8])}" if missing else "incomplete"
            enq = self._enqueue_replay_once(sid, reason=reason)
            if enq:
                # touch journal index forward to avoid immediate re-scan spam
                try:
                    self.redis.zadd(self.journal.settings.index_key, {sid: float(_now_ms())})
                except Exception:
                    pass
        finally:
            self.lease.release(sid, token)

    def run(self) -> None:
        logger.info(
            "SignalReconciler started redis=%s idx=%s replay=%s scan=%d stale_ms=%d poll=%.1fs",
            self.redis_url,
            self.journal.settings.index_key,
            self.replay_stream,
            self.scan_count,
            self.stale_ms,
            self.poll_sec,
        )
        while True:
            try:
                sids = self._scan_candidates()
                if not sids:
                    time.sleep(self.poll_sec)
                    continue
                for sid in sids:
                    self._process_sid(sid)
            except KeyboardInterrupt:
                logger.info("SignalReconciler stopped")
                return
            except Exception as exc:
                logger.error("Reconciler loop error: %s", exc, exc_info=True)
                time.sleep(self.poll_sec)


if __name__ == "__main__":
    SignalReconciler().run()
