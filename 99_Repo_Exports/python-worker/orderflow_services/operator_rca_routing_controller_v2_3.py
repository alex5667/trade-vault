from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server
import contextlib

STREAM_GOVERNOR_DECISIONS = os.getenv(
    "ML_OPERATOR_RCA_GOVERNOR_DECISIONS_STREAM",
    "stream:ml:operator_rca_governor_decisions",
)
STREAM_ROUTING_DECISIONS = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_DECISIONS_STREAM",
    "stream:ml:operator_rca_routing_decisions",
)
STREAM_ROUTING_AUDIT = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_AUDIT_STREAM",
    "stream:ml:operator_rca_routing_audit",
)
STREAM_OPERATOR_RCA_REQUESTS = os.getenv(
    "ML_OPERATOR_RCA_REQUESTS_STREAM",
    "stream:ml:operator_rca_requests",
)
STREAM_OPERATOR_RCA_REQUESTS_ROUTED = os.getenv(
    "ML_OPERATOR_RCA_REQUESTS_ROUTED_STREAM",
    "stream:ml:operator_rca_requests_routed",
)

HASH_ROUTING_LAST = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_LAST_HASH", "metrics:ml:operator_rca_routing:last"
)
HASH_ACTIVE_POLICY = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_ACTIVE_HASH", "cfg:ml:operator_rca:routing:active"
)

GROUP_DECISIONS = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_DECISIONS_GROUP", "cg:ml_operator_rca_routing_decisions"
)
GROUP_REQUESTS = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_REQUESTS_GROUP", "cg:ml_operator_rca_requests"
)
CONSUMER = os.getenv("HOSTNAME", "scanner-ml-operator-rca-routing-v2-3")


RUNS = Counter(
    "ml_operator_rca_routing_runs_total",
    "Routing controller loop runs",
    ["mode", "status"],
)
DECISIONS = Counter(
    "ml_operator_rca_routing_decisions_total",
    "Routing decisions emitted",
    ["decision", "scope"],
)
ROUTED = Counter(
    "ml_operator_rca_requests_routed_total",
    "Operator RCA requests routed",
    ["provider", "model_name", "prompt_version", "mode"],
)
LAST_RUN_TS = Gauge(
    "ml_operator_rca_routing_last_run_ts_seconds",
    "Last successful routing controller loop",
)
QUEUE_LAG_MS = Gauge(
    "ml_operator_rca_routing_queue_lag_ms",
    "Approximate queue lag for routing streams",
    ["stream"],
)
LOOP_LAT = Histogram(
    "ml_operator_rca_routing_loop_seconds",
    "Routing controller loop latency",
)
ACTIVE_ROUTE = Gauge(
    "ml_operator_rca_routing_active_route_info",
    "Active route marker",
    ["provider", "model_name", "prompt_version", "policy_version", "mode"],
)


def _now_ms() -> int:
    return get_ny_time_millis()


def _decode(fields: dict[Any, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


def _jloads(raw: str, default: Any) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return default


def _sha_fields(provider: str, model_name: str, prompt_version: str, policy_version: str) -> str:
    return f"{provider}|{model_name}|{prompt_version}|{policy_version}"


@dataclass
class GovernorDecision:
    scope: str
    decision: str
    action_type: str
    provider: str
    model_name: str
    prompt_version: str
    policy_version: str
    score: float
    ts_ms: int
    reason_codes: list[str]


def parse_governor_decision(fields: dict[Any, Any]) -> GovernorDecision | None:
    d = _decode(fields)
    try:
        return GovernorDecision(
            scope=(d.get("scope", "provider_prompt")),
            decision=(d.get("decision", "HOLD")).upper(),
            action_type=(d.get("action_type", "*")),
            provider=(d.get("provider", "vertex")),
            model_name=(d.get("model_name", "gemini-2.5-flash-lite")),
            prompt_version=(d.get("prompt_version", "ml_triage_v1")),
            policy_version=(d.get("policy_version", "policy_v1")),
            score=float(d.get("score", "0") or 0.0),
            ts_ms=int(d.get("ts_ms", "0") or 0),
            reason_codes=_jloads(d.get("reason_codes_json", "[]"), []),
        )
    except Exception:
        return None


def choose_route(
    *,
    current: dict[str, str],
    decisions: Iterable[GovernorDecision],
    allow_promote: bool,
    allow_suppress: bool,
    fallback_provider: str,
    fallback_model: str,
    fallback_prompt_version: str,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    route = dict(current)
    audit: list[dict[str, Any]] = []
    promoted: GovernorDecision | None = None
    suppressed_keys: set[str] = set()

    for dec in decisions:
        key = _sha_fields(dec.provider, dec.model_name, dec.prompt_version, dec.policy_version)
        if dec.decision == "SUPPRESS" and allow_suppress:
            suppressed_keys.add(key)
            audit.append(
                {
                    "event": "SUPPRESS_SEEN",
                    "key": key,
                    "score": dec.score,
                    "reason_codes": dec.reason_codes,
                }
            )
        elif dec.decision == "PROMOTE" and allow_promote:
            if promoted is None or dec.score > promoted.score:
                promoted = dec

    if promoted is not None:
        pkey = _sha_fields(
            promoted.provider,
            promoted.model_name,
            promoted.prompt_version,
            promoted.policy_version,
        )
        if pkey not in suppressed_keys:
            route["provider"] = promoted.provider
            route["model_name"] = promoted.model_name
            route["prompt_version"] = promoted.prompt_version
            route["policy_version"] = promoted.policy_version
            audit.append(
                {
                    "event": "PROMOTE_SELECTED",
                    "key": pkey,
                    "score": promoted.score,
                    "reason_codes": promoted.reason_codes,
                }
            )
        else:
            audit.append(
                {
                    "event": "PROMOTE_SUPPRESSED_CONFLICT",
                    "key": pkey,
                    "score": promoted.score,
                }
            )

    active_key = _sha_fields(
        route.get("provider", fallback_provider),
        route.get("model_name", fallback_model),
        route.get("prompt_version", fallback_prompt_version),
        route.get("policy_version", "policy_v1"),
    )
    if active_key in suppressed_keys:
        route["provider"] = fallback_provider
        route["model_name"] = fallback_model
        route["prompt_version"] = fallback_prompt_version
        route["policy_version"] = route.get("policy_version", "policy_v1")
        audit.append({"event": "FALLBACK_SELECTED", "key": active_key})

    return route, audit


class RoutingController:
    def __init__(self, redis_url: str) -> None:
        if redis is None:  # pragma: no cover
            raise RuntimeError("redis.asyncio is required")
        self.r = redis.from_url(redis_url, decode_responses=False)
        self.mode = os.getenv("ML_OPERATOR_RCA_ROUTING_MODE", "DRY_RUN").upper()
        self.allow_promote = int(os.getenv("ML_OPERATOR_RCA_ROUTING_ALLOW_PROMOTE", "1")) == 1
        self.allow_suppress = int(os.getenv("ML_OPERATOR_RCA_ROUTING_ALLOW_SUPPRESS", "1")) == 1
        self.default_provider = os.getenv("ML_OPERATOR_RCA_DEFAULT_PROVIDER", "vertex")
        self.default_model = os.getenv("ML_OPERATOR_RCA_DEFAULT_MODEL", "gemini-2.5-flash-lite")
        self.default_prompt_version = os.getenv(
            "ML_OPERATOR_RCA_DEFAULT_PROMPT_VERSION", "ml_triage_v1"
        )
        self.default_policy_version = os.getenv(
            "ML_OPERATOR_RCA_DEFAULT_POLICY_VERSION", "policy_v1"
        )
        self.loop_sleep_sec = float(os.getenv("ML_OPERATOR_RCA_ROUTING_LOOP_SLEEP_SEC", "1.0"))
        self.max_decisions = int(os.getenv("ML_OPERATOR_RCA_ROUTING_DECISIONS_BATCH", "100"))
        self.max_requests = int(os.getenv("ML_OPERATOR_RCA_ROUTING_REQUESTS_BATCH", "100"))
        self.decisions_buffer: list[GovernorDecision] = []
        self.current_route: dict[str, str] = {
            "provider": self.default_provider,
            "model_name": self.default_model,
            "prompt_version": self.default_prompt_version,
            "policy_version": self.default_policy_version,
            "mode": self.mode,
        }

    async def ensure_groups(self) -> None:
        for stream, group in ((STREAM_GOVERNOR_DECISIONS, GROUP_DECISIONS), (STREAM_OPERATOR_RCA_REQUESTS, GROUP_REQUESTS)):
            with contextlib.suppress(Exception):
                await self.r.xgroup_create(stream, group, id="0", mkstream=True)

    async def load_active_route(self) -> None:
        try:
            data = await self.r.hgetall(HASH_ACTIVE_POLICY)
            if not data:
                return
            d = _decode(data)
            self.current_route.update(
                {
                    "provider": d.get("provider", self.default_provider),
                    "model_name": d.get("model_name", self.default_model),
                    "prompt_version": d.get("prompt_version", self.default_prompt_version),
                    "policy_version": d.get("policy_version", self.default_policy_version),
                    "mode": d.get("mode", self.mode),
                }
            )
        except Exception:
            return

    async def handle_governor_decisions(self) -> int:
        rows = await self.r.xreadgroup(
            GROUP_DECISIONS,
            CONSUMER,
            {STREAM_GOVERNOR_DECISIONS: ">"},
            count=self.max_decisions,
            block=100,
        )
        total = 0
        for _stream, messages in rows:
            for msg_id, fields in messages:
                total += 1
                dec = parse_governor_decision(fields)
                if dec is not None:
                    self.decisions_buffer.append(dec)
                await self.r.xack(STREAM_GOVERNOR_DECISIONS, GROUP_DECISIONS, msg_id)
        return total

    async def maybe_refresh_route(self) -> None:
        route, audit = choose_route(
            current=self.current_route,
            decisions=self.decisions_buffer[-500:],
            allow_promote=self.allow_promote,
            allow_suppress=self.allow_suppress,
            fallback_provider=self.default_provider,
            fallback_model=self.default_model,
            fallback_prompt_version=self.default_prompt_version,
        )
        changed = route != self.current_route
        self.current_route = route

        payload = {
            "ts_ms": _now_ms(),
            "provider": route["provider"],
            "model_name": route["model_name"],
            "prompt_version": route["prompt_version"],
            "policy_version": route["policy_version"],
            "mode": self.mode,
            "changed": int(changed),
            "audit_json": json.dumps(audit, ensure_ascii=False),
        }
        await self.r.xadd(STREAM_ROUTING_DECISIONS, payload, maxlen=10000, approximate=True)
        await self.r.hset(HASH_ROUTING_LAST, mapping={k: str(v) for k, v in payload.items()})
        if self.mode == "COMMIT":
            await self.r.hset(HASH_ACTIVE_POLICY, mapping={k: str(v) for k, v in payload.items() if k != "audit_json"})
        await self.r.xadd(
            STREAM_ROUTING_AUDIT,
            {
                "ts_ms": _now_ms(),
                "event": "ROUTE_REFRESH",
                "changed": int(changed),
                "provider": route["provider"],
                "model_name": route["model_name"],
                "prompt_version": route["prompt_version"],
                "policy_version": route["policy_version"],
                "mode": self.mode,
            }, maxlen=20000,
            approximate=True,
        )
        DECISIONS.labels("ROUTE_REFRESH", "provider_prompt").inc()
        ACTIVE_ROUTE.labels(
            route["provider"],
            route["model_name"],
            route["prompt_version"],
            route["policy_version"],
            self.mode,
        ).set(1)

    async def route_requests(self) -> int:
        rows = await self.r.xreadgroup(
            GROUP_REQUESTS,
            CONSUMER,
            {STREAM_OPERATOR_RCA_REQUESTS: ">"},
            count=self.max_requests,
            block=100,
        )
        total = 0
        now_ms = _now_ms()
        for _stream, messages in rows:
            for msg_id, fields in messages:
                total += 1
                decoded = _decode(fields)
                routed = dict(decoded)
                routed["provider"] = self.current_route["provider"]
                routed["model_name"] = self.current_route["model_name"]
                routed["prompt_version"] = self.current_route["prompt_version"]
                routed["policy_version"] = self.current_route["policy_version"]
                routed["routing_mode"] = self.mode
                routed["routing_ts_ms"] = str(now_ms)
                await self.r.xadd(
                    STREAM_OPERATOR_RCA_REQUESTS_ROUTED,
                    routed,
                    maxlen=200000,
                    approximate=True,
                )
                await self.r.xack(STREAM_OPERATOR_RCA_REQUESTS, GROUP_REQUESTS, msg_id)
                ROUTED.labels(
                    self.current_route["provider"],
                    self.current_route["model_name"],
                    self.current_route["prompt_version"],
                    self.mode,
                ).inc()
        return total

    async def loop_once(self) -> tuple[int, int]:
        t0 = time.perf_counter()
        dec_n = await self.handle_governor_decisions()
        if dec_n > 0:
            await self.maybe_refresh_route()
        req_n = await self.route_requests()
        LAST_RUN_TS.set(time.time())
        LOOP_LAT.observe(time.perf_counter() - t0)
        RUNS.labels(self.mode, "ok").inc()
        return dec_n, req_n

    async def run_forever(self) -> None:
        await self.ensure_groups()
        await self.load_active_route()
        await self.maybe_refresh_route()
        while True:
            try:
                await self.loop_once()
            except Exception:
                RUNS.labels(self.mode, "err").inc()
            await self._set_lag_metrics()
            await self._sleep()

    async def _set_lag_metrics(self) -> None:
        now_ms = _now_ms()
        for stream in (STREAM_GOVERNOR_DECISIONS, STREAM_OPERATOR_RCA_REQUESTS):
            try:
                last = await self.r.xrevrange(stream, count=1)
                if not last:
                    continue
                msg_id, fields = last[0]
                decoded = _decode(fields)
                ts_ms = int(decoded.get("ts_ms", "0") or 0)
                if ts_ms > 0:
                    QUEUE_LAG_MS.labels(stream).set(max(0, now_ms - ts_ms))
            except Exception:
                continue

    async def _sleep(self) -> None:
        await self.r.client_setname(CONSUMER)
        await __import__("asyncio").sleep(self.loop_sleep_sec)


async def _amain() -> None:
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_ROUTING_METRICS_PORT", "9874")))
    ctrl = RoutingController(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ctrl.run_forever()


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(_amain())
