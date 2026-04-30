from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Dict, List, Tuple, Optional

import requests


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _split_csv(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _http_check(url: str, *, verify_tls: bool, timeout_s: float = 6.0) -> Tuple[bool, str, float]:
    t0 = time.time()
    try:
        r = requests.get(url, verify=verify_tls, timeout=timeout_s)
        dt = (time.time() - t0) * 1000.0
        if r.status_code == 200:
            return True, "ok", dt
        return False, f"status:{r.status_code}", dt
    except Exception as e:
        dt = (time.time() - t0) * 1000.0
        return False, f"exc:{type(e).__name__}", dt


def _mk_url(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").strip()
    if not p:
        return b
    if not p.startswith("/"):
        p = "/" + p
    return b + p


def _smoke_contract_targets(public_base: str) -> Tuple[List[str], List[str]]:
    """Return (runbooks_urls, dashboards_urls) for contract checks."""
    runbooks = _split_csv(_env("SMOKE_RUNBOOK_PATHS", "/runbooks/web_uptime.md,/runbooks/promote_freeze.md,/runbooks/chatops_security.md"))
    dashboards = _split_csv(_env("SMOKE_DASHBOARD_PATHS", "/grafana/d/edge_stack_overview/edge-stack-overview?orgId=1,/grafana/d/chatops_security/chatops-security?orgId=1"))
    runbook_urls = [_mk_url(public_base, p) for p in runbooks]
    dash_urls = [_mk_url(public_base, p) for p in dashboards]
    return runbook_urls, dash_urls


def _telegram_links_smoke(webhook_url: str, *, verify_tls: bool) -> Tuple[bool, str]:
    payload = {
        "status": "firing"
        "alerts": [{"labels": {"alertname": "SmokeTest"}}]
        "commonAnnotations": {"smoke": "1"}
        "commonLabels": {"smoke": "1"}
    }
    # Usually alertmanager webhook supports ?dry_run=1 to just generate links without sending Telegram msg
    url = f"{webhook_url}?dry_run=1"
    try:
        r = requests.post(url, json=payload, verify=verify_tls, timeout=5.0)
        if r.status_code != 200:
            return False, f"status:{r.status_code}"
        d = r.json()
        text = d.get("text", "")
        rb_base = _env("RUNBOOKS_BASE_URL", "")
        graf_base = _env("GRAFANA_BASE_URL", "")
        if rb_base and rb_base not in text:
            return False, "missing_runbook_base"
        if graf_base and graf_base not in text:
            return False, "missing_grafana_base"
        return True, "ok"
    except Exception as e:
        return False, f"exc:{type(e).__name__}"


def _write_redis(hash_key: str, fields: Dict[str, str]) -> None:
    try:
        import redis
        url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = redis.Redis.from_url(url)
        r.hset(hash_key, mapping=fields)
    except Exception:
        pass


def main() -> int:
    public_base = _env("PUBLIC_BASE_URL", "")
    if not public_base:
        print("PUBLIC_BASE_URL not set")
        return 1

    # In dev, we might use self-signed certs for localhost public_base
    verify_tls = _env("SMOKE_VERIFY_TLS", "0") in ("1", "true", "yes")

    health_urls = [
        f"{public_base}/grafana/api/health"
        f"{public_base}/runbooks/healthz"
        f"{public_base}/alertmanager/-/ready"
        f"{public_base}/prometheus/-/ready"
    ]
    runbook_urls, dash_urls = _smoke_contract_targets(public_base)

    checks: List[Dict[str, str]] = []
    ok_all = True

    # 1) Public proxy health checks
    for u in health_urls:
        ok, reason, dt_ms = _http_check(u, verify_tls=verify_tls)
        checks.append({"kind": "health", "url": u, "ok": "1" if ok else "0", "reason": reason, "latency_ms": f"{dt_ms:.1f}"})
        ok_all = ok_all and ok

    # 2) Monitoring contract checks: runbooks + dashboards exist and route under public-proxy.
    runbooks_ok = True
    dashboards_ok = True
    for u in runbook_urls:
        ok, reason, dt_ms = _http_check(u, verify_tls=verify_tls)
        checks.append({"kind": "runbook", "url": u, "ok": "1" if ok else "0", "reason": reason, "latency_ms": f"{dt_ms:.1f}"})
        runbooks_ok = runbooks_ok and ok
        ok_all = ok_all and ok
    for u in dash_urls:
        ok, reason, dt_ms = _http_check(u, verify_tls=verify_tls)
        checks.append({"kind": "dashboard", "url": u, "ok": "1" if ok else "0", "reason": reason, "latency_ms": f"{dt_ms:.1f}"})
        dashboards_ok = dashboards_ok and ok
        ok_all = ok_all and ok

    webhook_url = _env("ALERT_WEBHOOK_URL", "http://alertmanager-telegram-webhook:8081/alert")
    ok2, reason2 = _telegram_links_smoke(webhook_url, verify_tls=verify_tls)
    checks.append({"kind": "telegram", "url": webhook_url, "ok": "1" if ok2 else "0", "reason": reason2, "latency_ms": "0"})
    ok_all = ok_all and ok2

    ts_ms = _now_ms()
    out = {"success": ok_all, "updated_ts_ms": ts_ms, "checks": checks}
    print(json.dumps(out, ensure_ascii=False))

    key = _env("MONITORING_SMOKE_METRICS_KEY", "metrics:monitoring_smoke:last")
    failed_checks = [c for c in checks if c.get("ok") != "1"]
    failed_health = [c for c in failed_checks if c.get("kind") == "health"]
    failed_contract = [c for c in failed_checks if c.get("kind") in ("runbook", "dashboard")]
    fields = {
        "success": "1" if ok_all else "0"
        "runbooks_ok": "1" if runbooks_ok else "0"
        "dashboards_ok": "1" if dashboards_ok else "0"
        "updated_ts_ms": str(ts_ms)
        "reason": "ok" if ok_all else "failed"
        "public_base_url": public_base
        "failed_total": str(len(failed_checks))
        "failed_health": json.dumps(failed_health, ensure_ascii=False)[:1200]
        "failed_contract": json.dumps(failed_contract, ensure_ascii=False)[:1200]
    }
    _write_redis(key, fields)
    return 0 if ok_all else 2


if __name__ == "__main__":
    raise SystemExit(main())
