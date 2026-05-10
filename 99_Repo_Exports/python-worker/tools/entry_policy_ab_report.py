from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _day_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


@dataclass
class ABReportCfg:
    audit_stream: str = RS.ENTRY_AUDIT
    out_dir: str = "/var/log/trade"
    lookback_sec: int = 24 * 3600
    limit: int = 5000
    min_n: int = 100
    publish_winner_suggestion: bool = False
    suggestion_key: str = "cfg:suggestions:entry_policy:latest"
    suggestions_ttl_sec: int = 7 * 24 * 3600

    @staticmethod
    def from_env() -> ABReportCfg:
        return ABReportCfg(
            audit_stream=os.getenv("TRADE_ENTRY_AUDIT_STREAM", RS.ENTRY_AUDIT),
            out_dir=os.getenv("EP_REPORT_DIR", "/var/log/trade"),
            lookback_sec=int(os.getenv("EP_AB_LOOKBACK_SEC", str(24 * 3600))),
            limit=int(os.getenv("EP_AB_AUDIT_LIMIT", "5000")),
            min_n=int(os.getenv("EP_AB_MIN_N", "100")),
            publish_winner_suggestion=bool(int(os.getenv("EP_AB_PUBLISH_WINNER_SUGG", "0"))),
            suggestion_key=os.getenv("EP_SUGGESTIONS_REDIS_KEY", "cfg:suggestions:entry_policy:latest"),
            suggestions_ttl_sec=int(os.getenv("EP_SUGGESTIONS_TTL_SEC", str(7 * 24 * 3600))),
        )


async def read_audits(r: aioredis.Redis, stream: str, since_ms: int, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        entries = await r.xrevrange(stream, max="+", min="-", count=int(limit))
    except Exception:
        return out
    for _msg_id, fields in entries:
        try:
            payload = json.loads(fields.get("payload", "") or "{}")
            if not isinstance(payload, dict):
                continue
            ts = int(payload.get("ts_ms") or 0)
            if ts < since_ms:
                break
            if payload.get("type") != "entry_policy_audit":
                continue
            out.append(payload)
        except Exception:
            continue
    return out


def summarize_by_arm(audits: list[dict[str, Any]]) -> dict[str, Any]:
    by_arm: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for a in audits:
        arm = (a.get("arm", "NA") or "NA").upper()
        by_arm[arm].append(a)

    def _sum(xs: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(xs)
        allow = sum(1 for x in xs if int(x.get("ok", 0) or 0) == 1)
        deny = total - allow
        allow_rate = (allow / max(total, 1)) * 100.0
        by_reason = Counter((x.get("reason_code", "na")) for x in xs)
        by_regime = Counter((x.get("regime", "na")) for x in xs)
        # quality proxies (averages on allowed only)
        allowed = [x for x in xs if int(x.get("ok", 0) or 0) == 1]
        avg_of = sum(_f(x.get("of_confirm_score", 0.0)) for x in allowed) / max(len(allowed), 1)
        avg_coh = sum(_f(x.get("coh", 0.0)) for x in allowed) / max(len(allowed), 1)
        avg_lcs = sum(_f(x.get("leader_conf_score", 0.0)) for x in allowed) / max(len(allowed), 1)
        return {
            "total": total,
            "allow": allow,
            "deny": deny,
            "allow_rate": allow_rate,
            "top_reason": by_reason.most_common(10),
            "top_regime": by_regime.most_common(10),
            "avg_of_confirm_score_allow": avg_of,
            "avg_coh_allow": avg_coh,
            "avg_leader_conf_allow": avg_lcs,
        }

    out = {arm: _sum(xs) for arm, xs in by_arm.items()}
    return out


def pick_winner_any(summary: dict[str, Any], min_n: int, arms: list[str]) -> tuple[str, str]:
    """
    Conservative winner selection among arms.
    Requirements:
      - each candidate arm must have >= min_n samples
    Score:
      score = pen * (avg_coh_allow + avg_leader_conf_allow)
      pen reduces if allow_rate is wildly higher than median (too permissive).
    """
    candidates: list[tuple[str, float, str]] = []
    ars: list[float] = []
    for arm in arms:
        s = summary.get(arm) or {}
        if int(s.get("total", 0) or 0) < min_n:
            continue
        ars.append(float(s.get("allow_rate", 0.0) or 0.0))
    if len(ars) < 2:
        return "NA", "insufficient_samples"
    ars_sorted = sorted(ars)
    med_ar = ars_sorted[len(ars_sorted)//2]
    for arm in arms:
        s = summary.get(arm) or {}
        if int(s.get("total", 0) or 0) < min_n:
            continue
        ar = float(s.get("allow_rate", 0.0) or 0.0)
        pen = 1.0
        if med_ar > 0 and ar > 2.0 * med_ar:
            pen = 0.85
        score = pen * (float(s.get("avg_coh_allow", 0.0) or 0.0) + float(s.get("avg_leader_conf_allow", 0.0) or 0.0))
        candidates.append((arm, float(score), f"score={score:.4f} pen={pen:.2f} ar={ar:.2f}% med_ar={med_ar:.2f}%"))
    if not candidates:
        return "NA", "insufficient_samples"
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0], candidates[0][2]


def _is_thin(regime: str) -> bool:
    rg = (regime or "na").strip().lower()
    return rg in ("thin", "news", "illiquid")


def split_by_group(audits: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {"thin": [], "default": []}
    for a in audits:
        rg = (a.get("regime", "na") or "na")
        out["thin" if _is_thin(rg) else "default"].append(a)
    return out


def render_md(day: str, since_ms: int, until_ms: int, by_arm: dict[str, Any], winner: tuple[str, str]) -> str:
    lines: list[str] = []
    lines.append(f"# Entry Policy A/B Report — {day}")
    lines.append("")
    lines.append(f"- window: {since_ms} .. {until_ms} (ms)")
    lines.append(f"- winner: **{winner[0]}** — {winner[1]}")
    lines.append("")
    for arm in ("A", "B", "C"):
        s = by_arm.get(arm) or {}
        if not s:
            continue
        lines.append(f"## Arm {arm}")
        lines.append(f"- total: **{int(s.get('total',0) or 0)}**")
        lines.append(f"- allow_rate: **{float(s.get('allow_rate',0.0) or 0.0):.2f}%**")
        lines.append(f"- avg_coh_allow: **{float(s.get('avg_coh_allow',0.0) or 0.0):.4f}**")
        lines.append(f"- avg_leader_conf_allow: **{float(s.get('avg_leader_conf_allow',0.0) or 0.0):.4f}**")
        lines.append(f"- avg_of_confirm_score_allow: **{float(s.get('avg_of_confirm_score_allow',0.0) or 0.0):.4f}**")
        lines.append("")
        def _tbl(title: str, items: list[tuple[str, int]]) -> None:
            lines.append(f"### {title}")
            lines.append("| key | count |")
            lines.append("|---|---:|")
            for k, c in items:
                lines.append(f"| {k} | {c} |")
            lines.append("")
        _tbl("Top reason_code", list(s.get("top_reason", [])))
        _tbl("Top regimes", list(s.get("top_regime", [])))
    return "\n".join(lines)


async def main() -> int:
    cfg = ABReportCfg.from_env()
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

    day = _day_utc()
    until = _now_ms()
    since = until - int(cfg.lookback_sec) * 1000

    audits = await read_audits(r, cfg.audit_stream, since_ms=since, limit=int(cfg.limit))
    by_arm = summarize_by_arm(audits)
    arms = ["A", "B", "C"]
    winner = pick_winner_any(by_arm, min_n=int(cfg.min_n), arms=arms)

    # per-regime-group winners
    groups = split_by_group(audits)
    by_arm_thin = summarize_by_arm(groups["thin"])
    by_arm_def = summarize_by_arm(groups["default"])
    win_thin = pick_winner_any(by_arm_thin, min_n=max(30, int(cfg.min_n // 3)), arms=arms)
    win_def = pick_winner_any(by_arm_def, min_n=max(50, int(cfg.min_n // 2)), arms=arms)

    md = render_md(day, since, until, by_arm, winner)
    # Append group winners
    md += "\n\n## Winners by regime group\n"
    md += f"- default: **{win_def[0]}** — {win_def[1]}\n"
    md += f"- thin/news/illiquid: **{win_thin[0]}** — {win_thin[1]}\n"

    md_path = out_dir / f"entry_policy_ab_report_{day}.md"
    js_path = out_dir / f"entry_policy_ab_summary_{day}.json"
    md_path.write_text(md, encoding="utf-8")
    js_path.write_text(json.dumps({
        "day": day,
        "since_ms": since,
        "until_ms": until,
        "by_arm": by_arm,
        "winner": winner,
        "winner_default": win_def,
        "winner_thin": win_thin,
        "by_arm_default": by_arm_def,
        "by_arm_thin": by_arm_thin,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # Optional: publish winner suggestion (writes into cfg:suggestions:entry_policy:latest)
    # NOTE: this is still gated by your 2-man approval flow.
    if cfg.publish_winner_suggestion and winner[0] in ("A", "B"):
        key = cfg.suggestion_key
        # read overrides of winner from Redis
        from services.config_overrides import fetch_overrides
        # Determine keys from AB config if present; else defaults
        ab_raw = await r.get(os.getenv("CFG_ENTRY_POLICY_AB_KEY", "cfg:entry_policy:ab:config"))
        ab_cfg = None
        if ab_raw:
            try:
                from services.ab_router import ABConfig
                d = json.loads(ab_raw)
                if isinstance(d, dict):
                    ab_cfg = ABConfig.from_dict(d)
            except Exception:
                ab_cfg = None
        key_a = (ab_cfg.key_a if ab_cfg else "cfg:entry_policy:overrides:A")
        key_b = (ab_cfg.key_b if ab_cfg else "cfg:entry_policy:overrides:B")
        key_c = (ab_cfg.key_c if ab_cfg else "cfg:entry_policy:overrides:C")

        # choose base winner globally, and optionally merge thin knobs from thin-winner if stricter
        def _key_for_arm(a: str) -> str:
            if a == "A":
                return str(key_a)
            if a == "B":
                return str(key_b)
            return str(key_c)

        base_arm = winner[0]
        base_key = _key_for_arm(base_arm)
        base_ver, base_ov = await fetch_overrides(r=r, key=base_key)

        # optional merge
        merged_ov = dict(base_ov)
        merge_note = ""
        thin_arm = win_thin[0]
        if thin_arm in ("A","B","C") and thin_arm != base_arm:
            thin_key = _key_for_arm(thin_arm)
            thin_ver, thin_ov = await fetch_overrides(r=r, key=thin_key)
            # Merge only thin knobs if they tighten risk:
            # - MAX_ZONE_BP_THIN: lower is stricter
            # - OBI_MIN_SEC: higher is stricter
            try:
                z_base = float(merged_ov.get("SMT_ENTRY_MAX_ZONE_BP_THIN", "0") or 0)
                z_thin = float(thin_ov.get("SMT_ENTRY_MAX_ZONE_BP_THIN", "0") or 0)
                if z_base > 0 and z_thin > 0 and z_thin < z_base:
                    merged_ov["SMT_ENTRY_MAX_ZONE_BP_THIN"] = str(z_thin)
                obi_base = float(merged_ov.get("SMT_ENTRY_OBI_MIN_SEC", "0") or 0)
                obi_thin = float(thin_ov.get("SMT_ENTRY_OBI_MIN_SEC", "0") or 0)
                if obi_base > 0 and obi_thin > 0 and obi_thin > obi_base:
                    merged_ov["SMT_ENTRY_OBI_MIN_SEC"] = str(obi_thin)
                merge_note = f"merged thin knobs from thin_winner={thin_arm} key={thin_key} ver={thin_ver}"
            except Exception:
                merge_note = ""

        # build suggestion doc compatible with approval/apply pipeline
        sugg = {
            "ts_ms": _now_ms(),
            "enable": 1,
            "safe_to_apply": 1,
            "current": {},
            "proposed": {k: v for k, v in merged_ov.items() if k != "ENTRY_POLICY_SHADOW"},
            "changes": [{"key": k, "from": None, "to": v, "why": f"promote ABC winner={winner[0]}"} for k, v in merged_ov.items() if k != "ENTRY_POLICY_SHADOW"],
            "rationales": [
                f"ABC winner={winner[0]} {winner[1]}",
                f"default_winner={win_def[0]} {win_def[1]}",
                f"thin_winner={win_thin[0]} {win_thin[1]}",
                f"source_key={base_key} ver={base_ver}",
                merge_note if merge_note else "",
            ],
            "stats": {
                "abc_winner": winner[0],
                "abc_reason": winner[1],
                "winner_default": win_def,
                "winner_thin": win_thin,
                "by_arm": by_arm,
                "by_arm_default": by_arm_def,
                "by_arm_thin": by_arm_thin,
            },
        }
        await r.set(key, json.dumps(sugg, ensure_ascii=False, separators=(",", ":")), ex=int(cfg.suggestions_ttl_sec))

    return 0


if __name__ == "__main__":
    raise SystemExit(__import__("asyncio").run(main()))
