from __future__ import annotations

import json
import os
from collections import Counter

from services.entry_policy_core import EntryPolicyCfg, evaluate_entry_policy


def main() -> None:
    path = os.getenv("IN", "entry_policy_inputs.ndjson")
    out_path = os.getenv("OUT", "entry_policy_replay.ndjson")
    use_event_ts = bool(int(os.getenv("USE_EVENT_TS", "1")))

    cfg = EntryPolicyCfg(
        coh_thr=float(os.getenv("SMT_COH_THRESHOLD", "0.65")),
        leader_conf_min=float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "0.65")),
        min_of_score=float(os.getenv("SMT_ENTRY_MIN_OF_SCORE", "1.0")),
        max_zone_bp=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP", "15")),
        max_zone_bp_thin=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP_THIN", "10")),
        obi_min_sec=float(os.getenv("SMT_ENTRY_OBI_MIN_SEC", "1.5")),
        dedup_ms=int(os.getenv("SMT_ENTRY_DEDUP_MS", "60000")),
        allow_zone_id_change_if_near=bool(int(os.getenv("ENTRY_POLICY_ALLOW_ZONE_CHANGE_IF_NEAR", "0"))),
    )

    dedup_state: dict[str, int] = {}
    stats = Counter()
    by_reason = Counter()
    by_regime = Counter()
    by_symbol = Counter()

    with open(path, encoding="utf-8") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            cand = rec.get("cand", {}) or {}
            snap = rec.get("snap", {}) or {}
            bundle = rec.get("bundle", {}) or {}
            now_ms = int(cand.get("ts_ms") or 0) if use_event_ts else int(rec.get("captured_ts_ms") or 0)
            dec = evaluate_entry_policy(now_ms=now_ms, cand=cand, snap=snap, bundle=bundle, cfg=cfg, dedup_state=dedup_state)

            sym = (cand.get("symbol", "") or "")
            reg = (snap.get("regime", "na") or "na")
            stats["total"] += 1
            by_symbol[sym] += 1
            by_regime[reg] += 1
            by_reason[dec.reason_code] += 1
            if dec.ok:
                stats["allow"] += 1
            else:
                stats["deny"] += 1

            out = {
                "msg_id": rec.get("msg_id"),
                "symbol": sym,
                "regime": reg,
                "ok": 1 if dec.ok else 0,
                "reason_code": dec.reason_code,
                "notes": dec.notes,
            }
            f_out.write(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n")

    # Print summary to stdout
    allow_rate = (stats["allow"] / max(stats["total"], 1)) * 100.0
    print(f"total={stats['total']} allow={stats['allow']} deny={stats['deny']} allow_rate={allow_rate:.2f}%")
    print("top_reason_codes:", by_reason.most_common(10))
    print("top_regimes:", by_regime.most_common(10))
    print("top_symbols:", by_symbol.most_common(10))


if __name__ == "__main__":
    main()
