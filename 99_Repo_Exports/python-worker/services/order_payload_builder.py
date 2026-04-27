from __future__ import annotations

from typing import Dict, Any


def build_order_payload(signal: Dict[str, Any]) -> Dict[str, Any]:
    meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
    prov = meta.get("policy_provenance", {}) if isinstance(meta.get("policy_provenance"), dict) else {}

    policy_tag = str(prov.get("policy_tag") or "")
    comment_suffix = f":pol={policy_tag}" if policy_tag else ""

    return {
        "signal_id": signal.get("signal_id", ""),
        "action": "OPEN",
        "symbol": signal.get("symbol", ""),
        "side": signal.get("side", ""),
        "entry_price": signal.get("entry_price", 0.0),
        "sl_price": signal.get("sl_price", 0.0),
        "tp1_price": signal.get("tp1_price", 0.0),
        "risk_pct": signal.get("risk_pct", 1.0),
        "comment": f"{signal.get('source','')}:{signal.get('kind','')}:{signal.get('confidence',0)}{comment_suffix}",
        # optional backward-compatible fields
        "atr_policy_ver": signal.get("atr_policy_ver", 0),
        "atr_policy_tag": signal.get("atr_policy_tag", ""),
        "atr_recovery_run_id": signal.get("atr_recovery_run_id", ""),
        "atr_restore_cert_status": signal.get("atr_restore_cert_status", ""),
        "policy_provenance": prov,
    }
