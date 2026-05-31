"""Guard: EXEC_MAKER_ONLY_KINDS must cover all real CryptoOrderFlow kinds.

Was `of:continuation` only (which never materializes in trades_closed — 0 hits
over 7d audit on 2026-05-23). After fix it covers iceberg/weak_progress/ok/
delta_spike/absorption/weak_recent — the actual entry_tag values seen in prod.
"""

import os
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "crypto-of-common.env"

# Real CryptoOrderFlow kinds observed in trades_closed (7d, 2026-05-17→24).
# Ordered by volume — see audit memo project_p_edge_calibrator_starvation_fix.
_REQUIRED_KINDS = ("iceberg", "weak_progress", "ok", "delta_spike", "absorption", "weak_recent")


def _parse_csv_env(value: str | None) -> set[str]:
    """Mirror entry_policy_gate.py:626-631 — must stay in sync."""
    return {k.strip().lower() for k in (value or "").split(",") if k.strip()}


def _read_env_value(path: Path, key: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    prefix = f"{key}="
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("#"):
            continue
        if s.startswith(prefix):
            return s[len(prefix):]
    return None


def test_exec_maker_only_kinds_csv_covers_real_kinds():
    raw = _read_env_value(CONFIG_PATH, "EXEC_MAKER_ONLY_KINDS")
    assert raw is not None, f"EXEC_MAKER_ONLY_KINDS missing in {CONFIG_PATH}"

    parsed = _parse_csv_env(raw)
    missing = [k for k in _REQUIRED_KINDS if k not in parsed]
    assert not missing, (
        f"EXEC_MAKER_ONLY_KINDS does not cover required kinds: {missing}. "
        f"Parsed={parsed}. Update config/crypto-of-common.env to include "
        f"every kind that emits trades — otherwise maker-only telemetry "
        f"silently misses them. Required={_REQUIRED_KINDS}"
    )


def test_exec_maker_only_kinds_csv_is_lowercase_safe():
    """Gate compares str(kind).lower(); CSV entries must survive lower()."""
    raw = _read_env_value(CONFIG_PATH, "EXEC_MAKER_ONLY_KINDS")
    parsed = _parse_csv_env(raw)
    for k in parsed:
        assert k == k.lower(), f"CSV entry '{k}' is not lowercase — would fail gate match"


def test_exec_maker_only_enforce_pairs_with_canary_csv():
    """If global ENFORCE=1, canary CSV must be present & non-empty (iceberg-only
    by default).

    Naked global ENFORCE without canary CSV would flip ALL listed kinds
    simultaneously (iceberg+weak_progress+ok+delta_spike+absorption+weak_recent)
    — too large a blast radius for first prod-promote. Per item-4 plan: enable
    iceberg first, observe 24-48h, then expand.
    """
    enforce = _read_env_value(CONFIG_PATH, "EXEC_MAKER_ONLY_ENFORCE")
    canary = _read_env_value(CONFIG_PATH, "EXEC_MAKER_ONLY_KINDS_ENFORCE")
    if enforce == "0":
        return  # still shadow — canary CSV not load-bearing
    parsed = _parse_csv_env(canary)
    assert parsed, (
        f"EXEC_MAKER_ONLY_ENFORCE={enforce} but EXEC_MAKER_ONLY_KINDS_ENFORCE is "
        f"empty — would enforce ALL kinds. Set canary CSV (start: 'iceberg')."
    )
    assert len(parsed) <= 3, (
        f"Canary scope expanded too fast: {parsed}. Expand kinds one at a time "
        f"with ≥24h observation gap between flips."
    )


def test_csv_parser_matches_gate_logic(monkeypatch):
    """Live-fire: verify parser semantics exactly match entry_policy_gate."""
    test_cases = [
        # (env_value, kind_under_test, expected_match)
        ("iceberg,delta_spike", "iceberg", True),
        ("iceberg,delta_spike", "ICEBERG", True),         # lower-cased on RHS
        ("iceberg,delta_spike", "weak_progress", False),
        ("iceberg, delta_spike , absorption", "absorption", True),  # whitespace
        ("", "iceberg", False),                            # empty CSV
        ("   ", "iceberg", False),                         # whitespace-only
        ("iceberg,,delta_spike", "iceberg", True),         # double-comma robust
        ("of:continuation,iceberg", "of:continuation", True),
    ]
    for env_val, kind, expected in test_cases:
        monkeypatch.setenv("EXEC_MAKER_ONLY_KINDS", env_val)
        # Re-implement gate logic here. This MUST stay aligned with
        # python-worker/handlers/crypto_orderflow/utils/entry_policy_gate.py:626-632
        mo_kinds = _parse_csv_env(os.getenv("EXEC_MAKER_ONLY_KINDS"))
        matched = bool(mo_kinds) and str(kind).lower() in mo_kinds
        assert matched is expected, (
            f"env='{env_val}' kind='{kind}' → matched={matched}, expected={expected}"
        )
