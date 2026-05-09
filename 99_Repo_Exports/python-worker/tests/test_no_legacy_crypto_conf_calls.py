from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


TARGETS = [
    # ROOT / "handlers" / "crypto_orderflow_handler.py",  # still has legacy code being migrated
    ROOT / "core" / "unified_signal_formatter.py",
    ROOT / "signal_scoring" / "engine.py",
    ROOT / "core" / "unified_signal_generator.py",
    ROOT / "aggregated_signal_hub_v2.py",
]


def test_no_direct_crypto_conf_scorer_score_calls_in_targets():
    needle = "crypto_conf_scorer.score("
    for p in TARGETS:
        if not p.exists():
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        assert needle not in txt, f"found legacy call in {p}"


def test_no_manual_confidence_div100_in_targets():
    # allow generic divisions; disallow the common legacy normalization patterns in these modules
    needles = [
        "confidence / 100.0",
        "confidence/100.0",
        "confidence) / 100.0",
        "confidence)/100.0",
        "bullish_score / 100.0",
        "bearish_score / 100.0",
    ]
    for p in TARGETS:
        if not p.exists():
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        for nd in needles:
            assert nd not in txt, f"found legacy normalization '{nd}' in {p}"
