from pathlib import Path


def _read(rel: str) -> str:
    # Repo root: orderflow_services/tests/ -> parents[2]
    root = Path(__file__).resolve().parents[2]
    return (root / rel).read_text(encoding="utf-8")


def _assert_present_before_build(txt: str, *, rel: str) -> None:
    # Contract: both keys exist in indicators BEFORE the first of_engine.build call.
    # Cheap guardrail against refactors that move/forget the surfacing.
    p_reason = txt.find('"book_seq_last_reason"')
    p_ema = txt.find('"book_missing_seq_ema"')
    p_build = txt.find("of_engine.build")

    assert p_ema != -1, f"{rel}: missing book_missing_seq_ema surfacing"
    assert p_reason != -1, f"{rel}: missing book_seq_last_reason surfacing"
    assert p_build != -1, f"{rel}: cannot locate of_engine.build for ordering check"
    assert p_reason < p_build, f"{rel}: book_seq_last_reason must be set before of_engine.build"
    assert p_ema < p_build, f"{rel}: book_missing_seq_ema must be set before of_engine.build"


def test_a2_surface_book_seq_indicators_sot_and_mirror_v1() -> None:
    for rel in (
        "python-worker/services/orderflow/components/tick_processor.py",
        "reference/services/orderflow/components/tick_processor.py",
    ):
        txt = _read(rel)
        _assert_present_before_build(txt, rel=rel)
