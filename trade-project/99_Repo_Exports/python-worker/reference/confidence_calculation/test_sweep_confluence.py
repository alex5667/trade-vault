from types import SimpleNamespace

from signal_confidence import ConfidenceScorer


def _ctx(**kw):
    # Minimal ctx for the generic delta_spike path
    base = dict(
        delta_z=3.2,
        obi_avg=0.0,
        obi_sustained=False,
        confirmations=[],
        # optional knobs
        sweep_legacy_fallback=0,
        sweep_legacy_score=0.4,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def main():
    scorer = ConfidenceScorer()

    c0, _ = scorer.score(kind="delta_spike", side="LONG", ctx=_ctx(confirmations=[]))
    c1, _ = scorer.score(kind="delta_spike", side="LONG", ctx=_ctx(confirmations=["sweep_eqh=1"]))

    assert c1 > c0, f"Expected c1 > c0, got c1={c1}, c0={c0}"

    c2, _ = scorer.score(
        kind="delta_spike",
        side="LONG",
        ctx=_ctx(confirmations=["sweep=1"], sweep_legacy_fallback=1, sweep_legacy_score=0.4),
    )

    assert c2 > c0, f"Expected c2 > c0, got c2={c2}, c0={c0}"

    print("OK", {"no_sweep": c0, "sweep_eqh": c1, "sweep_legacy": c2})


if __name__ == "__main__":
    main()
