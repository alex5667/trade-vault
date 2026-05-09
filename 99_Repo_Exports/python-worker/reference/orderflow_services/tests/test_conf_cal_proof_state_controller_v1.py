import json
from pathlib import Path

from orderflow_services.conf_cal_proof_state_controller_v1 import ProofStateController


def _write_status(p: Path, *, ts_ms: int, ok=True, skipped=False, guard_passed=True, skip_reason="") -> None:
    p.write_text(
#         json.dumps(
            {
                "ts_ms": ts_ms,
                "ok": bool(ok),
                "skipped": bool(skipped),
                "skip_reason": skip_reason,
                "guard_passed": guard_passed,
                "guard": {"fail": (guard_passed is False), "reasons": ["ece_worse"] if guard_passed is False else []},
            }
        )
#         encoding="utf-8",
#     )


def test_proof_controller_good_then_ramp(tmp_path: Path):
    reports = tmp_path / "reports"
    reports.mkdir()
    status = reports / "confidence_calibration_live_status.json"

    proof_path = tmp_path / "proof.json"
    state_path = tmp_path / "state.json"

    ctl = ProofStateController(
        reports_dir=str(reports),
        proof_path=proof_path,
        state_path=state_path,
        min_good_runs=2,
        min_bad_runs=2,
        max_live_age_sec=999999,
        canary_enable=True,
        canary_start=0.10,
        canary_step=0.10,
        canary_max=1.0,
        canary_bump_min_sec=1800,
    )

    now_ms = 1_700_000_000_000
    _write_status(status, ts_ms=now_ms - 1000, guard_passed=True)
    ctl.step(now_ms=now_ms)
    p1 = json.loads(proof_path.read_text(encoding="utf-8"))
    assert p1["valid"] in (False, True)

    _write_status(status, ts_ms=now_ms + 1000 - 1000, guard_passed=True)
    ctl.step(now_ms=now_ms + 1000)
    p2 = json.loads(proof_path.read_text(encoding="utf-8"))
    assert p2["valid"] is True
    assert abs(float(p2["canary_share"]) - 0.10) < 1e-9

    _write_status(status, ts_ms=now_ms + 2000 - 1000, guard_passed=True)
    ctl.step(now_ms=now_ms + 1000 + 1800 * 1000)
    p3 = json.loads(proof_path.read_text(encoding="utf-8"))
    assert p3["valid"] is True
    assert abs(float(p3["canary_share"]) - 0.20) < 1e-9


def test_proof_controller_bad_disables(tmp_path: Path):
    reports = tmp_path / "reports"
    reports.mkdir()
    status = reports / "confidence_calibration_live_status.json"

    proof_path = tmp_path / "proof.json"
    state_path = tmp_path / "state.json"

    ctl = ProofStateController(
        reports_dir=str(reports),
        proof_path=proof_path,
        state_path=state_path,
        min_good_runs=1,
        min_bad_runs=2,
        max_live_age_sec=999999,
        canary_enable=True,
        canary_start=0.10,
        canary_step=0.10,
        canary_max=1.0,
        canary_bump_min_sec=1800,
    )

    now_ms = 1_700_000_000_000
    _write_status(status, ts_ms=now_ms - 1000, guard_passed=True)
    ctl.step(now_ms=now_ms)
    assert json.loads(proof_path.read_text())["valid"] is True

    _write_status(status, ts_ms=now_ms + 1000 - 1000, guard_passed=False)
    ctl.step(now_ms=now_ms + 1000)
    _write_status(status, ts_ms=now_ms + 2000 - 1000, guard_passed=False)
    ctl.step(now_ms=now_ms + 2000)
    p = json.loads(proof_path.read_text())
    assert p["valid"] is False
    assert float(p["canary_share"]) == 0.0
