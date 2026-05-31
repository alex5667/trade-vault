
import os
from unittest.mock import MagicMock, patch

from services.orderflow.decision_binding_v1 import bind_rule_ml_v1
from services.orderflow.decision_record_v1 import _drift_state_from_indicators, build_decision_record_v1


class TestP51DriftAlignment:

    def test_binding_drift_block(self):
        # Case 1: Drift BLOCK -> deny
        res = bind_rule_ml_v1(
            rule_ok=1, rule_ok_soft=0,
            ml_state="allow", dq_state="ok",
            drift_state="block"
        )
        assert res["action"] == "deny"
        assert res["reason_code"] == "DRIFT_BLOCK"
        assert res["source"] == "drift"

    def test_binding_drift_warn(self):
        # Case 2: Drift WARN -> allow but with suffix
        res = bind_rule_ml_v1(
            rule_ok=1, rule_ok_soft=0,
            ml_state="allow", dq_state="ok",
            drift_state="warn"
        )
        assert res["action"] == "allow"
        assert "DRIFT_WARN" in res["reason_code"]
        assert res["source"] == "both"

    def test_binding_drift_block_override(self):
        # Case 3: Drift BLOCK but override enabled
        with patch.dict(os.environ, {"BIND_ALLOW_RULE_STRONG_ON_BAD_DRIFT": "1"}):
             res = bind_rule_ml_v1(
                rule_ok=1, rule_ok_soft=0,
                ml_state="allow", dq_state="ok",
                drift_state="block"
            )
             assert res["action"] == "allow"
             assert "OVERRIDE" in res["reason_code"]
             assert res["source"] == "rule"

    def test_drift_state_extraction(self):
        # From struct
        ind = {"drift": {"drift_state_24h": "2"}}
        assert _drift_state_from_indicators(ind) == "block"

        ind = {"drift": {"drift_state_24h": "1"}}
        assert _drift_state_from_indicators(ind) == "warn"

        ind = {"drift": {"drift_state_24h": "0"}}
        assert _drift_state_from_indicators(ind) == "ok"

        # From fallback
        ind = {"drift_state": "warn"}
        assert _drift_state_from_indicators(ind) == "warn"

    def test_decision_record_structure(self):
        runtime = MagicMock()
        runtime.symbol = "BTCUSDT"

        signal = {
            "sid": "test_sig_1",
            "ts_ms": 1700000000000,
            "direction": "LONG",
            "indicators": {
                "drift": {
                    "drift_state_24h": "2",
                    "psi_max_24h": 0.5,
                    "feature_drift_max_z_24h": 7.5
                },
                "of_confirm": {"ok": 1}
            }
        }

        rec = build_decision_record_v1(
            runtime=runtime,
            signal=signal,
            stage="test",
            final_actual="veto",
            veto_reason="DRIFT_BLOCK"
        )

        assert rec["drift_state"] == "block"
        assert rec["drift_psi_max_24h"] == 0.5
        assert rec["drift_z_max_24h"] == 7.5
        assert rec["binding_recommended_action"] == "deny"
        assert rec["binding_recommended_reason_code"] == "DRIFT_BLOCK"
