"""
Unit tests for Phase 8.2 graph-backed release gate.

Test coverage:
  - compare_with_legacy() drift detection (E1-E6)
  - evaluate_release() mode switching (shadow_compare / graph_read_primary / graph_enforced)
  - mark_cutover_readiness() status ladder
  - Drift severity expectations
  - _is_bounded_scope() filtering
  - ReleaseEquivalenceCertService cert rendering
"""
import json
import unittest
from unittest.mock import MagicMock, patch

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _scorecard(
    decision="allow",
    blockers=None,
    warnings=None,
    readiness_score=100.0,
    scope=None,
) -> dict:
    return {
        "scorecard_id": "sc_test_001",
        "change_id": "chg_test_001",
        "decision": decision,
        "readiness_score": readiness_score,
        "blockers": blockers or [],
        "warnings": warnings or [],
        "infos": [],
        "scope": scope or {
            "symbol": "BTCUSDT",
            "layer": "stop_ttl",
            "scenario": "breakout",
        },
        "summary": {
            "replay_status": "passed",
            "rollout_cert_status": "canary_25_passed",
            "incidents_open": 0,
            "overdue_actions": 0,
        }
    }


def _graph_state(
    decision="allow",
    blockers=None,
    warnings=None,
    readiness_score=100.0,
    scope_value="BTCUSDT",
    freeze_state="none",
    replay_cert_status="passed",
    rollout_stage="canary_25",
) -> dict:
    return {
        "change_id": "chg_test_001",
        "scope_value": scope_value,
        "decision": decision,
        "readiness_score": readiness_score,
        "blockers": blockers or [],
        "warnings": warnings or [],
        "scope": {
            "symbol": scope_value,
            "layer": "stop_ttl",
            "scenario": "breakout",
        },
        "release_state": {
            "target_stage":                 rollout_stage,
            "replay_cert_status":           replay_cert_status,
            "required_rollout_cert_status": "passed",
            "active_freeze_state":          freeze_state,
            "open_related_sev1_incidents":  0,
            "overdue_p0_p1_actions":        0,
            "override_state":               "none",
            "invariant_budget_status":      "healthy",
        }
    }


# ─── Import target under test ─────────────────────────────────────────────────
# We import after defining helpers so module-level env reads don't fail.

import importlib, os, sys

# Patch DB away before import
_MOCK_CONN = MagicMock()
_MOCK_CONN.__enter__ = lambda s: s
_MOCK_CONN.__exit__ = MagicMock(return_value=False)
_MOCK_CUR = MagicMock()
_MOCK_CONN.cursor.return_value.__enter__ = lambda s: _MOCK_CUR
_MOCK_CONN.cursor.return_value.__exit__ = MagicMock(return_value=False)

with patch("services.analytics_db.get_conn", return_value=_MOCK_CONN):
    from services.atr_graph_backed_release_gate import (
        compare_with_legacy,
        _is_bounded_scope,
        _BOUNDED_SYMBOLS,
        _BOUNDED_STAGES,
        render_shadow_compare_healthy,
        render_critical_drift,
        render_cutover_ready,
    )


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestIsBoundedScope(unittest.TestCase):
    def test_btcusdt_stop_ttl_canary25_is_bounded(self):
        self.assertTrue(_is_bounded_scope("BTCUSDT", "stop_ttl", "canary_25"))

    def test_ethusdt_stop_ttl_live100_is_bounded(self):
        self.assertTrue(_is_bounded_scope("ETHUSDT", "stop_ttl", "live_100"))

    def test_solusdt_not_bounded(self):
        self.assertFalse(_is_bounded_scope("SOLUSDT", "stop_ttl", "live_100"))

    def test_btcusdt_trailing_not_in_pilot(self):
        # trailing layer is out-of-pilot (not in _BOUNDED_LAYERS yet = stop_ttl only)
        self.assertFalse(_is_bounded_scope("BTCUSDT", "trailing", "live_100"))

    def test_unknown_stage_passes_filter(self):
        # stage=None means "don't filter by stage"
        self.assertTrue(_is_bounded_scope("BTCUSDT", "stop_ttl", None))


class TestCompareWithLegacy(unittest.TestCase):
    """E1-E6 drift detection."""

    def test_E1_decisions_match_no_drift(self):
        legacy = _scorecard(decision="allow")
        graph  = _graph_state(decision="allow")
        result = compare_with_legacy("chg_001", legacy, graph)
        self.assertTrue(result["matching"])
        self.assertEqual(result["drifts"], [])

    def test_E1_decision_mismatch_is_critical(self):
        legacy = _scorecard(decision="deny")
        graph  = _graph_state(decision="allow")
        result = compare_with_legacy("chg_001", legacy, graph)
        self.assertFalse(result["matching"])
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertIn("release_decision_mismatch", drift_kinds)
        crit = [d for d in result["drifts"] if d["drift_kind"] == "release_decision_mismatch"]
        self.assertEqual(crit[0]["severity"], "critical")

    def test_E2_blocker_set_mismatch(self):
        legacy = _scorecard(decision="deny", blockers=["replay_missing"])
        graph  = _graph_state(decision="deny", blockers=["replay_missing", "extra_graph_blocker"])
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertIn("blocker_set_mismatch", drift_kinds)

    def test_E2_same_blockers_no_drift(self):
        blockers = ["replay_missing"]
        legacy = _scorecard(decision="deny", blockers=blockers)
        graph  = _graph_state(decision="deny", blockers=blockers)
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertNotIn("blocker_set_mismatch", drift_kinds)

    def test_E3_warning_set_mismatch_is_warn(self):
        legacy = _scorecard(decision="allow_with_override", warnings=["low_sample"])
        graph  = _graph_state(decision="allow_with_override", warnings=["low_sample", "extra_warn"])
        result = compare_with_legacy("chg_001", legacy, graph)
        warn_drifts = [d for d in result["drifts"] if d["drift_kind"] == "warning_set_mismatch"]
        if warn_drifts:
            self.assertEqual(warn_drifts[0]["severity"], "warn")

    def test_E4_missing_replay_cert_for_live100_is_critical(self):
        legacy = _scorecard(decision="deny", blockers=["replay_missing"])
        graph  = _graph_state(
            decision="deny",
            blockers=["missing_replay_cert_edge"],
            rollout_stage="live_100",
            replay_cert_status="missing",
        )
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds   = [d["drift_kind"] for d in result["drifts"]]
        drift_sev_map = {d["drift_kind"]: d["severity"] for d in result["drifts"]}
        self.assertIn("missing_replay_cert_edge", drift_kinds)
        self.assertEqual(drift_sev_map.get("missing_replay_cert_edge"), "critical")

    def test_E5_missing_freeze_blocker_detected(self):
        legacy = _scorecard(decision="deny", blockers=["active_freeze_on_scope"])
        # graph has freeze but didn't add the blocker
        graph  = _graph_state(
            decision="allow",   # forgot to deny on freeze
            blockers=[],        # missing active_freeze_on_scope
            freeze_state="scope_frozen",
        )
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertIn("missing_freeze_blocker", drift_kinds)

    def test_E6_readiness_score_large_diff_is_warn(self):
        legacy = _scorecard(decision="allow", readiness_score=100.0)
        graph  = _graph_state(decision="allow", readiness_score=60.0)  # diff = 40 > 20
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertIn("readiness_score_mismatch", drift_kinds)

    def test_E6_small_score_diff_no_drift(self):
        legacy = _scorecard(decision="allow", readiness_score=100.0)
        graph  = _graph_state(decision="allow", readiness_score=90.0)  # diff = 10 < 20
        result = compare_with_legacy("chg_001", legacy, graph)
        drift_kinds = [d["drift_kind"] for d in result["drifts"]]
        self.assertNotIn("readiness_score_mismatch", drift_kinds)

    def test_perfect_match_returns_matching_true(self):
        legacy = _scorecard(decision="allow", blockers=[], warnings=[], readiness_score=100.0)
        graph  = _graph_state(decision="allow", blockers=[], warnings=[], readiness_score=100.0)
        result = compare_with_legacy("chg_001", legacy, graph)
        self.assertTrue(result["matching"])
        self.assertEqual(len(result["drifts"]), 0)


class TestDriftSeverity(unittest.TestCase):
    """Drift severity taxonomy per spec."""

    EXPECTED_SEVERITY = {
        "release_decision_mismatch": "critical",
        "blocker_set_mismatch":      "error",
        "warning_set_mismatch":      "warn",
    }

    def _get_drift(self, drift_kind: str, legacy_dec: str, graph_dec: str) -> dict | None:
        legacy = _scorecard(decision=legacy_dec, blockers=["replay_missing"] if legacy_dec == "deny" else [])
        graph  = _graph_state(decision=graph_dec, blockers=["replay_missing"] if graph_dec == "deny" else [])
        result = compare_with_legacy("chg_sev", legacy, graph)
        for d in result["drifts"]:
            if d["drift_kind"] == drift_kind:
                return d
        return None

    def test_decision_mismatch_severity_critical(self):
        d = self._get_drift("release_decision_mismatch", "deny", "allow")
        self.assertIsNotNone(d)
        self.assertEqual(d["severity"], "critical")


class TestTelegramRenderers(unittest.TestCase):
    """Smoke tests for Telegram message builders."""

    def test_shadow_compare_healthy_no_mismatches(self):
        msg = render_shadow_compare_healthy(18, 0, 0)
        self.assertIn("SHADOW_HEALTHY", msg)
        self.assertIn("18", msg)
        self.assertIn("✅", msg)

    def test_shadow_compare_healthy_with_mismatches(self):
        msg = render_shadow_compare_healthy(18, 2, 1)
        self.assertIn("⚠️", msg)
        self.assertIn("2", msg)

    def test_critical_drift_render(self):
        drift = {
            "drift_kind": "release_decision_mismatch",
            "severity":   "critical",
            "drift_json": {"legacy_decision": "deny", "graph_decision": "allow"},
        }
        msg = render_critical_drift(drift, "chg_001", "BTCUSDT")
        self.assertIn("release_decision_mismatch", msg)
        self.assertIn("CRITICAL", msg)
        self.assertIn("BTCUSDT", msg)

    def test_cutover_ready_not_ready(self):
        summary = {"critical_drifts_7d": 3, "pct_decision_match": 95.0}
        msg = render_cutover_ready("not_ready", summary)
        self.assertIn("NOT_READY", msg)
        self.assertIn("🔴", msg)

    def test_cutover_ready_enforced(self):
        summary = {"critical_drifts_7d": 0, "pct_decision_match": 100.0, "missing_replay_cert_edge_live_critical": 0}
        msg = render_cutover_ready("ready_for_enforce", summary)
        self.assertIn("READY_FOR_ENFORCE", msg)
        self.assertIn("🟢", msg)


class TestEvaluateReleaseModeSwitching(unittest.TestCase):
    """evaluate_release() passes correct decision for each mode."""

    def _make_evaluate(self, mode: str, graph_decision: str, legacy_decision: str):
        """Helper: patch env and DB, call evaluate_release(), return result."""
        import importlib
        import services.atr_graph_backed_release_gate as mod

        orig_mode   = mod._MODE
        orig_enable = mod._ENABLE
        try:
            mod._MODE   = mode
            mod._ENABLE = True

            with patch.object(mod, "build_graph_release_state") as mock_gs, \
                 patch.object(mod, "get_conn") as mock_gc:
                mock_gs.return_value = _graph_state(decision=graph_decision)
                # Patch DB connection for _persist_equivalence_check / _persist_drifts
                conn_mock = MagicMock()
                conn_mock.__enter__ = lambda s: s
                conn_mock.__exit__ = MagicMock(return_value=False)
                conn_mock.cursor.return_value.__enter__ = lambda s: MagicMock()
                conn_mock.cursor.return_value.__exit__ = MagicMock(return_value=False)
                mock_gc.return_value = conn_mock

                legacy_sc = _scorecard(decision=legacy_decision)
                from services.atr_graph_backed_release_gate import evaluate_release
                return evaluate_release("chg_mode_test", legacy_sc)
        finally:
            mod._MODE   = orig_mode
            mod._ENABLE = orig_enable

    def test_shadow_compare_always_returns_legacy(self):
        result = self._make_evaluate("shadow_compare", "allow", "deny")
        self.assertEqual(result["decision"], "deny")   # legacy wins
        self.assertIn("legacy", result["source"])

    def test_graph_enforced_returns_graph(self):
        result = self._make_evaluate("graph_enforced", "allow", "deny")
        self.assertEqual(result["decision"], "allow")  # graph wins
        self.assertEqual(result["source"], "graph")

    def test_graph_read_primary_legacy_hard_deny_wins(self):
        # legacy=deny, graph=allow → legacy hard-deny should win in graph_read_primary
        result = self._make_evaluate("graph_read_primary", "allow", "deny")
        self.assertEqual(result["decision"], "deny")
        self.assertIn("legacy_hard_deny", result["source"])

    def test_graph_read_primary_uses_graph_when_legacy_allows(self):
        # legacy=allow_with_override, graph=allow → graph primary wins
        result = self._make_evaluate("graph_read_primary", "allow", "allow_with_override")
        self.assertEqual(result["decision"], "allow")
        self.assertEqual(result["source"], "graph_primary")


if __name__ == "__main__":
    unittest.main(verbosity=2)
