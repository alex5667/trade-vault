import sys
import os
import json

from services.atr_change_control_service import submit_change
from services.atr_release_gate_service import build_scorecard, decide_release, record_release_decision
from services.analytics_db import get_conn

def setup_test():
    with get_conn() as conn, conn.cursor() as cur:
        # cleanup
        cur.execute("DELETE FROM atr_release_decisions WHERE change_id = 'test_chg_r1'")
        cur.execute("DELETE FROM atr_release_scorecards WHERE change_id = 'test_chg_r1'")
        cur.execute("DELETE FROM atr_change_transitions WHERE change_id = 'test_chg_r1'")
        cur.execute("DELETE FROM atr_change_requests WHERE change_id = 'test_chg_r1'")
        conn.commit()

        # submit change
        submit_change(
            change_id='test_chg_r1',
            change_type='policy_update',
            scope_kind='symbol',
            title='Test Release Gate',
            author='system',
            owner='system',
            risk_level='high',
            reason_code='TEST_01',
            request_data={},
            symbol='BTCUSDT'
        )

        try:
            # test build scorecard without any passing certs/replays -> Should deny because high risk
            sc = build_scorecard('test_chg_r1')
            print("Initial scorecard:", json.dumps(sc, indent=2))
            assert sc['decision'] == 'deny', "Should deny due to missing replay on high risk"
        except AssertionError as e:
            print(f"Assertion failed: {e}")

        # mark replay passed
        cur.execute("INSERT INTO atr_replay_manifests (replay_id, change_id, replay_kind, baseline_ref, candidate_ref, datasets_json, config_json, thresholds_json, status) VALUES ('rep_test_1', 'test_chg_r1', 'gate_replay', 'base', 'cand', '{}', '{}', '{}', 'passed')")
        conn.commit()

        try:
            # test scorecard -> Should still deny due to missing rollout cert on high risk
            sc2 = build_scorecard('test_chg_r1')
            print("With replay passed:", json.dumps(sc2, indent=2))
            assert sc2['decision'] == 'deny', "Should deny due to missing rollout cert on high risk"
        except AssertionError as e:
            print(f"Assertion failed: {e}")
        
        # mark rollout cert passed
        cur.execute("INSERT INTO atr_rollout_certifications (cert_id, change_id, rollout_stage, scope_kind, status, monitoring_window_from, monitoring_window_to, thresholds_json, checks_json, summary_json) VALUES ('cert_test_1', 'test_chg_r1', 'canary_25', 'symbol', 'passed', now(), now(), '{}', '{}', '{}')")
        conn.commit()
        
        try:
            # test scorecard -> Should allow 
            sc3 = build_scorecard('test_chg_r1')
            print("With rollout cert passed:", json.dumps(sc3, indent=2))
            assert sc3['decision'] == 'allow', "Should allow if replay and rollout cert passed and no incidents"
            print("All tests passed!")
        except AssertionError as e:
            print(f"Assertion failed: {e}")

if __name__ == '__main__':
    setup_test()
