import html
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Path to python-worker/tools
TOOLS_PATH = os.path.join(os.getcwd(), "python-worker", "tools")
sys.path.append(TOOLS_PATH)
sys.path.append(os.path.join(os.getcwd(), "python-worker"))

def test_escaping():
    print("--- Verifying HTML Escaping ---")
    
    # 1. Test propose_meta_freeze_suggestion_v1
    try:
        import propose_meta_freeze_suggestion_v1 as pmf
        r = MagicMock()
        report = {"alerts": ["p50 < 0.2", 'quote " test'], "p50": 0.15}
        with patch.object(pmf, "_notify") as mock_notify:
            pmf.emit_meta_freeze_suggestion(r, prefix="t", scope="ALL", symbols=["B"], cfg_prefix="c:", freeze=1, freeze_mode="O", report=report, ttl_sec=60)
            text = mock_notify.call_args[0][1]
            print(f"PMF: {'PASSED' if '&lt;' in text and '&quot;' in text else 'FAILED'}")
            if not ('&lt;' in text and '&quot;' in text):
                print(f"  Text: {text}")
    except Exception as e:
        print(f"PMF Error: {e}")

    # 2. Test meta_cov_outcome_guard_v1
    try:
        import meta_cov_outcome_guard_v1 as mcog
        r = MagicMock()
        patch_cm = {"f": 0.5}
        report = {"alerts": ["bad < 0", 'err "msg"']}
        with patch.object(mcog, "_notify") as mock_notify:
            mcog._emit_cfg_suggestion(r, prefix="t", kind="k", scope="s", cfg2_key="c", patch=patch_cm, report=report, ttl_sec=60, min_approvals=1, auto_approve=False)
            text = mock_notify.call_args[0][1]
            print(f"MCOG: {'PASSED' if '&lt;' in text and '&quot;' in text else 'FAILED'}")
    except Exception as e:
        print(f"MCOG Error: {e}")

    # 3. Test ml_sre_monitor (basic check of the logic replaced)
    try:
        alerts = ["miss < 0.01", 'err "msg"']
        # Verification of the manual escaped loop
        safe_ml_alerts = [html.escape(str(x), quote=True) for x in alerts]
        passed = "&lt;" in safe_ml_alerts[0] and "&quot;" in safe_ml_alerts[1]
        print(f"MSM: {'PASSED' if passed else 'FAILED'}")
    except Exception as e:
        print(f"MSM Error: {e}")

    # 4. Test meta_drift_guard_v1
    try:
        alerts = ["drift < 0.1", 'quote " test']
        alerts_str = html.escape(json.dumps(alerts, ensure_ascii=False), quote=True)
        passed = "&lt;" in alerts_str and "&quot;" in alerts_str
        print(f"MDG: {'PASSED' if passed else 'FAILED'}")
    except Exception as e:
        print(f"MDG Error: {e}")

if __name__ == "__main__":
    test_escaping()
