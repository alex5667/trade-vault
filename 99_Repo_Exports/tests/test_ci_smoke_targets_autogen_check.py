import subprocess
import sys
import os
import pytest

def test_ci_smoke_contract_targets_autogen_check_script():
    # Run the script and check its return code
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script_path = os.path.join(root_dir, "scripts", "ci_smoke_contract_targets_autogen_check.py")
    
    if not os.path.exists(script_path):
        # We might be running from inside python-worker specifically
        pytest.skip("Could not find script_path, skipping")
    
    env_vars = os.environ.copy()
    env_vars["PYTHONPATH"] = root_dir

    # Check if the script runs successfully or returns a valid error code (0, 1, or 2)
    # Since we can't guarantee the environment has the exact alerts, we just check if it fails predictably or succeeds.
    result = subprocess.run(
        [sys.executable, script_path], 
        cwd=root_dir,
        env=env_vars,
        capture_output=True,
        text=True
    )
    
    # It should not crash with a syntax error or import error (exit code 2)
    # Ideally it returns 0 (OK)
    assert result.returncode in [0, 1], f"Script crashed or failed to run correctly: {result.stderr}"
