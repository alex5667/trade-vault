import subprocess

import sys

from pathlib import Path



def test_ofc_capture_tooling_smoke(tmp_path):

    root = Path(__file__).resolve().parents[1]  # python-worker/

    inp = root / "tests" / "data" / "ofc_capture_sample.ndjson"

    out = tmp_path / "filled.ndjson"

    replay_out = tmp_path / "replay.ndjson"


    # Fill expected outputs

    subprocess.run(

        [

            sys.executable,

            str(root / "tools" / "ofc_capture_fill_expected.py"),

            "--input",

            str(inp),

            "--output",

            str(out),

            "--sort",

            "bucket_id",

            "--gate-state",

            "import_before",

        ],

        cwd=str(root),

        check=True,

        capture_output=True,

        text=True,

    )


    # Validate schema (best-effort)

    subprocess.run(

        [sys.executable, str(root / "tools" / "ofc_validate_capture.py"), "--input", str(out), "--max-rows", "3"],

        cwd=str(root),

        check=False,  # validator can warn; should not fail the whole suite

        capture_output=True,

        text=True,

    )


    # Replay strict (must match filled expected)

    subprocess.run(

        [

            sys.executable,

            str(root / "tools" / "ofc_replay.py"),

            "--input",

            str(out),

            "--out",

            str(replay_out),

            "--sort",

            "bucket_id",

            "--gate-state",

            "import_before",

            "--strict",

        ],

        cwd=str(root),

        check=True,

        capture_output=True,

        text=True,

    )
