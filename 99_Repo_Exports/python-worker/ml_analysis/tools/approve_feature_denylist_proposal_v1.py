from __future__ import annotations
"""Approve a denylist proposal after replay/AB evidence.

This tool is intentionally non-invasive:
  - It does NOT apply patches.
  - It only transitions proposal manifest status -> approved
    and stores a small approval record.

Hard gate (P106): approval is blocked unless:
  - manifest.status == 'ab_done'
  - manifest.ab.gate_pass == 1
  - AB report json gate_pass == 1

"""


import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

UTC = timezone.utc


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to denylist_proposal_*.manifest.json")
    ap.add_argument(
        "--ab-report-json",
        required=False,
        default="",
        help="Optional. If omitted, will use manifest['ab']['report_json'].",
    )
    ap.add_argument("--approve", type=int, default=0, help="1 = write approval record and set status=approved")

    args = ap.parse_args()

    mp = Path(args.manifest).expanduser().resolve()
    rp = None

    if not mp.exists():
        print(f"manifest not found: {mp}")
        return 2

    m = json.loads(mp.read_text(encoding="utf-8"))
    if not isinstance(m, dict):
        print("bad manifest format")
        return 2

    if int(args.approve) != 1:
        print("dry-run: pass --approve 1 to approve")
        print(f"current status: {m.get('status')}")
        return 0

    # Hard gate: approval is only allowed after AB/replay gate passed.
    st = (m.get("status") or "").strip()
    if st != "ab_done":
        print(f"ERROR: cannot approve: status must be 'ab_done' (got '{st}')")
        return 2

    ab = m.get("ab") or {}
    try:
        gate_pass = int(ab.get("gate_pass") or 0)
    except Exception:
        gate_pass = 0
    if gate_pass != 1:
        print("ERROR: cannot approve: AB gate did not pass (ab.gate_pass!=1)")
        return 2

    manifest_report = (ab.get("report_json") or "").strip()
    cli_report = (args.ab_report_json or "").strip()
    report_json = cli_report or manifest_report
    if not report_json:
        print("ERROR: missing AB report json (use --ab-report-json or set ab.report_json)")
        return 2
    if cli_report and manifest_report and Path(cli_report).resolve() != Path(manifest_report).resolve():
        print("ERROR: --ab-report-json mismatch with manifest ab.report_json")
        print(f"  cli: {cli_report}")
        print(f"  manifest: {manifest_report}")
        return 2

    rp = Path(report_json).expanduser().resolve()
    if not rp.exists():
        print(f"ab-report-json not found: {rp}")
        return 2
    rep = json.loads(rp.read_text(encoding="utf-8"))
    try:
        rep_gate_pass = int(rep.get("gate_pass") or 0)
    except Exception:
        rep_gate_pass = 0
    if rep_gate_pass != 1:
        print("ERROR: AB report gate_pass != 1")
        return 2

    m["status"] = "approved"
    m["approved_utc"] = _utc_now()
    m["ab_report_json"] = str(rp)
    m["approved_gate_pass"] = 1

    mp.write_text(json.dumps(m, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    approval = {
        "kind": "feature_denylist_approval",
        "approved_utc": _utc_now(),
        "proposal_hash": m.get("proposal_hash"),
        "manifest": str(mp),
        "ab_report_json": str(rp),
        "approved_gate_pass": 1,
    }
    apath = mp.with_suffix(".approval.json")
    apath.write_text(json.dumps(approval, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"approved: {mp}")
    print(f"approval record: {apath}")
    print("next:")
    for cmd in (m.get("apply_instructions") or []):
        print(f"  {cmd}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
