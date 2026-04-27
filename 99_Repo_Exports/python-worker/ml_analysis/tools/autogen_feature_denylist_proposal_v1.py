"""Autogen actionable output for feature-selection loop.

Creates a candidate unified diff that moves noisy v5 extras into denylist,
so you can train on v5_of_stable (v5_of minus denylist) without touching
online schema immediately.

Safety properties:
  - Does NOT apply anything.
  - Writes proposal manifest with status=pending_ab.
  - Has dedup window (N days) to avoid spamming same proposal.

Expected input:
  - stability_table.csv produced by your feature-selection loop.

Output:
  - proposals/denylist_proposal_YYYYMMDD_<hash8>.diff
  - proposals/denylist_proposal_YYYYMMDD_<hash8>.manifest.json

"""

from __future__ import annotations

import argparse
import csv
import difflib
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


import sys

# Ensure `core.*` imports work (repo uses tick_flow_full as PYTHONPATH root).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_TICK_FLOW_FULL = _REPO_ROOT / "tick_flow_full"
if _TICK_FLOW_FULL.exists() and str(_TICK_FLOW_FULL) not in sys.path:
    sys.path.insert(0, str(_TICK_FLOW_FULL))


UTC = timezone.utc


@dataclass(frozen=True)
class Candidate:
    key: str
    kind: str  # 'num' | 'bool'
    score: float
    reason: str


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _sha1_8(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def _read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def _try_float(x: object, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return default
        return float(s)
    except Exception:
        return default


def _parse_feature_name(raw: str) -> Tuple[str, Optional[str]]:
    """Return (key, kind_hint) where kind_hint in {'num','bool',None}."""
    s = (raw or "").strip()
    if s.startswith("n:"):
        return s[2:], "num"
    if s.startswith("b:"):
        return s[2:], "bool"
    if s.startswith("f_"):
        return s[2:], "num"
    # column safe name from FeatureRegistry: n_xxx / b_xxx
    if s.startswith("n_"):
        return s[2:], "num"
    if s.startswith("b_"):
        return s[2:], "bool"
    return s, None


def _load_registry_keys() -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
    """Return (v4_num, v4_bool, v5_num, v5_bool) sets of raw keys (no prefixes)."""
    try:
        from core.feature_registry import get_schema_info

        v4 = get_schema_info("v4_of").feature_names
        v5 = get_schema_info("v5_of").feature_names

        def split(names: Sequence[str]) -> Tuple[Set[str], Set[str]]:
            n: Set[str] = set()
            b: Set[str] = set()
            for nm in names:
                if nm.startswith("n:"):
                    n.add(nm[2:])
                elif nm.startswith("b:"):
                    b.add(nm[2:])
            return n, b

        v4n, v4b = split(v4)
        v5n, v5b = split(v5)
        return v4n, v4b, v5n, v5b
    except Exception:
        # best-effort fallback
        return set(), set(), set(), set()


def _load_denylist_json(path: Path) -> Dict:
    if not path.exists():
        return {
            "ver": "v1",
            "updated_utc": "",
            "deny_num": [],
            "deny_bool": [],
            "notes": "",
        }
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {
        "ver": "v1",
        "updated_utc": "",
        "deny_num": [],
        "deny_bool": [],
        "notes": "",
    }


def _unified_diff(a_text: str, b_text: str, a_path: str, b_path: str) -> str:
    a_lines = a_text.splitlines(True)
    b_lines = b_text.splitlines(True)
    diff = difflib.unified_diff(
        a_lines,
        b_lines,
        fromfile=f"a/{a_path}",
        tofile=f"b/{b_path}",
        lineterm="",
    )
    return "\n".join(diff) + "\n"


def _find_stability_table(fs_run_dir: Path) -> Optional[Path]:
    if fs_run_dir.is_file() and fs_run_dir.name.endswith(".csv"):
        return fs_run_dir
    p = fs_run_dir / "stability_table.csv"
    if p.exists():
        return p
    # common alt names
    for name in ("stability.csv", "stability_table_v1.csv"):
        q = fs_run_dir / name
        if q.exists():
            return q
    return None


def _list_recent_manifests(proposals_dir: Path, since: datetime) -> List[Path]:
    out: List[Path] = []
    if not proposals_dir.exists():
        return out
    for p in sorted(proposals_dir.glob("denylist_proposal_*.manifest.json")):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
            if mtime >= since:
                out.append(p)
        except Exception:
            continue
    return out


def _load_drift_hints(path: Optional[Path]) -> Dict[Tuple[str, str], Dict[str, object]]:
    """Load feature drift hints keyed by (kind, key).

    The report is advisory only: it never auto-applies a denylist, it only boosts
    the candidate score/reason for already-noisy or strongly drifted features.
    """
    if path is None or not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = obj.get("features") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return {}
    out: Dict[Tuple[str, str], Dict[str, object]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        feat = str(r.get("feature") or "").strip()
        if not feat:
            continue
        key, kind_hint = _parse_feature_name(feat)
        kind = kind_hint or ("bool" if str(feat).startswith(("b:", "b_")) else "num")
        out[(kind, key)] = {
            "flag_crit": int(bool(r.get("flag_crit"))),
            "flag_warn": int(bool(r.get("flag_warn"))),
            "denylist_suggested": int(bool(r.get("denylist_suggested"))),
            "shadow_disable_suggested": int(bool(r.get("shadow_disable_suggested"))),
            "psi": _try_float(r.get("psi"), 0.0),
            "ks_stat": _try_float(r.get("ks_stat"), 0.0),
        }
    return out


def _select_candidates(
    rows: List[Dict[str, str]],
    extras_num: Set[str],
    extras_bool: Set[str],
    v5_num: Set[str],
    v5_bool: Set[str],
    max_features: int,
    min_importance: float,
    max_cv: float,
    drift_hints: Optional[Dict[Tuple[str, str], Dict[str, object]]] = None,
) -> List[Candidate]:
    cands: List[Candidate] = []

    for r in rows:
        feat = (r.get("feature") or r.get("name") or r.get("col") or "").strip()
        if not feat:
            continue

        key, kind_hint = _parse_feature_name(feat)

        # infer kind by registry if possible
        kind = kind_hint
        if kind is None:
            if key in v5_bool:
                kind = "bool"
            else:
                kind = "num"

        # only consider v5 extras (protect core)
        if kind == "num" and key not in extras_num:
            continue
        if kind == "bool" and key not in extras_bool:
            continue

        # Noise criteria
        flag_noise = (r.get("flag_noise") or r.get("is_noise") or "").strip().lower() in ("1", "true", "yes")
        imp = _try_float(r.get("global_perm_auc_drop") or r.get("perm_importance") or r.get("importance") or 0.0)
        cv_reg = _try_float(r.get("regime_cv") or r.get("cv_regime") or 0.0)
        cv_hour = _try_float(r.get("hour_cv") or r.get("cv_hour") or 0.0)
        cv = max(cv_reg, cv_hour)

        hint = (drift_hints or {}).get((kind, key), {})
        drift_crit = int(bool(hint.get("flag_crit")))
        drift_warn = int(bool(hint.get("flag_warn")))
        drift_deny = int(bool(hint.get("denylist_suggested")))
        drift_shadow = int(bool(hint.get("shadow_disable_suggested")))

        # Conservative defaults: require either explicit flag, (low importance + high instability),
        # or strong P3 drift hints for the same extra feature.
        is_noise = bool(flag_noise) or (imp <= min_importance and cv >= max_cv) or bool(drift_deny)
        if not is_noise:
            continue

        # score: prioritize instability, then tiny importance, then strong drift hints.
        score = (cv * 10.0) + (min_importance - imp)
        if drift_warn:
            score += 1.0
        if drift_shadow:
            score += 2.0
        if drift_crit:
            score += 4.0
        if drift_deny:
            score += 5.0
        reason = "flag_noise" if flag_noise else f"imp<= {min_importance:.4g} & cv>= {max_cv:.3g}"
        if drift_deny:
            reason += "; drift_denylist_suggested=1"
        elif drift_crit:
            reason += "; drift_crit=1"

        cands.append(Candidate(key=key, kind=kind, score=float(score), reason=reason))

    # sort desc by score
    cands.sort(key=lambda x: x.score, reverse=True)

    # dedup by key
    seen: Set[Tuple[str, str]] = set()
    out: List[Candidate] = []
    for c in cands:
        k = (c.kind, c.key)
        if k in seen:
            continue
        out.append(c)
        seen.add(k)
        if len(out) >= max_features:
            break

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--fs-run-dir",
        default=os.environ.get("FEATURE_SELECTION_RUN_DIR", ""),
        help="Directory containing stability_table.csv from feature selection loop.",
    )
    ap.add_argument(
        "--denylist-path",
        default=os.environ.get(
            "ML_FEATURE_DENYLIST_PATH",
            str(Path("tick_flow_full/core/feature_denylist_v1.json")),
        ),
        help="Path to feature_denylist_v1.json in the repo.",
    )
    ap.add_argument(
        "--proposals-dir",
        default=os.environ.get("FEATURE_DENYLIST_PROPOSALS_DIR", ""),
        help="Output dir for proposals (default: <fs-run-dir>/proposals).",
    )
    ap.add_argument("--dedup-days", type=int, default=int(os.environ.get("FEATURE_DENYLIST_DEDUP_DAYS", "7")))
    ap.add_argument("--max-features", type=int, default=int(os.environ.get("FEATURE_DENYLIST_MAX", "12")))
    ap.add_argument("--min-importance", type=float, default=float(os.environ.get("FEATURE_DENYLIST_MIN_IMPORTANCE", "0.0001")))
    ap.add_argument("--max-cv", type=float, default=float(os.environ.get("FEATURE_DENYLIST_MIN_CV", "0.25")))
    ap.add_argument("--drift_report_json", default=os.environ.get("FEATURE_DRIFT_REPORT_JSON", ""), help="Optional P3 feature drift report JSON; boosts denylist candidate ranking.")

    args = ap.parse_args()

    fs_run_dir = Path(args.fs_run_dir).expanduser().resolve() if args.fs_run_dir else Path("")
    if not fs_run_dir:
        print("[denylist] no --fs-run-dir; nothing to do")
        return 0

    st_path = _find_stability_table(fs_run_dir)
    if not st_path:
        print(f"[denylist] no stability_table.csv in {fs_run_dir}")
        return 0

    denylist_path = Path(args.denylist_path).expanduser().resolve()

    proposals_dir = Path(args.proposals_dir).expanduser().resolve() if args.proposals_dir else (fs_run_dir / "proposals")
    proposals_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_csv_dicts(st_path)

    v4n, v4b, v5n, v5b = _load_registry_keys()
    extras_num = (v5n - v4n) if v5n else set()
    extras_bool = (v5b - v4b) if v5b else set()

    # if registry unavailable, fallback: allow all features
    if not v5n and not v5b:
        # try infer kind by prefix
        extras_num = set()
        extras_bool = set()
        for r in rows:
            feat = (r.get("feature") or r.get("name") or "").strip()
            if feat.startswith("n:") or feat.startswith("f_") or feat.startswith("n_"):
                extras_num.add(_parse_feature_name(feat)[0])
            elif feat.startswith("b:") or feat.startswith("b_"):
                extras_bool.add(_parse_feature_name(feat)[0])

    drift_hints = _load_drift_hints(Path(args.drift_report_json)) if str(args.drift_report_json or "").strip() else {}
    cands = _select_candidates(
        rows=rows,
        extras_num=extras_num,
        extras_bool=extras_bool,
        v5_num=v5n,
        v5_bool=v5b,
        max_features=int(args.max_features),
        min_importance=float(args.min_importance),
        max_cv=float(args.max_cv),
        drift_hints=drift_hints,
    )

    if not cands:
        print("[denylist] no candidates")
        return 0

    current = _load_denylist_json(denylist_path)
    deny_num: Set[str] = set(current.get("deny_num") or [])
    deny_bool: Set[str] = set(current.get("deny_bool") or [])

    added_num: List[str] = []
    added_bool: List[str] = []

    for c in cands:
        if c.kind == "bool":
            if c.key not in deny_bool:
                deny_bool.add(c.key)
                added_bool.append(c.key)
        else:
            if c.key not in deny_num:
                deny_num.add(c.key)
                added_num.append(c.key)

    if not added_num and not added_bool:
        print("[denylist] candidates already denylisted")
        return 0

    # proposal hash (stable)
    proposal_payload = json.dumps(
        {
            "deny_num": sorted(deny_num),
            "deny_bool": sorted(deny_bool),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    ph = _sha1_8(proposal_payload)

    # dedup by manifests in last N days
    since = _utc_now() - timedelta(days=int(args.dedup_days))
    for mp in _list_recent_manifests(proposals_dir, since=since):
        try:
            m = json.loads(mp.read_text(encoding="utf-8"))
            if isinstance(m, dict) and str(m.get("proposal_hash") or "") == ph:
                print(f"[denylist] dedup hit: {ph} already proposed recently ({mp.name})")
                return 0
        except Exception:
            continue

    # build new JSON
    updated = dict(current)
    updated["updated_utc"] = _utc_now().isoformat()
    updated["deny_num"] = sorted(deny_num)
    updated["deny_bool"] = sorted(deny_bool)

    a_text = denylist_path.read_text(encoding="utf-8") if denylist_path.exists() else json.dumps(current, indent=2, ensure_ascii=False) + "\n"
    b_text = json.dumps(updated, indent=2, ensure_ascii=False) + "\n"

    # relative paths in diff
    try:
        repo_root = Path.cwd().resolve()
        a_rel = str(denylist_path.relative_to(repo_root))
    except Exception:
        a_rel = str(denylist_path)

    diff_txt = _unified_diff(a_text=a_text, b_text=b_text, a_path=a_rel, b_path=a_rel)

    date_s = _utc_now().strftime("%Y%m%d")
    diff_name = f"denylist_proposal_{date_s}_{ph}.diff"
    manifest_name = f"denylist_proposal_{date_s}_{ph}.manifest.json"

    diff_path = proposals_dir / diff_name
    manifest_path = proposals_dir / manifest_name

    diff_path.write_text(diff_txt, encoding="utf-8")

    manifest = {
        "kind": "feature_denylist_proposal",
        "created_utc": _utc_now().isoformat(),
        "proposal_hash": ph,
        "status": "pending_ab",
        "inputs": {
            "fs_run_dir": str(fs_run_dir),
            "stability_table": str(st_path),
            "denylist_path": a_rel,
        },
        "adds": {
            "deny_num": sorted(added_num),
            "deny_bool": sorted(added_bool),
        },
        "denylist_after": {
            "deny_num": sorted(deny_num),
            "deny_bool": sorted(deny_bool),
        },
        "selection_thresholds": {
            "min_importance": float(args.min_importance),
            "max_cv": float(args.max_cv),
            "max_features": int(args.max_features),
        },
        "drift_report_json": str(args.drift_report_json or ""),
        "required_confirmation": {
            "type": "replay_ab",
            "must": [
                "Replay/AB compare v5_of (full) vs v5_of_stable (denylist applied) on same input window",
                "Check model metrics (AUC/MCC) AND trading metrics (PnL/DD) AND stability by regime/hour buckets",
                "Only after approval apply the diff and bump model artifact/version if you promote",
            ],
        },
        "apply_instructions": [
            f"python -m ml_analysis.tools.apply_feature_denylist_proposal_v1 --manifest {manifest_path} --apply 1",
            f"(or for code-review: git apply {diff_path})",
        ],
        "notes": "This proposal only affects v5_of_stable (training baseline). v5_of remains unchanged unless you switch schema_ver.",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"[denylist] wrote: {diff_path}")
    print(f"[denylist] manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
