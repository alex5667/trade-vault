#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
from pathlib import Path
from typing import Any

from utils.time_utils import get_ny_time_millis


def _load_registry_helpers():
    # Prefer direct file loading to avoid importing common.__init__ with unrelated side effects.
    # Search canonical python-worker path first, then legacy tick_flow_full mirror.
    here = Path(__file__).resolve()
    mod_path = None
    for p in here.parents:
        canonical = p / 'common' / 'model_registry.py'
        legacy = p / 'tick_flow_full' / 'common' / 'model_registry.py'
        if canonical.exists():
            mod_path = canonical
            break
        if legacy.exists():
            mod_path = legacy
            break
    if mod_path is None:
        raise RuntimeError('cannot locate common/model_registry.py (canonical or tick_flow_full legacy)')
    spec = importlib.util.spec_from_file_location('ofc_model_registry_direct', mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'cannot import model_registry from {mod_path}')
    mod = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod.ensure_dir, mod.version_stamp, mod.write_json_atomic, mod.promote_bundle_dir


ensure_dir, version_stamp, write_json_atomic, promote_bundle_dir = _load_registry_helpers()


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding='utf-8') as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f'expected dict json: {path}')
    return obj


def _copy2(src: str, dst: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(dst)) or '.', exist_ok=True)
    shutil.copy2(src, dst)


def build_bundle(*, exec_cost_model_path: str, rule_success_model_path: str, registry_dir: str, gate_cfg_path: str | None = None, out_bundle_dir: str | None = None, kind: str = 'ofc_ctx_bundle', promote_dir: str | None = None) -> dict[str, Any]:
    registry_dir = ensure_dir(registry_dir)
    version = version_stamp()
    # Always materialize the versioned bundle inside registry_dir so promote_bundle_dir can find it.
    registry_bundle_dir = Path(registry_dir) / f'{kind}.{version}'
    registry_bundle_dir.mkdir(parents=True, exist_ok=True)

    exec_cost = _load_json(exec_cost_model_path)
    rule_success = _load_json(rule_success_model_path)
    gate_cfg = _load_json(gate_cfg_path) if gate_cfg_path else {
        'p_min_default': float(rule_success.get('defaults', {}).get('score_min_ctx', 0.55) or 0.55),
        'edge_floor_p50_bps': 0.0,
        'edge_floor_p90_bps': -2.0,
        'mode': 'shadow',
    }
    manifest = {
        'kind': kind,
        'bundle_version': version,
        'created_ts_ms': get_ny_time_millis(),
        'exec_cost_model_ver': (exec_cost.get('version', '')),
        'rule_success_model_ver': (rule_success.get('version', '')),
        'gate_cfg_ver': (gate_cfg.get('version', 'gate_v1')),
    }
    write_json_atomic(str(registry_bundle_dir / 'manifest.json'), manifest)
    _copy2(exec_cost_model_path, str(registry_bundle_dir / 'exec_cost_model.json'))
    _copy2(rule_success_model_path, str(registry_bundle_dir / 'rule_success_model.json'))
    write_json_atomic(str(registry_bundle_dir / 'gate_cfg.json'), gate_cfg)

    # Mirror to out_bundle_dir if requested (working copy outside registry).
    bundle_dir = Path(out_bundle_dir) if out_bundle_dir else registry_bundle_dir
    if out_bundle_dir and Path(out_bundle_dir).resolve() != registry_bundle_dir.resolve():
        bundle_dir.mkdir(parents=True, exist_ok=True)
        for fname in ('manifest.json', 'exec_cost_model.json', 'rule_success_model.json', 'gate_cfg.json'):
            _copy2(str(registry_bundle_dir / fname), str(bundle_dir / fname))

    pointer = None
    if promote_dir:
        pointer = promote_bundle_dir(registry_dir=registry_dir, kind=kind, version=version, dst_dir=str(promote_dir))
    return {'bundle_dir': str(bundle_dir), 'version': version, 'pointer': pointer, 'manifest': manifest}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='Build and optionally promote OFC contextual bundle')
    ap.add_argument('--exec_cost_model_path', required=True)
    ap.add_argument('--rule_success_model_path', required=True)
    ap.add_argument('--registry_dir', required=True)
    ap.add_argument('--gate_cfg_path', default='')
    ap.add_argument('--out_bundle_dir', default='')
    ap.add_argument('--promote_dir', default='')
    ap.add_argument('--kind', default='ofc_ctx_bundle')
    args = ap.parse_args(argv)
    build_bundle(
        exec_cost_model_path=str(args.exec_cost_model_path),
        rule_success_model_path=str(args.rule_success_model_path),
        registry_dir=str(args.registry_dir),
        gate_cfg_path=str(args.gate_cfg_path or '') or None,
        out_bundle_dir=str(args.out_bundle_dir or '') or None,
        promote_dir=str(args.promote_dir or '') or None,
        kind=str(args.kind),
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
