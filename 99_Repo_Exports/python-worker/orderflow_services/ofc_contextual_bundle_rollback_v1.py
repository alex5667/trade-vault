from __future__ import annotations

import argparse
import os

from orderflow_services.ofc_contextual_rollout_controller_v1 import _parse_symbols, _rm, _touch, _write_overlay


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default=os.getenv("OFC_CTX_FORCE_MODE", "shadow"))
    ap.add_argument("--overlay-env-path", default=os.getenv("OFC_CTX_OVERLAY_ENV_PATH", "/var/lib/trade/ofc_contextual_runtime_overlay.env"))
    ap.add_argument("--rollback-flag-path", default=os.getenv("OFC_CTX_ROLLBACK_FLAG_PATH", "/var/lib/trade/ofc_contextual.rollback.flag"))
    ap.add_argument("--canary-symbols", default=os.getenv("OFC_CTX_CANARY_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))
    ap.add_argument("--clear-rollback-flag", type=int, default=0)
    args = ap.parse_args()

    mode = str(args.mode or "shadow").strip().lower()
    if mode not in ("off", "shadow", "tighten_only", "replace_score_veto"):
        raise SystemExit(f"unsupported mode: {mode}")
    _write_overlay(args.overlay_env_path, mode, _parse_symbols(args.canary_symbols), args.rollback_flag_path)
    if int(args.clear_rollback_flag) == 1:
        _rm(args.rollback_flag_path)
    else:
        _touch(args.rollback_flag_path)
    print(f"rollback overlay written: mode={mode} overlay={args.overlay_env_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
