from __future__ import annotations

from typing import Any

from core.meta_model_lr import MetaModelLR


def validate_meta_model(
    mm: MetaModelLR,
    *,
    require_signature: bool = True,
    pin_schema_name: str = "",
    pin_schema_hash: str = "",
    expected_schema_hash: str = "",
) -> tuple[bool, str, dict[str, Any]]:
    """Validate meta-model artifact for production use.

    Returns: (ok, reason, details)
      - ok: boolean
      - reason: short machine-friendly string
      - details: useful diagnostics for evidence/logging
    """
    details: dict[str, Any] = {
        "schema_name": str(getattr(mm, "schema_name", "") or ""),
        "schema_version": int(getattr(mm, "schema_version", 0) or 0),
        "schema_hash": str(getattr(mm, "schema_hash", "") or ""),
        "feature_cols_hash": str(getattr(mm, "feature_cols_hash", "") or ""),
        "sig_present": 1 if bool(getattr(mm, "model_signature", "") or "") else 0,
        "sig_ok": 1 if bool(getattr(mm, "signature_ok", lambda: False)()) else 0,
    }

    # 1) Signature integrity
    if require_signature:
        try:
            if not mm.signature_ok():
                return False, "bad_signature", details
        except Exception:
            return False, "bad_signature", details

    # 2) Explicit pins (ops override)
    if pin_schema_name:
        if str(getattr(mm, "schema_name", "") or "") != str(pin_schema_name):
            return False, "pin_schema_name_mismatch", details

    if pin_schema_hash:
        if str(getattr(mm, "schema_hash", "") or "") != str(pin_schema_hash):
            return False, "pin_schema_hash_mismatch", details

    # 3) Code-side expected schema hash (Train==Serve)
    if expected_schema_hash:
        got = str(getattr(mm, "schema_hash", "") or "")
        if not got:
            return False, "missing_schema_hash", details
        if got != str(expected_schema_hash):
            return False, "schema_hash_mismatch", details

    # 4) Self-consistency: feature_cols_hash must match model.features
    try:
        want_cols_hash = MetaModelLR.compute_feature_cols_hash(list(getattr(mm, "features", []) or []))
        got_cols_hash = str(getattr(mm, "feature_cols_hash", "") or "")
        if got_cols_hash and got_cols_hash != want_cols_hash:
            return False, "feature_cols_hash_mismatch", details
    except Exception:
        # If we cannot compute, do not block (fail-open here)
        pass

    return True, "ok", details
