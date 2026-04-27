"""
Backward-compat shim (internal use only).
Prefer importing QF from common.qf_codes and storing numeric codes in payload.
"""

from common.qf_codes import QF as QualityFlag
