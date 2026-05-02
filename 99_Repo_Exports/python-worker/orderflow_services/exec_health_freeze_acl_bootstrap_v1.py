#!/usr/bin/env python3
from __future__ import annotations
from services.orderflow.exec_health_freeze_service_identity import render_service_identity_env_templates
"""P11: Bootstrap-сервис для генерации Redis ACL профилей и загрузки Function Libraries.

Запуск:
  python orderflow_services/exec_health_freeze_acl_bootstrap_v1.py

Выводит JSON с:
- Redis ACL SETUSER командами для reader/writer/bootstrap ролей
- именами Redis Function Libraries, которые надо загрузить
- рекомендуемыми ENV переменными
""",
import json
import os
import sys
from typing import Dict, List

from services.orderflow.exec_health_freeze_sealed_state import FN_FORCE_SET, FN_SET, LIBRARY_NAME, SEAL_SECRET_ENV
from services.orderflow.exec_health_freeze_request_log import FN_APPROVE_CAS, FN_COMMIT_CAS, FN_PREPARE_CAS, REQUEST_LIBRARY_NAME


FREEZE_KEYS = [
    "cfg:orderflow:exec_health:freeze_control:v1",
    "cfg:orderflow:exec_health:auto_freeze:v1",
    "metrics:exec_health:slo:autoguard:state",
    "metrics:exec_health:freeze_tamper_guard:last",
    "ops:exec_health:freeze_events:v1",
    "ops:exec_health:freeze_requests:v1",
]


def render_acl_commands() -> List[str]:
    from services.orderflow.exec_health_freeze_acl_contract import render_all_setuser_commands
    return render_all_setuser_commands()


def main(argv: List[str] | None = None) -> int:
    _ = argv or sys.argv[1:]
    doc = {
        "library_names": [LIBRARY_NAME, REQUEST_LIBRARY_NAME],
        "function_names": [FN_SET, FN_FORCE_SET, FN_PREPARE_CAS, FN_APPROVE_CAS, FN_COMMIT_CAS],
        "freeze_keys": list(FREEZE_KEYS),
        "seal_secret_env": SEAL_SECRET_ENV,
        "acl_commands": render_acl_commands(),
        "service_identity_env_templates": render_service_identity_env_templates(),
        "recommended_env": {
            "EXEC_HEALTH_FREEZE_SEAL_SECRET": "<strong-random-secret>",
            "EXEC_HEALTH_FREEZE_SEAL_ENFORCE": "1",
            "EXEC_HEALTH_FREEZE_SEAL_ALLOW_UNSEALED_BOOTSTRAP": "1",
        }
    }
    print(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
