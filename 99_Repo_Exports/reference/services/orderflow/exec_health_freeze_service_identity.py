from __future__ import annotations

"""ExecHealth Redis service identity contract.

P13 closes the last deployment gap after ACL drift checks. Redis users alone are
not enough: rollout should also pin each trusted process to an expected
CLIENT LIST identity (user + name + lib-name). This module centralizes that
contract and provides helpers both for startup blockers and live drift export.
"""

from dataclasses import dataclass
import os
from typing import Any, Dict, Iterable, List, Mapping, Sequence
from urllib.parse import urlsplit

from services.orderflow.exec_health_freeze_acl_contract import AUDIT_USER, BOOTSTRAP_USER, DEFAULT_USER, WRITER_USER

IDENTITY_ENFORCE_ENV = 'EXEC_HEALTH_SERVICE_IDENTITY_ENFORCE'
IDENTITY_REQUIRE_LIBNAME_ENV = 'EXEC_HEALTH_SERVICE_IDENTITY_REQUIRE_LIB_NAME'


@dataclass(frozen=True)
class ServiceIdentity:
    service: str
    role: str
    redis_user: str
    client_name: str
    lib_name: str
    redis_url_env: str


def _b(x: Any, default: bool = False) -> bool:
    try:
        if isinstance(x, str):
            return x.strip().lower() in {'1', 'true', 'yes', 'on'}
        return bool(int(x))
    except Exception:
        return bool(default)


def _s(x: Any, d: str = '') -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def build_service_identity_contract() -> Dict[str, ServiceIdentity]:
    rows = [
        ServiceIdentity('exec_health_freeze_override_v1', 'writer', WRITER_USER, 'exec-health-freeze-override-v1', 'exec-health-freeze-writer', 'REDIS_URL'),
        ServiceIdentity('exec_health_slo_autoguard_v1', 'writer', WRITER_USER, 'exec-health-slo-autoguard-v1', 'exec-health-freeze-writer', 'REDIS_URL'),
        ServiceIdentity('exec_health_freeze_tamper_guard_v1', 'writer', WRITER_USER, 'exec-health-freeze-tamper-guard-v1', 'exec-health-freeze-writer', 'REDIS_URL'),
        ServiceIdentity('exec_health_freeze_acl_audit_exporter_v1', 'audit', AUDIT_USER, 'exec-health-freeze-acl-audit-exporter-v1', 'exec-health-freeze-audit', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
        ServiceIdentity('exec_health_freeze_acl_drift_exporter_v1', 'audit', AUDIT_USER, 'exec-health-freeze-acl-drift-exporter-v1', 'exec-health-freeze-audit', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
        ServiceIdentity('exec_health_freeze_client_name_audit_exporter_v1', 'audit', AUDIT_USER, 'exec-health-freeze-client-name-audit-exporter-v1', 'exec-health-freeze-audit', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
        ServiceIdentity('exec_health_freeze_acl_policy_v1', 'bootstrap', BOOTSTRAP_USER, 'exec-health-freeze-acl-policy-apply-v1', 'exec-health-freeze-bootstrap', 'EXEC_HEALTH_REDIS_BOOTSTRAP_URL'),
        ServiceIdentity('exec_health_freeze_reconnect_rollout_gate_v1', 'bootstrap', BOOTSTRAP_USER, 'exec-health-freeze-reconnect-rollout-gate-v1', 'exec-health-freeze-bootstrap', 'EXEC_HEALTH_REDIS_BOOTSTRAP_URL'),
        ServiceIdentity('exec_health_freeze_rollout_preflight_v1', 'audit', AUDIT_USER, 'exec-health-freeze-rollout-preflight-v1', 'exec-health-freeze-audit', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
        ServiceIdentity('exec_health_freeze_service_identity_exporter_v1', 'audit', AUDIT_USER, 'exec-health-freeze-service-identity-exporter-v1', 'exec-health-freeze-audit', 'EXEC_HEALTH_REDIS_AUDIT_URL'),
    ]
    return {r.service: r for r in rows}


def render_service_identity_env_templates(host: str = 'redis-worker-1', port: int = 6379, db: int = 0) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for s in build_service_identity_contract().values():
        out[s.service] = {
            s.redis_url_env: f'redis://{s.redis_user}:<password>@{host}:{port}/{db}',
            'EXEC_HEALTH_EXPECTED_REDIS_USER': s.redis_user,
            'EXEC_HEALTH_REDIS_CLIENT_NAME': s.client_name,
            'EXEC_HEALTH_REDIS_LIB_NAME': s.lib_name,
            'EXEC_HEALTH_SERVICE_IDENTITY_ENFORCE': '1',
            'EXEC_HEALTH_SERVICE_IDENTITY_REQUIRE_LIB_NAME': '1',
        }
    return out


def parse_redis_url_username(redis_url: str) -> str:
    raw = _s(redis_url).strip()
    if not raw:
        return ''
    try:
        u = urlsplit(raw)
        return _s(u.username)
    except Exception:
        return ''


def normalize_client_entry(raw: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            out[_s(k)] = _s(v)
        return out
    txt = _s(raw).strip()
    if not txt:
        return out
    # CLIENT LIST returns a single line with key=value pairs.
    line = txt.splitlines()[0]
    for part in line.split():
        if '=' not in part:
            continue
        k, v = part.split('=', 1)
        out[_s(k)] = _s(v)
    return out


def parse_client_list(raw: Any) -> List[Dict[str, str]]:
    if isinstance(raw, (list, tuple)):
        return [normalize_client_entry(x) for x in raw if normalize_client_entry(x)]
    txt = _s(raw).strip()
    if not txt:
        return []
    out: List[Dict[str, str]] = []
    for line in txt.splitlines():
        ent = normalize_client_entry(line)
        if ent:
            out.append(ent)
    return out


def _match_entry_to_service(entry: Mapping[str, Any], expected: ServiceIdentity) -> Dict[str, str]:
    got_user = _s(entry.get('user'))
    got_name = _s(entry.get('name'))
    got_lib_name = _s(entry.get('lib-name'))
    return {
        'user': got_user,
        'name': got_name,
        'lib_name': got_lib_name,
        'user_match': '1' if got_user == expected.redis_user else '0',
        'name_match': '1' if got_name == expected.client_name else '0',
        'lib_name_match': '1' if got_lib_name == expected.lib_name else '0',
    }


def evaluate_client_list_against_contract(raw_client_list: Any, *, required_services: Sequence[str] | None = None) -> Dict[str, Any]:
    contract = build_service_identity_contract()
    required = list(required_services or contract.keys())
    entries = parse_client_list(raw_client_list)
    by_name: Dict[str, List[Dict[str, str]]] = {}
    for ent in entries:
        name = _s(ent.get('name'))
        by_name.setdefault(name, []).append(ent)
    violations: List[Dict[str, str]] = []
    services: Dict[str, Dict[str, Any]] = {}
    known_names = {v.client_name for v in contract.values()}
    for service in required:
        exp = contract[service]
        rows = list(by_name.get(exp.client_name, []))
        if not rows:
            violations.append({'kind': 'service_missing', 'service': service})
            services[service] = {'seen': 0, 'user_match': 0, 'name_match': 0, 'lib_name_match': 0, 'role': exp.role, 'expected_user': exp.redis_user, 'expected_name': exp.client_name, 'expected_lib_name': exp.lib_name}
            continue
        if len(rows) > 1:
            violations.append({'kind': 'duplicate_service_connection', 'service': service})
        match = _match_entry_to_service(rows[0], exp)
        if match['user_match'] != '1':
            violations.append({'kind': 'wrong_user', 'service': service})
        if match['name_match'] != '1':
            violations.append({'kind': 'wrong_name', 'service': service})
        if match['lib_name_match'] != '1':
            violations.append({'kind': 'wrong_lib_name', 'service': service})
        services[service] = {
            'seen': len(rows),
            'role': exp.role,
            'expected_user': exp.redis_user,
            'expected_name': exp.client_name,
            'expected_lib_name': exp.lib_name,
            **{k: (int(v) if v in {'0','1'} else v) for k, v in match.items()},
        }
    for ent in entries:
        name = _s(ent.get('name'))
        if name.startswith('exec-health-freeze-') and name not in known_names:
            violations.append({'kind': 'unexpected_exec_health_client', 'service': name})
    return {'ok': not violations, 'services': services, 'violations': violations, 'required_services': required}


def get_expected_service(service: str) -> ServiceIdentity:
    try:
        return build_service_identity_contract()[service]
    except KeyError as exc:
        raise KeyError(f'unknown ExecHealth service identity contract: {service}') from exc


def _read_current_client_line_sync(r: Any) -> str:
    cid = int(r.execute_command('CLIENT', 'ID'))
    try:
        raw = r.execute_command('CLIENT', 'LIST', 'ID', cid)
    except Exception:
        raw = r.execute_command('CLIENT', 'LIST')
        for line in _s(raw).splitlines():
            ent = normalize_client_entry(line)
            if _s(ent.get('id')) == str(cid):
                return line
        raise
    return _s(raw)


async def _read_current_client_line_async(r: Any) -> str:
    cid = int(await r.execute_command('CLIENT', 'ID'))
    try:
        raw = await r.execute_command('CLIENT', 'LIST', 'ID', cid)
    except Exception:
        raw = await r.execute_command('CLIENT', 'LIST')
        for line in _s(raw).splitlines():
            ent = normalize_client_entry(line)
            if _s(ent.get('id')) == str(cid):
                return line
        raise
    return _s(raw)


def verify_entry_against_expected(entry: Mapping[str, Any], expected: ServiceIdentity, *, require_lib_name: bool | None = None) -> Dict[str, Any]:
    require_lib_name = _b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True) if require_lib_name is None else bool(require_lib_name)
    ent = normalize_client_entry(entry)
    got_user = _s(ent.get('user'))
    got_name = _s(ent.get('name'))
    got_lib_name = _s(ent.get('lib-name'))
    violations: List[str] = []
    if got_user != expected.redis_user:
        violations.append('wrong_user')
    if got_name != expected.client_name:
        violations.append('wrong_name')
    if require_lib_name and got_lib_name != expected.lib_name:
        violations.append('wrong_lib_name')
    return {'ok': not violations, 'entry': ent, 'violations': violations, 'expected': expected}


def ensure_service_identity_sync(r: Any, service: str, *, enforce: bool | None = None) -> Dict[str, Any]:
    expected = get_expected_service(service)
    enforce = _b(os.getenv(IDENTITY_ENFORCE_ENV, '1'), True) if enforce is None else bool(enforce)
    try:
        r.execute_command('CLIENT', 'SETNAME', expected.client_name)
    except Exception:
        if enforce:
            raise
    require_lib_name = _b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True)
    try:
        r.execute_command('CLIENT', 'SETINFO', 'LIB-NAME', expected.lib_name)
    except Exception:
        if enforce and require_lib_name:
            raise
    entry = normalize_client_entry(_read_current_client_line_sync(r))
    chk = verify_entry_against_expected(entry, expected, require_lib_name=require_lib_name)
    if enforce and not chk['ok']:
        raise RuntimeError(f'ExecHealth Redis service identity mismatch for {service}: {chk["violations"]}; entry={chk["entry"]}')
    return chk


async def ensure_service_identity_async(r: Any, service: str, *, enforce: bool | None = None) -> Dict[str, Any]:
    expected = get_expected_service(service)
    enforce = _b(os.getenv(IDENTITY_ENFORCE_ENV, '1'), True) if enforce is None else bool(enforce)
    try:
        await r.execute_command('CLIENT', 'SETNAME', expected.client_name)
    except Exception:
        if enforce:
            raise
    require_lib_name = _b(os.getenv(IDENTITY_REQUIRE_LIBNAME_ENV, '1'), True)
    try:
        await r.execute_command('CLIENT', 'SETINFO', 'LIB-NAME', expected.lib_name)
    except Exception:
        if enforce and require_lib_name:
            raise
    entry = normalize_client_entry(await _read_current_client_line_async(r))
    chk = verify_entry_against_expected(entry, expected, require_lib_name=require_lib_name)
    if enforce and not chk['ok']:
        raise RuntimeError(f'ExecHealth Redis service identity mismatch for {service}: {chk["violations"]}; entry={chk["entry"]}')
    return chk
