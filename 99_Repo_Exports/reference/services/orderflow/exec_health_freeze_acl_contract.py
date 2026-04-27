from __future__ import annotations

"""P12: Canonical ACL contract SoT for ExecHealth freeze-control Redis users.

This module is the single source of truth (SoT) for:
- Which Redis users exist for exec-health freeze-control
- What exact ACL rules each user must have
- Normalisation helpers so drift-check and tests compare the same representation
- CLIENT LIST parsing helpers for per-user connection auditing

All other modules (bootstrap, policy, drift exporter) import from here.
Direct edits to ACL rules belong here and nowhere else.

Redis ACL background:
- Named users and fine-grained ACL (Redis 6.0+)
- AUTH username password
- ACL LIST returns: `user <name> on/off #<pw-hash> ~<key-pattern> +<cmd> ...`
- ACL SETUSER applies a rule list
- ACL SAVE / ACL LOAD for file persistence
- ACL LOG tracks denied attempts
- CLIENT LIST returns per-connection metadata incl. `user=<name>`
"""

import re
from typing import Dict, List, Tuple

# ─── Canonical user list ────────────────────────────────────────────────────

# These are the users that MUST exist.  The order is the rollout order.
EXPECTED_USERS: Tuple[str, ...] = (
    "exec_health_freeze_reader",
    "exec_health_freeze_writer",
    "exec_health_freeze_audit",
    "exec_health_freeze_bootstrap",
    "go_gateway",
    "exec_projection",
    "entry_policy_safety",
    "liqmap_snapshot",
    "default",
)

DEFAULT_USER = "default"
READER_USER = "exec_health_freeze_reader"
WRITER_USER = "exec_health_freeze_writer"
AUDIT_USER = "exec_health_freeze_audit"
BOOTSTRAP_USER = "exec_health_freeze_bootstrap"
GO_GATEWAY_USER = "go_gateway"

# ─── Canonical ACL rule sets ─────────────────────────────────────────────────
#
# Rules are expressed as ACL SETUSER token sequences, exactly as you would
# pass them to Redis.  Passwords use the %REPLACE_ME_* placeholder convention;
# the apply step substitutes from ENV.
#
# Key:
#   reset           — flush existing rules first
#   on / off        — user enabled / disabled
#   >PASSWORD       — plain-text password token (Redis hashes it)
#   ~PATTERN        — key-pattern allow
#   %R~PATTERN      — key-pattern read-only
#   %W~PATTERN      — key-pattern write-only
#   +cmd / -cmd     — command allow / deny
#   nocommands      — deny all commands
#   nopass          — allow without password (dangerous; used only for off users)
#   allkeys         — unrestricted key access (bootstrap only, for Function LOAD)

EXPECTED_ACL_PROFILES: Dict[str, List[str]] = {
    # default must be disabled — no password auth via default user allowed
    "default": [
        "reset", "off", "nopass", "nocommands",
    ],

    # reader: read-only access to freeze-control keys, no write/scripting surface
    "exec_health_freeze_reader": [
        "reset", "on", "%REPLACE_ME_READER_PASS",
        "%R~cfg:orderflow:exec_health:*",
        "%R~metrics:exec_health:slo:autoguard:state",
        "%R~metrics:exec_health:freeze_tamper_guard:last",
        "%R~ops:exec_health:freeze_*",
        "+multi", "+exec", "+discard", "+get", "+hgetall", "+xrevrange", "+xrange", "+ping", "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-hset", "-hdel", "-del", "-unlink", "-eval", "-evalsha",
    ],

    # writer: FCALL + read/write on freeze-control surfaces, no direct hash ops
    "exec_health_freeze_writer": [
        "reset", "on", "%REPLACE_ME_WRITER_PASS",
        "%R~cfg:orderflow:exec_health:*", "%W~cfg:orderflow:exec_health:*",
        "%R~metrics:exec_health:*", "%W~metrics:exec_health:*",
        "%R~ops:exec_health:freeze_*", "%W~ops:exec_health:freeze_*",
        "%W~notify:telegram",
        "+multi", "+exec", "+discard", "+get", "+set", "+expire", "+pexpire", "+hgetall",
        "+xadd", "+xrevrange", "+xrange", "+fcall", "+ping", "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-hset", "-hdel", "-del", "-unlink", "-eval", "-evalsha",
    ],

    # audit: read-only audit surface — ACL LOG, CLIENT LIST, CONFIG GET aclfile
    "exec_health_freeze_audit": [
        "reset", "on", "%REPLACE_ME_AUDIT_PASS",
        "%R~metrics:exec_health:freeze_acl_*",
        "+multi", "+exec", "+discard", "+acl|log", "+client|list", "+config|get", "+ping", "+select", "+client|setname", "+client|setinfo", "+client|id", "+client|info",
    ],

    # bootstrap: loads Function Libraries, full key access during rollout only
    "exec_health_freeze_bootstrap": [
        "reset", "on", "%REPLACE_ME_BOOTSTRAP_PASS",
        "allkeys",
        "+multi", "+exec", "+discard", "+fcall", "+function", "+get", "+hgetall",
        "+set", "+expire", "+pexpire",
        "+xadd", "+xrevrange", "+xrange", "+ping",
        "+acl|setuser", "+acl|save", "+acl|load", "+acl|list", "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-hset", "-hdel", "-del", "-unlink", "-eval", "-evalsha",
    ],

    # exec_projection: full read/write access for the execution projection cluster.
    # Used by: execution-state-projection-worker, execution-state-projection-health,
    #           execution-bootstrap-health, binance-executor-supervised, binance-executor.
    # Needs: get/set/incr/expire/pexpire/xrange/xrevrange/xread/xadd/scan/del for
    #   cursor, lease, fencing-token, state-keys, orders:exec stream, user-stream status.
    # Needs: rpush/lpush/rpop/brpoplpush for binance-executor at-least-once queue delivery.
    "exec_projection": [
        "reset", "on", "%REPLACE_ME_EXEC_PROJECTION_PASS",
        "allkeys",
        "+multi", "+exec", "+discard", "+ping",
        "+get", "+set", "+incr", "+expire", "+pexpire",
        "+hgetall", "+hset", "+hdel", "+lrange", "+llen",
        "+lpush", "+rpush", "+rpop", "+brpoplpush", "+lmpop",
        "+zadd", "+zrem", "+zrange", "+zrangebyscore", "+zrevrange", "+zrevrangebyscore", "+zremrangebyscore", "+zremrangebyrank", "+zscore", "+zcard",
        "+xadd", "+xrevrange", "+xrange", "+xread",
        "+scan", "+del", "+unlink",
        "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-eval", "-evalsha", "-flushdb", "-flushall",
    ],

    # entry_policy_safety: safety guard that reads events:trades stream (consumer group),
    # writes stats hashes (hincrby/hincrbyfloat/hgetall), maintains context sets (sadd/smembers),
    # reads/writes active_arm keys (get/set), xadd to ledger and notify streams.
    "entry_policy_safety": [
        "reset", "on", "%REPLACE_ME_ENTRY_POLICY_SAFETY_PASS",
        "allkeys",
        "+multi", "+exec", "+discard", "+ping",
        "+get", "+set", "+expire", "+pexpire",
        "+hgetall", "+hincrby", "+hincrbyfloat",
        "+sadd", "+smembers",
        "+xadd", "+xread", "+xreadgroup", "+xack", "+xgroup",
        "+xrevrange", "+xrange",
        "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-eval", "-evalsha", "-flushdb", "-flushall",
    ],

    # liqmap_snapshot: liquidation-map snapshot service.
    # Reads stream:liq_evt via consumer group (xreadgroup/xack/xgroup/xclaim),
    # writes liqmap:snapshot:* keys (set/expire/hset), optionally publishes to
    # stream:liqmap_snapshot.  allkeys used for operational simplicity.
    "liqmap_snapshot": [
        "reset", "on", "%REPLACE_ME_LIQMAP_SNAPSHOT_PASS",
        "allkeys",
        "+multi", "+exec", "+discard", "+ping",
        "+get", "+set", "+expire", "+pexpire",
        "+hgetall", "+hset", "+hdel",
        "+xadd", "+xread", "+xreadgroup", "+xack", "+xgroup", "+xclaim",
        "+xrevrange", "+xrange", "+xlen", "+xpending",
        "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "-eval", "-evalsha", "-flushdb", "-flushall",
    ],

    # go_gateway: read/write access for order queue (lpush/rpush/rpop),
    # event stream (xadd), runtime data (get/hgetall/xrevrange/xrange).
    # Scoped to allkeys — gateway writes to orders:queue, stream:*, ta:*, book:*, pivots:* etc.
    "go_gateway": [
        "reset", "on", "%REPLACE_ME_GO_GATEWAY_PASS",
        "allkeys",
        "+multi", "+exec", "+discard",
        "+ping", "+mget", "+get", "+set", "+setex", "+getex", "+psetex",
        "+expire", "+pexpire", "+ttl", "+pttl", "+del", "+unlink", "+exists", "+type", "+keys", "+scan",
        "+hget", "+hset", "+hgetall", "+hdel", "+hincrby", "+hincrbyfloat", "+hkeys", "+hvals", "+hlen", "+hmget", "+hmset",
        "+lrange", "+llen", "+lpush", "+rpush", "+rpop", "+lmpop",
        "+zadd", "+zrem", "+zrange", "+zrangebyscore", "+zrevrange", "+zrevrangebyscore", "+zscore", "+zcard",
        "+sadd", "+smembers", "+scard", "+srem", "+sismember",
        "+incr", "+incrby", "+incrbyfloat", "+decr", "+decrby",
        "+xadd", "+xrevrange", "+xrange", "+xread", "+xreadgroup", "+xack", "+xclaim", "+xautoclaim", "+xgroup", "+xlen", "+xtrim", "+xinfo|stream", "+xinfo|groups", "+xinfo|consumers", "+xpending",
        "+client|setname", "+client|setinfo", "+client|id", "+client|info", "+client|list",
        "+eval", "+evalsha", "+script|load", "+script|exists", "-flushdb", "-flushall",
    ],
}


# ─── Rendering ───────────────────────────────────────────────────────────────

def render_setuser(user: str, rules: List[str]) -> str:
    """Return the full `ACL SETUSER <user> <rules...>` command string."""
    return "ACL SETUSER " + user + " " + " ".join(rules)


def render_all_setuser_commands() -> List[str]:
    """Return one ACL SETUSER command per expected user, in rollout order."""
    out: List[str] = []
    for user in EXPECTED_USERS:
        rules = EXPECTED_ACL_PROFILES.get(user, [])
        if rules:
            out.append(render_setuser(user, rules))
    return out


# ─── Normalisation ───────────────────────────────────────────────────────────
#
# `ACL LIST` returns lines like:
#   user default on nopass ~* &* +@all
#   user exec_health_freeze_reader on #abc123... %R~cfg:... +get ...
#
# We need to normalise these to the same token-set representation we use in
# EXPECTED_ACL_PROFILES so drift comparison works regardless of ordering.

_ACL_LIST_RE = re.compile(r'^user\s+(\S+)\s+(.*)')


def normalise_acl_line(line: str) -> Tuple[str, List[str]]:
    """Parse an ACL LIST line into (username, sorted_token_list).

    Returns ('', []) if the line does not match the expected format.
    """
    m = _ACL_LIST_RE.match(line.strip())
    if not m:
        return ("", [])
    user = m.group(1)
    tokens = sorted(m.group(2).split())
    return (user, tokens)


def normalise_setuser_rules(rules: List[str]) -> List[str]:
    """Sort ACL SETUSER rule tokens for canonical comparison.

    Password tokens (>..., #...) are stripped — we never compare secrets.
    """
    cleaned: List[str] = []
    for t in rules:
        t = t.strip()
        if not t:
            continue
        if t.startswith(">") or t.startswith("#") or t.startswith("%REPLACE_ME_"):
            continue  # skip password tokens
        cleaned.append(t)
    return sorted(cleaned)


def compare_acl(actual_list_line: str, expected_rules: List[str]) -> bool:
    """Return True if actual ACL LIST line matches expected rules (ignoring passwords).

    Both sides are normalised and sorted before comparison.
    """
    _user, actual_tokens = normalise_acl_line(actual_list_line)
    # strip password hashes from actual_tokens too
    actual_clean = sorted(t for t in actual_tokens if not t.startswith("#"))
    expected_clean = normalise_setuser_rules(expected_rules)
    return actual_clean == expected_clean


def is_default_user_disabled(acl_list_output: str) -> bool:
    """Return True if the default user line contains 'off' token."""
    for line in acl_list_output.splitlines():
        user, tokens = normalise_acl_line(line)
        if user == "default":
            return "off" in tokens
    return False  # default user line not found — not disabled


# ─── CLIENT LIST parsing ──────────────────────────────────────────────────────

def parse_client_list(output: str) -> List[Dict[str, str]]:
    """Parse Redis CLIENT LIST output into a list of field dicts.

    Redis CLIENT LIST format (one client per line):
      id=1 addr=127.0.0.1:12345 ... user=exec_health_freeze_writer ...

    Returns list of dicts with all key=value pairs.
    """
    clients: List[Dict[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        rec: Dict[str, str] = {}
        for part in line.split():
            if "=" in part:
                k, _, v = part.partition("=")
                rec[k] = v
        if rec:
            clients.append(rec)
    return clients


def count_connections_by_user(client_list_output: str) -> Dict[str, int]:
    """Return {username: connection_count} from CLIENT LIST output."""
    counts: Dict[str, int] = {}
    for client in parse_client_list(client_list_output):
        user = client.get("user", "unknown")
        counts[user] = counts.get(user, 0) + 1
    return counts


def unknown_user_connections(client_list_output: str) -> Dict[str, int]:
    """Return connections under users not in EXPECTED_USERS set."""
    known = set(EXPECTED_USERS)
    counts = count_connections_by_user(client_list_output)
    return {u: n for u, n in counts.items() if u not in known}
