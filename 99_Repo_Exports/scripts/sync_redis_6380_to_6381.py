#!/usr/bin/env python3
"""
Sync all keys / streams from Redis worker-1 to Redis worker-2.

Redis clients are created inside main() — NOT at module/import time,
which prevents accidental connections on import and allows easier testing.
"""
from __future__ import annotations

import sys
import time
from typing import Any

import redis


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def _make_clients() -> tuple[redis.Redis, redis.Redis]:
    """Lazily create source and target Redis clients."""
    common = dict(db=0, decode_responses=False, socket_timeout=30, socket_connect_timeout=30)
    source = redis.Redis(host='scanner-redis-worker-1', port=6379, **common)
    target = redis.Redis(host='scanner-redis-worker-2', port=6379, **common)
    return source, target


# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------

def check_connections(source: redis.Redis, target: redis.Redis) -> bool:
    """Ping both Redis instances; return False on any failure."""
    for client, label in [(source, 'source (worker-1)'), (target, 'target (worker-2)')]:
        try:
            client.ping()
            print(f"✅ Connected to Redis {label}")
        except Exception as exc:
            print(f"❌ Cannot reach Redis {label}: {exc}")
            return False
    return True


# ---------------------------------------------------------------------------
# Per-type copy helpers (each accepts explicit client references)
# ---------------------------------------------------------------------------

def _get_key_info(source: redis.Redis, key: bytes) -> dict[str, Any]:
    return {'type': source.type(key).decode('utf-8'), 'ttl': source.ttl(key)}


def _copy_string(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> None:
    value = source.get(key)
    if ttl > 0:
        target.setex(key, ttl, value)
    else:
        target.set(key, value)


def _copy_hash(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> None:
    data = source.hgetall(key)
    if data:
        target.delete(key)
        target.hset(key, mapping=data)
        if ttl > 0:
            target.expire(key, ttl)


def _copy_list(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> None:
    values = source.lrange(key, 0, -1)
    if values:
        target.delete(key)
        target.rpush(key, *values)
        if ttl > 0:
            target.expire(key, ttl)


def _copy_set(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> None:
    members = source.smembers(key)
    if members:
        target.delete(key)
        target.sadd(key, *members)
        if ttl > 0:
            target.expire(key, ttl)


def _copy_zset(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> None:
    members = source.zrange(key, 0, -1, withscores=True)
    if members:
        target.delete(key)
        target.zadd(key, {m: s for m, s in members})
        if ttl > 0:
            target.expire(key, ttl)


def _copy_stream(source: redis.Redis, target: redis.Redis, key: bytes, ttl: int) -> int:
    """Copy a stream; return the number of entries copied."""
    try:
        entries = source.xrange(key, '-', '+')
        if entries:
            target.delete(key)
            for entry_id, fields in entries:
                target.xadd(key, fields, id=entry_id)
            if ttl > 0:
                target.expire(key, ttl)
            return len(entries)
        return 0
    except Exception as exc:
        key_str = key.decode('utf-8', errors='ignore')
        print(f"⚠️  Error copying stream {key_str}: {exc}")
        return 0


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

_DISPATCH = {
    'string': _copy_string,
    'hash':   _copy_hash,
    'list':   _copy_list,
    'set':    _copy_set,
    'zset':   _copy_zset,
}


def sync_all_keys(source: redis.Redis, target: redis.Redis) -> None:
    """Scan and copy every key from *source* into *target*."""
    print("\n" + "=" * 60)
    print("🔄 SYNC START")
    print("=" * 60 + "\n")

    cursor = 0
    total = copied = errors = 0
    stats: dict[str, int] = dict.fromkeys(('string', 'hash', 'list', 'set', 'zset', 'stream', 'other'), 0)

    print("📊 Scanning keys from source Redis...\n")

    while True:
        cursor, keys = source.scan(cursor, count=100)

        for key in keys:
            total += 1
            try:
                info = _get_key_info(source, key)
                key_type, ttl = info['type'], info['ttl']

                if key_type in _DISPATCH:
                    _DISPATCH[key_type](source, target, key, ttl)
                    stats[key_type] += 1
                elif key_type == 'stream':
                    n = _copy_stream(source, target, key, ttl)
                    stats['stream'] += 1
                    if n > 0:
                        print(f"  📦 Stream: {key.decode('utf-8', errors='ignore')} ({n} entries)")
                else:
                    stats['other'] += 1
                    print(f"  ⚠️  Unknown type: {key_type} → {key.decode('utf-8', errors='ignore')}")

                copied += 1
                if total % 100 == 0:
                    print(f"  ⏳ Processed: {total} keys...")

            except Exception as exc:
                errors += 1
                print(f"  ❌ Error copying {key.decode('utf-8', errors='ignore')}: {exc}")

        if cursor == 0:
            break

    print("\n" + "=" * 60)
    print("✅ SYNC COMPLETE")
    print("=" * 60 + "\n")
    print("📊 Stats:")
    print(f"  • Total processed : {total}")
    print(f"  • Copied OK       : {copied}")
    print(f"  • Errors          : {errors}")
    print("\n📈 By type:")
    for t, n in stats.items():
        print(f"  • {t.capitalize()}: {n}")

    src_sz = source.dbsize()
    tgt_sz = target.dbsize()
    print("\n🔍 Verification:")
    print(f"  • Source: {src_sz} keys")
    print(f"  • Target: {tgt_sz} keys")
    if src_sz == tgt_sz:
        print(f"\n✅ Sync successful! All {src_sz} keys copied.")
    else:
        print(f"\n⚠️  Difference: {src_sz - tgt_sz} keys")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point."""
    print("🚀 Redis Sync: worker-1 → worker-2")
    print("=" * 60 + "\n")

    source, target = _make_clients()

    if not check_connections(source, target):
        sys.exit(1)

    src_size = source.dbsize()
    tgt_size = target.dbsize()
    print("\n📊 Current state:")
    print(f"  • Source: {src_size} keys")
    print(f"  • Target: {tgt_size} keys")

    if src_size == 0:
        print("\n⚠️  Source is empty. Nothing to copy.")
        return

    print(f"\n⚠️  Will copy {src_size} keys. Existing target keys will be overwritten.")

    t0 = time.time()
    sync_all_keys(source, target)
    print(f"\n⏱️  Elapsed: {time.time() - t0:.2f}s")
    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
