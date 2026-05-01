from __future__ import annotations
"""
Утилита для подтверждения/отклонения предложений ML rollout guard.

Использует тот же механизм, что и Telegram callback:
- recs:preview2:{bundle_id}:{sig} - просмотр diff
- recs:confirm:{bundle_id}:{sig} - подтвердить
- recs:reject:{bundle_id}:{sig} - отклонить

Можно использовать для автоматизации или ручного управления.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hmac
import hashlib
import json
import os
import time
from typing import Any, Dict

import redis


def sign(bundle_id: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def find_pending_bundles(r: redis.Redis, *, limit: int = 100) -> list[Dict[str, Any]]:
    """Find pending bundles from recs:status keys."""
    bundles = []
    for key in r.scan_iter(match="recs:status:*", count=limit):
        bundle_id = key.split(":")[-1]
        status = r.get(key)
        if status == "PENDING":
            bundle_key = f"recs:bundle:{bundle_id}"
            bundle_raw = r.get(bundle_key)
            if bundle_raw:
                try:
                    bundle = json.loads(bundle_raw)
                    bundles.append(bundle)
                except Exception:
                    pass
    return bundles


def preview_bundle(r: redis.Redis, bundle_id: str, sig: str) -> None:
    """Preview bundle changes."""
    callback = f"recs:preview2:{bundle_id}:{sig}"
    r.xadd("notify:telegram", {
        "type": "callback",
        "callback": callback,
        "ts": str(get_ny_time_millis())
    }, maxlen=200000, approximate=True)
    print(f"✅ Preview запрос отправлен: {bundle_id}")


def confirm_bundle(r: redis.Redis, bundle_id: str, sig: str) -> None:
    """Confirm bundle changes."""
    callback = f"recs:confirm:{bundle_id}:{sig}"
    r.xadd("notify:telegram", {
        "type": "callback",
        "callback": callback,
        "ts": str(get_ny_time_millis())
    }, maxlen=200000, approximate=True)
    print(f"✅ Confirm запрос отправлен: {bundle_id}")


def reject_bundle(r: redis.Redis, bundle_id: str, sig: str) -> None:
    """Reject bundle changes."""
    callback = f"recs:reject:{bundle_id}:{sig}"
    r.xadd("notify:telegram", {
        "type": "callback",
        "callback": callback,
        "ts": str(get_ny_time_millis())
    }, maxlen=200000, approximate=True)
    print(f"✅ Reject запрос отправлен: {bundle_id}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Управление предложениями ML rollout guard")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--action", choices=["list", "preview", "confirm", "reject"], required=True,
                    help="Действие: list (список), preview (просмотр), confirm (подтвердить), reject (отклонить)")
    ap.add_argument("--bundle-id", default="", help="ID bundle (для preview/confirm/reject)")
    ap.add_argument("--auto-confirm", action="store_true",
                    help="Автоматически подтвердить все pending FREEZE предложения")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")

    if args.action == "list":
        bundles = find_pending_bundles(r)
        if not bundles:
            print("Нет pending предложений")
            return

        print(f"\n{'='*80}")
        print(f"PENDING ПРЕДЛОЖЕНИЯ ({len(bundles)}):")
        print(f"{'='*80}")
        for i, bundle in enumerate(bundles, 1):
            bundle_id = bundle.get("id", "unknown")
            meta = bundle.get("meta", {})
            title = meta.get("title", "unknown")
            details = meta.get("details", {})
            created_ms = bundle.get("created_ms", 0)
            
            import time
            if created_ms:
                ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_ms / 1000))
            else:
                ts_str = "unknown"
            
            print(f"\n{i}. [{ts_str}] {title}")
            print(f"   Bundle ID: {bundle_id}")
            if details:
                print(f"   Детали:")
                for k, v in details.items():
                    if isinstance(v, dict):
                        print(f"     {k}:")
                        for k2, v2 in v.items():
                            print(f"       {k2}: {v2}")
                    else:
                        print(f"     {k}: {v}")
            
            ops = bundle.get("ops", [])
            if ops:
                print(f"   Изменения:")
                for op in ops:
                    if op.get("op") == "HSET":
                        print(f"     {op.get('field')}: {op.get('value')}")

        print(f"\n{'='*80}\n")
        print("Использование:")
        print(f"  python ml_guard_approve.py --action preview --bundle-id <ID>")
        print(f"  python ml_guard_approve.py --action confirm --bundle-id <ID>")
        print(f"  python ml_guard_approve.py --action reject --bundle-id <ID>")

    elif args.action in ("preview", "confirm", "reject"):
        if not args.bundle_id:
            print("❌ Требуется --bundle-id")
            return

        bundle_key = f"recs:bundle:{args.bundle_id}"
        bundle_raw = r.get(bundle_key)
        if not bundle_raw:
            print(f"❌ Bundle {args.bundle_id} не найден")
            return

        try:
            bundle = json.loads(bundle_raw)
        except Exception:
            print(f"❌ Ошибка при парсинге bundle {args.bundle_id}")
            return

        sig = sign(args.bundle_id, secret)

        if args.action == "preview":
            preview_bundle(r, args.bundle_id, sig)
        elif args.action == "confirm":
            print(f"⚠️  Подтверждение изменений в bundle {args.bundle_id}")
            meta = bundle.get("meta", {})
            title = meta.get("title", "")
            print(f"   Название: {title}")
            ops = bundle.get("ops", [])
            for op in ops:
                if op.get("op") == "HSET":
                    print(f"   Изменение: {op.get('field')} = {op.get('value')}")
            confirm_bundle(r, args.bundle_id, sig)
        elif args.action == "reject":
            print(f"⚠️  Отклонение изменений в bundle {args.bundle_id}")
            reject_bundle(r, args.bundle_id, sig)

    elif args.action == "auto-confirm":
        bundles = find_pending_bundles(r)
        freeze_bundles = [
            b for b in bundles
            if "FREEZE" in b.get("meta", {}).get("title", "").upper()
        ]
        
        if not freeze_bundles:
            print("Нет pending FREEZE предложений")
            return

        print(f"Найдено {len(freeze_bundles)} pending FREEZE предложений")
        for bundle in freeze_bundles:
            bundle_id = bundle.get("id", "unknown")
            title = bundle.get("meta", {}).get("title", "")
            print(f"  - {bundle_id}: {title}")

        print("\n⚠️  Автоматическое подтверждение всех FREEZE предложений...")
        for bundle in freeze_bundles:
            bundle_id = bundle.get("id", "unknown")
            sig = sign(bundle_id, secret)
            confirm_bundle(r, bundle_id, sig)
            time.sleep(0.5)  # небольшая задержка между запросами

        print(f"\n✅ Подтверждено {len(freeze_bundles)} предложений")


if __name__ == "__main__":
    main()

