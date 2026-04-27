import argparse
import os
import redis


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sid", required=True)
    ap.add_argument("--approver", required=True)
    ap.add_argument("--ttl-sec", type=int, default=int(os.getenv("ENTRY_POLICY_APPROVALS_TTL_SEC", "1209600")))
    args = ap.parse_args()

    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(url, decode_responses=True)
    key = f"cfg:suggestions:entry_policy:approvals:{args.sid}"
    r.sadd(key, str(args.approver))
    if args.ttl_sec > 0:
        r.expire(key, int(args.ttl_sec))
    print(f"OK approve sid={args.sid} approver={args.approver}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
