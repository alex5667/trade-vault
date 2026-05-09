import argparse
import json
from pathlib import Path

from orderflow_services.conf_score_weight_calibrator_v1 import (
    ConfScoreWeightCalibratorV1,
    load_replay_ndjson,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", required=True, help="Path to replay ndjson")
    ap.add_argument("--out", default="", help="Output json path (optional)")
    ap.add_argument("--min-n", type=int, default=500, help="Min samples per feature to tune")
    args = ap.parse_args()

    cal = ConfScoreWeightCalibratorV1(min_n=args.min_n)

    for rec in load_replay_ndjson(args.replay):
        cal.ingest(rec)

    patch = cal.to_config_patch()

    js = json.dumps(patch, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(js, encoding="utf-8")
    else:
        print(js)

if __name__ == "__main__":
    main()
