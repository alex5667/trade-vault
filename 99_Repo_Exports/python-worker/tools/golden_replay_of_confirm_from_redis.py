from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def run(cmd: list[str]) -> int:
    """
    Выполняет команду и возвращает exit code.
    Выводит stdout/stderr в консоль.
    """
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.stdout:
        print(p.stdout.rstrip())
    return int(p.returncode)


def _safe_load_json(path: str) -> dict[str, Any]:
    """
    Безопасно загружает JSON файл.
    Возвращает пустой dict при ошибке.
    """
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _notify(redis_url: str, stream: str, text: str) -> None:
    """
    Отправляет уведомление в Redis stream для Telegram.
    Fail-open: не падает при ошибках Redis.
    """
    try:
        import html
        safe_text = html.escape(text)
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.xadd(
            stream,
            {
                "type": "report",
                "subtype": "of_confirm_replay",
                "ts_ms": str(get_ny_time_millis()),
                "text": safe_text,
            },
            maxlen=200000,
            approximate=True,
        )
    except Exception:
        pass


def main() -> None:
    """
    Golden replay wrapper для OFConfirm: экспорт -> replay -> diff -> уведомление.
    
    Всегда генерирует diff.json (если baseline указан).
    Сам решает, падать ли (exit 2) на основе fail_on_mismatch.
    Шлёт алерт в notify:telegram при mismatch.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--stream", default=os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS))
    ap.add_argument("--field", default=os.getenv("OF_INPUTS_STREAM_FIELD", "payload"))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("OF_INPUTS_SINCE_HOURS", "24")))
    ap.add_argument("--max-records", type=int, default=int(os.getenv("OF_INPUTS_MAX_RECORDS", "250000")))
    ap.add_argument("--baseline", default=os.getenv("OF_REPLAY_BASELINE", ""))
    ap.add_argument("--fail-on-mismatch", type=int, default=int(os.getenv("OF_REPLAY_FAIL_ON_MISMATCH", "1")))
    ap.add_argument("--state-file", default=os.getenv("OF_INPUTS_STATE_FILE", ""))
    ap.add_argument("--resume", type=int, default=int(os.getenv("OF_INPUTS_RESUME", "1")))
    ap.add_argument("--notify", type=int, default=int(os.getenv("OF_REPLAY_NOTIFY", "1")))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    inputs_path = str(out_dir / "of_inputs.ndjson")
    cand_path = str(out_dir / "of_replay_candidate.ndjson")
    diff_path = str(out_dir / "of_replay_diff.json")
    dbg_path = str(out_dir / "of_replay_debug.ndjson")

    # Шаг 1: Экспорт inputs из Redis stream
    rc = run([
        "python", "-m", "tools.export_of_confirm_inputs_ndjson",
        "--redis-url", str(args.redis_url),
        "--stream", str(args.stream),
        "--field", str(args.field),
        "--since-hours", str(args.since_hours),
        "--max-records", str(args.max_records),
        "--out", inputs_path,
        "--resume", str(int(args.resume)),
        "--state-file", str(args.state_file),
    ])
    if rc != 0:
        raise SystemExit(rc)

    # Шаг 2: Replay из inputs -> candidate
    rc = run([
        "python", "-m", "tools.of_confirm_replay_from_inputs",
        "--inputs", inputs_path,
        "--out", cand_path,
        "--debug-out", dbg_path,
    ])
    if rc != 0:
        raise SystemExit(rc)

    # Шаг 3: Diff (всегда генерируем, если baseline указан; wrapper сам решает, падать ли)
    failed = False
    report: dict[str, Any] = {}
    if str(args.baseline or "").strip():
        # Запускаем diff с fail-on-mismatch=0, чтобы всегда получить JSON
        rc = run([
            "python", "-m", "tools.of_confirm_diff_report",
            "--baseline", str(args.baseline),
            "--candidate", cand_path,
            "--out", diff_path,
            "--fail-on-mismatch", "0",  # Wrapper сам решает, падать ли
        ])
        if rc != 0:
            # diff tool failure — это инфраструктурная ошибка
            raise SystemExit(rc)

        # Загружаем report и анализируем
        report = _safe_load_json(diff_path)
        miss_b = int(report.get("missing_in_baseline", 0) or 0)
        miss_c = int(report.get("missing_in_candidate", 0) or 0)
        mism = int(report.get("mismatches", 0) or 0)
        failed = (miss_b > 0 or miss_c > 0 or mism > 0)

        # Отправляем уведомление при mismatch
        if failed and int(args.notify) == 1:
            top_groups = report.get("top_groups", []) if isinstance(report.get("top_groups", []), list) else []
            samples = report.get("samples", []) if isinstance(report.get("samples", []), list) else []
            sample_keys = []
            for s in samples[:3]:
                with contextlib.suppress(Exception):
                    sample_keys.append((s.get("k", "")))
            msg = (
                "OF CONFIRM GOLDEN REPLAY MISMATCH\n"
                f"baseline={args.baseline}\n"
                f"candidate={cand_path}\n"
                f"missing_in_baseline={int(report.get('missing_in_baseline',0) or 0)} "
                f"missing_in_candidate={int(report.get('missing_in_candidate',0) or 0)} "
                f"mismatches={int(report.get('mismatches',0) or 0)}\n"
                f"mismatch_types={report.get('mismatch_types',{})}\n"
                f"top_groups={top_groups[:5]}\n"
                f"samples={sample_keys}"
            )
            _notify(str(args.redis_url), str(args.notify_stream), msg)

    # Выводим итоговый JSON с флагом failed
    print(json.dumps({
        "ok": True,
        "inputs": inputs_path,
        "candidate": cand_path,
        "diff": diff_path,
        "debug": dbg_path,
        "failed": failed,
    }, ensure_ascii=False, indent=2))

    # Падаем только если fail_on_mismatch=1 и есть mismatches
    if failed and int(args.fail_on_mismatch) == 1:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

