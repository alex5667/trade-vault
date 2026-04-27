# -*- coding: utf-8 -*-
"""tools.autopilot_run_once

One-shot autopilot runner:
  1) export trades_closed to NDJSON
  2) run tm_policy_tuner to produce report + (optional) proposal writes
  3) send report to Telegram (optional)

Used by services.autopilot_scheduler_service.

This file intentionally uses subprocess to reuse CLI tools, so behavior
matches exactly what you run manually.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Tuple

from core.telegram_client import TelegramConfig, send_message


def _now_ms() -> int:
    return get_ny_time_millis()


def _pyworker_dir() -> Path:
    # python-worker/tools/autopilot_run_once.py -> python-worker/
    return Path(__file__).resolve().parents[1]


def _run_cmd(*, cwd: Path, args: list[str], env: dict) -> Tuple[int, str]:
    """Run command and return (rc, stdout+stderr)."""
    p = subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    out = p.stdout or ""
    return int(p.returncode), out


def run_once(
    *,
    since_hours: int,
    window_days: int,
    out_dir: Path,
    redis_write: bool,
    telegram: bool,
    telegram_parse_mode: str,
) -> int:
    """Returns process exit code (0 ok)."""
    base = _pyworker_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    ndjson_path = out_dir / f"closed_{since_hours}h.ndjson"
    report_md = out_dir / f"tm_policy_report_{window_days}d.md"
    report_json = out_dir / f"tm_policy_report_{window_days}d.json"

    env = os.environ.copy()
    # ensure local imports work the same as Makefile
    env["PYTHONPATH"] = f"{env.get('PYTHONPATH','')}:.:.."
    
    # Keep Redis URL consistent across all sub-tools (tuner uses REDIS_URL).
    redis_url = os.getenv("AUTOPILOT_REDIS_URL") or os.getenv("REDIS_URL") or ""
    if redis_url:
        env["REDIS_URL"] = redis_url
        env["AUTOPILOT_REDIS_URL"] = redis_url

    # 1) export
    rc1, out1 = _run_cmd(
        cwd=base,
        args=[
            sys.executable,
            "tools/export_trade_closed_ndjson.py",
            "--since-hours",
            str(int(since_hours)),
            "--out",
            str(ndjson_path),
        ],
        env=env,
    )
    if rc1 != 0:
        print(f"❌ Export failed (rc={rc1}):\n{out1}")
        if telegram:
            _send_telegram_text(
                text=(
                    f"Autopilot: export failed (rc={rc1})\n"
                    f"since_hours={since_hours} window_days={window_days}\n\n"
                    f"{out1[-3500:]}"
                ),
                parse_mode=telegram_parse_mode,
            )
        return rc1

    # 2) tuner
    tuner_args = [
        sys.executable,
        "tools/tm_policy_tuner.py",
        "--input",
        str(ndjson_path),
        "--window-days",
        str(int(window_days)),
        # Note: some newer versions of tm_policy_tuner might not support --out-md/--out-json directly via CLI 
        # if they were simplified. Let's assume the version we have uses stdout for the report 
        # and we capture it.
    ]
    if redis_write:
        tuner_args.append("--redis-write")
    
    rc2, out2 = _run_cmd(cwd=base, args=tuner_args, env=env)
    if rc2 != 0:
        print(f"❌ Tuner failed (rc={rc2}):\n{out2}")
        if telegram:
            _send_telegram_text(
                text=(
                    f"Autopilot: tuner failed (rc={rc2})\n"
                    f"ndjson={ndjson_path.name}\n\n"
                    f"{out2[-3500:]}"
                ),
                parse_mode=telegram_parse_mode,
            )
        return rc2

    # 3) telegram report (best-effort)
    if telegram:
        # We capture stdout from tuner (out2) as the report.
        # Minimal filtering to skip "redis_written=N" prefix if present.
        report_text = out2
        if "redis_written=" in out2:
            parts = out2.split("\n", 1)
            if len(parts) > 1:
                report_text = parts[1]

        # Use plaintext by default; Telegram markdown is fragile with tables.
        _send_telegram_text(
            text=(
                f"Autopilot report ({window_days}d) @ {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n"
                f"since_hours={since_hours} redis_write={1 if redis_write else 0}\n\n"
                f"{report_text}"
            ),
            parse_mode=telegram_parse_mode,
        )
        
        # Also save to file for audit
        try:
            report_md.write_text(report_text, encoding="utf-8")
        except Exception: pass

    return 0


def _send_telegram_text(*, text: str, parse_mode: str = "") -> None:
    tok = str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat = str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
    if not tok or not chat:
        return
    cfg = TelegramConfig(token=tok, chat_id=chat)
    send_message(cfg=cfg, text=text, parse_mode=parse_mode)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=int, default=int(os.getenv("AUTOPILOT_SINCE_HOURS", "168")))
    ap.add_argument("--window-days", type=int, default=int(os.getenv("AUTOPILOT_WINDOW_DAYS", "7")))
    ap.add_argument("--out-dir", type=str, default=str(os.getenv("AUTOPILOT_OUT_DIR", "/tmp/autopilot")))
    
    rw_default = bool(int(os.getenv("AUTOPILOT_REDIS_WRITE", "0")))
    ap.add_argument("--redis-write", action="store_true", default=rw_default)
    ap.add_argument("--no-redis-write", action="store_false", dest="redis_write")
    
    ap.add_argument("--telegram", action="store_true", default=True)
    ap.add_argument("--no-telegram", action="store_false", dest="telegram")
    
    ap.add_argument(
        "--telegram-parse-mode",
        type=str,
        default=str(os.getenv("AUTOPILOT_TG_PARSE_MODE", "")),
        help="Telegram parse_mode ('' recommended, or 'MarkdownV2' if you escape)",
    )
    args = ap.parse_args()
    return run_once(
        since_hours=int(args.since_hours),
        window_days=int(args.window_days),
        out_dir=Path(args.out_dir),
        redis_write=bool(args.redis_write),
        telegram=bool(args.telegram),
        telegram_parse_mode=str(args.telegram_parse_mode or ""),
    )


if __name__ == "__main__":
    raise SystemExit(main())
