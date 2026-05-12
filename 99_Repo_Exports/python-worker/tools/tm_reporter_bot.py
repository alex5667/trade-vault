from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradeMonitor Reporter Bot.
Orchestrates creating NDJSON exports, running policy tuner, and sending reports to Telegram.

Modes:
  --mode operational : Hourly report (lookback 6h & 24h).
  --mode decisive    : Daily report (lookback 7d & 3d) with recommendations.
  --daemon           : Run in loop (custom scheduler).

Usage:
  python tools/tm_reporter_bot.py --daemon
  python tools/tm_reporter_bot.py --mode operational
"""

import argparse
import datetime
import logging
import os
import subprocess
import sys
import tempfile
import time

import requests

# Hack to allow imports from local directories
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TmReporterBot")


def _get_telegram_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_REPORTER_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHAT_IDS")
    return token, chat_id


def _split_message(text: str, max_length: int = 4000) -> list[str]:
    if len(text) <= max_length:
        return [text]
    chunks = []
    for i in range(0, len(text), max_length):
        chunks.append(text[i:i+max_length])
    return chunks


def send_telegram_markdown(title: str, text: str):
    token, chat_id_raw = _get_telegram_config()
    if not token or not chat_id_raw:
        logger.warning(f"Telegram config missing (token={bool(token)}, chat={bool(chat_id_raw)}), skipping send.")
        return

    chat_ids = [c.strip() for c in str(chat_id_raw).split(",") if c.strip()]
    full_text = f"*{title}*\n\n{text}"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks = _split_message(full_text)

    for cid in chat_ids:
        for i, chunk in enumerate(chunks):
            payload = {
                "chat_id": cid,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            try:
                resp = requests.post(url, json=payload, timeout=10)
                if resp.status_code != 200:
                    logger.error(f"Telegram send failed: {resp.text}")
                else:
                    logger.info(f"Telegram sent chunk {i+1}/{len(chunks)} to {cid}")
            except Exception as e:
                logger.error(f"Telegram send error: {e}")
            time.sleep(0.5)


def run_export(lookback_hours: int, output_path: str, max_records: int = 50000):
    now_ms = get_ny_time_millis()
    since_ms = now_ms - (lookback_hours * 3600 * 1000)

    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "export_closed_trades_ndjson.py"),
        "--since-id", f"{since_ms}-0",
        "--out", output_path,
        "--max", str(max_records)
    ]
    logger.info(f"Running export: {' '.join(cmd)}")

    # Capture output to avoid noise unless error
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.error(f"Export failed output: {e.output.decode('utf-8', errors='ignore')}")
        raise RuntimeError("Export script failed")


def run_tune(input_path: str, output_md: str, min_n: int):
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "tm_policy_tuner.py"),
        "--in", input_path,
        "--out", output_md,
        "--min-n", str(min_n),
        "--conf", "0.90"
    ]
    logger.info(f"Running tuner: {' '.join(cmd)}")
    try:
        # Tuner prints report path to stdout, silence it
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.error(f"Tuner failed output: {e.output.decode('utf-8', errors='ignore')}")
        raise RuntimeError("Tuner script failed")


def task_operational():
    logger.info(">>> Starting Operational Report (6h / 24h)")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 6h
            ndjson_6h = os.path.join(tmpdir, "trades_6h.ndjson")
            md_6h = os.path.join(tmpdir, "report_6h.md")
            run_export(6, ndjson_6h)
            run_tune(ndjson_6h, md_6h, min_n=10)

            # 24h
            ndjson_24h = os.path.join(tmpdir, "trades_24h.ndjson")
            md_24h = os.path.join(tmpdir, "report_24h.md")
            run_export(24, ndjson_24h)
            run_tune(ndjson_24h, md_24h, min_n=30)

            final_md = "Operational Report\n\n"
            final_md += "--- 6H LOOKBACK ---\n"
            if os.path.exists(md_6h):
                with open(md_6h) as f:
                    final_md += f.read()

            final_md += "\n\n--- 24H LOOKBACK ---\n"
            if os.path.exists(md_24h):
                with open(md_24h) as f:
                    final_md += f.read()

            send_telegram_markdown("📡 TM Operational Report", final_md)
            logger.info("Operational report sent.")
    except Exception as e:
        logger.exception(f"Operational task failed: {e}")


def task_decisive():
    logger.info(">>> Starting Decisive Report (7d / 3d)")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 7d
            ndjson_7d = os.path.join(tmpdir, "trades_7d.ndjson")
            md_7d = os.path.join(tmpdir, "report_7d.md")
            run_export(24*7, ndjson_7d, max_records=500000)
            run_tune(ndjson_7d, md_7d, min_n=80)

            # 3d
            ndjson_3d = os.path.join(tmpdir, "trades_3d.ndjson")
            md_3d = os.path.join(tmpdir, "report_3d.md")
            run_export(24*3, ndjson_3d, max_records=200000)
            run_tune(ndjson_3d, md_3d, min_n=50)

            final_md = "Decisive Policy Report (Suggestions)\n\n"
            final_md += "--- 7 DAYS (Primary) ---\n"
            if os.path.exists(md_7d):
                with open(md_7d) as f:
                    final_md += f.read()

            final_md += "\n\n--- 3 DAYS (Freshness) ---\n"
            if os.path.exists(md_3d):
                with open(md_3d) as f:
                    final_md += f.read()

            send_telegram_markdown("⚖️ TM Decisive Report", final_md)
            logger.info("Decisive report sent.")

    except Exception as e:
        logger.exception(f"Decisive task failed: {e}")


def run_periodically():
    logger.info("Starting scheduler loop (custom simple scheduler)...")

    last_op_run = 0.0
    last_dec_run = 0.0

    while True:
        now = time.time()

        # Operational: Every 60 minutes
        if now - last_op_run >= 3600:
            task_operational()
            last_op_run = time.time()

        # Decisive: Every day at 00:10 UTC (approx check)
        # Check if current time is 00:10 UTC +/- 1 minute and we haven't run today
        # But simple loop logic is safer: just check if > 24h passed OR check UTC time.

        # Robust daily approach:
        # Check if "today's decisive" is done.
        # We can store last_dec_run date.
        dt_now = datetime.datetime.now(datetime.timezone.utc)
        if dt_now.hour == 0 and dt_now.minute >= 10 and dt_now.minute < 30:
            # It's time window
            # Check if we ran today
            last_dt = datetime.datetime.fromtimestamp(last_dec_run, tz=datetime.timezone.utc)
            if last_dt.date() < dt_now.date():
                task_decisive()
                last_dec_run = time.time()

        time.sleep(60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["operational", "decisive"])
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        run_periodically()
    elif args.mode == "operational":
        task_operational()
    elif args.mode == "decisive":
        task_decisive()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
