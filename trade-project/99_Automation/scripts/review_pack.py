#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib import request, error

DEFAULT_MODEL = "deepseek-r1:14b"
DEFAULT_API_URL = "http://127.0.0.1:11434/api/chat"

SYSTEM_RULES = """Ты senior reviewer для trade-проекта.
Отвечай строго на русском языке.
Не используй китайские, японские или случайные англоязычные вставки, кроме имён метрик, stream names, ENV и code symbols.
Не показывай chain-of-thought.
Используй только данные из context pack. Если чего-то нет в pack — помечай это как assumption.
Верни ответ строго в формате:
# Review

## Goal
...

## Facts
- ...

## Assumptions
- ...

## Risks
- ...

## Plan
1. ...

## Tests
- ...

## Metrics/Alerts
- ...

## Rollout/Rollback
- ...
"""


def build_user_prompt(task: str, pack_text: str) -> str:
    return f"""Ниже context pack из Obsidian vault.

Задача:
{task}

Context pack:
{pack_text}
"""


def call_ollama(api_url: str, model: str, user_prompt: str, timeout: int = 600) -> str:
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": SYSTEM_RULES},
            {"role": "user", "content": user_prompt},
        ],
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(api_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {e.code} from Ollama API: {detail}")
    except error.URLError as e:
        raise SystemExit(f"Cannot reach Ollama API at {api_url}: {e}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise SystemExit(f"Invalid JSON from Ollama API: {raw[:500]}")

    message = data.get("message") or {}
    content = message.get("content")
    if not content:
        raise SystemExit(f"Ollama API returned no assistant content: {json.dumps(data, ensure_ascii=False)[:1000]}")
    return content.strip()


def add_frontmatter(review_text: str, title: str, source_pack: Path, model: str) -> str:
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    fm = (
        "---\n"
        "type: document\n"
        "tags: [llm-review, generated, local-llm]\n"
        f'title: "{title}"\n'
        f'source_pack: "{source_pack.as_posix()}"\n'
        f'model: "{model}"\n'
        f'updated_at: "{now}"\n'
        "---\n\n"
    )
    if review_text.startswith("---\n"):
        return review_text
    return fm + review_text.rstrip() + "\n"


def derive_output_path(pack_path: Path) -> Path:
    if pack_path.suffix.lower() == ".md":
        return pack_path.with_name(pack_path.stem + ".review.md")
    return pack_path.with_suffix(pack_path.suffix + ".review.md")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a local LLM review from an Obsidian context pack.")
    parser.add_argument("pack", help="Path to the context pack markdown file")
    parser.add_argument("task", nargs="?", default="Сделай production review в формате: Goal / Facts / Assumptions / Risks / Plan / Tests / Metrics/Alerts / Rollout/Rollback.", help="Review task for the local model")
    parser.add_argument("--output", help="Path for the generated review markdown")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Model name for Ollama (default: {DEFAULT_MODEL})")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help=f"Ollama chat endpoint (default: {DEFAULT_API_URL})")
    args = parser.parse_args()

    pack_path = Path(args.pack).expanduser().resolve()
    if not pack_path.exists():
        raise SystemExit(f"Pack not found: {pack_path}")

    pack_text = pack_path.read_text(encoding="utf-8")
    user_prompt = build_user_prompt(args.task, pack_text)
    review = call_ollama(args.api_url, args.model, user_prompt)

    title = f"Review: {pack_path.stem}"
    review_doc = add_frontmatter(review, title, pack_path, args.model)
    output_path = Path(args.output).expanduser().resolve() if args.output else derive_output_path(pack_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(review_doc, encoding="utf-8")

    print(f"written: {output_path}")


if __name__ == "__main__":
    main()
