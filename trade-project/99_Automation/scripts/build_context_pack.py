#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

MAX_CHARS_PER_NOTE_DEFAULT = 4000
FRONTMATTER_RE = re.compile(r"^---\n.*?\n---\n?", re.DOTALL)

def strip_frontmatter(text: str) -> str:
    return FRONTMATTER_RE.sub("", text, count=1)

def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def smart_trim(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text.strip()
    cut = text[:limit]
    last_break = max(cut.rfind("\n## "), cut.rfind("\n### "), cut.rfind("\n- "), cut.rfind(". "))
    if last_break > int(limit * 0.55):
        cut = cut[:last_break]
    return cut.strip() + "\n\n[...trimmed...]"

def extract_excerpt(text: str, limit: int) -> str:
    text = strip_frontmatter(text)
    text = normalize_whitespace(text)
    if not text:
        return ""
    return smart_trim(text, limit)

def rel_to_vault(note: Path, vault_root: Path) -> str:
    try:
        return note.resolve().relative_to(vault_root.resolve()).as_posix()
    except Exception:
        return note.name

def build_pack(vault_root: Path, notes: list[Path], output_path: Path, title: str, task: str, per_note_limit: int) -> None:
    resolved_notes = []
    for p in notes:
        if not p.is_absolute():
            p = vault_root / p
        resolved_notes.append(p)

    sections = []
    relevant = []
    for note in resolved_notes:
        if not note.exists():
            sections.append(f"### Missing source\n- `{note}`")
            continue
        body = note.read_text(encoding="utf-8", errors="ignore")
        excerpt = extract_excerpt(body, per_note_limit)
        rel = rel_to_vault(note, vault_root)
        relevant.append(rel)
        sections.append(f"### {rel}\n```text\n{excerpt}\n```")

    notes_block = "\n".join(f"- [[{p}]]" if p.endswith(".md") else f"- `{p}`" for p in relevant)
    source_lines = "\n".join(f"  - {p}" for p in relevant) if relevant else "  - "

    content = f"""---
type: context_pack
tags: [context-pack, generated, llm]
topic: "{title}"
source_notes:
{source_lines}
updated_at: auto
---

# Context Pack: {title}

## Task
{task}

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
{notes_block if notes_block else "- "}

## Key excerpts

{chr(10).join(sections)}

## Ask for external model
Use only this context pack. Preserve contracts and invariants. Return:
- goal
- facts
- assumptions
- risks
- plan
- tests
- metrics/alerts
- rollout/rollback
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    print(f"written: {output_path}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Build a compact context pack from selected vault notes.")
    parser.add_argument("--vault", required=True, help="Path to Obsidian vault root")
    parser.add_argument("--title", required=True, help="Pack title")
    parser.add_argument("--task", required=True, help="Task for external model")
    parser.add_argument("--output", required=True, help="Output markdown path")
    parser.add_argument("--max-chars-per-note", type=int, default=MAX_CHARS_PER_NOTE_DEFAULT)
    parser.add_argument("notes", nargs="+", help="Relative or absolute note paths")
    args = parser.parse_args()

    vault_root = Path(args.vault).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    notes = [Path(n).expanduser() for n in args.notes]

    build_pack(
        vault_root=vault_root,
        notes=notes,
        output_path=output_path,
        title=args.title,
        task=args.task,
        per_note_limit=args.max_chars_per_note,
    )

if __name__ == "__main__":
    main()
