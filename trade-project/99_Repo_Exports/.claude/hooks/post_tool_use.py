#!/usr/bin/env python3
"""
PostToolUse hook: запускает проверки после редактирования файлов.

Триггеры:
- Edit / Write на .go файлы → go vet
- Edit / Write на .py файлы → ruff check (быстрый линтер)
- Edit / Write на .ts / .tsx файлы → tsc --noEmit (type check)
- Edit / Write на *_test.go / test_*.py → предлагает запустить тесты
"""

import json
import os
import subprocess
import sys


def run(cmd: list[str], cwd: str | None = None) -> tuple[int, str, str]:
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    return result.returncode, result.stdout, result.stderr


def main():
    hook_input = json.load(sys.stdin)
    tool_name = hook_input.get("tool_name", "")
    tool_input = hook_input.get("tool_input", {})

    # Only act on file write/edit tools
    if tool_name not in ("Write", "Edit", "MultiEdit", "str_replace_based_edit_tool"):
        sys.exit(0)

    file_path = tool_input.get("path", "")
    if not file_path:
        sys.exit(0)

    ext = os.path.splitext(file_path)[1].lower()
    cwd = os.path.dirname(os.path.abspath(file_path)) or "."

    issues = []

    # ──────────────────────────────────────────────────────────────────────────
    # Stream-key parity gate: mirrors .github/workflows/stream-key-parity.yml
    # Fires locally when keys.go OR redis_keys.py is edited.
    # Equivalent to the CI merge-blocker — provides pre-commit protection.
    # ──────────────────────────────────────────────────────────────────────────
    PARITY_TRIGGERS = {
        "go-worker/internal/streams/keys.go",
        "python-worker/core/redis_keys.py",
        "python-worker/tests/test_go_python_stream_key_parity.py",
        "python-worker/tests/test_stream_retention_parity.py",
    }
    # Normalise: strip leading ./ or absolute prefix to get a repo-relative path
    abs_path = os.path.abspath(file_path)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    try:
        rel_path = os.path.relpath(abs_path, repo_root)
    except ValueError:
        rel_path = ""

    if rel_path in PARITY_TRIGGERS:
        parity_cwd = os.path.join(repo_root, "python-worker")
        if os.path.isdir(parity_cwd):
            code, out, err = run(
                [
                    "python", "-m", "pytest",
                    "tests/test_go_python_stream_key_parity.py",
                    "tests/test_stream_retention_parity.py",
                    "-v", "--tb=short", "--no-header", "-q",
                ],
                cwd=parity_cwd,
            )
            if code != 0:
                issues.append(
                    f"⛔ Stream-key parity CHECK FAILED (mirrors CI stream-key-parity.yml):\n"
                    f"{(out or err)[:2000]}"
                )

    if ext == ".go":
        code, out, err = run(["go", "vet", "./..."], cwd=cwd)
        if code != 0:
            issues.append(f"go vet:\n{err or out}")

    elif ext == ".py":
        # ruff is fast; fall back silently if not installed
        code, out, err = run(["ruff", "check", "--quiet", file_path])
        if code != 0 and "No module" not in err:
            issues.append(f"ruff:\n{out or err}")

    elif ext in (".ts", ".tsx"):
        # find tsconfig root
        check_dir = os.path.dirname(os.path.abspath(file_path))
        while check_dir != "/":
            if os.path.exists(os.path.join(check_dir, "tsconfig.json")):
                code, out, err = run(
                    ["npx", "--no", "tsc", "--noEmit", "--pretty", "false"],
                    cwd=check_dir,
                )
                if code != 0:
                    # Show only first 20 lines to avoid noise
                    lines = (out or err).splitlines()[:20]
                    issues.append("tsc:\n" + "\n".join(lines))
                break
            parent = os.path.dirname(check_dir)
            if parent == check_dir:
                break
            check_dir = parent

    if issues:
        print("⚠️  Static checks failed after edit:", file=sys.stderr)
        for issue in issues:
            print(issue, file=sys.stderr)
        # Exit 2 = show warning but don't block Claude
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
