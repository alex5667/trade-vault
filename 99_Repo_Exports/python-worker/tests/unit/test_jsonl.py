from __future__ import annotations

import os
import tempfile

from replay.jsonl import JsonlWriter, iter_jsonl
import contextlib


def test_jsonl_roundtrip() -> None:
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    try:
        w = JsonlWriter(path, flush=True, fsync=False)
        w.write({"a": 1})
        w.write({"b": "x"})
        w.close()

        xs = list(iter_jsonl(path))
        assert xs == [{"a": 1}, {"b": "x"}]
    finally:
        with contextlib.suppress(Exception):
            os.remove(path)
