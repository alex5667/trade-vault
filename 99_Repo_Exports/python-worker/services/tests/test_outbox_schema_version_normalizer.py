"""Unit tests for `_normalize_schema_version` + `ACCEPTED_SCHEMA_VERSIONS`
introduced in Phase 3.2 for dual-typed schema_version support.

Scope: pure parsing semantics; dispatcher integration is covered by the
existing chaos/integration suites in this directory.
"""

from __future__ import annotations

import importlib
import os
from unittest import mock

import pytest

from services.signal_outbox_dispatcher import _normalize_schema_version


class TestNormalizeSchemaVersion:
    @pytest.mark.parametrize("raw, expected", [
        (1, 1),
        (2, 2),
        ("1", 1),
        ("  1  ", 1),
        ("1.0", 1),
        (1.0, 1),
        (0, 0),
    ])
    def test_accepts(self, raw, expected):
        assert _normalize_schema_version(raw) == expected

    @pytest.mark.parametrize("raw", [
        None,
        "",
        "   ",
        "abc",
        "1.5",        # non-integer float string
        1.5,
        True,         # bool is rejected (subclass of int but semantically distinct)
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        object(),
    ])
    def test_rejects(self, raw):
        assert _normalize_schema_version(raw) is None

    def test_bytes_input(self):
        assert _normalize_schema_version(b"1") == 1
        assert _normalize_schema_version(b"abc") is None


class TestAcceptedSchemaVersions:
    """Env-parser tests avoid `importlib.reload` because
    prometheus_client's global registry does not support re-registration.
    We call `_parse_accepted_versions` directly with a patched env instead.
    """

    def _call(self, env_value):
        from services.signal_outbox_dispatcher import _parse_accepted_versions
        if env_value is None:
            patch = {}
        else:
            patch = {"OUTBOX_ACCEPT_SCHEMA_VERSIONS": env_value}
        with mock.patch.dict(os.environ, patch, clear=False):
            if env_value is None:
                os.environ.pop("OUTBOX_ACCEPT_SCHEMA_VERSIONS", None)
            return _parse_accepted_versions(1)

    def test_default_is_current(self):
        assert self._call(None) == frozenset({1})

    def test_env_dual_read(self):
        assert self._call("1,2") == frozenset({1, 2})

    def test_env_ignores_garbage(self):
        assert self._call("1, foo ,2") == frozenset({1, 2})

    def test_env_empty_falls_back(self):
        assert self._call("  ") == frozenset({1})

    def test_env_all_invalid_falls_back(self):
        assert self._call("foo,bar") == frozenset({1})
