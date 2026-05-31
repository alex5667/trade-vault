from __future__ import annotations

"""
tests/news_pipeline/test_p6_stable_event_uuid.py

Unit tests for stable_event_uuid() determinism and format.

Verifies:
  U1. Same inputs → same UUID (deterministic)
  U2. Different doc_id → different UUID
  U3. Different prompt_ver → different UUID
  U4. Different model_id → different UUID
  U5. Provider change does NOT change UUID (intentional design)
  U6. Output is a valid UUID string format
"""

import uuid

from news_pipeline.p6_dto import stable_event_uuid


def test_deterministic_same_inputs():
    """Same (doc_id, prompt_ver, model_id) must always produce the same UUID."""
    uid1 = stable_event_uuid("doc-abc", "p1", "gemini-flash")
    uid2 = stable_event_uuid("doc-abc", "p1", "gemini-flash")
    assert uid1 == uid2


def test_different_doc_id_different_uuid():
    uid1 = stable_event_uuid("doc-abc", "p1", "m1")
    uid2 = stable_event_uuid("doc-xyz", "p1", "m1")
    assert uid1 != uid2


def test_different_prompt_ver_different_uuid():
    uid1 = stable_event_uuid("doc-abc", "p1", "m1")
    uid2 = stable_event_uuid("doc-abc", "p2", "m1")
    assert uid1 != uid2


def test_different_model_id_different_uuid():
    uid1 = stable_event_uuid("doc-abc", "p1", "gemini-flash")
    uid2 = stable_event_uuid("doc-abc", "p1", "gpt-4o")
    assert uid1 != uid2


def test_provider_not_included_in_key():
    """Provider intentionally excluded: fallback to different provider
    for same doc/prompt/model should produce the same event_id."""
    # This is the intended behaviour: provider may change on fallback
    # but semantically it's the same extraction pass.
    uid1 = stable_event_uuid("doc-abc", "p1", "gemini-flash")
    uid2 = stable_event_uuid("doc-abc", "p1", "gemini-flash")
    # (no provider arg) — same UUID regardless of which provider actually ran
    assert uid1 == uid2


def test_valid_uuid_format():
    """Result must be parseable as UUID (e.g. for Postgres UUID column)."""
    uid = stable_event_uuid("doc-test", "prompt-v1", "model-v1")
    parsed = uuid.UUID(uid)
    # UUIDv5
    assert parsed.version == 5
    assert str(parsed) == uid


def test_empty_strings_produce_stable_uuid():
    """Edge case: empty strings must not crash and must be deterministic."""
    uid1 = stable_event_uuid("", "", "")
    uid2 = stable_event_uuid("", "", "")
    assert uid1 == uid2
    uuid.UUID(uid1)  # must be valid UUID
