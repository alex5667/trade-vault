# Outbox golden fixtures

Byte-stable wire-level fixtures для `stream:signals:outbox`. Используются
`tests/test_outbox_envelope_golden.py` чтобы зафиксировать контракт
producer → dispatcher и заметить любой drift on-wire shape / schema-version.

Каждый файл хранит:
- `wire_fields` — словарь XADD-полей (всё в строках, как уходит в Redis).
- `expected_schema_version` — int, ожидаемый после `_normalize_schema_version`.
- `expected_sid_present` — должен ли `_parse_envelope` вернуть env с `sid`.
- `accepted_default` — accepted при дефолтном `ACCEPTED_SCHEMA_VERSIONS`
  (`{SCHEMA_VERSION} ∪ LEGACY_SCHEMA_VERSIONS`).
- `accepted_v2_only` — accepted при `OUTBOX_ACCEPT_SCHEMA_VERSIONS="2"`.

| Файл | Форма | Описание |
|---|---|---|
| `v1_canonical.json` | nested | Pre-bump producer, `schema_version=1` |
| `v2_canonical.json` | nested | Текущий канон, `schema_version=2`, `meta.payload_schema=outbox_envelope:v2` |
| `v2_flat_legacy.json` | flat | Legacy `OutboxWriter`/`OutboxEnvelope` flat shape: `signal_id` + `audit_payload`/`notify_payload` на верхнем уровне; проверяет auto-repair → `sid`/`targets` |
| `v999_unknown.json` | nested | Неизвестная версия, должна уходить в DLQ при single-read |
