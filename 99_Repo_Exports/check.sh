#!/bin/bash

echo "=== P0.1: ReasonCode ==="
find . -name "reason_codes.py" -exec cat {} \; | grep "class ReasonCode"

echo "=== P0.2 & P0.3: gates.py cached_on_ctx ==="
find . -name "gates.py" -exec grep -n "cached_on_ctx" {} /dev/null \;
find . -name "gates.py" -exec grep -n "state_hash" {} /dev/null \;

echo "=== P0.4: orchestrator.py _normalize_ts_ms ==="
find . -name "orchestrator.py" -exec grep -n "_normalize_ts_ms" {} /dev/null \;
find . -name "orchestrator.py" -exec grep -n "event_time_ms" {} /dev/null \;
find . -name "orchestrator.py" -exec grep -n "quality_flags" {} /dev/null \;

echo "=== P0.5: tick_ordering_violation_total ==="
find . -name "orchestrator.py" -exec grep -n "tick_ordering_violation_total" {} /dev/null \;
find . -name "orchestrator.py" -exec grep -n "_last_ts_ms" {} /dev/null \;

echo "=== P0.6: signal_outbox.py fingerprint ==="
find . -name "signal_outbox.py" -exec grep -n "detection_reason" {} /dev/null \;

echo "=== P0.7: SCHEMA_VERSION ==="
find . -name "outbox_envelope.py" -exec grep -n "SCHEMA_VERSION" {} /dev/null \;
find . -name "signal_outbox_dispatcher.py" -exec grep -n "SCHEMA_VERSION" {} /dev/null \;

echo "=== P0.8: audit_skipped_virtual_total ==="
find . -name "signal_outbox_dispatcher.py" -exec grep -n "audit_skipped_virtual_total" {} /dev/null \;

echo "=== P0.9: tests restored ==="
find . -path "*/tests/*.py" | grep -E "test_cfg_hash_deterministic|test_orchestrator_confidence_gates|test_orchestrator_payload_safety|test_orchestrator_reason_codes"

echo "=== P0.10: stream:notify in trade_back ==="
grep -rn "stream:notify" trade_back/ 2>/dev/null

echo "=== P1.1: DLQBookDeltas ==="
find . -name "*.go" -exec grep -Hn "DLQBookDeltas" {} \;
find . -name "*.go" -exec grep -Hn "DLQPrefix" {} \;

echo "=== P1.2 & P1.3: Go event_time_ms / quality_flags ==="
find . -name "*.go" -exec grep -Hn "event_id" {} \; | head -n 3
find . -name "*.go" -exec grep -Hn "trace_id" {} \; | head -n 3
find . -name "*.go" -exec grep -Hn "quality_flags" {} \; | head -n 3
find . -name "*.go" -exec grep -Hn "event_time_ms" {} \; | head -n 3

echo "=== P1.4: metrics_registry.py ==="
find . -name "metrics_registry.py" -exec grep -n "signals_veto_total" {} /dev/null \;
find . -name "metrics_registry.py" -exec grep -n "pipeline_" {} /dev/null \;

echo "=== P1.5: Prometheus alerts ==="
find . -path "*/prometheus/rules/*.yml" -type f -exec grep -H "OutboxBacklogHigh\|SignalDedupCollisionSpike\|GateVetoSpikeByKind\|TickMonotonicViolationRate\|TraceIdMissingRate" {} \; 2>/dev/null

echo "=== P1.6: target-ACK loop ==="
find . -name "signal_outbox_dispatcher.py" -exec grep -n "ack:" {} /dev/null \;

echo "=== P1.7: tests ==="
find . -path "*/tests/*.py" | grep -E "test_outbox_contract|test_payload_builder_contract|test_outbox_writer_meta_merge|test_replay_golden_determinism"

echo "=== P1.8: CI ==="
find .github -name "*.yml" -exec grep -H "test_orchestrator_" {} \; 2>/dev/null

echo "=== P1.9: OUTBOX_CONTRACT_MODE ==="
find . -name "outbox_contract.py" -exec grep -n "OUTBOX_CONTRACT_MODE" {} /dev/null \;

