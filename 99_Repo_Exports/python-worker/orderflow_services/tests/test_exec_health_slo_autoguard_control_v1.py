from __future__ import annotations

import asyncio

from orderflow_services.exec_health_slo_autoguard_v1 import AutoGuard, GuardCfg


class FakeAsyncRedis:
    """Fake async Redis for testing autoguard latch writes."""

    def __init__(self):
        self.hashes = {
            'metrics:exec_health:slo:last': {
                # cross_scope_mode_distinct > 1 triggers mode_mismatch_minutes=0 (instant trigger)
                'cross_scope_mode_distinct': '2',
                'rollout_drift_instances_total': '0',
            },
            'metrics:exec_health:slo:autoguard:state': {},
            'cfg:orderflow:exec_health:freeze_control:v1': {},
        }
        self.values = {
            'cfg:orderflow:overrides:v1:active_sid': 'a',
            'cfg:orderflow:overrides:v1:prev_sid': 'a',
        }
        self.streams: dict = {}

    async def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping):
        d = self.hashes.setdefault(key, {})
        d.update({str(k): str(v) for k, v in dict(mapping).items()})

    async def expire(self, key: str, ttl: int):
        return True

    async def set(self, key: str, value: str):
        self.values[key] = value

    async def pexpire(self, key: str, ttl: int):
        return True

    async def get(self, key: str):
        return self.values.get(key)

    async def xadd(self, key: str, mapping, maxlen=0):
        self.streams.setdefault(key, []).append(dict(mapping))
        return '1-0'


def _cfg() -> GuardCfg:
    return GuardCfg(
        redis_url='redis://test/0',
        summary_key='metrics:exec_health:slo:last',
        state_key='metrics:exec_health:slo:autoguard:state',
        freeze_key='cfg:orderflow:exec_health:auto_freeze:v1',
        control_key='cfg:orderflow:exec_health:freeze_control:v1',
        notify_stream='notify:telegram',
        event_stream='ops:exec_health:freeze_events:v1',
        loop_s=30,
        mode_mismatch_minutes=0,   # instant trigger
        drift_minutes=10,
        drift_instances_min=1,
        freeze_minutes=30,
        cooldown_minutes=30,
        rollback_enable=False,
        rollback_on_mode_mismatch=True,
        rollback_on_drift=False,
        enabled=True,
    )


def test_autoguard_writes_latched_control_state_and_pending_nonce() -> None:
    """P8: AutoGuard must write the control hash with manual_ack_required=1 and a pending nonce."""
    g = AutoGuard.__new__(AutoGuard)
    g.cfg = _cfg()
    g.r = FakeAsyncRedis()

    asyncio.run(g.run_once())

    ctl = g.r.hashes['cfg:orderflow:exec_health:freeze_control:v1']
    assert ctl.get('effective_freeze_active') == '1'
    assert ctl.get('manual_ack_required') == '1'
    assert ctl.get('control_source') == 'autoguard'
    # P8: pending nonce must be set
    assert str(ctl.get('expected_ack_nonce', '')) != ''


def test_autoguard_state_hash_has_manual_ack_fields() -> None:
    """P8: AutoGuard must also write manual_ack and nonce fields into the state hash (fallback path)."""
    g = AutoGuard.__new__(AutoGuard)
    g.cfg = _cfg()
    g.r = FakeAsyncRedis()

    asyncio.run(g.run_once())

    state = g.r.hashes['metrics:exec_health:slo:autoguard:state']
    assert state.get('manual_ack_required') == '1'
    assert state.get('effective_freeze_active') == '1'
    assert state.get('control_source') == 'autoguard'
    # P8: nonce fields in state hash
    assert str(state.get('expected_ack_nonce', '')) != ''
    assert str(state.get('last_trigger_nonce', '')) != ''


def test_autoguard_emits_latch_event_to_stream() -> None:
    """P8: AutoGuard must emit a autoguard_freeze_latch event to the freeze event stream on trigger."""
    g = AutoGuard.__new__(AutoGuard)
    g.cfg = _cfg()
    g.r = FakeAsyncRedis()

    asyncio.run(g.run_once())

    events = g.r.streams.get('ops:exec_health:freeze_events:v1', [])
    assert len(events) >= 1
    latch_events = [e for e in events if str(e.get('kind', '')) == 'autoguard_freeze_latch']
    assert len(latch_events) >= 1
    assert str(latch_events[0].get('ack_nonce', '')) != ''
