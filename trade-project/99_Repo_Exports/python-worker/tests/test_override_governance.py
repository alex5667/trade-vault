
import pytest

from services.atr_override_governance_service import ATROverrideGovernanceService


class MockDBConn:
    def __init__(self):
        self.log = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def cursor(self, cursor_factory=None):
        return MockCursor(self)

    def commit(self):
        pass

class MockCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        pass

    def fetchone(self):
        return {"c": 0}

    def fetchall(self):
        return []

@pytest.fixture
def override_svc(monkeypatch):
    monkeypatch.setattr('services.atr_override_governance_service.get_conn', MockDBConn)
    # Use a dummy redis
    class DummyRedis:
        def set(self, *args, **kwargs): pass
    monkeypatch.setattr('redis.Redis.from_url', lambda *args, **kwargs: DummyRedis())
    svc = ATROverrideGovernanceService()
    svc.enabled = True
    return svc

def test_authority_matrix(override_svc):
    assert override_svc._get_role("senior_op") == "senior_operator"
    assert override_svc._get_role("admin") == "technical_owner"
    assert override_svc._get_role("normal_guy") == "operator"

def test_hard_forbidden_rules(override_svc, monkeypatch):
    # Mocking db to return SEV1
    class MockCursorSev1(MockCursor):
        def fetchone(self):
            return {"c": 1}

    class MockConnSev1(MockDBConn):
        def cursor(self, cursor_factory=None):
            return MockCursorSev1(self)

    monkeypatch.setattr('services.atr_override_governance_service.get_conn', MockConnSev1)

    res = override_svc._check_hard_forbidden_rules("TEMP_CLIP_OVERRIDE", "clip", {"symbol": "BTC"})
    assert res == "FORBID_OVERRIDE_OPEN_SEV1_ON_RELATED_SCOPE"

def test_request_override(override_svc, monkeypatch):
    res = override_svc.request_override(
        "TEMP_CLIP_OVERRIDE",
        {"symbol": "ETH"},
        "scope_frozen",
        "clip",
        1800,
        "user1",
        "TEST"
    )
    assert res["status"] == "success"
    assert "ovr_" in res["override_id"]
