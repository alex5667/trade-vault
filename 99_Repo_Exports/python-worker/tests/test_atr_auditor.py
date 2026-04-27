import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock
import sys
import os

# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.atr_auditor_api import app

@pytest.fixture
def mock_db_pool():
    pool_mock = AsyncMock()
    conn_mock = AsyncMock()
    pool_mock.acquire.return_value.__aenter__.return_value = conn_mock
    app.state.db_pool = pool_mock
    return conn_mock

@pytest.mark.asyncio
async def test_healthz():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "atr-auditor-api"}

@pytest.mark.asyncio
async def test_get_release_board(mock_db_pool):
    mock_db_pool.fetch.return_value = [
        {"change_id": "CR-123", "owner": "test", "readiness_score": 100}
    ]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/auditor/release-board")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["change_id"] == "CR-123"

@pytest.mark.asyncio
async def test_get_incident_board(mock_db_pool):
    mock_db_pool.fetch.return_value = [
        {"incident_id": "INC-001", "severity": "SEV-1"}
    ]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/auditor/incidents")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["incident_id"] == "INC-001"

@pytest.mark.asyncio
async def test_no_put_post_endpoints():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/auditor/incidents", json={"new": "stuff"})
        assert response.status_code == 405
        response = await ac.delete("/auditor/release-board/CR-123")
        assert response.status_code == 405

