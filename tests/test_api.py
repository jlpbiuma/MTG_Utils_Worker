import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock
from src.main import app, worker_state

@pytest.mark.asyncio
async def test_health_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "set-worker"
        assert "is_running" in data

@pytest.mark.asyncio
async def test_status():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/status")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "set-worker"
        assert "config" in data
        assert "state" in data

@pytest.mark.asyncio
async def test_trigger_run():
    transport = ASGITransport(app=app)
    with patch("src.main.run_worker_task", new_callable=AsyncMock) as mock_task:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/trigger")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
