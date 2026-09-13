import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch, AsyncMock
from src.main import app, worker_state

@pytest.mark.asyncio
async def test_health_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "worker"
        assert "is_running" in data

@pytest.mark.asyncio
async def test_status():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/status")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "worker"
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

@pytest.mark.asyncio
async def test_enrich_card_synchronously_enriches_and_returns_catalog():
    mock_worker = AsyncMock()
    mock_worker.close = AsyncMock()
    mock_worker.download_priority_cards = AsyncMock(
        return_value={"requested": 1, "downloaded": 1, "errors": 0}
    )
    mock_worker.scryfall.fetch_cards_by_ids = AsyncMock(return_value={})
    catalog = MagicMock()
    catalog.id = "card-1"
    catalog.name = "Sol Ring"
    catalog.manaCost = "{1}"
    catalog.typeLine = "Artifact"
    catalog.detailsEs = {"id": "card-1", "name": "Sol Ring", "rarity": "uncommon"}
    mock_worker.db.cardcatalog.find_unique = AsyncMock(return_value=catalog)

    with patch("src.main.Worker", return_value=mock_worker):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/enrich-card", json={"name": "Sol Ring"})

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "enriched"
    assert data["card"]["id"] == "card-1"
    mock_worker.download_priority_cards.assert_awaited_once_with(
        ["Sol Ring"], include_all_printings=True, update_linked_cards=False
    )

@pytest.mark.asyncio
async def test_enrich_card_returns_error_when_download_fails():
    mock_worker = AsyncMock()
    mock_worker.close = AsyncMock()
    mock_worker.download_priority_cards = AsyncMock(
        return_value={"requested": 1, "downloaded": 0, "errors": 1}
    )
    with patch("src.main.Worker", return_value=mock_worker):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/enrich-card", json={"name": "Not A Card"})

    assert response.status_code == 200
    assert response.json()["status"] == "error"
