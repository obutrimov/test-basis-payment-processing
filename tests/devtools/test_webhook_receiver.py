"""
Тесты для dev-сервиса приёма вебхуков.

Покрывают:
  - POST /webhook  — приём и ответ 200
  - POST /webhook/fail — эмуляция ошибки и ответ 500
  - GET  /webhooks — список полученных вебхуков
"""

import pytest
from httpx import ASGITransport, AsyncClient

from src.devtools.webhook_receiver import _received_webhooks, app

# ===========================================================================
# Фикстура
# ===========================================================================


@pytest.fixture(autouse=True)
def _clear_store():
    """Очищает хранилище вебхуков перед каждым тестом."""
    _received_webhooks.clear()
    yield


# ===========================================================================
# POST /webhook
# ===========================================================================


async def test_post_webhook_returns_200():
    """POST /webhook с JSON-телом возвращает 200 и {"received": true}."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/webhook",
            json={"payment_id": "abc-123", "status": "succeeded"},
        )

    assert response.status_code == 200
    assert response.json() == {"received": True}


async def test_post_webhook_stores_record():
    """POST /webhook сохраняет запись с телом запроса в хранилище."""
    body = {"payment_id": "abc-123", "status": "succeeded"}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.post("/webhook", json=body)

    records = list(reversed(_received_webhooks))
    assert len(records) == 1
    assert records[0]["body"] == body
    assert "received_at" in records[0]
    assert "headers" in records[0]


# ===========================================================================
# POST /webhook/fail
# ===========================================================================


async def test_post_webhook_fail_returns_500():
    """POST /webhook/fail возвращает 500."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/webhook/fail",
            json={"payment_id": "fail-1", "status": "pending"},
        )

    assert response.status_code == 500


async def test_post_webhook_fail_stores_record():
    """POST /webhook/fail тоже сохраняет запись в хранилище."""
    body = {"payment_id": "fail-1", "status": "pending"}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.post("/webhook/fail", json=body)

    records = list(reversed(_received_webhooks))
    assert len(records) == 1
    assert records[0]["body"] == body


# ===========================================================================
# GET /webhooks
# ===========================================================================


async def test_get_webhooks_empty():
    """Пустое хранилище возвращает пустой список."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/webhooks")

    assert response.status_code == 200
    assert response.json() == []


async def test_get_webhooks_returns_newest_first():
    """GET /webhooks возвращает записи в порядке: новые первыми."""
    bodies = [
        {"order": 1},
        {"order": 2},
        {"order": 3},
    ]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        for b in bodies:
            await client.post("/webhook", json=b)

        response = await client.get("/webhooks")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    # Новые должны быть первыми: order 3, 2, 1
    assert data[0]["body"]["order"] == 3
    assert data[1]["body"]["order"] == 2
    assert data[2]["body"]["order"] == 1


async def test_get_webhooks_matches_posted_body():
    """Записи из GET /webhooks содержат точное тело POST /webhook."""
    payload = {"amount": 42, "currency": "RUB", "extra": [1, 2]}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.post("/webhook", json=payload)
        response = await client.get("/webhooks")

    assert response.json()[0]["body"] == payload
