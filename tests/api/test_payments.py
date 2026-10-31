"""
Интеграционные тесты для API /api/v1/payments.

Покрывают:
  - POST /payments  — создание, идемпотентность, валидация, авторизация
  - GET  /payments/{id} — получение существующего и несуществующего платежа
  - GET  /health — healthcheck
"""

import uuid
from decimal import Decimal

import pytest

from src.models import Currency, PaymentStatus

# ===========================================================================
# GET /health
# ===========================================================================


async def test_health_check(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ===========================================================================
# POST /api/v1/payments — авторизация
# ===========================================================================


async def test_create_payment_unauthorized(unauth_client):
    """Запрос без X-API-Key должен вернуть 401."""
    response = await unauth_client.post(
        "/api/v1/payments",
        json={
            "amount": "100.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/hook",
        },
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 401


async def test_create_payment_wrong_api_key(async_client):
    """Неверный API-ключ должен вернуть 401."""
    response = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "100.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/hook",
        },
        headers={
            "Idempotency-Key": str(uuid.uuid4()),
            "X-API-Key": "wrong-key",
        },
    )
    assert response.status_code == 401


# ===========================================================================
# POST /api/v1/payments — успешное создание
# ===========================================================================


async def test_create_payment_success(async_client):
    """Новый платёж создаётся со статусом 202 Accepted."""
    payload = {
        "amount": "250.50",
        "currency": "RUB",
        "description": "Order #42",
        "webhook_url": "https://example.com/webhook",
    }
    response = await async_client.post(
        "/api/v1/payments",
        json=payload,
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202
    data = response.json()
    assert "payment_id" in data
    assert data["status"] == PaymentStatus.PENDING


async def test_create_payment_without_description(async_client):
    """Поле description опционально."""
    payload = {
        "amount": "10.00",
        "currency": "USD",
        "webhook_url": "https://example.com/webhook",
    }
    response = await async_client.post(
        "/api/v1/payments",
        json=payload,
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202


async def test_create_payment_with_metadata(async_client):
    """Поле metadata принимается без ошибок."""
    payload = {
        "amount": "99.99",
        "currency": "EUR",
        "webhook_url": "https://example.com/webhook",
        "metadata": {"order_id": 7, "tags": ["vip"]},
    }
    response = await async_client.post(
        "/api/v1/payments",
        json=payload,
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202


async def test_create_payment_returns_pending_status(async_client):
    """Созданный платёж всегда в статусе PENDING."""
    response = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "1.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/webhook",
        },
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202
    assert response.json()["status"] == "pending"


# ===========================================================================
# POST /api/v1/payments — идемпотентность
# ===========================================================================


async def test_create_payment_idempotency_returns_200(async_client, make_payment):
    """
    Если платёж с данным idempotency_key уже существует в БД,
    POST возвращает 200 OK с данными существующего платежа.
    """
    idem_key = str(uuid.uuid4())
    existing = await make_payment(idempotency_key=idem_key)

    response = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "500.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/webhook",
        },
        headers={"Idempotency-Key": idem_key},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(existing.id)


async def test_idempotency_returns_existing_payment_data(async_client, make_payment):
    """При идемпотентном ответе возвращается полный PaymentResponseSchema."""
    idem_key = str(uuid.uuid4())
    existing = await make_payment(
        idempotency_key=idem_key,
        amount=Decimal("777.00"),
        currency=Currency.EUR,
        description="Idempotency check",
    )

    response = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "1.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/webhook",
        },
        headers={"Idempotency-Key": idem_key},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(existing.id)
    assert data["currency"] == "EUR"
    assert data["description"] == "Idempotency check"


# ===========================================================================
# POST /api/v1/payments — валидация входных данных
# ===========================================================================


@pytest.mark.parametrize(
    "bad_payload",
    [
        # amount <= 0
        {"amount": "0", "currency": "RUB", "webhook_url": "https://example.com/wh"},
        {"amount": "-50", "currency": "RUB", "webhook_url": "https://example.com/wh"},
        # amount > Numeric(10,2) limit
        {
            "amount": "100000000.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/wh",
        },
        # невалидная валюта
        {"amount": "10", "currency": "GBP", "webhook_url": "https://example.com/wh"},
        # невалидный webhook_url
        {"amount": "10", "currency": "RUB", "webhook_url": "not-a-url"},
        # отсутствует обязательное поле amount
        {"currency": "RUB", "webhook_url": "https://example.com/wh"},
    ],
)
async def test_create_payment_validation_error(async_client, bad_payload):
    """Неверные данные возвращают 422 Unprocessable Entity."""
    response = await async_client.post(
        "/api/v1/payments",
        json=bad_payload,
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 422


async def test_create_payment_missing_idempotency_key(async_client):
    """Отсутствие заголовка Idempotency-Key возвращает 422."""
    response = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/webhook",
        },
    )
    assert response.status_code == 422


# ===========================================================================
# GET /api/v1/payments/{payment_id}
# ===========================================================================


async def test_get_payment_success(async_client, make_payment):
    """Получение существующего платежа возвращает 200 и корректные данные."""
    payment = await make_payment(amount=Decimal("123.45"), currency=Currency.USD)

    response = await async_client.get(f"/api/v1/payments/{payment.id}")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == str(payment.id)
    assert data["status"] == PaymentStatus.PENDING
    assert data["currency"] == "USD"


async def test_get_payment_not_found(async_client):
    """Запрос несуществующего платежа возвращает 404."""
    response = await async_client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Payment not found"


async def test_get_payment_succeeded(async_client, make_payment):
    """Платёж в статусе SUCCEEDED корректно возвращается."""
    payment = await make_payment(status=PaymentStatus.SUCCEEDED)
    response = await async_client.get(f"/api/v1/payments/{payment.id}")
    assert response.status_code == 200
    assert response.json()["status"] == PaymentStatus.SUCCEEDED


async def test_get_payment_failed(async_client, make_payment):
    """Платёж в статусе FAILED корректно возвращается."""
    payment = await make_payment(status=PaymentStatus.FAILED)
    response = await async_client.get(f"/api/v1/payments/{payment.id}")
    assert response.status_code == 200
    assert response.json()["status"] == PaymentStatus.FAILED


async def test_get_payment_unauthorized(unauth_client, make_payment):
    """Запрос без ключа к GET /payments/{id} возвращает 401."""
    payment = await make_payment()
    response = await unauth_client.get(f"/api/v1/payments/{payment.id}")
    assert response.status_code == 401


async def test_get_payment_invalid_uuid(async_client):
    """Невалидный UUID в пути возвращает 422."""
    response = await async_client.get("/api/v1/payments/not-a-uuid")
    assert response.status_code == 422


# ===========================================================================
# Интеграционный тест: создать → получить
# ===========================================================================


async def test_create_then_get_payment(async_client):
    """Создаём платёж через POST, затем читаем через GET — данные совпадают."""
    idem_key = str(uuid.uuid4())
    payload = {
        "amount": "777.77",
        "currency": "EUR",
        "description": "Integration test",
        "webhook_url": "https://example.com/webhook",
    }

    create_resp = await async_client.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": idem_key}
    )
    assert create_resp.status_code == 202
    payment_id = create_resp.json()["payment_id"]

    get_resp = await async_client.get(f"/api/v1/payments/{payment_id}")
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["id"] == payment_id
    assert data["currency"] == "EUR"
    assert data["description"] == "Integration test"
    assert data["status"] == PaymentStatus.PENDING


async def test_create_payment_stores_webhook_url(async_client):
    """webhook_url корректно сохраняется и возвращается при GET."""
    idem_key = str(uuid.uuid4())
    webhook = "https://my-service.example.org/callbacks/payment"
    create_resp = await async_client.post(
        "/api/v1/payments",
        json={
            "amount": "50.00",
            "currency": "RUB",
            "webhook_url": webhook,
        },
        headers={"Idempotency-Key": idem_key},
    )
    assert create_resp.status_code == 202
    payment_id = create_resp.json()["payment_id"]

    get_resp = await async_client.get(f"/api/v1/payments/{payment_id}")
    assert get_resp.status_code == 200
    # webhook_url может иметь trailing slash от Pydantic HttpUrl
    assert webhook in get_resp.json()["webhook_url"]
