"""Локальный dev-сервис для приёма вебхуков от consumer.

Не зависит от БД, брокера или глобальной конфигурации приложения.
Назначение — ручная проверка доставки уведомлений и демонстрация retry/DLQ-логики.
Логирует полные тела и заголовки входящих запросов — только для разработки,
не направлять сюда реальные вебхуки с секретами.
"""

from collections import deque
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from structlog import get_logger

from src.logging_config import setup_logging

setup_logging()

logger = get_logger(__name__)

app = FastAPI(title="Webhook Receiver (dev)")

_received_webhooks: deque[dict[str, Any]] = deque(maxlen=100)


@app.post("/webhook")
async def receive_webhook(request: Request) -> dict[str, bool]:
    """Приём вебхука — ответ 200."""
    body = await request.json()
    headers = dict(request.headers)
    logger.info("webhook received", path="/webhook", headers=headers, body=body)

    _received_webhooks.append(
        {
            "received_at": datetime.now(tz=UTC).isoformat(),
            "headers": headers,
            "body": body,
        }
    )
    return {"received": True}


@app.post("/webhook/fail")
async def fail_webhook(request: Request) -> dict[str, Any]:
    """Эмуляция недоступного получателя — ответ 500."""
    body = await request.json()
    headers = dict(request.headers)
    logger.info(
        "webhook received (fail)",
        path="/webhook/fail",
        headers=headers,
        body=body,
    )

    _received_webhooks.append(
        {
            "received_at": datetime.now(tz=UTC).isoformat(),
            "headers": headers,
            "body": body,
        }
    )
    raise HTTPException(
        status_code=500, detail="Webhook receiver unavailable (emulated)"
    )


@app.get("/webhooks")
async def list_webhooks() -> list[dict[str, Any]]:
    """Список последних вебхуков, новые первыми."""
    return list(reversed(_received_webhooks))
