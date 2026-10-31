import asyncio
import random
import uuid
from datetime import UTC, datetime
from typing import Annotated

import httpx
import structlog
from faststream import Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.broker import app, broker, dlq, main_queue  # noqa: F401
from src.database import AsyncSessionLocal
from src.logging_config import setup_logging
from src.models import Payment, PaymentStatus

setup_logging()
logger = structlog.get_logger(__name__)


# Message Schema
class PaymentProcessMessage(BaseModel):
    payment_id: uuid.UUID


# Dependencies
async def get_db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def get_http_client() -> httpx.AsyncClient:
    async with httpx.AsyncClient() as client:
        yield client


# Экспоненциальный backoff для отправки webhook
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    reraise=True,
)
async def _send_webhook(http_client: httpx.AsyncClient, payment: Payment):
    webhook_payload = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "updated_at": payment.updated_at.isoformat(),
    }
    response = await http_client.post(
        str(payment.webhook_url), json=webhook_payload, timeout=10
    )
    response.raise_for_status()
    logger.info("Webhook sent", payment_id=payment.id, url=str(payment.webhook_url))


async def _execute_payment_logic(
    payment_id: uuid.UUID,
    session: AsyncSession,
    http_client: httpx.AsyncClient,
):
    # Row lock сериализует конкурирующие доставки одного платежа (at-least-once):
    # дубль ждёт освобождения row lock (коммит или откат первого обработчика)
    # и видит уже терминальный статус.
    payment = await session.get(Payment, payment_id, with_for_update=True)
    if not payment:
        logger.error("Payment not found, acknowledging message", payment_id=payment_id)
        return

    # Терминальный статус — только webhook, без повторной обработки
    if payment.status != PaymentStatus.PENDING:
        logger.info(
            "Payment already in terminal state, sending webhook only",
            payment_id=payment_id,
            current_status=payment.status.value,
        )
        # expunge отцепляет платёж (атрибуты дальше читаются без БД),
        # rollback освобождает row lock — не держим блокировку во время
        # сетевых ретраев webhook.
        session.expunge(payment)
        await session.rollback()
        await _send_webhook(http_client, payment)
        return

    logger.info(
        "Payment processing started",
        payment_id=payment_id,
        current_status=payment.status.value,
    )

    processing_time = random.uniform(2, 5)
    await asyncio.sleep(processing_time)

    is_successful = random.random() < 0.9  # 90% success rate
    new_status = PaymentStatus.SUCCEEDED if is_successful else PaymentStatus.FAILED
    logger.info(
        "Payment processing completed",
        payment_id=payment_id,
        result=new_status.value,
        processing_time_s=round(processing_time, 2),
    )

    payment.status = new_status
    payment.processed_at = datetime.now(tz=UTC)
    await session.commit()
    await session.refresh(payment)

    await _send_webhook(http_client, payment)


# Подписчик FastStream без retry — retry управляет tenacity внутри
@broker.subscriber(main_queue, retry=0)
async def process_payment(
    message: PaymentProcessMessage,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    http_client: Annotated[httpx.AsyncClient, Depends(get_http_client)],
):
    logger.info("Processing payment", payment_id=message.payment_id)

    try:
        await _execute_payment_logic(
            payment_id=message.payment_id,
            session=session,
            http_client=http_client,
        )
    except (httpx.HTTPStatusError, httpx.RequestError) as e:
        logger.error(
            "Failed to send webhook after all tenacity retries, moving to DLQ",
            payment_id=message.payment_id,
            error=str(e),
        )
        raise


@broker.subscriber(dlq, retry=False)
async def handle_dead_letter(message: PaymentProcessMessage):
    """Обработка сообщений из DLQ — только логирование."""
    logger.error(
        "Message received from DLQ",
        payment_id=message.payment_id,
        message_body=message.model_dump(),
    )
