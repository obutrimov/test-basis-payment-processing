import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.models import Outbox, Payment
from src.schemas import (
    PaymentAcceptedSchema,
    PaymentCreateSchema,
    PaymentResponseSchema,
)

router = APIRouter(prefix="/payments", tags=["Payments"])


@router.post(
    "",
    response_model=PaymentAcceptedSchema,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": PaymentResponseSchema},
    },
)
async def create_payment(
    payment_data: PaymentCreateSchema,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    session: AsyncSession = Depends(get_async_session),
):
    """
    Создает новый платеж.
    - Если платеж с таким `Idempotency-Key` уже существует,
      возвращает существующий платеж со статусом 200 OK.
    - В противном случае создает новый платеж, сохраняет его со статусом `pending`,
      планирует сообщение для обработки через паттерн Outbox и возвращает
      статус 202 Accepted.
    """
    # Сразу пытаемся создать платёж, обрабатывая ошибку уникальности ключа.
    # Это устраняет состояние гонки (race condition).
    try:
        payment_uuid = uuid.uuid4()
        payload = {
            k: v
            for k, v in payment_data.model_dump(
                exclude={"webhook_url", "metadata"}
            ).items()
        }
        new_payment = Payment(
            id=payment_uuid,
            metadata_=payment_data.metadata,
            webhook_url=str(payment_data.webhook_url),
            idempotency_key=idempotency_key,
            **payload,
        )
        outbox_message = Outbox(
            topic="payments.new",
            payload={"payment_id": str(payment_uuid)},
        )
        session.add(new_payment)
        session.add(outbox_message)

        await session.flush()

        response_data = PaymentAcceptedSchema(
            payment_id=payment_uuid,
            status=new_payment.status,
            created_at=new_payment.created_at,
        )
        await session.commit()
        return response_data

    except IntegrityError:
        await session.rollback()
        # Если платёж с таким ключом уже существует, находим его и возвращаем.
        existing_payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing_payment is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Payment conflict",
            )
        return Response(
            content=PaymentResponseSchema.model_validate(
                existing_payment
            ).model_dump_json(),
            status_code=status.HTTP_200_OK,
            media_type="application/json",
        )


@router.get("/{payment_id}", response_model=PaymentResponseSchema)
async def get_payment(
    payment_id: uuid.UUID, session: AsyncSession = Depends(get_async_session)
):
    payment = await session.get(Payment, payment_id)
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )
    return payment
