import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, HttpUrl

from src.models import Currency, PaymentStatus


class PaymentCreateSchema(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=2, le=Decimal("99999999.99"))
    currency: Currency
    description: str | None = Field(None, max_length=255)
    metadata: dict | None = None
    webhook_url: HttpUrl


class PaymentResponseSchema(BaseModel):
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    description: str | None
    metadata: dict | None = Field(default=None, validation_alias="metadata_")
    webhook_url: HttpUrl
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None = None

    model_config = {
        "from_attributes": True,
    }


class PaymentAcceptedSchema(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime
