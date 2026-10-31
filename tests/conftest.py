"""
Глобальные фикстуры для всех тестов.

Стратегия изоляции БД:
  - SQLite (aiosqlite) in-memory через StaticPool — быстро, без реального Postgres.
  - Postgres-специфичные типы (JSONB, UUID, PgEnum) подменяются на совместимые
    с SQLite аналоги через патчинг на уровне модуля до импорта моделей.
  - Перед каждым тестом создаётся схема, после — удаляется (function scope).
  - Зависимость get_async_session переопределяется через dependency_overrides.
"""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest_asyncio

# ---------------------------------------------------------------------------
# Патчим Postgres-специфичные типы ДО импорта src.models
# Это позволяет SQLite корректно создавать таблицы
# ---------------------------------------------------------------------------
import sqlalchemy.dialects.postgresql as pg
from httpx import ASGITransport, AsyncClient
from sqlalchemy import JSON, String, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import TypeDecorator


# --- UUID: заменяем PostgreSQL UUID на строковый тип ---
class StrUUID(TypeDecorator):
    """Хранит UUID как строку в SQLite, прозрачен для Postgres."""

    impl = String(36)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return uuid.UUID(value)


# Патчим pg.UUID чтобы при вызове UUID(as_uuid=True) возвращался StrUUID
original_pg_uuid = pg.UUID


class _PatchedUUID:
    def __new__(cls, as_uuid=False, **kw):
        return StrUUID()


pg.UUID = _PatchedUUID

# Патчим pg.JSONB → JSON (поддерживается SQLite)
pg.JSONB = JSON

# Теперь безопасно импортируем остальное
from src.api.main import app  # noqa: E402
from src.database import Base, get_async_session  # noqa: E402
from src.models import Currency, Outbox, Payment, PaymentStatus  # noqa: E402

# ---------------------------------------------------------------------------
# Тестовый движок SQLite (in-memory, общее соединение через StaticPool)
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
)


@event.listens_for(test_engine.sync_engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


TestSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ---------------------------------------------------------------------------
# Фикстуры БД
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_database():
    """Создаёт схему перед тестом и удаляет после."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Возвращает сессию БД для прямого взаимодействия в тестах."""
    async with TestSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Переопределение зависимостей FastAPI
# ---------------------------------------------------------------------------


async def override_get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with TestSessionLocal() as session:
        yield session


app.dependency_overrides[get_async_session] = override_get_async_session


# ---------------------------------------------------------------------------
# HTTP-клиенты для API-тестов
# ---------------------------------------------------------------------------

TEST_API_KEY = "super-secret-api-key"


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Асинхронный HTTPX-клиент с авторизационным заголовком по умолчанию."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": TEST_API_KEY},
    ) as client:
        yield client


@pytest_asyncio.fixture
async def unauth_client() -> AsyncGenerator[AsyncClient, None]:
    """Асинхронный HTTPX-клиент БЕЗ API-ключа (для проверки 401)."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Фабричные фикстуры для создания тестовых сущностей
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def make_payment(db_session: AsyncSession):
    """
    Фабрика платежей. Возвращает callable, создающий Payment в БД.

    Использование:
        payment = await make_payment()
        payment2 = await make_payment(
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("50.00")
        )
    """

    async def _factory(
        amount: Decimal = Decimal("100.00"),
        currency: Currency = Currency.RUB,
        status: PaymentStatus = PaymentStatus.PENDING,
        description: str | None = "Test payment",
        metadata: dict | None = None,
        idempotency_key: str | None = None,
        webhook_url: str = "https://example.com/webhook",
        processed_at: datetime | None = None,
    ) -> Payment:
        payment = Payment(
            id=uuid.uuid4(),
            amount=amount,
            currency=currency,
            status=status,
            description=description,
            metadata_=metadata,
            idempotency_key=idempotency_key or str(uuid.uuid4()),
            webhook_url=webhook_url,
            created_at=datetime.now(tz=UTC),
            updated_at=datetime.now(tz=UTC),
            processed_at=processed_at,
        )
        db_session.add(payment)
        await db_session.commit()
        await db_session.refresh(payment)
        return payment

    return _factory


@pytest_asyncio.fixture
async def make_outbox(db_session: AsyncSession):
    """
    Фабрика Outbox-записей. Возвращает callable, создающий Outbox в БД.
    """

    async def _factory(
        topic: str = "payments.new",
        payload: dict | None = None,
    ) -> Outbox:
        msg = Outbox(
            id=uuid.uuid4(),
            topic=topic,
            payload=payload or {"payment_id": str(uuid.uuid4())},
            created_at=datetime.now(tz=UTC),
        )
        db_session.add(msg)
        await db_session.commit()
        await db_session.refresh(msg)
        return msg

    return _factory
