import asyncio

import structlog
from sqlalchemy import select

from src.broker import broker
from src.database import AsyncSessionLocal
from src.logging_config import setup_logging
from src.models import Outbox

setup_logging()
logger = structlog.get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
BATCH_SIZE = 100


async def poll_and_publish():
    """Опрашивает таблицу outbox, отправляет сообщения в RabbitMQ и удаляет их."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            try:
                # Выбираем и блокируем сообщения для обработки
                stmt = (
                    select(Outbox)
                    .order_by(Outbox.created_at)
                    .limit(BATCH_SIZE)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                messages = result.scalars().all()

                if not messages:
                    return

                logger.info("Found messages in outbox to relay", count=len(messages))

                for msg in messages:
                    try:
                        await broker.publish(msg.payload, queue=msg.topic, persist=True)
                        await session.delete(msg)
                        logger.info(
                            "Relayed and deleted outbox message",
                            message_id=msg.id,
                            payload=msg.payload,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to publish message, will be retried later",
                            message_id=msg.id,
                            error=str(e),
                        )

            except Exception as e:
                logger.error(
                    "An error occurred during the outbox polling", error=str(e)
                )


MAX_RETRIES = 10
INITIAL_BACKOFF_SECONDS = 5


async def main_loop():
    """Основной цикл работы сервиса-реле."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await broker.start()
            break
        except Exception as e:
            backoff = INITIAL_BACKOFF_SECONDS * attempt
            logger.warning(
                "Failed to connect to RabbitMQ, retrying",
                attempt=attempt,
                max_retries=MAX_RETRIES,
                backoff_seconds=backoff,
                error=str(e),
            )
            if attempt == MAX_RETRIES:
                logger.error("Could not connect to RabbitMQ after max retries")
                raise
            await asyncio.sleep(backoff)

    logger.info("Starting outbox relay service")
    while True:
        await poll_and_publish()
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":

    async def _main():
        try:
            await main_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Outbox relay service stopping")
        finally:
            try:
                await broker.stop()
            except Exception:
                logger.warning("Failed to stop broker gracefully", exc_info=True)

    asyncio.run(_main())
