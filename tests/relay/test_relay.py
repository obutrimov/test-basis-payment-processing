"""
Юнит-тесты для src/relay/main.py.

Покрывают:
  - poll_and_publish: пустая очередь, успешная отправка и удаление,
    частичный сбой публикации одного сообщения
  - main_loop: успешное подключение к RabbitMQ, retry-логика при сбоях
"""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.relay.main import poll_and_publish

# ---------------------------------------------------------------------------
# Вспомогательные фабрики
# ---------------------------------------------------------------------------


def make_outbox_msg(topic: str = "payments.new") -> MagicMock:
    msg = MagicMock()
    msg.id = uuid.uuid4()
    msg.topic = topic
    msg.payload = {"payment_id": str(uuid.uuid4())}
    msg.created_at = datetime.now(tz=UTC)
    return msg


# ===========================================================================
# poll_and_publish
# ===========================================================================


async def test_poll_and_publish_empty_outbox():
    """Если outbox пуст — broker.publish не вызывается."""
    mock_broker = AsyncMock()

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin)

    with (
        patch("src.relay.main.AsyncSessionLocal", return_value=mock_cm),
        patch("src.relay.main.broker", mock_broker),
    ):
        await poll_and_publish()

    mock_broker.publish.assert_not_awaited()


async def test_poll_and_publish_sends_and_deletes_messages():
    """Каждое сообщение публикуется и затем удаляется из outbox."""
    messages = [make_outbox_msg() for _ in range(3)]

    mock_broker = AsyncMock()

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = messages

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.delete = AsyncMock()

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin)

    with (
        patch("src.relay.main.AsyncSessionLocal", return_value=mock_cm),
        patch("src.relay.main.broker", mock_broker),
    ):
        await poll_and_publish()

    assert mock_broker.publish.await_count == 3
    assert mock_session.delete.await_count == 3


async def test_poll_and_publish_publishes_correct_payload():
    """broker.publish вызывается с payload и queue из outbox-сообщения."""
    msg = make_outbox_msg(topic="payments.new")
    mock_broker = AsyncMock()

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [msg]

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.delete = AsyncMock()

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin)

    with (
        patch("src.relay.main.AsyncSessionLocal", return_value=mock_cm),
        patch("src.relay.main.broker", mock_broker),
    ):
        await poll_and_publish()

    mock_broker.publish.assert_awaited_once_with(
        msg.payload, queue=msg.topic, persist=True
    )


async def test_poll_and_publish_skips_failed_message():
    """
    Если publish для одного сообщения упал — оно остаётся в outbox,
    а следующие сообщения продолжают обрабатываться.
    """
    good_msg = make_outbox_msg()
    bad_msg = make_outbox_msg()
    messages = [bad_msg, good_msg]

    publish_call_count = 0

    async def side_effect_publish(payload, *, queue, persist):
        nonlocal publish_call_count
        publish_call_count += 1
        if payload == bad_msg.payload:
            raise Exception("RabbitMQ unavailable")

    mock_broker = AsyncMock()
    mock_broker.publish = AsyncMock(side_effect=side_effect_publish)

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = messages

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.delete = AsyncMock()

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin)

    with (
        patch("src.relay.main.AsyncSessionLocal", return_value=mock_cm),
        patch("src.relay.main.broker", mock_broker),
    ):
        # Не должно бросить исключение
        await poll_and_publish()

    # Только good_msg был удалён
    deleted_calls = mock_session.delete.await_args_list
    assert len(deleted_calls) == 1
    assert deleted_calls[0] == call(good_msg)


async def test_poll_and_publish_outer_exception_handled():
    """Если session.execute бросает — poll_and_publish не падает наружу."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=RuntimeError("DB error"))

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin)

    with patch("src.relay.main.AsyncSessionLocal", return_value=mock_cm):
        await poll_and_publish()  # Должна завершиться без raise


# ===========================================================================
# main_loop — retry-логика подключения к RabbitMQ
# ===========================================================================


async def test_main_loop_connects_on_first_attempt():
    """main_loop подключается к брокеру и входит в цикл."""
    mock_broker = AsyncMock()
    mock_broker.start = AsyncMock()

    call_count = 0

    async def fake_poll():
        nonlocal call_count
        call_count += 1
        if call_count >= 1:
            raise asyncio.CancelledError

    import asyncio

    from src.relay.main import main_loop

    with (
        patch("src.relay.main.broker", mock_broker),
        patch("src.relay.main.poll_and_publish", side_effect=fake_poll),
        patch("src.relay.main.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(asyncio.CancelledError):
            await main_loop()

    mock_broker.start.assert_awaited_once()


async def test_main_loop_retries_on_broker_connection_failure():
    """main_loop повторяет подключение при сбое и в итоге подключается."""
    import asyncio

    mock_broker = AsyncMock()
    attempt = 0

    async def flaky_start():
        nonlocal attempt
        attempt += 1
        if attempt < 3:
            raise Exception("Connection refused")

    mock_broker.start = AsyncMock(side_effect=flaky_start)

    call_count = 0

    async def fake_poll():
        nonlocal call_count
        call_count += 1
        raise asyncio.CancelledError

    from src.relay.main import main_loop

    with (
        patch("src.relay.main.broker", mock_broker),
        patch("src.relay.main.poll_and_publish", side_effect=fake_poll),
        patch("src.relay.main.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(asyncio.CancelledError):
            await main_loop()

    assert mock_broker.start.await_count == 3


async def test_main_loop_raises_after_max_retries():
    """После MAX_RETRIES неудачных попыток main_loop бросает исключение."""

    mock_broker = AsyncMock()
    mock_broker.start = AsyncMock(side_effect=Exception("Always fails"))

    from src.relay.main import MAX_RETRIES, main_loop

    with (
        patch("src.relay.main.broker", mock_broker),
        patch("src.relay.main.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(Exception, match="Always fails"):
            await main_loop()

    assert mock_broker.start.await_count == MAX_RETRIES
