# Payment Processing Service

Асинхронный сервис процессинга платежей, построенный на паттерне **Transactional Outbox** для надёжной доставки событий.

## Стек технологий

| Компонент | Технология |
|-----------|-----------|
| API | FastAPI 0.111, Python 3.11 |
| Брокер сообщений | FastStream 0.5 + RabbitMQ 3.13 |
| База данных | PostgreSQL 15 + SQLAlchemy 2.0 (asyncpg) |
| Миграции | Alembic |
| Контейнеризация | Docker + Docker Compose |
| Тесты | pytest-asyncio, pytest-httpx |
| Линтинг | Ruff |

---

## Архитектура

Сервис состоит из трёх независимых процессов, взаимодействующих через Postgres и RabbitMQ:

```
Клиент
  │
  ▼
┌─────────┐   Transactional   ┌──────────┐   RabbitMQ    ┌──────────────┐
│   API   │ ──── Outbox ────▶ │  Relay   │ ─────────────▶│   Consumer   │
│(FastAPI)│                   │ (poller) │  payments.new  │(FastStream)  │
└─────────┘                   └──────────┘                └──────────────┘
     │                              │                            │
     └──────────────────────────────┼────────────────────────────┘
                                    │
                              ┌─────┴──────┐
                              │ PostgreSQL  │
                              │  payments  │
                              │   outbox   │
                              └────────────┘
```

### Компоненты

- **`api`** — FastAPI-сервис. Принимает `POST /api/v1/payments`, в одной транзакции сохраняет `Payment` в таблицу `payments` и событие в `outbox`. Поддерживает идемпотентность через заголовок `Idempotency-Key`.
- **`relay`** — сервис-реле. Периодически (каждые 5 сек) опрашивает таблицу `outbox`, публикует события в RabbitMQ и удаляет их. Изолирует API от сбоев брокера, гарантирует доставку **at-least-once**. Сообщения публикуются с `persist=True` — они переживают рестарт RabbitMQ.
- **`consumer`** — FastStream-подписчик. Получает `payment_id` из очереди `payments.new`, эмулирует обработку (90% успех), **сначала** обновляет статус в БД и делает `commit`, **затем** отправляет webhook клиенту. Поддерживает идемпотентность: при повторной доставке сообщения для платежа в терминальном статусе (`succeeded` / `failed`) повторяется только отправка webhook без повторной обработки. Неудачные webhook-отправки уходят в DLQ `payments.new.dlq`.

### Гарантии доставки relay

Relay публикует сообщения в RabbitMQ с параметром `persist=True`. Это означает, что сообщения не теряются при рестарте RabbitMQ — они сохраняются на диск и будут доставлены после восстановления. Publisher confirms включены по умолчанию в aio-pika.

#### Переживание сбоев брокера

Рестарт RabbitMQ переживается корректно: `persist=True` + durable-очереди + publisher confirms (включены в aio-pika по умолчанию) — persistent-сообщения восстанавливаются с диска, unacked-сообщения доставляются повторно, идемпотентный consumer это закрывает.

Пересоздание стека (`docker-compose down && docker-compose up`) стирает очереди и сообщения: у сервиса `rabbitmq` нет тома. Решение — named volume при необходимости:

```yaml
services:
  rabbitmq:
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
volumes:
  rabbitmq_data:
```

После добавления тома `docker-compose down` (без `-v`) и последующий `up` сохранят durable-очереди и persistent-сообщения.

---

## Схема взаимодействия

```
1. POST /api/v1/payments
       │
2.     ▼  (одна транзакция)
   payments ← INSERT Payment (status=pending)
   outbox   ← INSERT Outbox  (payload={payment_id})
       │
3.     ▼  Relay (каждые 5 сек, SELECT ... FOR UPDATE SKIP LOCKED)
   RabbitMQ ← PUBLISH {payment_id} → queue: payments.new (persist=True)
   outbox   ← DELETE
       │
4.     ▼  Consumer
   payments ← UPDATE status = succeeded | failed
   payments ← SET processed_at = NOW()
   await session.commit()  ← commit ПЕРЕД webhook
   webhook  ← POST {payment_id, status, updated_at}  ← финальный статус
       │
5.     ▼  При повторной доставке (idempotency)
   if status in (succeeded, failed):
       webhook  ← POST {payment_id, status, updated_at}  ← без повторной обработки
```

### Порядок операций в consumer

1. Загрузка платежа из БД с `FOR UPDATE` (row lock). При at-least-once доставке дубль сообщения ждёт освобождения row lock (коммит или откат первого обработчика) и видит терминальный статус — двойной обработки и перезаписи статуса больше нет.
2. Если статус терминальный (`succeeded` / `failed`) — row lock отпускается (`expunge` + `rollback`), затем **только** отправка webhook, возврат.
3. Эмуляция обработки (случайная задержка 2–5 сек, 90% успех).
4. Установка нового статуса и `processed_at`.
5. `session.commit()` + `session.refresh()`.
6. Отправка webhook с телом `{payment_id, status, updated_at}` — статус в payload уже финальный.

---

## Масштабирование

Сервисы спроектированы для горизонтального масштабирования, доработки кода для запуска нескольких экземпляров не требуются:

- **API** — stateless; идемпотентность `Idempotency-Key` обеспечивает уникальный индекс в PostgreSQL, гонки между репликами разруливаются базой.
- **Consumer** — несколько экземпляров работают как competing consumers одной durable-очереди; конкурентные дубли одного платежа сериализуются row lock (`FOR UPDATE`).
- **Relay** — `FOR UPDATE SKIP LOCKED` позволяет запускать несколько поллеров outbox без конфликтов и дублей публикации.
- Доставка at-least-once с персистентными сообщениями — падение любого экземпляра не теряет платежи.

Масштабирование — прежде всего задача деплоя. Что нужно сделать на уровне инфраструктуры:

- **Docker Compose**: убрать фиксированные `container_name` и проброску портов у масштабируемых сервисов (`--scale`), вынести миграции (`alembic upgrade head`) в отдельный шаг перед стартом реплик, поставить балансировщик (nginx) перед API.
- **Kubernetes**: Deployment для api/consumer/relay; конфигурация уже читается из переменных окружения (Secret/ConfigMap); миграции — Job/initContainer; `terminationGracePeriodSeconds` ≥ 60 с (покрывает эмуляцию и ретраи webhook); автоскейл consumer — KEDA по глубине очереди.

Возможные доработки кода — незначительные и только по мере роста числа экземпляров: настраиваемый из env пул соединений к БД (дефолт asyncpg при многих репликах упирается в `max_connections` PostgreSQL), проверка БД в `/health` для readiness-проб, опционально health-эндпоинты в consumer/relay (без них liveness делается exec-пробами — тоже без правок кода). Для отказоустойчивости самого брокера при кластере RabbitMQ — quorum-очереди.

---

## Быстрый старт

### Требования

- Docker & Docker Compose
- (для локальной разработки) Python 3.11+, [Poetry](https://python-poetry.org/)

### 1. Настройка окружения

```bash
cp .env.example .env
# Отредактируйте .env при необходимости
```

Переменные окружения:

| Переменная | Описание | Пример |
|------------|----------|--------|
| `POSTGRES_USER` | Пользователь PostgreSQL | `user` |
| `POSTGRES_PASSWORD` | Пароль PostgreSQL | `password` |
| `POSTGRES_DB` | Имя базы данных | `payments` |
| `RABBITMQ_DEFAULT_USER` | Пользователь RabbitMQ | `user` |
| `RABBITMQ_DEFAULT_PASS` | Пароль RabbitMQ | `password` |
| `API_KEY` | Ключ авторизации API (`X-API-Key`) | `super-secret-api-key` |

### 2. Запуск через Docker Compose

```bash
docker-compose up --build
```

Это запустит все сервисы: Postgres, RabbitMQ, API, Relay, Consumer.
Миграции применяются автоматически при старте `api`.

**Адреса сервисов после запуска:**

| Сервис | URL |
|--------|-----|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| ReDoc | http://localhost:8000/redoc |
| RabbitMQ Management UI | http://localhost:15672 |

### 3. Остановка

```bash
docker-compose down          # остановить контейнеры
docker-compose down -v       # + удалить volumes (данные БД)
```

---

## Локальная разработка

### Установка зависимостей

```bash
poetry install
```

### Применение миграций

```bash
# Убедитесь что Postgres запущен (например через docker-compose up postgres)
poetry run alembic upgrade head
```

### Создание новой миграции

```bash
poetry run alembic revision --autogenerate -m "описание изменения"
```

### Запуск сервисов локально

```bash
# API
poetry run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Consumer
poetry run faststream run src.consumer.main:app

# Relay
poetry run python -m src.relay.main
```

### Локальная проверка webhook

Для удобной отладки webhook предусмотрен devtools-сервис `webhook-receiver`.
Сервис только для разработки: он логирует полные тела и заголовки входящих запросов —
не направляйте на него реальные вебхуки с секретами.

**В Docker Compose:**
```bash
# Старт всех сервисов (включая webhook-receiver)
docker-compose up --build
```
Используйте в запросе:
```
webhook_url = http://webhook-receiver:9000/webhook
```

**Локально без Docker:**
```bash
poetry run uvicorn src.devtools.webhook_receiver:app --port 9000
```
Используйте:
```
webhook_url = http://localhost:9000/webhook
```

Просмотр полученных webhook:
```
GET http://localhost:9000/webhooks
```

Для проверки retry и DLQ — используйте эндпоинт, отвечающий 500:
```
webhook_url = http://localhost:9000/webhook/fail
```

---

## Dead Letter Queue (DLQ)

При неудачной отправке webhook (исчерпаны все попытки tenacity retry) сообщение попадает в очередь `payments.new.dlq`. Consumer объявляет эту очередь и принимает сообщения обработчиком `handle_dead_letter`, который логирует `payment_id` и тело сообщения.

### Replay сообщений из DLQ

Текущая процедура через Get Messages нерабочая: у очереди `payments.new.dlq` есть live-подписчик `handle_dead_letter` — сообщения из DLQ потребляются и логируются сразу и не накапливаются; Get Messages в Management UI — деструктивное чтение, оно не возвращает сообщение в `payments.new`. Рабочая процедура — републикация в `payments.new`:

- Management UI: Queues -> payments.new -> Publish message, payload `{"payment_id": "<uuid>"}`, `content_type: application/json`;
- или HTTP API (логин/пароль из `.env`):

  ```bash
  curl -u user:password -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
    -H "Content-Type: application/json" \
    -d '{"properties":{"content_type":"application/json"},"routing_key":"payments.new","payload":"{\"payment_id\": \"<uuid>\"}","payload_encoding":"string"}'
  ```

- ожидаемый результат для платежа в терминальном статусе (типичный случай для сообщений из DLQ): в логах consumer «Payment already in terminal state, sending webhook only»; статус и `processed_at` платежа не меняются; webhook отправляется повторно (at-least-once); если `webhook_url` по-прежнему падает — сообщение снова уйдёт в DLQ. Если платёж ещё `pending` — дубль дождётся освобождения row lock и корректно дообработается; если платёж удалён — consumer залогирует «Payment not found» и подтвердит сообщение.

---

## Тестирование

### Запуск тестов локально (без Postgres/RabbitMQ)

Тесты используют **SQLite in-memory** — внешние сервисы не нужны:

```bash
# Установить зависимости (включая aiosqlite)
poetry install
pip install aiosqlite   # если не установлен

# Запустить все тесты
poetry run pytest

# С подробным выводом
poetry run pytest -v

# Только API-тесты
poetry run pytest tests/api/ -v

# Только юнит-тесты (consumer + relay)
poetry run pytest tests/consumer/ tests/relay/ -v
```

### Запуск тестов в Docker (с реальным Postgres)

```bash
docker-compose --profile test up --build --abort-on-container-exit
```

### Структура тестов

```
tests/
├── conftest.py              # Глобальные фикстуры: БД, клиенты, фабрики
├── api/
│   └── test_payments.py    # Интеграционные тесты API
├── consumer/
│   └── test_consumer.py    # Юнит-тесты consumer-логики
├── relay/
│   └── test_relay.py       # Юнит-тесты relay-сервиса
└── devtools/               # Тесты devtools-сервисов
    └── test_webhook_receiver.py # Тесты сервиса приёма вебхуков
```

### Линтинг и форматирование

```bash
# Проверка
poetry run ruff check .
poetry run ruff format --check .

# Автоисправление
poetry run ruff check . --fix
poetry run ruff format .
```

---

## API Reference

### Авторизация

Все запросы к `/api/v1/*` требуют заголовок `X-API-Key`.

### Endpoints

#### `POST /api/v1/payments` — Создать платёж

**Заголовки:**

| Заголовок | Описание |
|-----------|----------|
| `X-API-Key` | API-ключ авторизации |
| `Idempotency-Key` | Уникальный ключ для идемпотентности (UUID) |

**Тело запроса:**

```json
{
  "amount": "1500.50",
  "currency": "RUB",
  "description": "Оплата заказа #123",
  "webhook_url": "https://example.com/webhook",
  "metadata": {"customer_id": "user-123"}
}
```

| Поле | Тип | Описание |
|------|-----|----------|
| `amount` | `decimal` | Сумма платежа, > 0, максимум 99999999.99 |
| `currency` | `enum` | `RUB`, `USD`, `EUR` |
| `description` | `string?` | Описание (до 255 символов) |
| `metadata` | `object?` | Произвольные метаданные |
| `webhook_url` | `url` | URL для отправки результата |

**Ответы:**

- `202 Accepted` — платёж принят в обработку (новый)
- `200 OK` — платёж уже существует с таким `Idempotency-Key` (возвращает существующий)
- `401 Unauthorized` — неверный API-ключ
- `409 Conflict` — конфликт при создании (уникальность idempotency_key нарушена)
- `422 Unprocessable Entity` — ошибка валидации

#### `GET /api/v1/payments/{payment_id}` — Получить платёж

- `200 OK` — платёж найден
- `404 Not Found` — платёж не существует

**Ответ (200):**

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `uuid` | Идентификатор платежа |
| `amount` | `decimal` | Сумма |
| `currency` | `enum` | Валюта |
| `status` | `enum` | Статус платежа |
| `description` | `string?` | Описание |
| `metadata` | `object?` | Метаданные (из column БД "metadata") |
| `webhook_url` | `url` | URL вебхука |
| `created_at` | `datetime` | Время создания |
| `updated_at` | `datetime` | Время последнего обновления |
| `processed_at` | `datetime?` | Время завершения обработки (set при SUCCEEDED/FAILED) |

#### `GET /health` — Healthcheck

```json
{"status": "ok"}
```

### Статусы платежа

| Статус | Описание |
|--------|----------|
| `pending` | Принят, ожидает обработки |
| `succeeded` | Успешно обработан |
| `failed` | Обработка завершилась ошибкой |

---

## Примеры запросов

### Создание платежа

```bash
curl -X POST "http://localhost:8000/api/v1/payments" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: super-secret-api-key" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Оплата заказа #123",
    "webhook_url": "https://webhook.site/YOUR_UNIQUE_ID",
    "metadata": {"customer_id": "user-123"}
  }'
```

**Ответ (202):**
```json
{
  "payment_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "pending",
  "created_at": "2026-08-21T01:00:00Z"
}
```

### Получение статуса платежа

```bash
curl -X GET "http://localhost:8000/api/v1/payments/3fa85f64-5717-4562-b3fc-2c963f66afa6" \
  -H "X-API-Key: super-secret-api-key"
```

**Ответ (200):**
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "amount": "1500.50",
  "currency": "RUB",
  "status": "succeeded",
  "description": "Оплата заказа #123",
  "metadata": {"customer_id": "user-123"},
  "webhook_url": "https://webhook.site/YOUR_UNIQUE_ID",
  "created_at": "2026-08-21T01:00:00Z",
  "updated_at": "2026-08-21T01:00:05Z",
  "processed_at": "2026-08-21T01:00:05Z"
}
```

### Тестирование webhook

В Docker Compose запущен локальный webhook-receiver. Используйте `http://webhook-receiver:9000/webhook` как `webhook_url` при создании платежа.

Просмотр полученных вебхуков:
```bash
curl http://localhost:9000/webhooks
```

Для проверки retry/DLQ — используйте `http://localhost:9000/webhook/fail` (возвращает 500).

Consumer отправляет POST на `webhook_url` с телом:
```json
{
  "payment_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "succeeded",
  "updated_at": "2026-08-21T01:00:05Z"
}
```

Статус в payload — финальный (`succeeded` или `failed`), уже закоммиченный в БД.
