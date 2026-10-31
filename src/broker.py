from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue

from src.config import settings

# Define DLQ
dlq = RabbitQueue("payments.new.dlq", durable=True)

# Define main queue with DLQ configuration
main_queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",  # default exchange
        "x-dead-letter-routing-key": dlq.name,
    },
)

# Define the broker
broker = RabbitBroker(url=str(settings.RABBITMQ_URL))

# Define the FastStream app
app = FastStream(broker)
