import json
import logging
from typing import Any, Callable
from datetime import datetime
from uuid import uuid4

from confluent_kafka import Producer, Consumer, KafkaError
from pydantic import BaseModel

from shared.models import KafkaEvent

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = "kafka:9092", client_id: str = "marketplace"):
        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
        }
        self._producer = Producer(self._config)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Message delivery failed for topic %s: %s", msg.topic(), err)
        else:
            logger.info(
                "Message delivered to %s [partition %d] at offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def publish(self, topic: str, event: KafkaEvent) -> None:
        serialized = event.model_dump_json()
        self._producer.produce(
            topic=topic,
            value=serialized.encode("utf-8"),
            key=str(event.event_id).encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._producer.poll(0)
        logger.info("Published event %s to topic %s", event.event_id, topic)

    def publish_dict(self, topic: str, payload: dict[str, Any], source_service: str = "") -> None:
        event = KafkaEvent(
            topic=topic,
            payload=payload,
            timestamp=datetime.utcnow(),
            event_id=uuid4(),
            source_service=source_service,
        )
        self.publish(topic, event)

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self.flush(timeout=10.0)


class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        group_id: str = "marketplace-consumer",
    ):
        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self._consumer = Consumer(self._config)
        self._handlers: dict[str, Callable[[KafkaEvent], None]] = {}
        self._running = False

    def subscribe(self, topic: str, handler: Callable[[KafkaEvent], None]) -> None:
        self._handlers[topic] = handler
        topics = list(self._handlers.keys())
        self._consumer.subscribe(topics)
        logger.info("Subscribed to topic %s (total subscriptions: %d)", topic, len(topics))

    def _process_message(self, raw_message: Any) -> None:
        topic = raw_message.topic()
        handler = self._handlers.get(topic)
        if handler is None:
            logger.warning("No handler registered for topic %s", topic)
            return

        try:
            value = raw_message.value().decode("utf-8")
            event = KafkaEvent.model_validate_json(value)
            handler(event)
            self._consumer.commit(message=raw_message)
        except Exception:
            logger.exception("Error processing message from topic %s", topic)

    def run(self, poll_timeout: float = 1.0) -> None:
        self._running = True
        logger.info("Starting consumer loop for topics: %s", list(self._handlers.keys()))

        while self._running:
            msg = self._consumer.poll(timeout=poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue
            self._process_message(msg)

    def stop(self) -> None:
        self._running = False
        self._consumer.close()
        logger.info("Consumer stopped")
