import logging
from uuid import UUID

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from shared.kafka_client import KafkaConsumer
from shared.models import KafkaEvent, OrderStatus
from order_service.models import Order, OrderStatusHistory

logger = logging.getLogger(__name__)

NOTIFICATION_SERVICE_URL = "http://notification-service:8004"

engine = create_engine("postgresql://orders:orders@order-db:5432/orders")
SessionLocal = sessionmaker(bind=engine)


def get_db_session() -> Session:
    return SessionLocal()


def update_order_status(order_id: UUID, new_status: OrderStatus, changed_by: str) -> None:
    db = get_db_session()
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if order is None:
            logger.error("Order %s not found for status update", order_id)
            return

        history = OrderStatusHistory(
            order_id=order.id,
            old_status=order.status,
            new_status=new_status,
            changed_by=changed_by,
        )
        db.add(history)
        order.status = new_status
        db.commit()
        logger.info("Updated order %s status to %s", order_id, new_status.value)
    finally:
        db.close()


def send_notification(user_id: str, notification_type: str, content: str) -> None:
    try:
        requests.post(
            f"{NOTIFICATION_SERVICE_URL}/notifications/send",
            json={
                "user_id": user_id,
                "type": notification_type,
                "channel": "email",
                "content": content,
            },
            timeout=10,
        )
        logger.info("Notification sent to user %s: %s", user_id, notification_type)
    except requests.RequestException as e:
        logger.error("Failed to send notification to user %s: %s", user_id, e)


def handle_payment_completed(event: KafkaEvent) -> None:
    payload = event.payload
    order_id = UUID(payload["order_id"])
    user_id = payload.get("user_id", "")

    update_order_status(order_id, OrderStatus.PAID, changed_by="payment-completed")

    send_notification(
        user_id=user_id,
        notification_type="payment_confirmation",
        content=f"Payment for order {order_id} has been successfully processed.",
    )


def handle_payment_failed(event: KafkaEvent) -> None:
    payload = event.payload
    order_id = UUID(payload["order_id"])
    user_id = payload.get("user_id", "")
    reason = payload.get("reason", "Unknown error")

    update_order_status(order_id, OrderStatus.PAYMENT_FAILED, changed_by="payment-failed")

    send_notification(
        user_id=user_id,
        notification_type="payment_failure",
        content=f"Payment for order {order_id} failed: {reason}. Please try again.",
    )


def start_consumers() -> None:
    consumer = KafkaConsumer(group_id="order-service-consumers")
    consumer.subscribe("payment.completed", handle_payment_completed)
    consumer.subscribe("payment.failed", handle_payment_failed)
    logger.info("Order service consumers started")
    consumer.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_consumers()
