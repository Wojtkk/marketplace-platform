import logging
import os
from datetime import datetime
from typing import Any
from uuid import UUID

import requests
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from shared.kafka_client import KafkaConsumer
from shared.models import KafkaEvent
from notification_service.models import Base, Notification, NotificationPreference

logger = logging.getLogger(__name__)

app = FastAPI(title="Notification Service", version="1.0.0")

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "SG.placeholder")
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "AC_placeholder")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "placeholder_token")
TWILIO_FROM_NUMBER = os.environ.get("TWILIO_FROM_NUMBER", "+15551234567")

engine = create_engine("postgresql://notifications:notifications@notification-db:5432/notifications")
SessionLocal = sessionmaker(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def send_email(to: str, subject: str, body: str) -> bool:
    try:
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": to}]}],
                "from": {"email": "noreply@marketplace.com", "name": "Marketplace"},
                "subject": subject,
                "content": [{"type": "text/html", "value": body}],
            },
            timeout=15,
        )
        if response.status_code in (200, 202):
            logger.info("Email sent to %s: %s", to, subject)
            return True
        logger.error("SendGrid error %d: %s", response.status_code, response.text)
        return False
    except requests.RequestException as e:
        logger.error("Failed to send email to %s: %s", to, e)
        return False


def send_sms(phone: str, message: str) -> bool:
    try:
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            data={
                "From": TWILIO_FROM_NUMBER,
                "To": phone,
                "Body": message,
            },
            timeout=15,
        )
        if response.status_code == 201:
            logger.info("SMS sent to %s", phone)
            return True
        logger.error("Twilio error %d: %s", response.status_code, response.text)
        return False
    except requests.RequestException as e:
        logger.error("Failed to send SMS to %s: %s", phone, e)
        return False


def check_user_preference(db: Session, user_id: UUID, channel: str) -> bool:
    pref = (
        db.query(NotificationPreference)
        .filter(
            NotificationPreference.user_id == user_id,
            NotificationPreference.channel == channel,
        )
        .first()
    )
    if pref is None:
        return True
    return pref.enabled


@app.post("/notifications/send")
def send_notification(
    notification_data: dict[str, Any],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    user_id = UUID(notification_data["user_id"])
    channel = notification_data.get("channel", "email")
    content = notification_data["content"]
    notification_type = notification_data.get("type", "general")
    subject = notification_data.get("subject", "Marketplace Notification")

    if not check_user_preference(db, user_id, channel):
        return {"status": "skipped", "reason": "User has disabled this channel"}

    notification = Notification(
        user_id=user_id,
        type=notification_type,
        channel=channel,
        content=content,
        subject=subject,
    )

    delivered = False
    if channel == "email":
        recipient_email = notification_data.get("email", f"{user_id}@marketplace.com")
        delivered = send_email(recipient_email, subject, content)
    elif channel == "sms":
        phone = notification_data.get("phone")
        if phone:
            delivered = send_sms(phone, content)
        else:
            logger.warning("No phone number provided for SMS notification to user %s", user_id)

    notification.is_delivered = delivered
    db.add(notification)
    db.commit()
    db.refresh(notification)

    return {
        "notification_id": str(notification.id),
        "status": "delivered" if delivered else "failed",
        "channel": channel,
    }


@app.get("/notifications/{user_id}")
def get_user_notifications(
    user_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    notifications = (
        db.query(Notification)
        .filter(Notification.user_id == user_id)
        .order_by(Notification.sent_at.desc())
        .limit(50)
        .all()
    )
    return {
        "notifications": [
            {
                "id": str(n.id),
                "type": n.type,
                "channel": n.channel,
                "content": n.content,
                "sent_at": n.sent_at.isoformat() if n.sent_at else None,
                "read_at": n.read_at.isoformat() if n.read_at else None,
                "is_delivered": n.is_delivered,
            }
            for n in notifications
        ]
    }


@app.post("/notifications/{notification_id}/read")
def mark_as_read(
    notification_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    notification = db.query(Notification).filter(Notification.id == notification_id).first()
    if notification is None:
        raise HTTPException(status_code=404, detail="Notification not found")

    notification.read_at = datetime.utcnow()
    db.commit()
    return {"status": "marked_as_read"}


def handle_order_created(event: KafkaEvent) -> None:
    payload = event.payload
    user_id = payload.get("user_id", "")
    order_id = payload.get("order_id", "")
    total = payload.get("total_amount", 0)
    items_count = len(payload.get("items", []))

    db = SessionLocal()
    try:
        content = (
            f"<h2>Order Confirmation</h2>"
            f"<p>Your order <strong>{order_id}</strong> has been placed successfully.</p>"
            f"<p>Items: {items_count} | Total: ${total:.2f}</p>"
            f"<p>We'll notify you when your payment is processed.</p>"
        )

        notification = Notification(
            user_id=UUID(user_id) if user_id else None,
            type="order_confirmation",
            channel="email",
            content=content,
            subject=f"Order Confirmation - {order_id}",
        )

        delivered = send_email(
            f"{user_id}@marketplace.com",
            f"Order Confirmation - {order_id}",
            content,
        )
        notification.is_delivered = delivered
        db.add(notification)
        db.commit()
    finally:
        db.close()


def start_consumers() -> None:
    consumer = KafkaConsumer(group_id="notification-service-consumers")
    consumer.subscribe("order.created", handle_order_created)
    logger.info("Notification service consumers started")
    consumer.run()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "notifications"}
