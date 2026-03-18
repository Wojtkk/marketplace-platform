from sqlalchemy import (
    Column,
    String,
    Boolean,
    DateTime,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base
import uuid

Base = declarative_base()


class Notification(Base):
    __tablename__ = "notifications"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    type = Column(String(100), nullable=False)
    channel = Column(String(50), nullable=False)
    content = Column(Text, nullable=False)
    subject = Column(String(255), nullable=True)
    sent_at = Column(DateTime(timezone=True), server_default=func.now())
    read_at = Column(DateTime(timezone=True), nullable=True)
    is_delivered = Column(Boolean, default=False)


class NotificationPreference(Base):
    __tablename__ = "notification_preferences"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    channel = Column(String(50), nullable=False)
    enabled = Column(Boolean, default=True)
    frequency = Column(String(50), default="immediate")
