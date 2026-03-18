from enum import Enum
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PAID = "paid"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    PAYMENT_FAILED = "payment_failed"
    REFUNDED = "refunded"


class UserDTO(BaseModel):
    id: UUID
    email: str
    name: str
    phone: Optional[str] = None
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ProductDTO(BaseModel):
    id: UUID
    name: str
    description: str
    price: float
    stock_quantity: int
    category_id: Optional[UUID] = None
    image_urls: list[str] = Field(default_factory=list)


class OrderDTO(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: UUID
    status: OrderStatus = OrderStatus.PENDING
    total_amount: float
    items: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class PaymentDTO(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    amount: float
    currency: str = "usd"
    status: str = "pending"
    stripe_charge_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class KafkaEvent(BaseModel):
    topic: str
    payload: dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_id: UUID = Field(default_factory=uuid4)
    source_service: str = ""
