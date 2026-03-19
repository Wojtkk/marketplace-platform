import logging
from typing import Any
from uuid import UUID

import requests
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from shared.kafka_client import KafkaProducer
from shared.models import OrderStatus, KafkaEvent, OrderDTO
from order_service.models import Base, Order, OrderItem, OrderStatusHistory, OrderReturn

logger = logging.getLogger(__name__)

app = FastAPI(title="Order Service", version="1.0.0")

CATALOG_SERVICE_URL = "http://catalog-service:8001"
PAYMENT_SERVICE_URL = "http://payment-service:8003"

kafka_producer = KafkaProducer(client_id="order-service")


def get_db() -> Session:
    engine = create_engine("postgresql://orders:orders@order-db:5432/orders")
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_product_stock(product_id: UUID, requested_quantity: int) -> dict[str, Any]:
    response = requests.get(
        f"{CATALOG_SERVICE_URL}/products/{product_id}",
        timeout=10,
    )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    response.raise_for_status()

    product = response.json()
    if product["stock_quantity"] < requested_quantity:
        raise HTTPException(
            status_code=409,
            detail=f"Insufficient stock for product {product_id}. "
                   f"Available: {product['stock_quantity']}, requested: {requested_quantity}",
        )
    return product


def initiate_payment(order_id: UUID, amount: float, user_id: UUID) -> dict[str, Any]:
    response = requests.post(
        f"{PAYMENT_SERVICE_URL}/payments/charge",
        json={
            "order_id": str(order_id),
            "amount": amount,
            "currency": "usd",
            "user_id": str(user_id),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def record_status_change(
    db: Session, order: Order, new_status: OrderStatus, changed_by: str = "system"
) -> None:
    history = OrderStatusHistory(
        order_id=order.id,
        old_status=order.status,
        new_status=new_status,
        changed_by=changed_by,
    )
    db.add(history)
    order.status = new_status


@app.post("/orders")
def create_order(
    order_data: dict[str, Any],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    user_id = UUID(order_data["user_id"])
    items_data = order_data.get("items", [])
    if not items_data:
        raise HTTPException(status_code=422, detail="Order must contain at least one item")

    total_amount = 0.0
    verified_items = []

    for item in items_data:
        product_id = UUID(item["product_id"])
        quantity = item["quantity"]
        product = verify_product_stock(product_id, quantity)
        unit_price = product["price"]
        total_amount += unit_price * quantity
        verified_items.append({
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
        })

    order = Order(
        user_id=user_id,
        status=OrderStatus.PENDING,
        total_amount=total_amount,
        shipping_address=order_data.get("shipping_address"),
    )
    db.add(order)
    db.flush()

    for vi in verified_items:
        order_item = OrderItem(
            order_id=order.id,
            product_id=vi["product_id"],
            quantity=vi["quantity"],
            unit_price=vi["unit_price"],
        )
        db.add(order_item)

    record_status_change(db, order, OrderStatus.CONFIRMED, changed_by="order-creation")
    db.commit()
    db.refresh(order)

    kafka_producer.publish_dict(
        topic="order.created",
        payload={
            "order_id": str(order.id),
            "user_id": str(user_id),
            "total_amount": total_amount,
            "items": [
                {"product_id": str(vi["product_id"]), "quantity": vi["quantity"]}
                for vi in verified_items
            ],
        },
        source_service="order-service",
    )

    try:
        payment_result = initiate_payment(order.id, total_amount, user_id)
        logger.info("Payment initiated for order %s: %s", order.id, payment_result)
    except requests.RequestException as e:
        logger.error("Failed to initiate payment for order %s: %s", order.id, e)

    return {
        "id": str(order.id),
        "status": order.status.value,
        "total_amount": order.total_amount,
        "items_count": len(verified_items),
    }


@app.get("/orders/{order_id}")
def get_order(
    order_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == order_id).first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    return {
        "id": str(order.id),
        "user_id": str(order.user_id),
        "status": order.status.value,
        "total_amount": order.total_amount,
        "shipping_address": order.shipping_address,
        "created_at": order.created_at.isoformat() if order.created_at else None,
        "items": [
            {
                "product_id": str(item.product_id),
                "quantity": item.quantity,
                "unit_price": item.unit_price,
                "subtotal": item.subtotal,
            }
            for item in order.items
        ],
    }


@app.get("/orders")
def list_orders(
    user_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    orders = (
        db.query(Order)
        .filter(Order.user_id == user_id)
        .order_by(Order.created_at.desc())
        .all()
    )
    return {
        "orders": [
            {
                "id": str(o.id),
                "status": o.status.value,
                "total_amount": o.total_amount,
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "items_count": len(o.items),
            }
            for o in orders
        ]
    }


@app.post("/orders/{order_id}/cancel")
def cancel_order(
    order_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == order_id).first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    non_cancellable = {OrderStatus.SHIPPED, OrderStatus.DELIVERED, OrderStatus.CANCELLED}
    if order.status in non_cancellable:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel order in status {order.status.value}",
        )

    record_status_change(db, order, OrderStatus.CANCELLED, changed_by="user-cancellation")
    db.commit()

    kafka_producer.publish_dict(
        topic="order.cancelled",
        payload={
            "order_id": str(order.id),
            "user_id": str(order.user_id),
            "previous_status": order.status.value,
        },
        source_service="order-service",
    )

    logger.info("Order %s cancelled", order_id)
    return {"id": str(order.id), "status": "cancelled"}


@app.post("/orders/{order_id}/return")
def return_order(
    order_id: UUID,
    return_data: dict[str, Any],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == order_id).first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    if order.status != OrderStatus.DELIVERED:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot return order in status {order.status.value}",
        )

    reason = return_data.get("reason", "")
    refund_amount = return_data.get("refund_amount", order.total_amount)

    order_return = OrderReturn(
        order_id=order.id,
        reason=reason,
        refund_amount=refund_amount,
    )
    db.add(order_return)

    record_status_change(db, order, OrderStatus.CANCELLED, changed_by="return-request")
    db.commit()

    # Cross-repo: request refund from payment service
    try:
        refund_response = requests.post(
            f"{PAYMENT_SERVICE_URL}/payments/refund",
            json={
                "order_id": str(order.id),
                "amount": refund_amount,
                "reason": reason,
            },
            timeout=15,
        )
        refund_response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Refund request failed for order %s: %s", order_id, e)

    # Cross-repo: restock items via catalog service
    for item in order.items:
        try:
            requests.put(
                f"{CATALOG_SERVICE_URL}/products/{item.product_id}/restock",
                json={"quantity": item.quantity},
                timeout=10,
            )
        except requests.RequestException as e:
            logger.error("Restock failed for product %s: %s", item.product_id, e)

    kafka_producer.publish_dict(
        topic="order.returned",
        payload={
            "order_id": str(order.id),
            "refund_amount": refund_amount,
            "reason": reason,
        },
        source_service="order-service",
    )

    return {
        "id": str(order_return.id),
        "order_id": str(order.id),
        "status": "return_initiated",
        "refund_amount": refund_amount,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "orders"}
