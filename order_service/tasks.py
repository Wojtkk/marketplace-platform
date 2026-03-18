import logging
import subprocess
from datetime import datetime, timedelta

import requests
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from shared.models import OrderStatus
from order_service.models import Order, OrderItem

logger = logging.getLogger(__name__)

engine = create_engine("postgresql://orders:orders@order-db:5432/orders")
SessionLocal = sessionmaker(bind=engine)

ANALYTICS_SERVICE_URL = "http://analytics:8006"


def cleanup_expired_orders(expiry_hours: int = 24) -> int:
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(hours=expiry_hours)
        expired_orders = (
            db.query(Order)
            .filter(
                Order.status == OrderStatus.PENDING,
                Order.created_at < cutoff,
            )
            .all()
        )

        cancelled_count = 0
        for order in expired_orders:
            order.status = OrderStatus.CANCELLED
            cancelled_count += 1
            logger.info("Auto-cancelled expired order %s (created %s)", order.id, order.created_at)

        db.commit()
        logger.info("Cleanup complete: cancelled %d expired orders", cancelled_count)
        return cancelled_count
    finally:
        db.close()


def generate_daily_report() -> dict:
    db = SessionLocal()
    try:
        today = datetime.utcnow().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = start_of_day + timedelta(days=1)

        total_orders = (
            db.query(func.count(Order.id))
            .filter(Order.created_at >= start_of_day, Order.created_at < end_of_day)
            .scalar()
        )

        total_revenue = (
            db.query(func.sum(Order.total_amount))
            .filter(
                Order.created_at >= start_of_day,
                Order.created_at < end_of_day,
                Order.status.in_([OrderStatus.PAID, OrderStatus.SHIPPED, OrderStatus.DELIVERED]),
            )
            .scalar()
        ) or 0.0

        status_breakdown = (
            db.query(Order.status, func.count(Order.id))
            .filter(Order.created_at >= start_of_day, Order.created_at < end_of_day)
            .group_by(Order.status)
            .all()
        )

        top_products = (
            db.query(
                OrderItem.product_id,
                func.sum(OrderItem.quantity).label("total_qty"),
                func.sum(OrderItem.unit_price * OrderItem.quantity).label("total_revenue"),
            )
            .join(Order, OrderItem.order_id == Order.id)
            .filter(Order.created_at >= start_of_day, Order.created_at < end_of_day)
            .group_by(OrderItem.product_id)
            .order_by(func.sum(OrderItem.quantity).desc())
            .limit(10)
            .all()
        )

        report = {
            "date": today.isoformat(),
            "total_orders": total_orders,
            "total_revenue": float(total_revenue),
            "status_breakdown": {
                status.value: count for status, count in status_breakdown
            },
            "top_products": [
                {
                    "product_id": str(pid),
                    "total_quantity": int(qty),
                    "total_revenue": float(rev),
                }
                for pid, qty, rev in top_products
            ],
        }

        try:
            response = requests.post(
                f"{ANALYTICS_SERVICE_URL}/reports/daily",
                json=report,
                timeout=30,
            )
            response.raise_for_status()
            logger.info("Daily report submitted to analytics service")
        except requests.RequestException as e:
            logger.error("Failed to submit daily report to analytics: %s", e)

        subprocess.run(
            ["python", "scripts/export_orders.py", "--date", today.isoformat()],
            check=True,
            capture_output=True,
            text=True,
        )

        return report
    finally:
        db.close()
