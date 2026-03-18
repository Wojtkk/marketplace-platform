import logging
import os
from uuid import UUID

import requests
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from shared.kafka_client import KafkaProducer
from payment_service.models import Base, Payment, Refund

logger = logging.getLogger(__name__)

app = Flask(__name__)

STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "sk_test_placeholder")
STRIPE_API_URL = "https://api.stripe.com/v1"

engine = create_engine("postgresql://payments:payments@payment-db:5432/payments")
SessionLocal = sessionmaker(bind=engine)

kafka_producer = KafkaProducer(client_id="payment-service")


def get_db() -> Session:
    return SessionLocal()


def charge_stripe(amount: float, currency: str, source: str = "tok_visa") -> dict:
    response = requests.post(
        f"{STRIPE_API_URL}/charges",
        auth=(STRIPE_API_KEY, ""),
        data={
            "amount": int(amount * 100),
            "currency": currency,
            "source": source,
            "description": "Marketplace payment",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def refund_stripe(charge_id: str, amount: float) -> dict:
    response = requests.post(
        f"{STRIPE_API_URL}/refunds",
        auth=(STRIPE_API_KEY, ""),
        data={
            "charge": charge_id,
            "amount": int(amount * 100),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


@app.route("/payments/charge", methods=["POST"])
def process_payment():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body required"}), 400

    order_id = data.get("order_id")
    amount = data.get("amount")
    currency = data.get("currency", "usd")
    user_id = data.get("user_id")

    if not order_id or not amount:
        return jsonify({"error": "order_id and amount are required"}), 422

    db = get_db()
    try:
        payment = Payment(
            order_id=UUID(order_id),
            amount=amount,
            currency=currency,
            status="processing",
        )
        db.add(payment)
        db.commit()
        db.refresh(payment)

        try:
            stripe_result = charge_stripe(amount, currency)
            payment.stripe_charge_id = stripe_result["id"]
            payment.status = "completed"
            db.commit()

            kafka_producer.publish_dict(
                topic="payment.completed",
                payload={
                    "payment_id": str(payment.id),
                    "order_id": order_id,
                    "user_id": user_id or "",
                    "amount": amount,
                    "currency": currency,
                    "stripe_charge_id": stripe_result["id"],
                },
                source_service="payment-service",
            )

            logger.info(
                "Payment %s completed for order %s (stripe: %s)",
                payment.id,
                order_id,
                stripe_result["id"],
            )
            return jsonify({
                "payment_id": str(payment.id),
                "status": "completed",
                "stripe_charge_id": stripe_result["id"],
            }), 200

        except requests.RequestException as e:
            payment.status = "failed"
            payment.failure_reason = str(e)
            db.commit()

            kafka_producer.publish_dict(
                topic="payment.failed",
                payload={
                    "payment_id": str(payment.id),
                    "order_id": order_id,
                    "user_id": user_id or "",
                    "amount": amount,
                    "reason": str(e),
                },
                source_service="payment-service",
            )

            logger.error("Payment failed for order %s: %s", order_id, e)
            return jsonify({
                "payment_id": str(payment.id),
                "status": "failed",
                "error": str(e),
            }), 402

    finally:
        db.close()


@app.route("/payments/<payment_id>", methods=["GET"])
def get_payment(payment_id: str):
    db = get_db()
    try:
        payment = db.query(Payment).filter(Payment.id == UUID(payment_id)).first()
        if payment is None:
            return jsonify({"error": "Payment not found"}), 404

        return jsonify({
            "id": str(payment.id),
            "order_id": str(payment.order_id),
            "amount": payment.amount,
            "currency": payment.currency,
            "status": payment.status,
            "stripe_charge_id": payment.stripe_charge_id,
            "created_at": payment.created_at.isoformat() if payment.created_at else None,
            "refunds": [
                {
                    "id": str(r.id),
                    "amount": r.amount,
                    "reason": r.reason,
                    "status": r.status,
                }
                for r in payment.refunds
            ],
        }), 200
    finally:
        db.close()


@app.route("/payments/refund", methods=["POST"])
def process_refund():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body required"}), 400

    payment_id = data.get("payment_id")
    amount = data.get("amount")
    reason = data.get("reason", "")

    if not payment_id:
        return jsonify({"error": "payment_id is required"}), 422

    db = get_db()
    try:
        payment = db.query(Payment).filter(Payment.id == UUID(payment_id)).first()
        if payment is None:
            return jsonify({"error": "Payment not found"}), 404

        if payment.status != "completed":
            return jsonify({"error": "Can only refund completed payments"}), 409

        refund_amount = amount if amount else payment.amount
        if refund_amount > payment.amount:
            return jsonify({"error": "Refund amount exceeds payment amount"}), 422

        refund = Refund(
            payment_id=payment.id,
            amount=refund_amount,
            reason=reason,
            status="processing",
        )
        db.add(refund)
        db.commit()
        db.refresh(refund)

        try:
            stripe_result = refund_stripe(payment.stripe_charge_id, refund_amount)
            refund.stripe_refund_id = stripe_result["id"]
            refund.status = "completed"
            payment.status = "refunded"
            db.commit()

            logger.info("Refund %s completed for payment %s", refund.id, payment.id)
            return jsonify({
                "refund_id": str(refund.id),
                "status": "completed",
                "amount": refund_amount,
            }), 200

        except requests.RequestException as e:
            refund.status = "failed"
            db.commit()
            logger.error("Refund failed for payment %s: %s", payment_id, e)
            return jsonify({"refund_id": str(refund.id), "status": "failed", "error": str(e)}), 500

    finally:
        db.close()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "payments"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8003, debug=False)
