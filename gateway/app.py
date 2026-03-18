import logging
from typing import Any, Optional
from uuid import UUID

import requests
from fastapi import FastAPI, HTTPException, Query

from gateway.middleware import AuthMiddleware, RateLimitMiddleware, LoggingMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(title="Marketplace API Gateway", version="1.0.0")
app.add_middleware(AuthMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(LoggingMiddleware)

CATALOG_SERVICE_URL = "http://catalog-service:8001"
ORDER_SERVICE_URL = "http://order-service:8002"
PAYMENT_SERVICE_URL = "http://payment-service:8003"

SERVICE_URLS = {
    "catalog": CATALOG_SERVICE_URL,
    "order": ORDER_SERVICE_URL,
    "payment": PAYMENT_SERVICE_URL,
}


def check_service_health(url: str) -> dict[str, Any]:
    try:
        response = requests.get(f"{url}/health", timeout=5)
        return {
            "url": url,
            "status": "healthy" if response.status_code == 200 else "unhealthy",
            "status_code": response.status_code,
        }
    except requests.ConnectionError:
        return {"url": url, "status": "unreachable", "status_code": None}
    except requests.Timeout:
        return {"url": url, "status": "timeout", "status_code": None}


@app.get("/api/health")
def health_check() -> dict[str, Any]:
    results = {}
    for name, url in SERVICE_URLS.items():
        results[name] = check_service_health(url)
    all_healthy = all(r["status"] == "healthy" for r in results.values())
    return {"gateway": "healthy", "services": results, "all_healthy": all_healthy}


@app.get("/api/products")
def list_products(
    category_id: Optional[UUID] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    params: dict[str, Any] = {"page": page, "page_size": page_size}
    if category_id:
        params["category_id"] = str(category_id)
    if min_price is not None:
        params["min_price"] = min_price
    if max_price is not None:
        params["max_price"] = max_price

    try:
        response = requests.get(
            f"{CATALOG_SERVICE_URL}/products",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to fetch products from catalog service: %s", e)
        raise HTTPException(status_code=502, detail="Catalog service unavailable")


@app.get("/api/products/{product_id}")
def get_product(product_id: UUID) -> dict[str, Any]:
    try:
        response = requests.get(
            f"{CATALOG_SERVICE_URL}/products/{product_id}",
            timeout=10,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Product not found")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to fetch product %s: %s", product_id, e)
        raise HTTPException(status_code=502, detail="Catalog service unavailable")


@app.post("/api/orders")
def create_order(order_data: dict[str, Any]) -> dict[str, Any]:
    try:
        response = requests.post(
            f"{ORDER_SERVICE_URL}/orders",
            json=order_data,
            timeout=30,
        )
        if response.status_code == 422:
            raise HTTPException(status_code=422, detail=response.json().get("detail"))
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to create order: %s", e)
        raise HTTPException(status_code=502, detail="Order service unavailable")


@app.get("/api/orders/{order_id}")
def get_order(order_id: UUID) -> dict[str, Any]:
    try:
        response = requests.get(
            f"{ORDER_SERVICE_URL}/orders/{order_id}",
            timeout=10,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Order not found")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to fetch order %s: %s", order_id, e)
        raise HTTPException(status_code=502, detail="Order service unavailable")


@app.post("/api/payments")
def process_payment(payment_data: dict[str, Any]) -> dict[str, Any]:
    try:
        response = requests.post(
            f"{PAYMENT_SERVICE_URL}/payments/charge",
            json=payment_data,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to process payment: %s", e)
        raise HTTPException(status_code=502, detail="Payment service unavailable")
