import logging
from typing import Any, Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Depends, Query
from sqlalchemy.orm import Session

from catalog_service.models import Base
from catalog_service.repository import (
    get_products,
    get_product_by_id,
    create_product,
    update_stock,
    get_categories,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Catalog Service", version="1.0.0")


def get_db() -> Session:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("postgresql://catalog:catalog@catalog-db:5432/catalog")
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/products")
def list_products(
    category_id: Optional[UUID] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    products = get_products(
        db,
        category_id=category_id,
        min_price=min_price,
        max_price=max_price,
        page=page,
        page_size=page_size,
    )
    return {
        "products": [
            {
                "id": str(p.id),
                "name": p.name,
                "description": p.description,
                "price": p.price,
                "stock_quantity": p.stock_quantity,
                "category": p.category.name if p.category else None,
                "images": [img.url for img in p.images],
            }
            for p in products
        ],
        "page": page,
        "page_size": page_size,
    }


@app.get("/products/{product_id}")
def get_single_product(
    product_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    product = get_product_by_id(db, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return {
        "id": str(product.id),
        "name": product.name,
        "description": product.description,
        "price": product.price,
        "stock_quantity": product.stock_quantity,
        "category": product.category.name if product.category else None,
        "images": [
            {"url": img.url, "is_primary": img.is_primary} for img in product.images
        ],
    }


@app.post("/products")
def create_new_product(
    product_data: dict[str, Any],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    required_fields = ["name", "price", "stock_quantity"]
    for field in required_fields:
        if field not in product_data:
            raise HTTPException(status_code=422, detail=f"Missing required field: {field}")

    product = create_product(
        db,
        name=product_data["name"],
        description=product_data.get("description", ""),
        price=product_data["price"],
        stock_quantity=product_data["stock_quantity"],
        category_id=product_data.get("category_id"),
        image_urls=product_data.get("image_urls"),
    )
    logger.info("Created product %s: %s", product.id, product.name)
    return {"id": str(product.id), "name": product.name, "status": "created"}


@app.put("/products/{product_id}/stock")
def update_product_stock(
    product_id: UUID,
    stock_data: dict[str, int],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    quantity = stock_data.get("quantity")
    if quantity is None or quantity < 0:
        raise HTTPException(status_code=422, detail="Invalid quantity")

    product = update_stock(db, product_id, quantity)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")

    logger.info("Updated stock for product %s to %d", product_id, quantity)
    return {
        "id": str(product.id),
        "stock_quantity": product.stock_quantity,
        "status": "updated",
    }


@app.put("/products/{product_id}/restock")
def restock_product(
    product_id: UUID,
    restock_data: dict[str, int],
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    quantity = restock_data.get("quantity", 0)
    if quantity <= 0:
        raise HTTPException(status_code=422, detail="Invalid restock quantity")

    product = get_product_by_id(db, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")

    new_stock = product.stock_quantity + quantity
    updated = update_stock(db, product_id, new_stock)
    logger.info("Restocked product %s: +%d (now %d)", product_id, quantity, new_stock)
    return {
        "id": str(updated.id),
        "stock_quantity": updated.stock_quantity,
        "restocked": quantity,
        "status": "restocked",
    }


@app.delete("/products/{product_id}")
def delete_product(
    product_id: UUID,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from catalog_service.models import Product
    product = db.query(Product).filter(Product.id == product_id).first()
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    db.delete(product)
    db.commit()
    logger.info("Deleted product %s", product_id)
    return {"id": str(product_id), "status": "deleted"}


@app.get("/categories")
def list_categories(db: Session = Depends(get_db)) -> dict[str, Any]:
    categories = get_categories(db)
    return {
        "categories": [
            {
                "id": str(c.id),
                "name": c.name,
                "parent_id": str(c.parent_category_id) if c.parent_category_id else None,
            }
            for c in categories
        ]
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": "catalog"}
