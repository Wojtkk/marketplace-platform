from typing import Optional
from uuid import UUID

from sqlalchemy import and_
from sqlalchemy.orm import Session, joinedload

from catalog_service.models import Product, Category, ProductImage


def get_products(
    db: Session,
    category_id: Optional[UUID] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    page: int = 1,
    page_size: int = 20,
) -> list[Product]:
    query = (
        db.query(Product)
        .options(joinedload(Product.category), joinedload(Product.images))
        .filter(Product.is_active == True)
    )

    filters = []
    if category_id:
        filters.append(Product.category_id == category_id)
    if min_price is not None:
        filters.append(Product.price >= min_price)
    if max_price is not None:
        filters.append(Product.price <= max_price)

    if filters:
        query = query.filter(and_(*filters))

    offset = (page - 1) * page_size
    return query.order_by(Product.created_at.desc()).offset(offset).limit(page_size).all()


def get_product_by_id(db: Session, product_id: UUID) -> Optional[Product]:
    return (
        db.query(Product)
        .options(joinedload(Product.category), joinedload(Product.images))
        .filter(Product.id == product_id, Product.is_active == True)
        .first()
    )


def create_product(
    db: Session,
    name: str,
    description: str,
    price: float,
    stock_quantity: int,
    category_id: Optional[UUID] = None,
    image_urls: Optional[list[str]] = None,
) -> Product:
    product = Product(
        name=name,
        description=description,
        price=price,
        stock_quantity=stock_quantity,
        category_id=category_id,
    )
    db.add(product)
    db.flush()

    if image_urls:
        for idx, url in enumerate(image_urls):
            image = ProductImage(
                product_id=product.id,
                url=url,
                is_primary=(idx == 0),
                sort_order=idx,
            )
            db.add(image)

    db.commit()
    db.refresh(product)
    return product


def update_stock(db: Session, product_id: UUID, quantity: int) -> Optional[Product]:
    product = db.query(Product).filter(Product.id == product_id).first()
    if product is None:
        return None

    product.stock_quantity = quantity
    db.commit()
    db.refresh(product)
    return product


def get_categories(db: Session) -> list[Category]:
    return (
        db.query(Category)
        .options(joinedload(Category.parent))
        .order_by(Category.name)
        .all()
    )


def get_category_by_id(db: Session, category_id: UUID) -> Optional[Category]:
    return db.query(Category).filter(Category.id == category_id).first()
