CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.customers
(
    customer_id Int32,
    name String,
    email String,
    phone String,
    address String,
    gender String,
    created_at DateTime,
    day_id UInt32
)
ENGINE = ReplacingMergeTree(customer_id)
PARTITION BY day_id
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS ecommerce.order_items
(
    order_item_id Int32,
    order_id Int32,
    product_id Int32,
    quantity Int32,
    unit_price Decimal(10, 2),
    created_at DateTime,
    day_id UInt32
)
ENGINE = ReplacingMergeTree(order_item_id)
PARTITION BY day_id
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS ecommerce.orders
(
    order_id Int32,
    customer_id Int32,
    created_at DateTime,
    day_id UInt32
)
ENGINE = ReplacingMergeTree(order_id)
PARTITION BY day_id
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS ecommerce.products
(
    product_id Int32,
    name String,
    description String,
    category String,
    price Decimal(10, 2),
    seller_id Int32,
    seller_name String,
    created_at DateTime,
    day_id UInt32
)
ENGINE = ReplacingMergeTree(product_id)
PARTITION BY day_id
ORDER BY product_id;


CREATE TABLE IF NOT EXISTS ecommerce.order_items
(
    order_item_id Int32,
    order_id Int32,
    product_id Int32,
    quantity Int32,
    unit_price Decimal(10, 2),
    created_at DateTime,
    day_id UInt32
)
ENGINE = MergeTree(order_item_id)
PARTITION BY day_id
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS ecommerce.order_items (order_item_id Int32, order_id Int32, product_id Int32, quantity Int32, unit_price Decimal(10,2), created_at DateTime, day_id String) ENGINE = MergeTree ORDER BY order_item_id PARTITION BY day_id