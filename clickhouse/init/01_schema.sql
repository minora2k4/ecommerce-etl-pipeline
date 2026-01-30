CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.products
(
    source String,
    product_id String,
    name String,
    price Float64,
    currency String,
    timestamp Float64,
    event_time DateTime DEFAULT toDateTime(timestamp),
    category Nullable(String),
    brand Nullable(String),
    rating Nullable(Float64),
    stock Nullable(Int32),
    thumbnail Nullable(String),
    rating_count Nullable(Int32),
    description Nullable(String),
    image Nullable(String),
    -- Đã xóa cột 'images' dạng Array để tránh lỗi JDBC Spark
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, event_time, product_id)
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS ecommerce.latest_products AS
SELECT
    source,
    product_id,
    name,
    price,
    currency,
    category,
    brand,
    rating,
    stock,
    event_time,
    inserted_at,
    image,
    description
FROM ecommerce.products
WHERE (source, product_id, event_time) IN (
    SELECT source, product_id, max(event_time) as max_time
    FROM ecommerce.products
    GROUP BY source, product_id
);

CREATE VIEW IF NOT EXISTS ecommerce.product_stats AS
SELECT
    source,
    category,
    count() as total_products,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    avg(rating) as avg_rating,
    sum(stock) as total_stock
FROM ecommerce.products
WHERE category IS NOT NULL
GROUP BY source, category
ORDER BY source, total_products DESC;