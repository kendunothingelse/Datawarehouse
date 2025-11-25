-- SQL script to create Data Warehouse tables (Star Schema) for Fahasa project
-- Run this after you have staging_books table and imported data

-- Dimension tables
CREATE TABLE IF NOT EXISTS dim_author (
    author_id SERIAL PRIMARY KEY,
    author_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_publisher (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id SERIAL PRIMARY KEY,
    category_1 TEXT,
    category_2 TEXT,
    category_3 TEXT,
    UNIQUE(category_1, category_2, category_3)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    title TEXT,
    language TEXT,
    page_count INTEGER,
    weight NUMERIC,
    dimensions TEXT,
    publish_year INTEGER,
    url TEXT,
    url_img TEXT,
    UNIQUE(title, language, page_count, weight, dimensions, publish_year)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    time_collect TIMESTAMP UNIQUE
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_book_sales (
    fact_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_product(product_id),
    author_id INTEGER REFERENCES dim_author(author_id),
    publisher_id INTEGER REFERENCES dim_publisher(publisher_id),
    supplier_id INTEGER REFERENCES dim_supplier(supplier_id),
    category_id INTEGER REFERENCES dim_category(category_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    original_price NUMERIC,
    discount_price NUMERIC,
    discount_percent NUMERIC,
    rating NUMERIC,
    rating_count INTEGER,
    sold_count_numeric INTEGER
);

-- Control/Log tables
CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    etl_step TEXT,
    status TEXT,
    message TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id SERIAL PRIMARY KEY,
    check_time TIMESTAMP,
    table_name TEXT,
    total_records INTEGER,
    null_count INTEGER,
    duplicate_count INTEGER,
    notes TEXT
);
