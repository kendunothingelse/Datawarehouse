-- SQL script to create staging table for books in PostgreSQL
CREATE TABLE IF NOT EXISTS staging_books (
    title TEXT,
    author TEXT,
    publisher TEXT,
    supplier TEXT,
    category_1 TEXT,
    category_2 TEXT,
    category_3 TEXT,
    original_price NUMERIC,
    discount_price NUMERIC,
    discount_percent NUMERIC,
    rating NUMERIC,
    rating_count INTEGER,
    sold_count TEXT,
    sold_count_numeric INTEGER,
    publish_year INTEGER,
    language TEXT,
    page_count INTEGER,
    weight NUMERIC,
    dimensions TEXT,
    url TEXT,
    url_img TEXT,
    time_collect TIMESTAMP
);

-- You can use the following command in psql to load data:
-- \copy staging_books FROM 'fahasa_complete_books.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
