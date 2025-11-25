# src/etl/run_etl.py
import psycopg2
from psycopg2.extras import execute_values

# === KẾT NỐI ===
def get_conn():
    return psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='123456',
        dbname='fahasa_dw'
    )

print("BẮT ĐẦU ETL...")

conn = get_conn()
cur = conn.cursor()
print("→ Đảm bảo dim_date có đủ cột...")
cur.execute("""
    ALTER TABLE dim_date 
    ADD COLUMN IF NOT EXISTS collect_date DATE,
    ADD COLUMN IF NOT EXISTS collect_year INTEGER,
    ADD COLUMN IF NOT EXISTS collect_month INTEGER,
    ADD COLUMN IF NOT EXISTS collect_day INTEGER,
    ADD COLUMN IF NOT EXISTS collect_hour INTEGER;
""")
conn.commit()
# === 1. DIM AUTHOR ===
print("DIM AUTHOR...")
cur.execute("""
    INSERT INTO dim_author (author_name)
    SELECT DISTINCT TRIM(author) FROM staging_books WHERE author IS NOT NULL AND TRIM(author) != ''
    ON CONFLICT (author_name) DO NOTHING;
""")

# === 2. DIM PUBLISHER ===
print("DIM PUBLISHER...")
cur.execute("""
    INSERT INTO dim_publisher (publisher_name)
    SELECT DISTINCT TRIM(publisher) FROM staging_books WHERE publisher IS NOT NULL AND TRIM(publisher) != ''
    ON CONFLICT (publisher_name) DO NOTHING;
""")

# === 3. DIM SUPPLIER ===
print("DIM SUPPLIER...")
cur.execute("""
    INSERT INTO dim_supplier (supplier_name)
    SELECT DISTINCT TRIM(supplier) FROM staging_books WHERE supplier IS NOT NULL AND TRIM(supplier) != ''
    ON CONFLICT (supplier_name) DO NOTHING;
""")

# === 4. DIM CATEGORY ===
print("DIM CATEGORY...")
cur.execute("""
    INSERT INTO dim_category (category_1, category_2, category_3)
    SELECT DISTINCT category_1, category_2, category_3
    FROM staging_books
    WHERE category_1 IS NOT NULL
    ON CONFLICT (category_1, category_2, category_3) DO NOTHING;
""")

# === 5. DIM PRODUCT ===
print("DIM PRODUCT...")
cur.execute("""
    INSERT INTO dim_product (title, language, page_count, weight, dimensions, publish_year, url, url_img)
    SELECT title, language, page_count, weight, dimensions, publish_year, url, url_img
    FROM staging_books
    ON CONFLICT (title, language, page_count, weight, dimensions, publish_year) DO NOTHING;
""")

# === 6. DIM DATE ===
print("DIM DATE...")
cur.execute("""
    INSERT INTO dim_date (time_collect)
    SELECT DISTINCT time_collect FROM staging_books
    ON CONFLICT (time_collect) DO NOTHING;
""")

# === 7. FACT BOOK SALES ===
print("FACT BOOK SALES...")
cur.execute("""
    INSERT INTO fact_book_sales (
        product_id, author_id, publisher_id, supplier_id, category_id, date_id,
        original_price, discount_price, discount_percent, rating, rating_count, sold_count_numeric
    )
    SELECT
        p.product_id,
        a.author_id,
        pub.publisher_id,
        s.supplier_id,
        c.category_id,
        d.date_id,
        sb.original_price,
        sb.discount_price,
        sb.discount_percent,
        sb.rating,
        sb.rating_count,
        sb.sold_count_numeric
    FROM staging_books sb
    LEFT JOIN dim_product p ON (
        p.title = sb.title AND
        COALESCE(p.language, '') = COALESCE(sb.language, '') AND
        COALESCE(p.page_count, 0) = COALESCE(sb.page_count, 0) AND
        COALESCE(p.weight, 0) = COALESCE(sb.weight, 0) AND
        COALESCE(p.dimensions, '') = COALESCE(sb.dimensions, '') AND
        COALESCE(p.publish_year, 0) = COALESCE(sb.publish_year, 0)
    )
    LEFT JOIN dim_author a ON TRIM(a.author_name) = TRIM(sb.author)
    LEFT JOIN dim_publisher pub ON TRIM(pub.publisher_name) = TRIM(sb.publisher)
    LEFT JOIN dim_supplier s ON TRIM(s.supplier_name) = TRIM(sb.supplier)
    LEFT JOIN dim_category c ON c.category_1 = sb.category_1 AND COALESCE(c.category_2, '') = COALESCE(sb.category_2, '') AND COALESCE(c.category_3, '') = COALESCE(sb.category_3, '')
    LEFT JOIN dim_date d ON d.time_collect = sb.time_collect
    ON CONFLICT DO NOTHING;
""")

conn.commit()
cur.close()
conn.close()

print("ETL HOÀN TẤT! DỮ LIỆU ĐÃ VÀO DATA WAREHOUSE")