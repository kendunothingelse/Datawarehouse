# /mnt/data/run_aggregation_fixed.py
import os
import psycopg2
from psycopg2 import sql

print("BẮT ĐẦU TRANSFORM + TẠO DATA MART...")

# Lấy thông tin kết nối từ biến môi trường (an toàn hơn)
DB_HOST = os.getenv("DW_HOST", "localhost")
DB_PORT = os.getenv("DW_PORT", "5432")
DB_USER = os.getenv("DW_USER", "postgres")
DB_PASS = os.getenv("DW_PASS", "123456")
DB_NAME = os.getenv("DW_NAME", "fahasa_dw")

conn = None
try:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
    )
    # set autocommit False, sẽ commit thủ công
    conn.autocommit = False
    with conn.cursor() as cur:
        # 1) Transform dim_date (một lần)
        print("→ Transform dim_date (tách ngày, tháng, năm)...")
        cur.execute("""
            UPDATE dim_date
            SET
                collect_date = DATE(time_collect),
                collect_year = EXTRACT(YEAR FROM time_collect)::INTEGER,
                collect_month = EXTRACT(MONTH FROM time_collect)::INTEGER,
                collect_day = EXTRACT(DAY FROM time_collect)::INTEGER,
                collect_hour = EXTRACT(HOUR FROM time_collect)::INTEGER
            WHERE time_collect IS NOT NULL;
        """)
        conn.commit()  # commit sau update

        # 2) Tạo index hỗ trợ trên fact nếu chưa có (tăng tốc group/join)
        print("→ Tạo index hỗ trợ trên fact_book_sales nếu cần...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fact_date_product ON fact_book_sales (date_id, product_id);
            CREATE INDEX IF NOT EXISTS idx_fact_category ON fact_book_sales (category_id);
        """)
        conn.commit()

        # 3) Tạo materialized view fahasa_sales_mart
        print("→ Tạo Materialized View: fahasa_sales_mart...")
        cur.execute("""
            DROP MATERIALIZED VIEW IF EXISTS fahasa_sales_mart;
            CREATE MATERIALIZED VIEW fahasa_sales_mart AS
            SELECT
                d.collect_year,
                d.collect_month,
                c.category_2,
                p.product_id,
                p.title,
                a.author_id,
                a.author_name,
                SUM(COALESCE(f.sold_count_numeric,0)) AS total_sold,
                SUM(COALESCE(f.discount_price,0) * COALESCE(f.sold_count_numeric,0)) AS total_revenue,
                AVG(f.rating) AS avg_rating,
                COUNT(*) AS record_count
            FROM fact_book_sales f
            JOIN dim_product p ON f.product_id = p.product_id
            JOIN dim_author a ON f.author_id = a.author_id
            JOIN dim_category c ON f.category_id = c.category_id
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE COALESCE(f.sold_count_numeric,0) > 0
            GROUP BY d.collect_year, d.collect_month, c.category_2, p.product_id, p.title, a.author_id, a.author_name;
        """)
        # Tạo index cho materialized view để truy xuất nhanh và hỗ trợ REFRESH CONCURRENTLY nếu cần
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fahasa_sales_mart_year_month_rev ON fahasa_sales_mart (collect_year, collect_month, total_revenue DESC);
        """)
        conn.commit()

        # 4) Tạo materialized view top_10_monthly
        print("→ Tạo Materialized View: top_10_monthly...")
        cur.execute("""
            DROP MATERIALIZED VIEW IF EXISTS top_10_monthly;
            CREATE MATERIALIZED VIEW top_10_monthly AS
            WITH ranked AS (
                SELECT
                    d.collect_year,
                    d.collect_month,
                    p.product_id,
                    p.title,
                    SUM(COALESCE(f.sold_count_numeric,0)) AS total_sold,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.collect_year, d.collect_month
                        ORDER BY SUM(COALESCE(f.sold_count_numeric,0)) DESC, p.product_id
                    ) AS rn
                FROM fact_book_sales f
                JOIN dim_date d ON f.date_id = d.date_id
                JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY d.collect_year, d.collect_month, p.product_id, p.title
            )
            SELECT collect_year, collect_month, product_id, title, total_sold
            FROM ranked
            WHERE rn <= 10;
        """)
        # Tạo index unique cần thiết nếu muốn REFRESH CONCURRENTLY (ví dụ: year+month+product_id là unique)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_top_10_monthly_year_month_product
            ON top_10_monthly (collect_year, collect_month, product_id);
        """)
        conn.commit()

        print("Tất cả thao tác SQL đã hoàn tất và commit thành công.")

except Exception as e:
    # in lỗi để debug, rollback nếu cần
    if conn:
        conn.rollback()
    print("LỖI KHI THỰC THI:", e)
    raise
finally:
    if conn:
        conn.close()

print("TRANSFORM + DATA MART HOÀN TẤT!")
