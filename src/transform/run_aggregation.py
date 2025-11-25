# src/transform/run_aggregation.py
import psycopg2

print("BẮT ĐẦU TRANSFORM: TÍNH TOÁN + TỔNG HỢP...")
# ket noi vao datawarehouse
conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='123456', dbname='fahasa_dw')
cur = conn.cursor()

#lam sach va chuan hoa du lieu truoc khi transform
print("→ Transform dim_date (tách ngày, tháng, năm)...")
cur.execute("""
    UPDATE dim_date 
    SET 
        collect_date = DATE(time_collect),
        collect_year = EXTRACT(YEAR FROM time_collect),
        collect_month = EXTRACT(MONTH FROM time_collect),
        collect_day = EXTRACT(DAY FROM time_collect),
        collect_hour = EXTRACT(HOUR FROM time_collect)
    WHERE time_collect IS NOT NULL;
""")
conn.commit()
# === 1. TRANSFORM dim_date: Tách ngày, tháng, năm ===
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

# === 2. TẠO DATA MART: Tổng hợp doanh thu, sách bán chạy ===
# gom du lieu, tinh toan, tong hop
print("→ Tạo Data Mart: fahasa_sales_mart...")
cur.execute("""
    DROP MATERIALIZED VIEW IF EXISTS fahasa_sales_mart;
    CREATE MATERIALIZED VIEW fahasa_sales_mart AS
    SELECT
        d.collect_year,
        d.collect_month,
        c.category_2,
        p.title,
        a.author_name,
        SUM(f.sold_count_numeric) AS total_sold,
        SUM(f.discount_price * f.sold_count_numeric) AS total_revenue,
        AVG(f.rating) AS avg_rating,
        COUNT(*) AS record_count
        
    FROM fact_book_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_author a ON f.author_id = a.author_id
    JOIN dim_category c ON f.category_id = c.category_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.sold_count_numeric > 0
    GROUP BY d.collect_year, d.collect_month, c.category_2, p.title, a.author_name
    HAVING SUM(f.sold_count_numeric) >= 1
    ORDER BY total_revenue DESC;
""")

# === 3. Tạo thêm Data Mart: Top 10 sách theo tháng ===
print("→ Tạo Data Mart: top_10_monthly...")
cur.execute("""
    DROP MATERIALIZED VIEW IF EXISTS top_10_monthly;
    CREATE MATERIALIZED VIEW top_10_monthly AS
    WITH ranked AS (
        SELECT
            d.collect_year,
            d.collect_month,
            p.title,
            SUM(f.sold_count_numeric) AS total_sold,
            ROW_NUMBER() OVER (PARTITION BY d.collect_year, d.collect_month ORDER BY SUM(f.sold_count_numeric) DESC) AS rn
        FROM fact_book_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY d.collect_year, d.collect_month, p.title
    )
    SELECT collect_year, collect_month, title, total_sold
    FROM ranked
    WHERE rn <= 10;
""")
# luu va dong ket noi
conn.commit()
cur.close()
conn.close()
print("TRANSFORM + DATA MART HOÀN TẤT!")