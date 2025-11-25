import psycopg2
import pandas as pd
from psycopg2 import sql

# Cấu hình kết nối PostgreSQL
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'postgres'  # Thay bằng user của bạn
PG_PASSWORD = '123456'  # Thay bằng password của bạn
PG_DATABASE = 'fahasa_staging'
CSV_PATH = 'd:/Project_DW/script/fahasa_complete_books.csv'

# Câu lệnh tạo bảng staging_books
CREATE_TABLE_SQL = '''
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
'''

def main():
    # Kết nối PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )
    cur = conn.cursor()
    
    # Tạo bảng nếu chưa có
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print('✅ Đã tạo bảng staging_books (nếu chưa có)')

    # Đọc dữ liệu từ CSV
    df = pd.read_csv(CSV_PATH)
    print(f'📥 Đã đọc {len(df)} dòng từ file CSV')

    # Xóa dữ liệu cũ (nếu muốn làm sạch)
    cur.execute('TRUNCATE TABLE staging_books;')
    conn.commit()
    print('🧹 Đã làm sạch bảng staging_books')

    # Import dữ liệu vào PostgreSQL
    # Sử dụng copy_from để import nhanh
    import io
    output = io.StringIO()
    df.to_csv(output, sep='|', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'staging_books', sep='|')
    conn.commit()
    print('🚀 Đã import dữ liệu vào staging_books thành công!')

    # Kiểm tra
    cur.execute('SELECT COUNT(*) FROM staging_books;')
    count = cur.fetchone()[0]
    print(f'🔎 Số dòng trong staging_books: {count}')

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
