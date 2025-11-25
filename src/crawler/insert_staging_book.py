import psycopg2

# Cấu hình kết nối PostgreSQL
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'postgres'
PG_PASSWORD = '123456'
PG_DATABASE = 'fahasa_staging'

# Hàm kết nối

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )

# Hàm insert 1 book vào staging_books

def insert_book_staging(book_data):
    create_staging_table()
    conn = get_pg_connection()
    cur = conn.cursor()
    insert_sql = '''
        INSERT INTO staging_books (
            title, author, publisher, supplier, category_1, category_2, category_3,
            original_price, discount_price, discount_percent, rating, rating_count,
            sold_count, sold_count_numeric, publish_year, language, page_count,
            weight, dimensions, url, url_img, time_collect
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cur.execute(insert_sql, (
        book_data.get('title', ''),
        book_data.get('author', ''),
        book_data.get('publisher', ''),
        book_data.get('supplier', ''),
        book_data.get('category_1', ''),
        book_data.get('category_2', ''),
        book_data.get('category_3', ''),
        book_data.get('original_price', 0.0),
        book_data.get('discount_price', 0.0),
        book_data.get('discount_percent', 0.0),
        book_data.get('rating', 0.0),
        book_data.get('rating_count', 0),
        book_data.get('sold_count', ''),
        book_data.get('sold_count_numeric', 0),
        book_data.get('publish_year', 0),
        book_data.get('language', ''),
        book_data.get('page_count', 0),
        book_data.get('weight', 0.0),
        book_data.get('dimensions', ''),
        book_data.get('url', ''),
        book_data.get('url_img', ''),
        book_data.get('time_collect', None)
    ))
    conn.commit()
    cur.close()
    conn.close()

# Hàm test kết nối PostgreSQL
def test_pg_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()[0]
        print(f'✅ Kết nối thành công! PostgreSQL version: {version}')
        cur.close()
        conn.close()
    except Exception as e:
        print(f'❌ Kết nối thất bại: {e}')

# Ví dụ sử dụng
if __name__ == '__main__':
    test_pg_connection()
    # Ví dụ dữ liệu 1 book
    book = {
        'title': 'Sách mẫu',
        'author': 'Tác giả A',
        'publisher': 'NXB X',
        'supplier': 'Fahasa',
        'category_1': 'Sách trong nước',
        'category_2': 'Văn học',
        'category_3': 'Tiểu thuyết',
        'original_price': 100000,
        'discount_price': 90000,
        'discount_percent': 10.0,
        'rating': 4.5,
        'rating_count': 100,
        'sold_count': 'Đã bán 1000',
        'sold_count_numeric': 1000,
        'publish_year': 2023,
        'language': 'Tiếng Việt',
        'page_count': 300,
        'weight': 0.5,
        'dimensions': '20x14cm',
        'url': 'https://www.fahasa.com/sach-mau.html',
        'url_img': '',
        'time_collect': '2025-10-26 10:00:00'
    }
    insert_book_staging(book)
    print('Đã insert 1 book mẫu vào staging_books!')

def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DATABASE)

def create_staging_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS staging_books (
        title TEXT, author TEXT, publisher TEXT, supplier TEXT,
        category_1 TEXT, category_2 TEXT, category_3 TEXT,
        original_price NUMERIC, discount_price NUMERIC, discount_percent NUMERIC,
        rating NUMERIC, rating_count INTEGER, sold_count TEXT, sold_count_numeric INTEGER,
        publish_year INTEGER, language TEXT, page_count INTEGER,
        weight NUMERIC, dimensions TEXT, url TEXT, url_img TEXT,
        time_collect TIMESTAMP DEFAULT NOW(),
        UNIQUE(url)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Bảng staging_books đã sẵn sàng!")