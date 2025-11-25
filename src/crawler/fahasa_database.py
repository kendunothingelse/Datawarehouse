import sqlite3
import pandas as pd
import json
from datetime import datetime

class FahasaDatabase:
    def __init__(self, db_name="fahasa_books.db"):
        self.db_name = db_name
        self.create_database()
    
    def create_database(self):
        """Tạo database và bảng theo schema đã định"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Tạo bảng books với đầy đủ các trường
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                id_book INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                author TEXT,
                publisher TEXT,
                supplier TEXT,
                category_1 TEXT,
                category_2 TEXT,
                category_3 TEXT,
                original_price REAL,
                discount_price REAL,
                discount_percent REAL,
                rating REAL,
                rating_count INTEGER,
                sold_count TEXT,
                sold_count_numeric INTEGER,
                publish_year INTEGER,
                language TEXT,
                page_count INTEGER,
                weight REAL,
                dimensions TEXT,
                url TEXT,
                url_img TEXT,
                time_collect DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tạo indexes để tối ưu tìm kiếm
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON books(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_author ON books(author)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_publisher ON books(publisher)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_category_1 ON books(category_1)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price ON books(discount_price)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rating ON books(rating)')
        
        conn.commit()
        conn.close()
        
        print(f"✅ Đã tạo database {self.db_name}")
    
    def url_exists(self, url):
        """Kiểm tra URL đã tồn tại trong database chưa"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM books WHERE url = ?", (url,))
        result = cursor.fetchone()[0] > 0
        
        conn.close()
        return result
    
    def close(self):
        """Đóng kết nối database (placeholder method)"""
        pass
    
    def insert_book(self, book_data):
        """Chèn một sách vào database"""
        if not book_data:
            return False
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        try:
            # Kiểm tra xem sách đã tồn tại chưa
            cursor.execute('SELECT id_book FROM books WHERE url = ?', (book_data['url'],))
            existing = cursor.fetchone()
            
            if existing:
                conn.close()
                return False
            
            # Chèn dữ liệu mới
            insert_query = '''
                INSERT INTO books (
                    title, author, publisher, supplier, category_1, category_2, category_3,
                    original_price, discount_price, discount_percent, rating, rating_count,
                    sold_count, sold_count_numeric, publish_year, language, page_count,
                    weight, dimensions, url, url_img, time_collect
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            cursor.execute(insert_query, (
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
                book_data.get('time_collect', '')
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            conn.close()
            print(f"❌ Lỗi insert book: {e}")
            return False
    
    def insert_books(self, books_data):
        """Chèn dữ liệu sách vào database"""
        if not books_data:
            print("❌ Không có dữ liệu để chèn")
            return 0
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        insert_query = '''
            INSERT INTO books (
                title, author, publisher, supplier, category_1, category_2, category_3,
                original_price, discount_price, discount_percent, rating, rating_count,
                sold_count, sold_count_numeric, publish_year, language, page_count,
                weight, dimensions, url, url_img, time_collect
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        inserted_count = 0
        
        for book in books_data:
            try:
                # Kiểm tra xem sách đã tồn tại chưa (dựa vào URL)
                cursor.execute('SELECT id_book FROM books WHERE url = ?', (book['url'],))
                existing = cursor.fetchone()
                
                if existing:
                    print(f"⚠️ Sách đã tồn tại: {book['title']}")
                    continue
                
                # Chèn dữ liệu mới
                cursor.execute(insert_query, (
                    book.get('title', ''),
                    book.get('author', ''),
                    book.get('publisher', ''),
                    book.get('supplier', ''),
                    book.get('category_1', ''),
                    book.get('category_2', ''),
                    book.get('category_3', ''),
                    book.get('original_price', 0.0),
                    book.get('discount_price', 0.0),
                    book.get('discount_percent', 0.0),
                    book.get('rating', 0.0),
                    book.get('rating_count', 0),
                    book.get('sold_count', ''),
                    book.get('sold_count_numeric', 0),
                    book.get('publish_year', 0),
                    book.get('language', ''),
                    book.get('page_count', 0),
                    book.get('weight', 0.0),
                    book.get('dimensions', ''),
                    book.get('url', ''),
                    book.get('url_img', ''),
                    book.get('time_collect', '')
                ))
                
                inserted_count += 1
                print(f"✅ Đã chèn: {book.get('title', 'Unknown')}")
                
            except Exception as e:
                print(f"❌ Lỗi chèn sách {book.get('title', 'Unknown')}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"🎉 Đã chèn thành công {inserted_count} sách vào database")
        return inserted_count
    
    def get_statistics(self):
        """Lấy thống kê database"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        stats = {}
        
        # Tổng số sách
        cursor.execute('SELECT COUNT(*) FROM books')
        stats['total_books'] = cursor.fetchone()[0]
        
        # Số sách có giá
        cursor.execute('SELECT COUNT(*) FROM books WHERE discount_price > 0 OR original_price > 0')
        stats['books_with_price'] = cursor.fetchone()[0]
        
        # Số sách có tác giả
        cursor.execute('SELECT COUNT(*) FROM books WHERE author != ""')
        stats['books_with_author'] = cursor.fetchone()[0]
        
        # Số sách có nhà xuất bản
        cursor.execute('SELECT COUNT(*) FROM books WHERE publisher != ""')
        stats['books_with_publisher'] = cursor.fetchone()[0]
        
        # Giá trung bình
        cursor.execute('SELECT AVG(CASE WHEN discount_price > 0 THEN discount_price ELSE original_price END) FROM books WHERE discount_price > 0 OR original_price > 0')
        avg_price = cursor.fetchone()[0]
        stats['average_price'] = round(avg_price, 0) if avg_price else 0
        
        # Top 5 danh mục
        cursor.execute('SELECT category_1, COUNT(*) as count FROM books WHERE category_1 != "" GROUP BY category_1 ORDER BY count DESC LIMIT 5')
        stats['top_categories'] = cursor.fetchall()
        
        # Top 5 nhà xuất bản
        cursor.execute('SELECT publisher, COUNT(*) as count FROM books WHERE publisher != "" GROUP BY publisher ORDER BY count DESC LIMIT 5')
        stats['top_publishers'] = cursor.fetchall()
        
        conn.close()
        
        return stats
    
    def export_to_excel(self, filename=None):
        """Xuất dữ liệu ra file Excel"""
        if not filename:
            filename = f"fahasa_books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        conn = sqlite3.connect(self.db_name)
        
        # Đọc dữ liệu
        df = pd.read_sql_query('SELECT * FROM books ORDER BY created_at DESC', conn)
        
        # Tạo Excel với nhiều sheet
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet chính
            df.to_excel(writer, sheet_name='Books', index=False)
            
            # Sheet thống kê theo danh mục
            category_stats = pd.read_sql_query('''
                SELECT category_1, COUNT(*) as total_books, 
                       AVG(CASE WHEN discount_price > 0 THEN discount_price ELSE original_price END) as avg_price,
                       AVG(rating) as avg_rating
                FROM books 
                WHERE category_1 != "" 
                GROUP BY category_1 
                ORDER BY total_books DESC
            ''', conn)
            category_stats.to_excel(writer, sheet_name='Category_Stats', index=False)
            
            # Sheet thống kê theo nhà xuất bản
            publisher_stats = pd.read_sql_query('''
                SELECT publisher, COUNT(*) as total_books,
                       AVG(CASE WHEN discount_price > 0 THEN discount_price ELSE original_price END) as avg_price
                FROM books 
                WHERE publisher != "" 
                GROUP BY publisher 
                ORDER BY total_books DESC
                LIMIT 20
            ''', conn)
            publisher_stats.to_excel(writer, sheet_name='Publisher_Stats', index=False)
        
        conn.close()
        
        print(f"📊 Đã xuất dữ liệu ra file {filename}")
        return filename
    
    def search_books(self, keyword="", category="", min_price=0, max_price=999999999, min_rating=0):
        """Tìm kiếm sách"""
        conn = sqlite3.connect(self.db_name)
        
        query = '''
            SELECT title, author, publisher, category_1, 
                   CASE WHEN discount_price > 0 THEN discount_price ELSE original_price END as price,
                   rating, url
            FROM books 
            WHERE 1=1
        '''
        params = []
        
        if keyword:
            query += ' AND (title LIKE ? OR author LIKE ? OR publisher LIKE ?)'
            keyword_pattern = f'%{keyword}%'
            params.extend([keyword_pattern, keyword_pattern, keyword_pattern])
        
        if category:
            query += ' AND category_1 LIKE ?'
            params.append(f'%{category}%')
        
        if min_price > 0:
            query += ' AND (discount_price >= ? OR (discount_price = 0 AND original_price >= ?))'
            params.extend([min_price, min_price])
        
        if max_price < 999999999:
            query += ' AND (discount_price <= ? OR (discount_price = 0 AND original_price <= ?))'
            params.extend([max_price, max_price])
        
        if min_rating > 0:
            query += ' AND rating >= ?'
            params.append(min_rating)
        
        query += ' ORDER BY rating DESC, price ASC LIMIT 50'
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df

def load_json_to_database(json_file, db_name="fahasa_books.db"):
    """Load dữ liệu từ file JSON vào database"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            books_data = json.load(f)
        
        db = FahasaDatabase(db_name)
        inserted = db.insert_books(books_data)
        
        print(f"✅ Đã load {inserted} sách từ {json_file} vào database")
        
        # Hiển thị thống kê
        stats = db.get_statistics()
        print(f"\n📊 Thống kê database:")
        print(f"- Tổng số sách: {stats['total_books']}")
        print(f"- Sách có giá: {stats['books_with_price']}")
        print(f"- Giá trung bình: {stats['average_price']:,.0f} VNĐ")
        
        return db
        
    except Exception as e:
        print(f"❌ Lỗi load JSON: {e}")
        return None

def main():
    """Demo sử dụng database"""
    db = FahasaDatabase()
    
    # Ví dụ tìm kiếm
    print("🔍 Tìm kiếm sách có từ 'văn học':")
    results = db.search_books(keyword="văn học")
    print(results.head())
    
    # Xuất Excel
    excel_file = db.export_to_excel()
    print(f"📊 Đã tạo file Excel: {excel_file}")

if __name__ == "__main__":
    main()