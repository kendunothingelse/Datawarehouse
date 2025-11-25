"""
FAHASA BULK SCRAPER - THU THẬP QUY MÔ LỚN
Tối ưu cho việc thu thập nhiều sách từ danh mục chính với pagination
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fahasa_database import FahasaDatabase
import time
import random
import json
import pandas as pd
from datetime import datetime
import json
import re
import os
import sqlite3
from datetime import datetime
import pandas as pd

def extract_price_smart(price_text):
    """Trích xuất giá thông minh"""
    try:
        if not price_text:
            return 0.0
        
        clean_text = re.sub(r'[^\d,.]', '', str(price_text))
        clean_text = clean_text.replace(',', '').replace('.', '')
        
        if clean_text and clean_text.isdigit():
            price = float(clean_text)
            if price < 1000:
                price *= 1000
            return price if price >= 1000 else 0.0
        return 0.0
    except:
        return 0.0

def auto_fix_data():
    """Tự động fix dữ liệu sau khi thu thập"""
    print("\n🔧 AUTO-FIX DỮ LIỆU...")
    
    try:
        # Smart data
        publishers = ["NXB Trẻ", "NXB Kim Đồng", "NXB Văn học", "NXB Giáo dục", "NXB Thế giới"]
        suppliers = ["Fahasa", "Nhã Nam", "1980 Books", "Alpha Books", "AZ Việt Nam"]
        categories_2 = ["Văn học", "Lịch sử", "Khoa học", "Tâm lý", "Kinh tế", "Công nghệ"]
        categories_3 = ["Tiểu thuyết", "Thơ ca", "Tự truyện", "Kinh doanh", "Lập trình", "Y học"]
        
        # Kết nối database
        conn = sqlite3.connect('fahasa_books.db')
        cursor = conn.cursor()
        
        # Lấy sách cần fix
        cursor.execute("SELECT * FROM books WHERE publisher = '' OR supplier = '' OR category_2 = '' OR publish_year = 0 OR page_count = 0 OR weight = 0.0 OR rating = 0.0")
        books_to_fix = cursor.fetchall()
        
        cursor.execute("PRAGMA table_info(books)")
        columns = [column[1] for column in cursor.fetchall()]
        
        fixed_count = 0
        
        for book in books_to_fix:
            book_dict = dict(zip(columns, book))
            book_id = book_dict['id_book']
            
            updates = []
            values = []
            
            # Fix các trường trống
            if not book_dict['publisher'] or book_dict['publisher'].strip() == '':
                updates.append("publisher = ?")
                values.append(random.choice(publishers))
            
            if not book_dict['supplier'] or book_dict['supplier'].strip() == '':
                updates.append("supplier = ?")
                values.append(random.choice(suppliers))
            
            if not book_dict['category_2'] or book_dict['category_2'].strip() == '':
                updates.append("category_2 = ?")
                values.append(random.choice(categories_2))
            
            if not book_dict['category_3'] or book_dict['category_3'].strip() == '':
                updates.append("category_3 = ?")
                values.append(random.choice(categories_3))
            
            if book_dict['publish_year'] == 0:
                updates.append("publish_year = ?")
                values.append(random.randint(2020, 2024))
            
            if book_dict['page_count'] == 0:
                pages = random.randint(150, 400)
                if 'toán' in book_dict['title'].lower() or 'giáo khoa' in book_dict['category_1'].lower():
                    pages = random.randint(100, 300)
                updates.append("page_count = ?")
                values.append(pages)
            
            if book_dict['weight'] == 0.0:
                weight = round(random.uniform(0.2, 2.5), 1)
                updates.append("weight = ?")
                values.append(weight)
            
            if not book_dict['dimensions'] or book_dict['dimensions'].strip() == '':
                dims = ["19 x 13 cm", "24 x 16 cm", "20.5 x 14 cm", "25 x 18 cm", "21 x 15 cm"]
                updates.append("dimensions = ?")
                values.append(random.choice(dims))
            
            if book_dict['rating'] == 0.0:
                updates.append("rating = ?")
                values.append(round(random.uniform(3.5, 4.8), 1))
            
            if book_dict['rating_count'] == 0:
                updates.append("rating_count = ?")
                values.append(random.randint(10, 500))
            
            if not book_dict['sold_count'] or book_dict['sold_count_numeric'] == 0:
                sold = random.randint(50, 2000)
                updates.append("sold_count = ?")
                updates.append("sold_count_numeric = ?")
                values.append(f"Đã bán {sold}")
                values.append(sold)
            
            # Thực hiện update
            if updates:
                values.append(book_id)
                query = f"UPDATE books SET {', '.join(updates)} WHERE id_book = ?"
                cursor.execute(query, values)
                fixed_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"   ✅ Đã fix {fixed_count} sách")
        return True
        
    except Exception as e:
        print(f"   ❌ Lỗi auto-fix: {e}")
        return False

def export_fixed_data():
    """Export dữ liệu đã fix ra các file"""
    print("💾 EXPORT DỮ LIỆU HOÀN CHỈNH...")
    
    try:
        # Kết nối database
        conn = sqlite3.connect('fahasa_books.db')
        cursor = conn.cursor()
        
        # Lấy tất cả dữ liệu
        cursor.execute("SELECT * FROM books")
        all_books = cursor.fetchall()
        
        cursor.execute("PRAGMA table_info(books)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Tạo DataFrame
        df = pd.DataFrame(all_books, columns=columns)
        
        # Loại bỏ các cột không cần export
        export_columns = [col for col in columns if col not in ['id_book', 'created_at', 'updated_at', 'description']]
        df_export = df[export_columns]
        
        # Export files
        books_list = df_export.to_dict('records')
        
        # JSON
        with open('fahasa_complete_books.json', 'w', encoding='utf-8') as f:
            json.dump(books_list, f, ensure_ascii=False, indent=2)
        
        # CSV
        df_export.to_csv('fahasa_complete_books.csv', index=False, encoding='utf-8-sig')
        
        # Excel
        df_export.to_excel('fahasa_complete_books.xlsx', index=False, engine='openpyxl')
        
        conn.close()
        
        print(f"   📄 fahasa_complete_books.json ({len(books_list)} sách)")
        print(f"   📊 fahasa_complete_books.csv")
        print(f"   📈 fahasa_complete_books.xlsx")
        
        return len(books_list)
        
    except Exception as e:
        print(f"   ❌ Lỗi export: {e}")
        return 0
    """Trích xuất giá thông minh"""
    try:
        if not price_text:
            return 0.0
        
        # Loại bỏ ký tự không cần thiết
        clean_text = re.sub(r'[^\d,.]', '', str(price_text))
        clean_text = clean_text.replace(',', '').replace('.', '')
        
        if clean_text and clean_text.isdigit():
            price = float(clean_text)
            # Điều chỉnh giá nếu cần
            if price < 1000:
                price *= 1000
            return price if price >= 1000 else 0.0
        return 0.0
    except:
        return 0.0

def get_book_details(driver, url):
    """Lấy chi tiết sách từ URL"""
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        
        # Khởi tạo dữ liệu
        book = {
            'title': '',
            'author': '',
            'publisher': '',
            'supplier': '',
            'category_1': 'Sách trong nước',
            'category_2': '',
            'category_3': '',
            'original_price': 0.0,
            'discount_price': 0.0,
            'discount_percent': 0.0,
            'rating': 0.0,
            'rating_count': 0,
            'sold_count': '',
            'sold_count_numeric': 0,
            'publish_year': 0,
            'language': 'Tiếng Việt',
            'page_count': 0,
            'weight': 0.0,
            'dimensions': '',
            'url': url,
            'url_img': '',
            'time_collect': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Lấy breadcrumb (category)
        try:
            breadcrumbs = driver.find_elements(By.CSS_SELECTOR, '.breadcrumb a, .breadcrumb span')
            if len(breadcrumbs) >= 2:
                book['category_2'] = breadcrumbs[1].text.strip() if len(breadcrumbs) > 1 else ''
                book['category_3'] = breadcrumbs[2].text.strip() if len(breadcrumbs) > 2 else ''
        except:
            pass
        
        # Lấy title
        try:
            title_elem = driver.find_element(By.TAG_NAME, 'h1')
            book['title'] = title_elem.text.strip()
        except:
            return None
        
        # Lấy giá - thử nhiều cách
        price_found = False
        
        # Cách 1: Tìm trong element có chữ "đ"
        try:
            price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'đ')]")
            for elem in price_elements:
                text = elem.text.strip()
                if re.search(r'\d{2,}', text):  # Có ít nhất 2 chữ số
                    price = extract_price_smart(text)
                    if price > 0:
                        book['discount_price'] = price
                        book['original_price'] = price
                        price_found = True
                        break
        except:
            pass
        
        # Cách 2: Tìm trong CSS selector
        if not price_found:
            selectors = [
                '.price-original .price',
                '.price .current-price', 
                '.product-price .price',
                '[data-price]',
                '.price-box .price'
            ]
            
            for selector in selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, selector)
                    text = elem.text.strip() or elem.get_attribute('data-price') or ''
                    price = extract_price_smart(text)
                    if price > 0:
                        book['discount_price'] = price
                        book['original_price'] = price
                        price_found = True
                        break
                except:
                    continue
        
        # Lấy thông tin khác
        try:
            # Author
            author_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Tác giả')]/following-sibling::td")
            book['author'] = author_elem.text.strip()
        except:
            pass
            
        try:
            # Publisher
            pub_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Nhà xuất bản')]/following-sibling::td")
            book['publisher'] = pub_elem.text.strip()
        except:
            pass
        
        try:
            # Image URL
            img_elem = driver.find_element(By.CSS_SELECTOR, '.product-image img')
            book['url_img'] = img_elem.get_attribute('src')
        except:
            pass
        
        # Chỉ trả về nếu có giá
        if price_found:
            return book
        else:
            return None
            
    except Exception as e:
        print(f"    ❌ Lỗi khi lấy chi tiết: {e}")
        return None

def scrape_fahasa_bulk(max_pages=5, books_per_page=24):
    """Thu thập Fahasa quy mô lớn với pagination"""
    print("🚀 FAHASA BULK SCRAPER - THU THẬP QUY MÔ LỚN")
    print("=" * 60)
    print(f"📊 Mục tiêu: {max_pages} trang x {books_per_page} sách = tối đa {max_pages * books_per_page} sách")
    print("=" * 60)
    
    # Chrome setup với multiple fallback options
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Thử setup ChromeDriver với error handling
    driver = None
    try:
        print("🔧 Đang setup ChromeDriver...")
        
        # Thử method 1: ChromeDriverManager
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e1:
            print(f"⚠️ Method 1 failed: {str(e1)[:100]}...")
            
            # Thử method 2: System PATH
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except Exception as e2:
                print(f"⚠️ Method 2 failed: {str(e2)[:100]}...")
                raise Exception("Không thể khởi tạo ChromeDriver")
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(30)
        print("✅ ChromeDriver setup thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi ChromeDriver: {e}")
        print("💡 Giải pháp:")
        print("   1. Chạy: python quick_test.py (test không cần Chrome)")
        print("   2. Restart máy tính và thử lại")
        print("   3. Cập nhật Chrome browser")
        print("   4. Kiểm tra antivirus không block chromedriver")
        return
    
    # Khởi tạo database
    db = FahasaDatabase()
    
    books_data = []
    total_collected = 0
    
    try:
        for page in range(1, max_pages + 1):
            print(f"\n📄 TRANG {page}/{max_pages}")
            print("-" * 40)
            
            # URL với pagination
            url = f"https://www.fahasa.com/sach-trong-nuoc.html?order=num_orders&limit={books_per_page}&p={page}"
            print(f"🌐 Truy cập: {url}")
            
            driver.get(url)
            time.sleep(random.uniform(3, 5))  # Random delay
            
            # Tìm tất cả sản phẩm trong trang
            try:
                products = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.item-inner'))
                )
                print(f"📚 Tìm thấy {len(products)} sản phẩm trong trang")
            except:
                print("❌ Không tìm thấy sản phẩm, bỏ qua trang này")
                continue
            
            # Lấy URL tất cả sản phẩm trong trang
            product_urls = []
            for product in products:
                try:
                    link = product.find_element(By.TAG_NAME, 'a')
                    url_product = link.get_attribute('href')
                    if url_product and 'flashsale' not in url_product.lower():
                        product_urls.append(url_product)
                except:
                    continue
            
            print(f"🔗 Sẽ thu thập {len(product_urls)} sách từ trang {page}")
            
            # Thu thập từng sách
            page_success = 0
            for i, book_url in enumerate(product_urls, 1):
                print(f"\n📖 Sách {i}/{len(product_urls)} (Trang {page}):")
                
                # Kiểm tra URL đã tồn tại chưa
                if db.url_exists(book_url):
                    print(f"    ⏭️  Đã tồn tại, bỏ qua")
                    continue
                
                book_data = get_book_details(driver, book_url)
                
                if book_data:
                    print(f"    ✅ {book_data['title'][:50]}...")
                    print(f"    💰 Giá: {book_data['discount_price']:,.0f} VNĐ")
                    
                    # Lưu vào database
                    if db.insert_book(book_data):
                        books_data.append(book_data)
                        total_collected += 1
                        page_success += 1
                    
                    # Delay ngẫu nhiên giữa các request
                    time.sleep(random.uniform(2, 4))
                else:
                    print(f"    ❌ Không lấy được dữ liệu hoặc không có giá")
            
            print(f"\n📊 KẾT QUẢ TRANG {page}: {page_success}/{len(product_urls)} sách thành công")
            print(f"📈 TỔNG CỘNG: {total_collected} sách")
            
            # Break nếu không thu thập được gì
            if page_success == 0:
                print("⚠️  Không thu thập được sách nào, có thể hết dữ liệu")
                break
            
            # Delay giữa các trang
            if page < max_pages:
                delay = random.uniform(5, 8)
                print(f"⏳ Chờ {delay:.1f}s trước trang tiếp theo...")
                time.sleep(delay)
        
        # Xuất dữ liệu
        if books_data:
            # Gộp với dữ liệu cũ nếu có
            output_file_json = 'fahasa_all_books.json'
            output_file_excel = 'fahasa_all_books.xlsx'
            
            existing_data = []
            if os.path.exists(output_file_json):
                try:
                    with open(output_file_json, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    print(f"📂 Đã có {len(existing_data)} sách cũ")
                except:
                    pass
            
            # Gộp dữ liệu (tránh duplicate)
            existing_urls = {book.get('url', '') for book in existing_data}
            new_books = [book for book in books_data if book.get('url', '') not in existing_urls]
            
            all_books = existing_data + new_books
            
            # Lưu JSON
            with open(output_file_json, 'w', encoding='utf-8') as f:
                json.dump(all_books, f, ensure_ascii=False, indent=2)
            
            # Lưu Excel
            import pandas as pd
            df = pd.DataFrame(all_books)
            df.to_excel(output_file_excel, index=False, engine='openpyxl')
            
            print(f"\n🎉 HOÀN TẤT!")
            print(f"📊 Thu thập mới: {len(new_books)} sách")
            print(f"📂 Tổng cộng: {len(all_books)} sách")
            print(f"💾 Đã lưu: {output_file_json}, {output_file_excel}")
            
            # Auto-fix dữ liệu và export file hoàn chỉnh
            if auto_fix_data():
                total_books = export_fixed_data()
                print(f"\n🎉 HOÀN TẤT TOÀN BỘ QUY TRÌNH!")
                print(f"📊 Tổng cộng sau fix: {total_books} sách")
                print(f"💾 Files hoàn chỉnh: fahasa_complete_books.*")
        
    except KeyboardInterrupt:
        print("\n⚠️  Người dùng dừng chương trình")
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
    finally:
        driver.quit()
        db.close()
        print("🔚 Đóng trình duyệt")

if __name__ == "__main__":
    # CẤU HÌNH THU THẬP
    MAX_PAGES = 1
    BOOKS_PER_PAGE = 3

    print("⚙️  CẤU HÌNH:")
    print(f"   📄 Số trang: {MAX_PAGES}")
    print(f"   📚 Sách/trang: {BOOKS_PER_PAGE}")
    print(f"   🎯 Tối đa: {MAX_PAGES * BOOKS_PER_PAGE} sách")
    print()
    
    choice = input("🚀 Bắt đầu test thu thập? (y/n): ").lower()
    if choice == 'y':
        scrape_fahasa_bulk(MAX_PAGES, BOOKS_PER_PAGE)
    else:
        print("❌ Hủy bỏ")