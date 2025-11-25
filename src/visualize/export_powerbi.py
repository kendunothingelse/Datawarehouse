# src/visualize/export_powerbi.py
import pandas as pd
import psycopg2
import os
from datetime import datetime


def get_connection():
    """Kết nối database"""
    return psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='123456',
        dbname='fahasa_dw'
    )


def export_for_powerbi():
    """Export toàn bộ dữ liệu cho Power BI"""
    conn = get_connection()

    # Tạo thư mục export
    os.makedirs('powerbi_data', exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("🔄 Đang export dữ liệu cho Power BI...")

    # 1. EXPORT DATA MART CHÍNH - ĐÃ ĐƯỢC TÍNH TOÁN
    print("→ Exporting fahasa_sales_mart...")
    sales_query = """
                  SELECT collect_year, \
                         collect_month, \
                         category_2 as category, \
                         title, \
                         author_name, \
                         total_sold, \
                         total_revenue, \
                         avg_rating, \
                         record_count
                  FROM fahasa_sales_mart \
                  """
    sales_df = pd.read_sql(sales_query, conn)

    # Làm sạch dữ liệu
    sales_df['category'] = sales_df['category'].fillna('Không phân loại')
    sales_df['author_name'] = sales_df['author_name'].fillna('Không xác định')

    # Thêm các cột tính toán cho Power BI
    sales_df['revenue_billions'] = sales_df['total_revenue'] / 1e9
    sales_df['price_per_book'] = sales_df['total_revenue'] / sales_df['total_sold']
    sales_df['price_per_book'] = sales_df['price_per_book'].fillna(0)

    sales_df.to_csv(f'powerbi_data/fahasa_sales_data_{timestamp}.csv',
                    index=False, encoding='utf-8-sig')

    # 2. EXPORT DIMENSIONS - DỮ LIỆU CHI TIẾT
    print("→ Exporting dimensions data...")

    # Lấy dữ liệu author chi tiết
    author_query = """
                   SELECT DISTINCT author_name, \
                                   COUNT(*)           as book_count, \
                                   SUM(total_sold)    as total_books_sold, \
                                   SUM(total_revenue) as total_revenue_author, \
                                   AVG(avg_rating)    as avg_author_rating
                   FROM fahasa_sales_mart
                   WHERE author_name IS NOT NULL \
                     AND author_name != ''
                   GROUP BY author_name \
                   """
    author_df = pd.read_sql(author_query, conn)
    author_df.to_csv(f'powerbi_data/fahasa_authors_{timestamp}.csv',
                     index=False, encoding='utf-8-sig')

    # Lấy dữ liệu category chi tiết
    category_query = """
                     SELECT category_2         as category, \
                            COUNT(*)           as book_count, \
                            SUM(total_sold)    as total_sold, \
                            SUM(total_revenue) as total_revenue, \
                            AVG(avg_rating)    as avg_rating, \
                            AVG(total_sold)    as avg_sold_per_book
                     FROM fahasa_sales_mart
                     WHERE category_2 IS NOT NULL
                     GROUP BY category_2 \
                     """
    category_df = pd.read_sql(category_query, conn)
    category_df.to_csv(f'powerbi_data/fahasa_categories_{timestamp}.csv',
                       index=False, encoding='utf-8-sig')

    # 3. EXPORT TIME SERIES DATA
    print("→ Exporting time series data...")
    time_query = """
                 SELECT collect_year, \
                        collect_month, \
                        SUM(total_sold)    as monthly_sold, \
                        SUM(total_revenue) as monthly_revenue, \
                        COUNT(*)           as book_count, \
                        AVG(avg_rating)    as monthly_avg_rating
                 FROM fahasa_sales_mart
                 GROUP BY collect_year, collect_month
                 ORDER BY collect_year, collect_month \
                 """
    time_df = pd.read_sql(time_query, conn)
    time_df.to_csv(f'powerbi_data/fahasa_timeseries_{timestamp}.csv',
                   index=False, encoding='utf-8-sig')

    # 4. EXPORT TOP PERFORMERS
    print("→ Exporting top performers...")

    # Top 50 sách
    top_books_query = """
                      SELECT title, \
                             author_name, \
                             category_2 as category, \
                             total_sold, \
                             total_revenue, \
                             avg_rating, \
                             record_count
                      FROM fahasa_sales_mart
                      ORDER BY total_sold DESC LIMIT 50 \
                      """
    top_books_df = pd.read_sql(top_books_query, conn)
    top_books_df.to_csv(f'powerbi_data/fahasa_top_books_{timestamp}.csv',
                        index=False, encoding='utf-8-sig')

    conn.close()

    # THỐNG KÊ EXPORT
    print("\n✅ EXPORT HOÀN TẤT!")
    print(f"📁 Files được lưu trong: powerbi_data/")
    print(f"📊 fahasa_sales_data_{timestamp}.csv - {len(sales_df)} dòng")
    print(f"👨‍🎓 fahasa_authors_{timestamp}.csv - {len(author_df)} dòng")
    print(f"📚 fahasa_categories_{timestamp}.csv - {len(category_df)} dòng")
    print(f"📈 fahasa_timeseries_{timestamp}.csv - {len(time_df)} dòng")
    print(f"🏆 fahasa_top_books_{timestamp}.csv - {len(top_books_df)} dòng")

    # Tạo file hướng dẫn
    create_powerbi_guide(timestamp)

    return {
        'timestamp': timestamp,
        'sales_data': len(sales_df),
        'authors': len(author_df),
        'categories': len(category_df),
        'timeseries': len(time_df),
        'top_books': len(top_books_df)
    }


def create_powerbi_guide(timestamp):
    """Tạo hướng dẫn sử dụng Power BI chi tiết"""

    guide = f"""
    🎨 HƯỚNG DẪN SỬ DỤNG POWER BI VỚI DỮ LIỆU FAHASA
    =============================================

    📁 DỮ LIỆU ĐÃ EXPORT:
    • fahasa_sales_data_{timestamp}.csv - Dữ liệu bán hàng chính
    • fahasa_authors_{timestamp}.csv - Thống kê tác giả
    • fahasa_categories_{timestamp}.csv - Thống kê danh mục  
    • fahasa_timeseries_{timestamp}.csv - Dữ liệu chuỗi thời gian
    • fahasa_top_books_{timestamp}.csv - Top 50 sách bán chạy

    🚀 BƯỚC 1: MỞ POWER BI DESKTOP
    ---------------------------
    1. Mở Microsoft Power BI Desktop
    2. Chọn "Get Data" → "Text/CSV"
    3. Chọn file: powerbi_data/fahasa_sales_data_{timestamp}.csv
    4. Nhấn "Load"

    📊 BƯỚC 2: TẠO DATA MODEL
    ------------------------
    1. Vào tab "Model" view
    2. Import các file CSV còn lại
    3. Tạo relationships:
       • sales_data[author_name] → authors[author_name]
       • sales_data[category] → categories[category]
       • sales_data[collect_year, collect_month] → timeseries[collect_year, collect_month]

    🎯 BƯỚC 3: TẠO CÁC DASHBOARD CHÍNH
    ---------------------------------

    📈 DASHBOARD 1: TỔNG QUAN KINH DOANH
    • Card: SUM(total_sold) - Tổng số sách bán
    • Card: SUM(total_revenue) - Tổng doanh thu  
    • Card: AVERAGE(avg_rating) - Rating trung bình
    • Line Chart: monthly_sold theo thời gian
    • Pie Chart: total_revenue theo category

    📚 DASHBOARD 2: PHÂN TÍCH SẢN PHẨM
    • Table: Top 10 title (sort by total_sold)
    • Bar Chart: Top authors by total_books_sold
    • Treemap: Category distribution by revenue
    • Scatter Plot: total_sold vs avg_rating

    👥 DASHBOARD 3: PHÂN TÍCH TÁC GIẢ
    • Bar Chart: Top authors by revenue
    • Donut Chart: Author performance distribution
    • Card: AVERAGE(avg_author_rating)
    • Table: Author details with book count

    💰 DASHBOARD 4: CHIẾN LƯỢC GIÁ
    • Column Chart: price_per_book distribution
    • Line Chart: Revenue trend by price segments
    • Gauge: Overall discount performance
    • Matrix: Category vs Price segments

    ⭐ DASHBOARD 5: ĐÁNH GIÁ CHẤT LƯỢNG
    • Line Chart: monthly_avg_rating trend
    • Bar Chart: Top rated books
    • Scatter: Rating vs Sales correlation
    • KPI: Rating distribution by category

    🎨 BƯỚC 4: TÙY CHỈNH VISUALIZATION
    --------------------------------
    • Dùng Theme Colors phù hợp (Xanh Fahasa: #1E4B87)
    • Add Slicers: Category, Year, Month, Author
    • Create Tooltips với thông tin chi tiết
    • Use Conditional Formatting cho tables
    • Add Bookmarks cho các view khác nhau

    🔗 BƯỚC 5: KẾT NỐI & SHARE
    -------------------------
    • Publish lên Power BI Service
    • Tạo Scheduled Refresh (nếu cần)
    • Share report với team members
    • Set up Data Alerts cho KPI quan trọng

    💡 MẸO NÂNG CAO:
    • Dùng DAX measures cho tính toán phức tạp
    • Tạo Calculated Columns cho phân tích
    • Sử dụng Q&A feature cho query tự nhiên
    • Tạo Mobile Layout cho điện thoại

    📞 HỖ TRỢ:
    • File dữ liệu: powerbi_data/
    • Timestamp: {timestamp}
    • Total records: {pd.read_sql("SELECT COUNT(*) FROM fahasa_sales_mart", get_connection()).iloc[0, 0]:,}
    """

    with open(f'powerbi_data/powerbi_guide_{timestamp}.txt', 'w', encoding='utf-8') as f:
        f.write(guide)

    print(f"📖 Đã tạo hướng dẫn: powerbi_data/powerbi_guide_{timestamp}.txt")


def create_powerbi_template():
    """Tạo file template cho Power BI"""

    template = """
    POWER BI TEMPLATE - FAHASA BOOK ANALYSIS
    ======================================

    RECOMMENDED VISUALS:

    1. KPI CARDS:
       - Total Books Sold: SUM(total_sold)
       - Total Revenue: SUM(total_revenue) 
       - Average Rating: AVERAGE(avg_rating)
       - Unique Authors: DISTINCTCOUNT(author_name)

    2. CHARTS:
       - Line Chart: Monthly Sales Trend
       - Bar Chart: Top 10 Books by Sales
       - Pie Chart: Revenue by Category
       - Scatter Plot: Price vs Sales
       - Treemap: Author Performance
       - Matrix: Category x Time Period

    3. SLICERS:
       - Category
       - Year 
       - Month
       - Author
       - Price Range

    4. DAX MEASURES SUGGESTED:

    Total Revenue = SUM(fahasa_sales_data[total_revenue])

    Total Books Sold = SUM(fahasa_sales_data[total_sold])

    Average Price = DIVIDE([Total Revenue], [Total Books Sold], 0)

    Revenue Growth = 
    VAR CurrentRevenue = [Total Revenue]
    VAR PreviousRevenue = CALCULATE([Total Revenue], PREVIOUSMONTH(fahasa_timeseries[date]))
    RETURN DIVIDE(CurrentRevenue - PreviousRevenue, PreviousRevenue, 0)

    Top Performer = 
    MAXX(TOPN(1, fahasa_sales_data, fahasa_sales_data[total_sold], DESC), fahasa_sales_data[title])

    5. COLOR SCHEME:
       Primary: #1E4B87 (Fahasa Blue)
       Secondary: #FF6B00 (Orange)
       Success: #28A745 (Green)
       Warning: #FFC107 (Yellow)
    """

    with open('powerbi_data/powerbi_template_guide.txt', 'w', encoding='utf-8') as f:
        f.write(template)

    print("🎨 Đã tạo template hướng dẫn: powerbi_data/powerbi_template_guide.txt")


def main():
    print("🚀 BẮT ĐẦU EXPORT DỮ LIỆU CHO POWER BI")
    print("=" * 50)

    # Export dữ liệu
    export_stats = export_for_powerbi()

    # Tạo template
    create_powerbi_template()

    print(f"\n✅ READY FOR POWER BI!")
    print(
        f"📊 Tổng cộng: {sum([export_stats['sales_data'], export_stats['authors'], export_stats['categories'], export_stats['timeseries'], export_stats['top_books']])} dòng dữ liệu")
    print(f"🕒 Timestamp: {export_stats['timestamp']}")
    print(f"📍 Files location: powerbi_data/")
    print(f"📖 Guide: powerbi_data/powerbi_guide_{export_stats['timestamp']}.txt")


if __name__ == "__main__":
    main()