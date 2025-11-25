# src/visualize/full_visualization_actual.py
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Setup style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['figure.figsize'] = (12, 8)


def get_connection():
    """Kết nối database"""
    return psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='1234',
        dbname='fahasa_dw'
    )


def create_visualizations():
    """Tạo visualization với cấu trúc thực tế"""
    conn = get_connection()

    print("📊 Đang tải dữ liệu từ fahasa_sales_mart...")

    # Lấy toàn bộ dữ liệu
    query = "SELECT * FROM fahasa_sales_mart"
    sales_df = pd.read_sql(query, conn)
    conn.close()

    print(f"✅ Đã tải {len(sales_df)} dòng dữ liệu")
    print("📋 Các cột có sẵn:", sales_df.columns.tolist())

    # Tạo thư mục lưu chart
    os.makedirs('visualizations', exist_ok=True)

    print("🎨 Bắt đầu tạo visualization...")

    # 1. TOP 10 SÁCH BÁN CHẠY NHẤT
    print("→ Top 10 sách bán chạy...")
    top_books = sales_df.nlargest(10, 'total_sold')[['title', 'total_sold', 'author_name', 'collect_year']]

    plt.figure(figsize=(16, 10))
    bars = plt.barh(range(len(top_books)), top_books['total_sold'], color='#FF6B6B')
    plt.yticks(range(len(top_books)), top_books['title'], fontsize=9)
    plt.xlabel('Tổng số lượng bán')
    plt.title('TOP 10 SÁCH BÁN CHẠY NHẤT', fontsize=16, fontweight='bold', pad=20)

    # Thêm số liệu trên bars
    for i, bar in enumerate(bars):
        plt.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                 f'{int(bar.get_width()):,}', ha='left', va='center', fontsize=10)

    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('visualizations/top_10_bestsellers.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 2. TOP 10 SÁCH DOANH THU CAO NHẤT
    print("→ Top 10 sách doanh thu cao...")
    top_revenue = sales_df.nlargest(10, 'total_revenue')[['title', 'total_revenue', 'author_name', 'total_sold']]

    plt.figure(figsize=(16, 10))
    bars = plt.barh(range(len(top_revenue)), top_revenue['total_revenue'], color='#4ECDC4')
    plt.yticks(range(len(top_revenue)), top_revenue['title'], fontsize=9)
    plt.xlabel('Tổng doanh thu (VND)')
    plt.title('TOP 10 SÁCH DOANH THU CAO NHẤT', fontsize=16, fontweight='bold', pad=20)

    # Format doanh thu
    for i, bar in enumerate(bars):
        revenue_billions = bar.get_width() / 1e9
        plt.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                 f'{revenue_billions:.1f}B VND', ha='left', va='center', fontsize=10)

    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('visualizations/top_10_revenue.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 3. PHÂN BỐ DANH MỤC CHÍNH
    print("→ Phân bố danh mục...")
    category_dist = sales_df['category_2'].value_counts().head(15)

    plt.figure(figsize=(14, 10))
    colors = plt.cm.Set3(range(len(category_dist)))
    bars = plt.barh(range(len(category_dist)), category_dist.values, color=colors)
    plt.yticks(range(len(category_dist)), category_dist.index, fontsize=10)
    plt.xlabel('Số lượng sách')
    plt.title('PHÂN BỐ SÁCH THEO DANH MỤC', fontsize=16, fontweight='bold', pad=20)

    # Thêm số liệu
    for i, bar in enumerate(bars):
        plt.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                 f'{int(bar.get_width())}', ha='left', va='center', fontsize=9)

    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('visualizations/category_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 4. TOP 10 TÁC GIẢ BÁN CHẠY
    print("→ Top 10 tác giả...")
    author_sales = sales_df.groupby('author_name').agg({
        'total_sold': 'sum',
        'total_revenue': 'sum',
        'title': 'count'
    }).nlargest(10, 'total_sold')

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

    # Biểu đồ tổng số bán
    bars1 = ax1.barh(range(len(author_sales)), author_sales['total_sold'], color='#45B7D1')
    ax1.set_yticks(range(len(author_sales)))
    ax1.set_yticklabels(author_sales.index, fontsize=10)
    ax1.set_xlabel('Tổng số sách bán')
    ax1.set_title('TOP 10 TÁC GIẢ - TỔNG SỐ BÁN', fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)

    # Thêm số liệu
    for i, bar in enumerate(bars1):
        ax1.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                 f'{int(bar.get_width()):,}', ha='left', va='center', fontsize=9)

    # Biểu đồ tổng doanh thu
    bars2 = ax2.barh(range(len(author_sales)), author_sales['total_revenue'] / 1e9, color='#96CE54')
    ax2.set_yticks(range(len(author_sales)))
    ax2.set_yticklabels(author_sales.index, fontsize=10)
    ax2.set_xlabel('Tổng doanh thu (Tỷ VND)')
    ax2.set_title('TOP 10 TÁC GIẢ - TỔNG DOANH THU', fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)

    # Thêm số liệu
    for i, bar in enumerate(bars2):
        ax2.text(bar.get_width() + bar.get_width() * 0.01, bar.get_y() + bar.get_height() / 2,
                 f'{bar.get_width():.1f}B', ha='left', va='center', fontsize=9)

    plt.tight_layout()
    plt.savefig('visualizations/top_authors.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 5. PHÂN TÍCH THEO NĂM VÀ THÁNG
    print("→ Phân tích theo thời gian...")

    # Phân tích theo năm
    yearly_stats = sales_df.groupby('collect_year').agg({
        'total_sold': 'sum',
        'total_revenue': 'sum',
        'title': 'count'
    }).reset_index()

    # Phân tích theo tháng (nếu có nhiều tháng)
    monthly_stats = sales_df.groupby('collect_month').agg({
        'total_sold': 'sum',
        'total_revenue': 'sum'
    }).reset_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Biểu đồ theo năm
    if len(yearly_stats) > 1:
        ax1.bar(yearly_stats['collect_year'], yearly_stats['total_sold'], color='#FECA57', alpha=0.7)
        ax1.set_xlabel('Năm')
        ax1.set_ylabel('Tổng số bán')
        ax1.set_title('DOANH SỐ THEO NĂM', fontweight='bold')
        ax1.grid(True, alpha=0.3)

        # Thêm số liệu trên cột
        for i, v in enumerate(yearly_stats['total_sold']):
            ax1.text(i, v + v * 0.01, f'{int(v):,}', ha='center', va='bottom', fontsize=9)
    else:
        ax1.text(0.5, 0.5, 'Chỉ có dữ liệu 1 năm', ha='center', va='center', transform=ax1.transAxes, fontsize=12)
        ax1.set_title('DOANH SỐ THEO NĂM', fontweight='bold')

    # Biểu đồ theo tháng
    if len(monthly_stats) > 1:
        ax2.bar(monthly_stats['collect_month'], monthly_stats['total_sold'], color='#FF9FF3', alpha=0.7)
        ax2.set_xlabel('Tháng')
        ax2.set_ylabel('Tổng số bán')
        ax2.set_title('DOANH SỐ THEO THÁNG', fontweight='bold')
        ax2.grid(True, alpha=0.3)

        # Thêm số liệu trên cột
        for i, v in enumerate(monthly_stats['total_sold']):
            ax2.text(i + 1, v + v * 0.01, f'{int(v):,}', ha='center', va='bottom', fontsize=9)
    else:
        ax2.text(0.5, 0.5, 'Chỉ có dữ liệu 1 tháng', ha='center', va='center', transform=ax2.transAxes, fontsize=12)
        ax2.set_title('DOANH SỐ THEO THÁNG', fontweight='bold')

    plt.tight_layout()
    plt.savefig('visualizations/time_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

    # 6. PHÂN TÍCH RATING
    print("→ Phân tích rating...")

    if 'avg_rating' in sales_df.columns:
        plt.figure(figsize=(12, 8))

        # Lọc rating hợp lệ
        valid_ratings = sales_df[sales_df['avg_rating'] > 0]['avg_rating']

        if len(valid_ratings) > 0:
            plt.hist(valid_ratings, bins=20, alpha=0.7, color='#54A0FF', edgecolor='black')
            plt.xlabel('Rating trung bình')
            plt.ylabel('Số sách')
            plt.title('PHÂN BỐ RATING SÁCH', fontsize=16, fontweight='bold', pad=20)
            plt.grid(True, alpha=0.3)

            # Thêm thống kê
            mean_rating = valid_ratings.mean()
            plt.axvline(mean_rating, color='red', linestyle='--', linewidth=2,
                        label=f'Rating trung bình: {mean_rating:.2f}')
            plt.legend()
        else:
            plt.text(0.5, 0.5, 'Không có dữ liệu rating', ha='center', va='center',
                     transform=plt.gca().transAxes, fontsize=14)

        plt.tight_layout()
        plt.savefig('visualizations/rating_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    # 7. TƯƠNG QUAN GIỮA SỐ BÁN VÀ DOANH THU
    print("→ Phân tích tương quan...")

    plt.figure(figsize=(12, 8))
    plt.scatter(sales_df['total_sold'], sales_df['total_revenue'], alpha=0.6,
                c=sales_df['avg_rating'] if 'avg_rating' in sales_df.columns else 'blue',
                cmap='viridis', s=100)

    if 'avg_rating' in sales_df.columns:
        plt.colorbar(label='Rating trung bình')

    plt.xlabel('Tổng số lượng bán')
    plt.ylabel('Tổng doanh thu (VND)')
    plt.title('TƯƠNG QUAN: SỐ LƯỢNG BÁN vs DOANH THU', fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3)

    # Format trục y (doanh thu)
    plt.ticklabel_format(style='plain', axis='y')

    plt.tight_layout()
    plt.savefig('visualizations/correlation_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

    print(f"✅ Đã tạo {len([f for f in os.listdir('visualizations') if f.endswith('.png')])} file visualization!")
    return sales_df


def create_dashboard_html(sales_df):
    """Tạo dashboard HTML tổng hợp"""

    # Tính các thống kê
    total_books = len(sales_df)
    total_sold = sales_df['total_sold'].sum()
    total_revenue = sales_df['total_revenue'].sum()
    total_authors = sales_df['author_name'].nunique()
    total_categories = sales_df['category_2'].nunique()

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>FAHASA DATA VISUALIZATION DASHBOARD</title>
        <meta charset="UTF-8">
        <style>
            body {{ 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; 
                padding: 20px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                overflow: hidden;
            }}
            .header {{ 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; 
                padding: 30px; 
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }}
            .header p {{
                margin: 10px 0 0 0;
                font-size: 1.2em;
                opacity: 0.9;
            }}
            .stats {{ 
                display: grid; 
                grid-template-columns: repeat(4, 1fr); 
                gap: 20px; 
                padding: 30px;
                background: #f8f9fa;
            }}
            .stat-card {{ 
                background: white;
                padding: 25px; 
                border-radius: 12px; 
                text-align: center; 
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                border-left: 5px solid #667eea;
                transition: transform 0.3s ease;
            }}
            .stat-card:hover {{
                transform: translateY(-5px);
            }}
            .stat-card h3 {{
                margin: 0 0 15px 0;
                color: #666;
                font-size: 1em;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            .stat-card .number {{
                font-size: 2.2em;
                font-weight: bold;
                color: #333;
                margin: 10px 0;
            }}
            .stat-card .unit {{
                color: #667eea;
                font-weight: 600;
            }}
            .chart-grid {{ 
                display: grid; 
                grid-template-columns: repeat(2, 1fr); 
                gap: 25px; 
                padding: 30px;
            }}
            .chart-item {{ 
                background: white;
                padding: 20px; 
                border-radius: 12px; 
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                border: 1px solid #eaeaea;
            }}
            .chart-item h3 {{
                margin: 0 0 15px 0;
                color: #333;
                font-size: 1.3em;
                font-weight: 600;
                padding-bottom: 10px;
                border-bottom: 2px solid #667eea;
            }}
            .chart-item img {{ 
                width: 100%; 
                height: auto; 
                border-radius: 8px;
                transition: transform 0.3s ease;
            }}
            .chart-item img:hover {{
                transform: scale(1.02);
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                background: #f8f9fa;
                color: #666;
                border-top: 1px solid #eaeaea;
            }}
            @media (max-width: 768px) {{
                .stats {{ grid-template-columns: repeat(2, 1fr); }}
                .chart-grid {{ grid-template-columns: 1fr; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 FAHASA DATA VISUALIZATION DASHBOARD</h1>
                <p>Phân tích dữ liệu sách từ Fahasa - Hoàn toàn bằng Python</p>
            </div>

            <div class="stats">
                <div class="stat-card">
                    <h3>📚 Tổng Số Sách</h3>
                    <div class="number">{total_books:,}</div>
                    <div class="unit">quyển sách</div>
                </div>
                <div class="stat-card">
                    <h3>💰 Tổng Doanh Thu</h3>
                    <div class="number">{total_revenue:,.0f}</div>
                    <div class="unit">VND</div>
                </div>
                <div class="stat-card">
                    <h3>🛒 Tổng Số Bán</h3>
                    <div class="number">{total_sold:,}</div>
                    <div class="unit">lượt bán</div>
                </div>
                <div class="stat-card">
                    <h3>👨‍🎓 Tác Giả</h3>
                    <div class="number">{total_authors}</div>
                    <div class="unit">tác giả</div>
                </div>
            </div>

            <div class="chart-grid">
    """

    # Thêm tất cả charts
    chart_files = [f for f in os.listdir('visualizations') if f.endswith('.png')]
    for chart_file in chart_files:
        chart_name = chart_file.replace('.png', '').replace('_', ' ').title()
        html_content += f"""
                <div class="chart-item">
                    <h3>{chart_name}</h3>
                    <img src="{chart_file}" alt="{chart_file}">
                </div>
        """

    html_content += """
            </div>

            <div class="footer">
                <p>Generated with Python • Matplotlib • Seaborn • {datetime}</p>
            </div>
        </div>
    </body>
    </html>
    """.format(datetime=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))

    with open('visualizations/fahasa_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("🌐 Đã tạo dashboard tổng hợp: visualizations/fahasa_dashboard.html")


def main():
    print("🎨 BẮT ĐẦU TẠO VISUALIZATION VỚI CẤU TRÚC THỰC TẾ")
    print("=" * 60)

    # Tạo visualizations
    sales_df = create_visualizations()

    # Tạo dashboard HTML
    create_dashboard_html(sales_df)

    print("\n✅ HOÀN TẤT VISUALIZATION!")
    print("📁 Tất cả file được lưu trong: visualizations/")
    print("🌐 Mở file: visualizations/fahasa_dashboard.html để xem kết quả")
    print("\n📊 THỐNG KÊ:")
    print(f"   • Tổng sách: {len(sales_df):,}")
    print(f"   • Tổng số bán: {sales_df['total_sold'].sum():,}")
    print(f"   • Tổng doanh thu: {sales_df['total_revenue'].sum():,.0f} VND")
    print(f"   • Số tác giả: {sales_df['author_name'].nunique()}")
    print(f"   • Số danh mục: {sales_df['category_2'].nunique()}")


if __name__ == "__main__":
    main()