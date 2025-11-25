
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def generate_insights():
    sales_df, top_df = get_data_for_visualization()

    print("=== INSIGHTS FROM FAHASA DATA ===")

    # 1. Top authors by sales
    top_authors = sales_df.groupby('author_name')['sold_count_numeric'].sum().nlargest(5)
    print("\n📚 Top 5 tác giả bán chạy:")
    print(top_authors)

    # 2. Price analysis
    avg_price_by_category = sales_df.groupby('category_1')['discount_price'].mean()
    print("\n💰 Giá trung bình theo danh mục:")
    print(avg_price_by_category)

    # 3. Discount effectiveness
    discount_vs_sales = sales_df[['discount_percent', 'sold_count_numeric']].corr()
    print("\n🎯 Tương quan discount vs số lượng bán:")
    print(discount_vs_sales)