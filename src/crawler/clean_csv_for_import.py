import pandas as pd
import random
from datetime import datetime

INPUT_CSV = 'data-1761453259849.csv'
OUTPUT_CSV = 'data_cleaned_for_import.csv'

def clean_value(val, default):
    if pd.isna(val) or val == '' or val == 0 or val == '0' or val == '0.0':
        return default
    return val

def main():
    df = pd.read_csv(INPUT_CSV)

    # Danh sách giá trị mẫu để fill
    publishers = ["NXB Trẻ", "NXB Kim Đồng", "NXB Văn học", "NXB Giáo dục", "NXB Thế giới"]
    suppliers = ["Fahasa", "Nhã Nam", "1980 Books", "Alpha Books", "AZ Việt Nam"]
    categories_2 = ["Văn học", "Lịch sử", "Khoa học", "Tâm lý", "Kinh tế", "Công nghệ"]
    categories_3 = ["Tiểu thuyết", "Thơ ca", "Tự truyện", "Kinh doanh", "Lập trình", "Y học"]
    dims = ["19 x 13 cm", "24 x 16 cm", "20.5 x 14 cm", "25 x 18 cm", "21 x 15 cm"]

    for idx, row in df.iterrows():
        # publisher
        if not row['publisher'] or row['publisher'] == '':
            df.at[idx, 'publisher'] = random.choice(publishers)
        # supplier
        if not row['supplier'] or row['supplier'] == '':
            df.at[idx, 'supplier'] = random.choice(suppliers)
        # category_2
        if not row['category_2'] or row['category_2'] == '':
            df.at[idx, 'category_2'] = random.choice(categories_2)
        # category_3
        if not row['category_3'] or row['category_3'] == '':
            df.at[idx, 'category_3'] = random.choice(categories_3)
        # publish_year
        if not row['publish_year'] or row['publish_year'] == 0:
            df.at[idx, 'publish_year'] = random.randint(2020, 2025)
        # page_count
        if not row['page_count'] or row['page_count'] == 0:
            df.at[idx, 'page_count'] = random.randint(100, 400)
        # weight
        if not row['weight'] or row['weight'] == 0 or row['weight'] == '0.0':
            df.at[idx, 'weight'] = round(random.uniform(0.2, 2.5), 1)
        # dimensions
        if not row['dimensions'] or row['dimensions'] == '':
            df.at[idx, 'dimensions'] = random.choice(dims)
        # rating
        if not row['rating'] or row['rating'] == 0:
            df.at[idx, 'rating'] = round(random.uniform(3.5, 4.8), 1)
        # rating_count
        if not row['rating_count'] or row['rating_count'] == 0:
            df.at[idx, 'rating_count'] = random.randint(10, 500)
        # sold_count, sold_count_numeric
        if not row['sold_count'] or not row['sold_count_numeric'] or row['sold_count_numeric'] == 0:
            sold = random.randint(50, 2000)
            df.at[idx, 'sold_count'] = f"Đã bán {sold}"
            df.at[idx, 'sold_count_numeric'] = sold
        # original_price, discount_price
        for col in ['original_price', 'discount_price']:
            try:
                df.at[idx, col] = float(row[col])
            except:
                df.at[idx, col] = 0.0
        # discount_percent
        try:
            df.at[idx, 'discount_percent'] = float(row['discount_percent'])
        except:
            df.at[idx, 'discount_percent'] = 0.0
        # time_collect
        if not row['time_collect'] or row['time_collect'] == '':
            df.at[idx, 'time_collect'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Ghi ra file mới
    df.to_csv(OUTPUT_CSV, index=False)
    print(f'✅ Đã làm sạch dữ liệu và lưu ra {OUTPUT_CSV}')

if __name__ == '__main__':
    main()
