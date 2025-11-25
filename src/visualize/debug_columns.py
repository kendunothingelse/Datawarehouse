# src/visualize/debug_columns.py
import pandas as pd
import psycopg2


def check_table_structure():
    """Kiểm tra cấu trúc bảng fahasa_sales_mart"""
    conn = psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='1234',
        dbname='fahasa_dw'
    )

    # Kiểm tra cấu trúc bảng
    print("🔍 KIỂM TRA CẤU TRÚC BẢNG...")

    # Lấy danh sách cột
    column_query = """
                   SELECT column_name, data_type
                   FROM information_schema.columns
                   WHERE table_name = 'fahasa_sales_mart'
                   ORDER BY ordinal_position; \
                   """

    columns_df = pd.read_sql(column_query, conn)
    print("📋 CÁC CỘT TRONG FAHASA_SALES_MART:")
    print(columns_df.to_string(index=False))

    # Lấy 5 dòng dữ liệu mẫu
    sample_query = "SELECT * FROM fahasa_sales_mart LIMIT 5;"
    sample_df = pd.read_sql(sample_query, conn)

    print("\n📊 DỮ LIỆU MẪU:")
    print(sample_df.to_string(index=False))

    conn.close()

    return columns_df


if __name__ == "__main__":
    check_table_structure()