# 📚 Fahasa Book Scraper & Data Warehouse

Web scraping project for Fahasa.com with a professional Data Warehouse pipeline (PostgreSQL, Star Schema).

## 🚀 Quick Start

```bash
pip install -r requirements.txt
python src/crawler/fahasa_bulk_scraper.py
```

## ✨ Features

- Selenium scraper: crawl real data from Fahasa.com (no auto-fix, no fake fields)
- 22+ data fields per book (title, author, price, rating, sold count, etc.)
- Directly insert to PostgreSQL staging table (no SQLite, no local data files)
- Star Schema Data Warehouse (1 Fact + 6 Dimensions + 2 Logs)
- ETL scripts: import, clean, and transform data for analytics
- Multi-format export: JSON, CSV, Excel (optional)

## 🛠️ Tech Stack

Python 3.12 • Selenium • Pandas • psycopg2 • PostgreSQL

---

⭐ [sayuchan2302](https://github.com/sayuchan2302) • For educational purposes only
