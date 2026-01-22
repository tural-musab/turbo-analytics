"""
Turbo.az Analytics - Database Module
SQLite veritabani islemleri
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "turbo_analytics.db")


def init_database() -> None:
    """Veritabanini olustur."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY,
            turbo_id TEXT UNIQUE,
            url TEXT,
            name TEXT,
            brand TEXT,
            model TEXT,
            price INTEGER,
            currency TEXT,
            year INTEGER,
            engine TEXT,
            mileage INTEGER,
            city TEXT,
            views INTEGER DEFAULT 0,
            is_new BOOLEAN,
            is_vip BOOLEAN,
            is_premium BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_data TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            total_cars INTEGER,
            new_cars INTEGER,
            updated_cars INTEGER,
            status TEXT
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_views ON cars(views DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_brand ON cars(brand)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price ON cars(price)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON cars(year)")

    conn.commit()
    conn.close()


def save_car(car_data: Dict) -> bool:
    """Arac kaydet veya guncelle."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO cars (
                turbo_id,
                url,
                name,
                brand,
                model,
                price,
                currency,
                year,
                engine,
                mileage,
                city,
                views,
                is_new,
                is_vip,
                is_premium,
                created_at,
                raw_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(turbo_id) DO UPDATE SET
                views = excluded.views,
                price = excluded.price,
                updated_at = CURRENT_TIMESTAMP,
                raw_data = excluded.raw_data
            """,
            (
                car_data.get("turbo_id"),
                car_data.get("url"),
                car_data.get("name"),
                car_data.get("brand"),
                car_data.get("model"),
                car_data.get("price"),
                car_data.get("currency", "AZN"),
                car_data.get("year"),
                car_data.get("engine"),
                car_data.get("mileage"),
                car_data.get("city"),
                car_data.get("views", 0),
                car_data.get("is_new", False),
                car_data.get("is_vip", False),
                car_data.get("is_premium", False),
                datetime.now().isoformat(),
                json.dumps(car_data, ensure_ascii=False),
            ),
        )
        conn.commit()
        return True
    except Exception as exc:
        print(f"Kayit hatasi: {exc}")
        return False
    finally:
        conn.close()


def save_cars_batch(cars: List[Dict]) -> int:
    """Toplu arac kaydet."""
    saved = 0
    for car in cars:
        if save_car(car):
            saved += 1
    return saved


def get_top_viewed(limit: int = 10) -> List[Dict]:
    """En cok goruntulenen araclari getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT * FROM cars
        ORDER BY views DESC
        LIMIT ?
        """,
        (limit,),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_stats() -> Dict:
    """Istatistikleri getir."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stats: Dict[str, object] = {}

    cursor.execute("SELECT COUNT(*) FROM cars")
    stats["total_cars"] = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(views) FROM cars")
    stats["total_views"] = cursor.fetchone()[0] or 0

    cursor.execute("SELECT AVG(price) FROM cars WHERE currency = 'AZN'")
    stats["avg_price_azn"] = round(cursor.fetchone()[0] or 0, 2)

    cursor.execute(
        """
        SELECT brand, COUNT(*) as count
        FROM cars
        GROUP BY brand
        ORDER BY count DESC
        LIMIT 10
        """
    )
    stats["top_brands"] = [
        {"brand": row[0], "count": row[1]} for row in cursor.fetchall()
    ]

    cursor.execute(
        """
        SELECT year, COUNT(*) as count
        FROM cars
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 10
        """
    )
    stats["year_distribution"] = [
        {"year": row[0], "count": row[1]} for row in cursor.fetchall()
    ]

    conn.close()
    return stats


def search_cars(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    sort_by: str = "views",
    limit: int = 50,
) -> List[Dict]:
    """Arac ara ve filtrele."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM cars WHERE 1=1"
    params: List[object] = []

    if brand:
        query += " AND brand LIKE ?"
        params.append(f"%{brand}%")

    if min_price is not None:
        query += " AND price >= ?"
        params.append(min_price)

    if max_price is not None:
        query += " AND price <= ?"
        params.append(max_price)

    if min_year is not None:
        query += " AND year >= ?"
        params.append(min_year)

    sort_options = {
        "views": "views DESC",
        "price_asc": "price ASC",
        "price_desc": "price DESC",
        "year": "year DESC",
        "newest": "created_at DESC",
    }
    query += f" ORDER BY {sort_options.get(sort_by, 'views DESC')}"
    query += " LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


if __name__ == "__main__":
    init_database()
    print("Veritabani hazir.")
