"""
Turbo.az Analytics - Trend Analysis Module
Fiyat ve goruntulenme trend analizi fonksiyonlari
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

DB_PATH = "turbo_analytics.db"


def get_db_connection():
    """Veritabani baglantisi olustur."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def calculate_price_trends(
    brand: Optional[str] = None,
    days: int = 30,
    limit: int = 50,
) -> Dict:
    """
    Marka bazli fiyat trendlerini hesapla.

    Returns:
        {
            "period_days": 30,
            "brand": "BMW",
            "trends": [
                {
                    "turbo_id": "123",
                    "name": "BMW X5",
                    "current_price": 50000,
                    "previous_price": 55000,
                    "price_change": -5000,
                    "change_percent": -9.09,
                    "change_count": 2,
                    "trend": "down"
                }
            ],
            "summary": {
                "total_with_changes": 15,
                "avg_change_percent": -3.5,
                "increased": 5,
                "decreased": 10
            }
        }
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    date_threshold = (datetime.now() - timedelta(days=days)).isoformat()

    # Fiyat degisimi olan araclari bul
    query = """
        SELECT
            c.turbo_id,
            c.name,
            c.brand,
            c.model,
            c.price as current_price,
            c.currency,
            c.year,
            c.url,
            COUNT(ph.id) as change_count,
            MIN(ph.old_price) as first_old_price,
            MAX(ph.recorded_at) as last_change
        FROM cars c
        JOIN price_history ph ON c.turbo_id = ph.turbo_id
        WHERE ph.recorded_at >= ?
    """

    params = [date_threshold]

    if brand:
        query += " AND LOWER(c.brand) = LOWER(?)"
        params.append(brand)

    query += """
        GROUP BY c.turbo_id
        ORDER BY change_count DESC, last_change DESC
        LIMIT ?
    """
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    trends = []
    increased = 0
    decreased = 0
    total_change_percent = 0

    for row in rows:
        # Ilk fiyati bul
        cursor.execute("""
            SELECT old_price FROM price_history
            WHERE turbo_id = ? AND recorded_at >= ?
            ORDER BY recorded_at ASC
            LIMIT 1
        """, (row["turbo_id"], date_threshold))

        first_record = cursor.fetchone()
        previous_price = first_record["old_price"] if first_record and first_record["old_price"] else row["current_price"]

        price_change = row["current_price"] - previous_price
        change_percent = (price_change / previous_price * 100) if previous_price else 0

        if price_change > 0:
            increased += 1
            trend = "up"
        elif price_change < 0:
            decreased += 1
            trend = "down"
        else:
            trend = "stable"

        total_change_percent += change_percent

        trends.append({
            "turbo_id": row["turbo_id"],
            "name": row["name"],
            "brand": row["brand"],
            "model": row["model"],
            "current_price": row["current_price"],
            "previous_price": previous_price,
            "price_change": price_change,
            "change_percent": round(change_percent, 2),
            "change_count": row["change_count"],
            "currency": row["currency"],
            "year": row["year"],
            "url": row["url"],
            "trend": trend,
            "last_change": row["last_change"],
        })

    conn.close()

    total_with_changes = len(trends)
    avg_change = round(total_change_percent / total_with_changes, 2) if total_with_changes else 0

    return {
        "period_days": days,
        "brand": brand,
        "trends": trends,
        "summary": {
            "total_with_changes": total_with_changes,
            "avg_change_percent": avg_change,
            "increased": increased,
            "decreased": decreased,
        }
    }


def calculate_view_trends(
    brand: Optional[str] = None,
    days: int = 7,
    limit: int = 50,
) -> Dict:
    """
    Goruntulenme trendlerini hesapla.
    En cok goruntulenme artisi olan araclari dondur.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    date_threshold = (datetime.now() - timedelta(days=days)).isoformat()

    query = """
        SELECT
            turbo_id,
            name,
            brand,
            model,
            price,
            currency,
            year,
            views,
            url,
            created_at,
            updated_at
        FROM cars
        WHERE updated_at >= ?
    """

    params = [date_threshold]

    if brand:
        query += " AND LOWER(brand) = LOWER(?)"
        params.append(brand)

    query += """
        ORDER BY views DESC
        LIMIT ?
    """
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    trends = []
    total_views = 0

    for row in rows:
        total_views += row["views"] or 0
        trends.append({
            "turbo_id": row["turbo_id"],
            "name": row["name"],
            "brand": row["brand"],
            "model": row["model"],
            "price": row["price"],
            "currency": row["currency"],
            "year": row["year"],
            "views": row["views"],
            "url": row["url"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        })

    conn.close()

    return {
        "period_days": days,
        "brand": brand,
        "trends": trends,
        "summary": {
            "total_cars": len(trends),
            "total_views": total_views,
            "avg_views": round(total_views / len(trends), 1) if trends else 0,
        }
    }


def get_market_summary(days: int = 30) -> Dict:
    """
    Genel pazar ozeti ve istatistikleri.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    date_threshold = (datetime.now() - timedelta(days=days)).isoformat()

    # Genel istatistikler
    cursor.execute("SELECT COUNT(*) as total FROM cars")
    total_cars = cursor.fetchone()["total"]

    # Son X gunde eklenen yeni araclar
    cursor.execute("""
        SELECT COUNT(*) as new_cars
        FROM cars
        WHERE created_at >= ?
    """, (date_threshold,))
    new_cars = cursor.fetchone()["new_cars"]

    # Son X gunde guncellenen araclar
    cursor.execute("""
        SELECT COUNT(*) as updated_cars
        FROM cars
        WHERE updated_at >= ? AND created_at < ?
    """, (date_threshold, date_threshold))
    updated_cars = cursor.fetchone()["updated_cars"]

    # Fiyat degisimi sayisi
    cursor.execute("""
        SELECT COUNT(*) as price_changes
        FROM price_history
        WHERE recorded_at >= ?
    """, (date_threshold,))
    price_changes = cursor.fetchone()["price_changes"]

    # Ortalama fiyat (AZN)
    cursor.execute("""
        SELECT AVG(price) as avg_price
        FROM cars
        WHERE currency = 'AZN' AND price > 0
    """)
    avg_price_azn = cursor.fetchone()["avg_price"] or 0

    # Marka dagilimi
    cursor.execute("""
        SELECT brand, COUNT(*) as count, AVG(price) as avg_price
        FROM cars
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY count DESC
        LIMIT 10
    """)
    top_brands = [
        {
            "brand": row["brand"],
            "count": row["count"],
            "avg_price": round(row["avg_price"] or 0, 0),
        }
        for row in cursor.fetchall()
    ]

    # Yil dagilimi
    cursor.execute("""
        SELECT year, COUNT(*) as count, AVG(price) as avg_price
        FROM cars
        WHERE year IS NOT NULL AND year > 1990
        GROUP BY year
        ORDER BY year DESC
        LIMIT 15
    """)
    year_distribution = [
        {
            "year": row["year"],
            "count": row["count"],
            "avg_price": round(row["avg_price"] or 0, 0),
        }
        for row in cursor.fetchall()
    ]

    # Sehir dagilimi
    cursor.execute("""
        SELECT city, COUNT(*) as count
        FROM cars
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city
        ORDER BY count DESC
        LIMIT 10
    """)
    city_distribution = [
        {"city": row["city"], "count": row["count"]}
        for row in cursor.fetchall()
    ]

    # Scrape session ozeti
    cursor.execute("""
        SELECT
            COUNT(*) as total_sessions,
            SUM(total_cars) as total_scraped,
            SUM(new_cars) as total_new,
            SUM(price_changes) as total_price_changes
        FROM scrape_sessions
        WHERE started_at >= ?
    """, (date_threshold,))
    session_stats = cursor.fetchone()

    conn.close()

    return {
        "period_days": days,
        "generated_at": datetime.now().isoformat(),
        "overview": {
            "total_cars": total_cars,
            "new_cars_period": new_cars,
            "updated_cars_period": updated_cars,
            "price_changes_period": price_changes,
            "avg_price_azn": round(avg_price_azn, 0),
        },
        "scraping": {
            "total_sessions": session_stats["total_sessions"] or 0,
            "total_scraped": session_stats["total_scraped"] or 0,
            "total_new_discovered": session_stats["total_new"] or 0,
            "total_price_changes": session_stats["total_price_changes"] or 0,
        },
        "distributions": {
            "top_brands": top_brands,
            "years": year_distribution,
            "cities": city_distribution,
        }
    }


def detect_price_anomalies(
    threshold_percent: float = 10.0,
    days: int = 7,
    limit: int = 50,
) -> Dict:
    """
    Buyuk fiyat degisikliklerini tespit et.
    threshold_percent: Minimum degisim yuzdesi (varsayilan %10)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    date_threshold = (datetime.now() - timedelta(days=days)).isoformat()

    cursor.execute("""
        SELECT
            ph.turbo_id,
            ph.old_price,
            ph.new_price,
            ph.currency,
            ph.recorded_at,
            c.name,
            c.brand,
            c.model,
            c.year,
            c.url,
            c.views
        FROM price_history ph
        JOIN cars c ON ph.turbo_id = c.turbo_id
        WHERE ph.recorded_at >= ?
            AND ph.old_price > 0
            AND ph.new_price > 0
        ORDER BY ph.recorded_at DESC
    """, (date_threshold,))

    rows = cursor.fetchall()
    conn.close()

    drops = []  # Fiyat dusenleri
    increases = []  # Fiyat artanlari

    for row in rows:
        change = row["new_price"] - row["old_price"]
        change_percent = (change / row["old_price"]) * 100

        if abs(change_percent) >= threshold_percent:
            item = {
                "turbo_id": row["turbo_id"],
                "name": row["name"],
                "brand": row["brand"],
                "model": row["model"],
                "year": row["year"],
                "old_price": row["old_price"],
                "new_price": row["new_price"],
                "change": change,
                "change_percent": round(change_percent, 2),
                "currency": row["currency"],
                "url": row["url"],
                "views": row["views"],
                "recorded_at": row["recorded_at"],
            }

            if change < 0:
                drops.append(item)
            else:
                increases.append(item)

    # En buyuk degisimler once
    drops.sort(key=lambda x: x["change_percent"])
    increases.sort(key=lambda x: x["change_percent"], reverse=True)

    return {
        "period_days": days,
        "threshold_percent": threshold_percent,
        "price_drops": drops[:limit],
        "price_increases": increases[:limit],
        "summary": {
            "total_drops": len(drops),
            "total_increases": len(increases),
            "biggest_drop_percent": drops[0]["change_percent"] if drops else 0,
            "biggest_increase_percent": increases[0]["change_percent"] if increases else 0,
        }
    }


def get_brand_comparison(brands: List[str], days: int = 30) -> Dict:
    """
    Birden fazla markayi karsilastir.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    comparisons = []

    for brand in brands:
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(year) as avg_year,
                AVG(views) as avg_views
            FROM cars
            WHERE LOWER(brand) = LOWER(?)
                AND price > 0
        """, (brand,))

        row = cursor.fetchone()

        if row and row["total"] > 0:
            comparisons.append({
                "brand": brand,
                "total_listings": row["total"],
                "avg_price": round(row["avg_price"] or 0, 0),
                "min_price": row["min_price"],
                "max_price": row["max_price"],
                "avg_year": round(row["avg_year"] or 0, 0),
                "avg_views": round(row["avg_views"] or 0, 1),
            })

    conn.close()

    return {
        "brands": brands,
        "period_days": days,
        "comparisons": comparisons,
    }
