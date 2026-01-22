"""
Turbo.az Analytics - Database Module
SQLite veritabani islemleri
Fiyat gecmisi ve scrape session takibi destekli
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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

    # Scrape session'lari - her scrape islemini takip eder
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT DEFAULT 'running',
            total_cars INTEGER DEFAULT 0,
            new_cars INTEGER DEFAULT 0,
            updated_cars INTEGER DEFAULT 0,
            price_changes INTEGER DEFAULT 0,
            filters_json TEXT
        )
        """
    )

    # Fiyat gecmisi - her fiyat degisikligini kaydeder
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turbo_id TEXT NOT NULL,
            old_price INTEGER,
            new_price INTEGER NOT NULL,
            currency TEXT DEFAULT 'AZN',
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            scrape_session_id INTEGER,
            FOREIGN KEY (turbo_id) REFERENCES cars(turbo_id),
            FOREIGN KEY (scrape_session_id) REFERENCES scrape_sessions(id)
        )
        """
    )

    # Session'da cekilen araclar - hangi araclar hangi session'da cekildi
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS session_cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_session_id INTEGER NOT NULL,
            turbo_id TEXT NOT NULL,
            is_new BOOLEAN DEFAULT FALSE,
            price_changed BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (scrape_session_id) REFERENCES scrape_sessions(id),
            FOREIGN KEY (turbo_id) REFERENCES cars(turbo_id)
        )
        """
    )

    # Marka cache tablosu
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cached_makes (
            id INTEGER PRIMARY KEY,
            turbo_id INTEGER UNIQUE NOT NULL,
            name TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Model cache tablosu
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cached_models (
            id INTEGER PRIMARY KEY,
            turbo_id INTEGER NOT NULL,
            make_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(turbo_id, make_id)
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_views ON cars(views DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_turbo ON price_history(turbo_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(recorded_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_cars ON session_cars(scrape_session_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_brand ON cars(brand)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price ON cars(price)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON cars(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cached_models_make ON cached_models(make_id)")

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


# ==================== SCRAPE SESSION FONKSIYONLARI ====================


def create_scrape_session(filters: Optional[Dict] = None) -> int:
    """Yeni scrape session olustur ve ID dondur."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO scrape_sessions (started_at, status, filters_json)
        VALUES (?, 'running', ?)
        """,
        (datetime.now().isoformat(), json.dumps(filters) if filters else None),
    )

    session_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return session_id


def finish_scrape_session(
    session_id: int,
    total_cars: int = 0,
    new_cars: int = 0,
    updated_cars: int = 0,
    price_changes: int = 0,
    status: str = "completed",
) -> None:
    """Scrape session'i tamamla ve istatistikleri kaydet."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE scrape_sessions
        SET finished_at = ?,
            status = ?,
            total_cars = ?,
            new_cars = ?,
            updated_cars = ?,
            price_changes = ?
        WHERE id = ?
        """,
        (
            datetime.now().isoformat(),
            status,
            total_cars,
            new_cars,
            updated_cars,
            price_changes,
            session_id,
        ),
    )

    conn.commit()
    conn.close()


def save_car_with_history(car_data: Dict, session_id: int) -> Dict[str, Any]:
    """
    Arac kaydet, fiyat degisikligini takip et ve session'a bagla.
    Returns: {"is_new": bool, "price_changed": bool, "old_price": int|None}
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    result = {"is_new": False, "price_changed": False, "old_price": None}

    try:
        turbo_id = car_data.get("turbo_id")
        new_price = car_data.get("price")
        currency = car_data.get("currency", "AZN")

        # Mevcut araci kontrol et
        cursor.execute(
            "SELECT id, price, currency FROM cars WHERE turbo_id = ?",
            (turbo_id,),
        )
        existing = cursor.fetchone()

        if existing:
            old_price = existing["price"]
            result["old_price"] = old_price

            # Fiyat degisti mi?
            if old_price != new_price:
                result["price_changed"] = True

                # Fiyat gecmisine kaydet
                cursor.execute(
                    """
                    INSERT INTO price_history
                    (turbo_id, old_price, new_price, currency, scrape_session_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (turbo_id, old_price, new_price, currency, session_id),
                )
        else:
            result["is_new"] = True

        # Araci kaydet/guncelle
        cursor.execute(
            """
            INSERT INTO cars (
                turbo_id, url, name, brand, model, price, currency,
                year, engine, mileage, city, views, is_new, is_vip,
                is_premium, created_at, raw_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(turbo_id) DO UPDATE SET
                views = excluded.views,
                price = excluded.price,
                updated_at = CURRENT_TIMESTAMP,
                raw_data = excluded.raw_data
            """,
            (
                turbo_id,
                car_data.get("url"),
                car_data.get("name"),
                car_data.get("brand"),
                car_data.get("model"),
                new_price,
                currency,
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

        # Session-car iliskisini kaydet
        cursor.execute(
            """
            INSERT INTO session_cars
            (scrape_session_id, turbo_id, is_new, price_changed)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, turbo_id, result["is_new"], result["price_changed"]),
        )

        conn.commit()
        return result

    except Exception as exc:
        print(f"Kayit hatasi: {exc}")
        conn.rollback()
        return result
    finally:
        conn.close()


def save_cars_batch_with_session(
    cars: List[Dict], filters: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Toplu arac kaydet, session olustur ve istatistikleri dondur.
    Returns: {session_id, total, new_cars, updated_cars, price_changes}
    """
    session_id = create_scrape_session(filters)

    stats = {
        "session_id": session_id,
        "total": 0,
        "new_cars": 0,
        "updated_cars": 0,
        "price_changes": 0,
    }

    for car in cars:
        result = save_car_with_history(car, session_id)
        stats["total"] += 1

        if result["is_new"]:
            stats["new_cars"] += 1
        else:
            stats["updated_cars"] += 1

        if result["price_changed"]:
            stats["price_changes"] += 1

    finish_scrape_session(
        session_id,
        total_cars=stats["total"],
        new_cars=stats["new_cars"],
        updated_cars=stats["updated_cars"],
        price_changes=stats["price_changes"],
    )

    return stats


# ==================== FIYAT GECMISI FONKSIYONLARI ====================


def get_price_history(turbo_id: str, limit: int = 50) -> List[Dict]:
    """Bir aracin fiyat gecmisini getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT old_price, new_price, currency, recorded_at
        FROM price_history
        WHERE turbo_id = ?
        ORDER BY recorded_at DESC
        LIMIT ?
        """,
        (turbo_id, limit),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_car_with_history(turbo_id: str) -> Optional[Dict]:
    """Arac bilgisi ve fiyat gecmisiyle birlikte getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM cars WHERE turbo_id = ?", (turbo_id,))
    car = cursor.fetchone()

    if not car:
        conn.close()
        return None

    result = dict(car)
    result["price_history"] = get_price_history(turbo_id)
    conn.close()
    return result


# ==================== SCRAPE SESSION SORGULARI ====================


def get_scrape_sessions(limit: int = 20) -> List[Dict]:
    """Son scrape session'lari getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT * FROM scrape_sessions
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_session_cars(session_id: int, limit: int = 100) -> List[Dict]:
    """Bir session'da cekilen araclari getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT c.*, sc.is_new, sc.price_changed
        FROM cars c
        INNER JOIN session_cars sc ON c.turbo_id = sc.turbo_id
        WHERE sc.scrape_session_id = ?
        ORDER BY c.views DESC
        LIMIT ?
        """,
        (session_id, limit),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_price_changes(days: int = 7, limit: int = 50) -> List[Dict]:
    """Son X gundeki fiyat degisikliklerini getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
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
            (ph.new_price - ph.old_price) as price_diff,
            ROUND(((ph.new_price - ph.old_price) * 100.0 / ph.old_price), 2) as price_change_percent
        FROM price_history ph
        INNER JOIN cars c ON ph.turbo_id = c.turbo_id
        WHERE ph.recorded_at >= datetime('now', '-' || ? || ' days')
        ORDER BY ABS(ph.new_price - ph.old_price) DESC
        LIMIT ?
        """,
        (days, limit),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_session_brands(session_id: int) -> List[Dict]:
    """Bir session'daki marka dagilimini getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT c.brand, COUNT(*) as count
        FROM cars c
        INNER JOIN session_cars sc ON c.turbo_id = sc.turbo_id
        WHERE sc.scrape_session_id = ?
        GROUP BY c.brand
        ORDER BY count DESC
        """,
        (session_id,),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def clear_old_data(keep_days: int = 30) -> Dict[str, int]:
    """
    Belirli gun sayisindan eski ve artik guncellenmeyen araclari temizle.
    Fiyat gecmisi ve session kayitlari korunur.
    Returns: {deleted_cars: int, deleted_sessions: int}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Eski ve guncellenmemis araclari bul
    cursor.execute(
        """
        SELECT COUNT(*) FROM cars
        WHERE updated_at < datetime('now', '-' || ? || ' days')
        """,
        (keep_days,),
    )
    old_cars_count = cursor.fetchone()[0]

    # Session-cars iliskilerini temizle (araclari silmeden once)
    cursor.execute(
        """
        DELETE FROM session_cars
        WHERE turbo_id IN (
            SELECT turbo_id FROM cars
            WHERE updated_at < datetime('now', '-' || ? || ' days')
        )
        """,
        (keep_days,),
    )

    # Eski araclari sil
    cursor.execute(
        """
        DELETE FROM cars
        WHERE updated_at < datetime('now', '-' || ? || ' days')
        """,
        (keep_days,),
    )

    # Bos session'lari temizle (hic arac kalmamis olanlar)
    cursor.execute(
        """
        DELETE FROM scrape_sessions
        WHERE id NOT IN (SELECT DISTINCT scrape_session_id FROM session_cars)
        AND id NOT IN (SELECT DISTINCT scrape_session_id FROM price_history WHERE scrape_session_id IS NOT NULL)
        """
    )
    deleted_sessions = cursor.rowcount

    conn.commit()
    conn.close()

    return {"deleted_cars": old_cars_count, "deleted_sessions": deleted_sessions}


def reset_database() -> Dict[str, int]:
    """
    Tum verileri temizle (tam sifirlama).
    Returns: {deleted_cars, deleted_sessions, deleted_history}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM cars")
    cars_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM scrape_sessions")
    sessions_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM price_history")
    history_count = cursor.fetchone()[0]

    cursor.execute("DELETE FROM session_cars")
    cursor.execute("DELETE FROM price_history")
    cursor.execute("DELETE FROM scrape_sessions")
    cursor.execute("DELETE FROM cars")

    conn.commit()
    conn.close()

    return {
        "deleted_cars": cars_count,
        "deleted_sessions": sessions_count,
        "deleted_history": history_count,
    }


# ==================== MARKA/MODEL CACHE FONKSIYONLARI ====================


def get_cached_makes() -> List[Dict]:
    """Cache'den marka listesini getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT turbo_id as id, name, count, cached_at
        FROM cached_makes
        ORDER BY name ASC
        """
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_cached_models(make_id: int) -> List[Dict]:
    """Cache'den belirli bir markanin modellerini getir."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT turbo_id as id, name, count, cached_at
        FROM cached_models
        WHERE make_id = ?
        ORDER BY name ASC
        """,
        (make_id,),
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def save_cached_makes(makes: List[Dict]) -> int:
    """Marka listesini cache'e kaydet."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Onceki cache'i temizle
    cursor.execute("DELETE FROM cached_makes")

    saved = 0
    for make in makes:
        try:
            cursor.execute(
                """
                INSERT INTO cached_makes (turbo_id, name, count)
                VALUES (?, ?, ?)
                """,
                (make.get("id"), make.get("name"), make.get("count", 0)),
            )
            saved += 1
        except Exception as e:
            print(f"Marka kayit hatasi: {e}")

    conn.commit()
    conn.close()
    return saved


def save_cached_models(make_id: int, models: List[Dict]) -> int:
    """Belirli bir markanin modellerini cache'e kaydet."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Bu markanin onceki modellerini temizle
    cursor.execute("DELETE FROM cached_models WHERE make_id = ?", (make_id,))

    saved = 0
    for model in models:
        try:
            cursor.execute(
                """
                INSERT INTO cached_models (turbo_id, make_id, name, count)
                VALUES (?, ?, ?, ?)
                """,
                (model.get("id"), make_id, model.get("name"), model.get("count", 0)),
            )
            saved += 1
        except Exception as e:
            print(f"Model kayit hatasi: {e}")

    conn.commit()
    conn.close()
    return saved


def is_cache_valid(cache_type: str, make_id: Optional[int] = None, max_age_hours: int = 24) -> bool:
    """Cache'in gecerli olup olmadigini kontrol et."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if cache_type == "makes":
        cursor.execute(
            """
            SELECT COUNT(*) FROM cached_makes
            WHERE cached_at >= datetime('now', '-' || ? || ' hours')
            """,
            (max_age_hours,),
        )
    elif cache_type == "models" and make_id is not None:
        cursor.execute(
            """
            SELECT COUNT(*) FROM cached_models
            WHERE make_id = ? AND cached_at >= datetime('now', '-' || ? || ' hours')
            """,
            (make_id, max_age_hours),
        )
    else:
        conn.close()
        return False

    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def get_cache_stats() -> Dict[str, Any]:
    """Cache istatistiklerini getir."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*), MIN(cached_at), MAX(cached_at) FROM cached_makes")
    makes_row = cursor.fetchone()

    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT make_id), MIN(cached_at), MAX(cached_at) FROM cached_models")
    models_row = cursor.fetchone()

    conn.close()

    return {
        "makes_count": makes_row[0],
        "makes_oldest": makes_row[1],
        "makes_newest": makes_row[2],
        "models_count": models_row[0],
        "models_makes_cached": models_row[1],
        "models_oldest": models_row[2],
        "models_newest": models_row[3],
    }


def clear_cache(cache_type: Optional[str] = None) -> Dict[str, int]:
    """Cache'i temizle. None ise tum cache temizlenir."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    result = {"makes_deleted": 0, "models_deleted": 0}

    if cache_type is None or cache_type == "makes":
        cursor.execute("SELECT COUNT(*) FROM cached_makes")
        result["makes_deleted"] = cursor.fetchone()[0]
        cursor.execute("DELETE FROM cached_makes")

    if cache_type is None or cache_type == "models":
        cursor.execute("SELECT COUNT(*) FROM cached_models")
        result["models_deleted"] = cursor.fetchone()[0]
        cursor.execute("DELETE FROM cached_models")

    conn.commit()
    conn.close()
    return result


if __name__ == "__main__":
    init_database()
    print("Veritabani hazir.")
