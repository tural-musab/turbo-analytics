"""
Turbo.az Analytics - Scheduler Module
Zamanli scraping sistemi
"""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import uuid

from database import save_cars_batch_with_session

DB_PATH = "turbo_analytics.db"


def get_db_connection():
    """Veritabani baglantisi olustur."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_scheduler_tables():
    """Scheduler tablolarini olustur."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            schedule_type TEXT NOT NULL,
            schedule_time TEXT NOT NULL,
            schedule_days TEXT,
            filters_json TEXT,
            max_pages INTEGER DEFAULT 50,
            with_details BOOLEAN DEFAULT TRUE,
            is_active BOOLEAN DEFAULT TRUE,
            last_run TIMESTAMP,
            next_run TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT DEFAULT 'running',
            session_id INTEGER,
            total_cars INTEGER DEFAULT 0,
            new_cars INTEGER DEFAULT 0,
            price_changes INTEGER DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (job_id) REFERENCES scheduled_jobs(id)
        )
    """)

    conn.commit()
    conn.close()


def create_scheduled_job(
    name: str,
    schedule_type: str,
    schedule_time: str,
    filters: Optional[Dict] = None,
    max_pages: int = 50,
    with_details: bool = True,
    schedule_days: Optional[str] = None,
) -> Dict:
    """
    Yeni zamanli is olustur.

    Args:
        name: Is adi
        schedule_type: 'daily', 'weekly', 'hourly'
        schedule_time: Saat (HH:MM formatinda)
        filters: Scraping filtreleri (make_id, model_id, etc.)
        max_pages: Maksimum sayfa sayisi
        with_details: Detay sayfalarini cek?
        schedule_days: Haftalik icin gunler (ornek: "1,3,5" = Pzt,Car,Cum)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    job_id = str(uuid.uuid4())[:8]
    filters_json = json.dumps(filters) if filters else None

    # Sonraki calisma zamanini hesapla
    next_run = calculate_next_run(schedule_type, schedule_time, schedule_days)

    cursor.execute("""
        INSERT INTO scheduled_jobs
        (id, name, schedule_type, schedule_time, schedule_days, filters_json,
         max_pages, with_details, next_run)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id, name, schedule_type, schedule_time, schedule_days,
        filters_json, max_pages, with_details, next_run.isoformat() if next_run else None
    ))

    conn.commit()
    conn.close()

    return {
        "id": job_id,
        "name": name,
        "schedule_type": schedule_type,
        "schedule_time": schedule_time,
        "schedule_days": schedule_days,
        "filters": filters,
        "max_pages": max_pages,
        "with_details": with_details,
        "next_run": next_run.isoformat() if next_run else None,
        "is_active": True,
    }


def calculate_next_run(
    schedule_type: str,
    schedule_time: str,
    schedule_days: Optional[str] = None,
) -> Optional[datetime]:
    """Sonraki calisma zamanini hesapla."""
    try:
        hour, minute = map(int, schedule_time.split(":"))
    except (ValueError, AttributeError):
        hour, minute = 9, 0  # Varsayilan

    now = datetime.now()
    today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if schedule_type == "hourly":
        # Her saat basinda
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return next_hour

    elif schedule_type == "daily":
        # Her gun belirli saatte
        if now >= today:
            return today + timedelta(days=1)
        return today

    elif schedule_type == "weekly":
        # Belirli gunlerde
        if schedule_days:
            days = [int(d) for d in schedule_days.split(",")]
        else:
            days = [1]  # Varsayilan Pazartesi

        for i in range(7):
            check_date = today + timedelta(days=i)
            if check_date.isoweekday() in days:
                if check_date > now:
                    return check_date
        # Gelecek haftaya sar
        for i in range(7, 14):
            check_date = today + timedelta(days=i)
            if check_date.isoweekday() in days:
                return check_date

    return today + timedelta(days=1)


def get_scheduled_jobs(include_inactive: bool = False) -> List[Dict]:
    """Tum zamanli isleri getir."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM scheduled_jobs"
    if not include_inactive:
        query += " WHERE is_active = TRUE"
    query += " ORDER BY next_run ASC"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    jobs = []
    for row in rows:
        jobs.append({
            "id": row["id"],
            "name": row["name"],
            "schedule_type": row["schedule_type"],
            "schedule_time": row["schedule_time"],
            "schedule_days": row["schedule_days"],
            "filters": json.loads(row["filters_json"]) if row["filters_json"] else None,
            "max_pages": row["max_pages"],
            "with_details": bool(row["with_details"]),
            "is_active": bool(row["is_active"]),
            "last_run": row["last_run"],
            "next_run": row["next_run"],
            "created_at": row["created_at"],
        })

    return jobs


def get_scheduled_job(job_id: str) -> Optional[Dict]:
    """Belirli bir zamanli isi getir."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM scheduled_jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row["id"],
        "name": row["name"],
        "schedule_type": row["schedule_type"],
        "schedule_time": row["schedule_time"],
        "schedule_days": row["schedule_days"],
        "filters": json.loads(row["filters_json"]) if row["filters_json"] else None,
        "max_pages": row["max_pages"],
        "with_details": bool(row["with_details"]),
        "is_active": bool(row["is_active"]),
        "last_run": row["last_run"],
        "next_run": row["next_run"],
        "created_at": row["created_at"],
    }


def update_scheduled_job(job_id: str, updates: Dict) -> Optional[Dict]:
    """Zamanli isi guncelle."""
    conn = get_db_connection()
    cursor = conn.cursor()

    allowed_fields = [
        "name", "schedule_type", "schedule_time", "schedule_days",
        "max_pages", "with_details", "is_active"
    ]

    set_clauses = []
    values = []

    for field in allowed_fields:
        if field in updates:
            set_clauses.append(f"{field} = ?")
            values.append(updates[field])

    if "filters" in updates:
        set_clauses.append("filters_json = ?")
        values.append(json.dumps(updates["filters"]) if updates["filters"] else None)

    if not set_clauses:
        conn.close()
        return get_scheduled_job(job_id)

    set_clauses.append("updated_at = ?")
    values.append(datetime.now().isoformat())

    values.append(job_id)

    cursor.execute(f"""
        UPDATE scheduled_jobs
        SET {", ".join(set_clauses)}
        WHERE id = ?
    """, values)

    # Sonraki calisma zamanini yeniden hesapla
    if any(f in updates for f in ["schedule_type", "schedule_time", "schedule_days"]):
        job = get_scheduled_job(job_id)
        if job:
            next_run = calculate_next_run(
                job["schedule_type"],
                job["schedule_time"],
                job["schedule_days"]
            )
            cursor.execute("""
                UPDATE scheduled_jobs SET next_run = ? WHERE id = ?
            """, (next_run.isoformat() if next_run else None, job_id))

    conn.commit()
    conn.close()

    return get_scheduled_job(job_id)


def delete_scheduled_job(job_id: str) -> bool:
    """Zamanli isi sil."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM scheduled_jobs WHERE id = ?", (job_id,))
    deleted = cursor.rowcount > 0

    conn.commit()
    conn.close()

    return deleted


def toggle_job_active(job_id: str, is_active: bool) -> Optional[Dict]:
    """Is aktif/pasif yap."""
    return update_scheduled_job(job_id, {"is_active": is_active})


def record_job_run_start(job_id: str) -> int:
    """Is calismasini baslat ve kaydet."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO job_runs (job_id, status)
        VALUES (?, 'running')
    """, (job_id,))

    run_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return run_id


def record_job_run_finish(
    run_id: int,
    status: str,
    session_id: Optional[int] = None,
    total_cars: int = 0,
    new_cars: int = 0,
    price_changes: int = 0,
    error_message: Optional[str] = None,
):
    """Is calismasini bitir ve sonuclari kaydet."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE job_runs
        SET finished_at = ?,
            status = ?,
            session_id = ?,
            total_cars = ?,
            new_cars = ?,
            price_changes = ?,
            error_message = ?
        WHERE id = ?
    """, (
        datetime.now().isoformat(),
        status,
        session_id,
        total_cars,
        new_cars,
        price_changes,
        error_message,
        run_id,
    ))

    # Ana is kaydini guncelle
    cursor.execute("""
        SELECT job_id FROM job_runs WHERE id = ?
    """, (run_id,))
    row = cursor.fetchone()

    if row:
        job_id = row["job_id"]
        job = get_scheduled_job(job_id)
        if job:
            next_run = calculate_next_run(
                job["schedule_type"],
                job["schedule_time"],
                job["schedule_days"]
            )
            cursor.execute("""
                UPDATE scheduled_jobs
                SET last_run = ?, next_run = ?
                WHERE id = ?
            """, (
                datetime.now().isoformat(),
                next_run.isoformat() if next_run else None,
                job_id,
            ))

    conn.commit()
    conn.close()


def get_job_runs(job_id: str, limit: int = 20) -> List[Dict]:
    """Bir isin calisma gecmisini getir."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM job_runs
        WHERE job_id = ?
        ORDER BY started_at DESC
        LIMIT ?
    """, (job_id, limit))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "job_id": row["job_id"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "status": row["status"],
            "session_id": row["session_id"],
            "total_cars": row["total_cars"],
            "new_cars": row["new_cars"],
            "price_changes": row["price_changes"],
            "error_message": row["error_message"],
        }
        for row in rows
    ]


def get_due_jobs() -> List[Dict]:
    """Calisma zamani gelmis isleri getir."""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().isoformat()

    cursor.execute("""
        SELECT * FROM scheduled_jobs
        WHERE is_active = TRUE
            AND next_run IS NOT NULL
            AND next_run <= ?
        ORDER BY next_run ASC
    """, (now,))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "name": row["name"],
            "schedule_type": row["schedule_type"],
            "filters": json.loads(row["filters_json"]) if row["filters_json"] else None,
            "max_pages": row["max_pages"],
            "with_details": bool(row["with_details"]),
        }
        for row in rows
    ]


class ScrapeScheduler:
    """Async scrape scheduler."""

    def __init__(self):
        self._running = False
        self._task = None

    async def start(self):
        """Scheduler'i baslat."""
        if self._running:
            return

        init_scheduler_tables()
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        print("Scheduler baslatildi.")

    async def stop(self):
        """Scheduler'i durdur."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        print("Scheduler durduruldu.")

    async def _run_loop(self):
        """Ana scheduler dongusu."""
        while self._running:
            try:
                due_jobs = get_due_jobs()

                for job in due_jobs:
                    print(f"Zamanli is calistiriliyor: {job['name']} ({job['id']})")
                    await self._run_job(job)

                # Her dakika kontrol et
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Scheduler hatasi: {e}")
                await asyncio.sleep(60)

    async def _run_job(self, job: Dict):
        """Tek bir isi calistir."""
        run_id = record_job_run_start(job["id"])

        try:
            # Scraper'i import et (circular import onlemek icin)
            from scraper import TurboAzScraper

            scraper = TurboAzScraper()
            filters = job.get("filters") or {}

            if filters.get("make_id") or filters.get("model_id"):
                # Filtreli scraping
                result = await scraper.run_filtered(
                    make_id=filters.get("make_id"),
                    model_id=filters.get("model_id"),
                    min_price=filters.get("min_price"),
                    max_price=filters.get("max_price"),
                    min_year=filters.get("min_year"),
                    max_year=filters.get("max_year"),
                    max_pages=job.get("max_pages", 50),
                    with_details=job.get("with_details", True),
                )
                cars = result["cars"]
            else:
                # Genel scraping
                cars = await scraper.run(
                    pages=job.get("max_pages", 50),
                    with_details=job.get("with_details", True),
                )

            # Kaydet
            stats = save_cars_batch_with_session(cars, filters=filters)

            record_job_run_finish(
                run_id=run_id,
                status="completed",
                session_id=stats["session_id"],
                total_cars=stats["total"],
                new_cars=stats["new_cars"],
                price_changes=stats["price_changes"],
            )

            print(f"Is tamamlandi: {job['name']} - {stats['total']} arac")

        except Exception as e:
            record_job_run_finish(
                run_id=run_id,
                status="failed",
                error_message=str(e),
            )
            print(f"Is basarisiz: {job['name']} - {e}")

    async def run_job_now(self, job_id: str) -> Dict:
        """Bir isi hemen calistir (manuel tetikleme)."""
        job = get_scheduled_job(job_id)
        if not job:
            return {"error": "Is bulunamadi"}

        await self._run_job(job)
        return {"status": "completed", "job_id": job_id}


# Global scheduler instance
scheduler = ScrapeScheduler()
