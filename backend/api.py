"""
Turbo.az Analytics - FastAPI Backend
REST API for frontend dashboard
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from database import (
    get_car_with_history,
    get_price_changes,
    get_price_history,
    get_scrape_sessions,
    get_session_cars,
    get_stats,
    get_top_viewed,
    init_database,
    save_cars_batch,
    save_cars_batch_with_session,
    search_cars,
)
from export import (
    export_cars_csv,
    export_cars_excel,
    generate_export_filename,
    EXCEL_AVAILABLE,
)
from analytics import (
    calculate_price_trends,
    calculate_view_trends,
    get_market_summary,
    detect_price_anomalies,
    get_brand_comparison,
)
from scheduler import (
    scheduler,
    init_scheduler_tables,
    create_scheduled_job,
    get_scheduled_jobs,
    get_scheduled_job,
    update_scheduled_job,
    delete_scheduled_job,
    toggle_job_active,
    get_job_runs,
)
from scraper import TurboAzScraper


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama yasam dongusu."""
    init_database()
    init_scheduler_tables()
    print("Veritabani hazir.")
    # Scheduler'i baslat (opsiyonel - production icin)
    # await scheduler.start()
    yield
    # Scheduler'i durdur
    # await scheduler.stop()


app = FastAPI(
    title="Turbo.az Analytics API",
    description="Azerbaycan otomobil pazari analiz platformu",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """API durumu."""
    return {
        "status": "online",
        "name": "Turbo.az Analytics API",
        "version": "1.0.0",
    }


@app.get("/api/stats")
async def get_statistics():
    """Genel istatistikler."""
    return get_stats()


@app.get("/api/top-viewed")
async def top_viewed(limit: int = Query(10, ge=1, le=100)):
    """En cok goruntulenen araclar."""
    return get_top_viewed(limit)


@app.get("/api/cars")
async def list_cars(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    sort_by: str = Query("views", pattern="^(views|price_asc|price_desc|year|newest)$"),
    limit: int = Query(50, ge=1, le=500),
):
    """Arac listesi ve filtreleme."""
    return search_cars(
        brand=brand,
        min_price=min_price,
        max_price=max_price,
        min_year=min_year,
        sort_by=sort_by,
        limit=limit,
    )


@app.post("/api/scrape")
async def trigger_scrape(pages: int = Query(5, ge=1, le=50)):
    """Manuel scraping tetikle (session takipli)."""
    scraper = TurboAzScraper()
    cars = await scraper.run(pages=pages, with_details=True)

    # Session ile kaydet
    stats = save_cars_batch_with_session(cars, filters={"pages": pages})

    return {
        "status": "completed",
        "session_id": stats["session_id"],
        "total_scraped": stats["total"],
        "new_cars": stats["new_cars"],
        "updated_cars": stats["updated_cars"],
        "price_changes": stats["price_changes"],
    }


@app.get("/api/brands")
async def get_brands():
    """Veritabanindaki marka listesi."""
    stats = get_stats()
    return stats.get("top_brands", [])


@app.get("/api/makes")
async def get_turbo_makes():
    """Turbo.az'daki tum marka listesi (ID ve isim)."""
    scraper = TurboAzScraper()
    await scraper.create_session()
    try:
        makes = await scraper.get_makes()
        return makes
    finally:
        await scraper.close_session()


@app.get("/api/models/{make_id}")
async def get_turbo_models(make_id: int):
    """Belirli bir marka icin model listesi."""
    scraper = TurboAzScraper()
    await scraper.create_session()
    try:
        models = await scraper.get_models(make_id)
        return models
    finally:
        await scraper.close_session()


@app.get("/api/filter-info")
async def get_filter_info(
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
):
    """Filtreye gore toplam sayfa ve tahmini arac sayisi."""
    scraper = TurboAzScraper()
    await scraper.create_session()
    try:
        total_pages = await scraper.get_total_pages(
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
        return {
            "total_pages": total_pages,
            "estimated_cars": total_pages * 36,
        }
    finally:
        await scraper.close_session()


@app.post("/api/scrape-filtered")
async def trigger_filtered_scrape(
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    max_pages: int = Query(50, ge=1, le=100),
    with_details: bool = True,
):
    """Filtrelenmiş scraping tetikle (session takipli)."""
    scraper = TurboAzScraper()
    result = await scraper.run_filtered(
        make_id=make_id,
        model_id=model_id,
        min_price=min_price,
        max_price=max_price,
        min_year=min_year,
        max_year=max_year,
        max_pages=max_pages,
        with_details=with_details,
    )

    # Filtreleri kaydet
    filters = {
        "make_id": make_id,
        "model_id": model_id,
        "min_price": min_price,
        "max_price": max_price,
        "min_year": min_year,
        "max_year": max_year,
        "max_pages": max_pages,
    }

    # Session ile kaydet
    stats = save_cars_batch_with_session(result["cars"], filters=filters)

    return {
        "status": "completed",
        "session_id": stats["session_id"],
        "total_pages": result["total_pages"],
        "scraped_pages": result["scraped_pages"],
        "total_scraped": stats["total"],
        "new_cars": stats["new_cars"],
        "updated_cars": stats["updated_cars"],
        "price_changes": stats["price_changes"],
        "elapsed_seconds": result["elapsed"],
    }


# ==================== SCRAPE SESSION ENDPOINT'LERI ====================


@app.get("/api/scrape-sessions")
async def list_scrape_sessions(limit: int = Query(20, ge=1, le=100)):
    """Son scrape session'larini listele."""
    return get_scrape_sessions(limit)


@app.get("/api/scrape-sessions/{session_id}/cars")
async def list_session_cars(
    session_id: int,
    limit: int = Query(100, ge=1, le=500),
):
    """Bir session'da cekilen araclari listele."""
    return get_session_cars(session_id, limit)


# ==================== FIYAT GECMISI ENDPOINT'LERI ====================


@app.get("/api/price-history/{turbo_id}")
async def get_car_price_history(
    turbo_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """Bir aracin fiyat gecmisini getir."""
    return get_price_history(turbo_id, limit)


@app.get("/api/cars/{turbo_id}")
async def get_car_details(turbo_id: str):
    """Arac detayi ve fiyat gecmisiyle birlikte getir."""
    car = get_car_with_history(turbo_id)
    if not car:
        return {"error": "Arac bulunamadi"}
    return car


@app.get("/api/price-changes")
async def list_price_changes(
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
):
    """Son X gundeki fiyat degisikliklerini listele."""
    return get_price_changes(days, limit)


# ==================== EXPORT ENDPOINT'LERI ====================


@app.get("/api/export/csv")
async def export_csv(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    sort_by: str = Query("views", pattern="^(views|price_asc|price_desc|year|newest)$"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Filtrelenmis araclari CSV olarak indir."""
    cars = search_cars(
        brand=brand,
        min_price=min_price,
        max_price=max_price,
        min_year=min_year,
        sort_by=sort_by,
        limit=limit,
    )

    csv_data = export_cars_csv(cars)
    filename = generate_export_filename("turbo_araclar", "csv")

    return Response(
        content=csv_data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@app.get("/api/export/excel")
async def export_excel(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    sort_by: str = Query("views", pattern="^(views|price_asc|price_desc|year|newest)$"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Filtrelenmis araclari Excel olarak indir."""
    if not EXCEL_AVAILABLE:
        return {"error": "Excel export icin openpyxl kurulu degil. pip install openpyxl"}

    cars = search_cars(
        brand=brand,
        min_price=min_price,
        max_price=max_price,
        min_year=min_year,
        sort_by=sort_by,
        limit=limit,
    )

    excel_data = export_cars_excel(cars)
    if excel_data is None:
        return {"error": "Excel olusturulamadi"}

    filename = generate_export_filename("turbo_araclar", "xlsx")

    return Response(
        content=excel_data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@app.get("/api/export/session/{session_id}/csv")
async def export_session_csv(session_id: int):
    """Bir session'daki araclari CSV olarak indir."""
    cars = get_session_cars(session_id, limit=5000)

    if not cars:
        return {"error": "Session bulunamadi veya bos"}

    csv_data = export_cars_csv(cars)
    filename = generate_export_filename(f"session_{session_id}", "csv")

    return Response(
        content=csv_data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# ==================== TREND ANALIZI ENDPOINT'LERI ====================


@app.get("/api/trends/prices")
async def get_price_trends(
    brand: Optional[str] = None,
    days: int = Query(30, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
):
    """Marka bazli fiyat trendlerini getir."""
    return calculate_price_trends(brand=brand, days=days, limit=limit)


@app.get("/api/trends/views")
async def get_view_trends(
    brand: Optional[str] = None,
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
):
    """Goruntulenme trendlerini getir."""
    return calculate_view_trends(brand=brand, days=days, limit=limit)


@app.get("/api/trends/market-summary")
async def get_market_summary_endpoint(
    days: int = Query(30, ge=1, le=90),
):
    """Genel pazar ozeti ve istatistikleri."""
    return get_market_summary(days=days)


@app.get("/api/trends/price-drops")
async def get_price_drops(
    threshold: float = Query(10.0, ge=5.0, le=50.0),
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
):
    """Buyuk fiyat dusumlerini getir."""
    return detect_price_anomalies(
        threshold_percent=threshold,
        days=days,
        limit=limit,
    )


@app.get("/api/trends/brand-comparison")
async def compare_brands(
    brands: str = Query(..., description="Virgul ile ayrilmis marka listesi (ornek: BMW,Mercedes,Audi)"),
    days: int = Query(30, ge=1, le=90),
):
    """Birden fazla markayi karsilastir."""
    brand_list = [b.strip() for b in brands.split(",") if b.strip()]
    if not brand_list:
        return {"error": "En az bir marka belirtmelisiniz"}
    return get_brand_comparison(brands=brand_list, days=days)


# ==================== SCHEDULER ENDPOINT'LERI ====================


@app.get("/api/schedules")
async def list_schedules(include_inactive: bool = False):
    """Tum zamanli isleri listele."""
    return get_scheduled_jobs(include_inactive=include_inactive)


@app.post("/api/schedules")
async def create_schedule(
    name: str = Query(..., description="Is adi"),
    schedule_type: str = Query(..., pattern="^(hourly|daily|weekly)$"),
    schedule_time: str = Query("09:00", pattern="^\\d{2}:\\d{2}$"),
    schedule_days: Optional[str] = Query(None, description="Haftalik icin gunler (1-7, virgul ile)"),
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    max_pages: int = Query(50, ge=1, le=100),
    with_details: bool = True,
):
    """Yeni zamanli is olustur."""
    filters = {}
    if make_id:
        filters["make_id"] = make_id
    if model_id:
        filters["model_id"] = model_id
    if min_price:
        filters["min_price"] = min_price
    if max_price:
        filters["max_price"] = max_price
    if min_year:
        filters["min_year"] = min_year
    if max_year:
        filters["max_year"] = max_year

    return create_scheduled_job(
        name=name,
        schedule_type=schedule_type,
        schedule_time=schedule_time,
        schedule_days=schedule_days,
        filters=filters if filters else None,
        max_pages=max_pages,
        with_details=with_details,
    )


@app.get("/api/schedules/{job_id}")
async def get_schedule(job_id: str):
    """Belirli bir zamanli isi getir."""
    job = get_scheduled_job(job_id)
    if not job:
        return {"error": "Is bulunamadi"}
    return job


@app.put("/api/schedules/{job_id}")
async def update_schedule(
    job_id: str,
    name: Optional[str] = None,
    schedule_type: Optional[str] = Query(None, pattern="^(hourly|daily|weekly)$"),
    schedule_time: Optional[str] = Query(None, pattern="^\\d{2}:\\d{2}$"),
    schedule_days: Optional[str] = None,
    max_pages: Optional[int] = Query(None, ge=1, le=100),
    with_details: Optional[bool] = None,
    is_active: Optional[bool] = None,
):
    """Zamanli isi guncelle."""
    updates = {}
    if name is not None:
        updates["name"] = name
    if schedule_type is not None:
        updates["schedule_type"] = schedule_type
    if schedule_time is not None:
        updates["schedule_time"] = schedule_time
    if schedule_days is not None:
        updates["schedule_days"] = schedule_days
    if max_pages is not None:
        updates["max_pages"] = max_pages
    if with_details is not None:
        updates["with_details"] = with_details
    if is_active is not None:
        updates["is_active"] = is_active

    result = update_scheduled_job(job_id, updates)
    if not result:
        return {"error": "Is bulunamadi"}
    return result


@app.delete("/api/schedules/{job_id}")
async def delete_schedule(job_id: str):
    """Zamanli isi sil."""
    deleted = delete_scheduled_job(job_id)
    if not deleted:
        return {"error": "Is bulunamadi"}
    return {"status": "deleted", "job_id": job_id}


@app.post("/api/schedules/{job_id}/toggle")
async def toggle_schedule(job_id: str, is_active: bool = True):
    """Is aktif/pasif yap."""
    result = toggle_job_active(job_id, is_active)
    if not result:
        return {"error": "Is bulunamadi"}
    return result


@app.post("/api/schedules/{job_id}/run")
async def run_schedule_now(job_id: str):
    """Zamanli isi hemen calistir."""
    return await scheduler.run_job_now(job_id)


@app.get("/api/schedules/{job_id}/runs")
async def get_schedule_runs(
    job_id: str,
    limit: int = Query(20, ge=1, le=100),
):
    """Is calisma gecmisini getir."""
    return get_job_runs(job_id, limit)


@app.post("/api/scheduler/start")
async def start_scheduler():
    """Scheduler'i baslat."""
    await scheduler.start()
    return {"status": "started"}


@app.post("/api/scheduler/stop")
async def stop_scheduler():
    """Scheduler'i durdur."""
    await scheduler.stop()
    return {"status": "stopped"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
