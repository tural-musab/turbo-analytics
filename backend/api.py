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
    clear_old_data,
    get_car_with_history,
    get_price_changes,
    get_price_history,
    get_scrape_sessions,
    get_session_brands,
    get_session_cars,
    get_stats,
    get_top_viewed,
    init_database,
    reset_database,
    save_cars_batch,
    save_cars_batch_with_session,
    search_cars,
    # Cache fonksiyonlari
    get_cached_makes,
    get_cached_models,
    save_cached_makes,
    save_cached_models,
    is_cache_valid,
    get_cache_stats,
    clear_cache,
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
from scraper import (
    TurboAzScraper,
    FastTurboScraper,
    run_scraper_async,
    run_filtered_scraper_async,
    run_scraper_fast_async,
    get_makes_async,
    get_models_async,
)


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
    cars = await run_scraper_async(pages=pages, with_details=True)

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
async def get_turbo_makes(refresh: bool = False):
    """
    Turbo.az'daki tum marka listesi (ID ve isim).
    Cache'den okur, refresh=true ile yeniler.
    """
    # Cache gecerli mi kontrol et (24 saat)
    if not refresh and is_cache_valid("makes", max_age_hours=24):
        cached = get_cached_makes()
        if cached:
            print(f"Cache'den okundu: {len(cached)} marka")
            return cached

    # Cache gecersiz veya bos - turbo.az'dan cek
    try:
        makes = await get_makes_async()
        if makes:
            save_cached_makes(makes)
        return makes
    except Exception as e:
        # Hata durumunda cache'den dondur (eski olsa bile)
        cached = get_cached_makes()
        if cached:
            return cached
        return {"error": str(e), "message": "Marka listesi alinamadi ve cache bos"}


@app.get("/api/models/{make_id}")
async def get_turbo_models(make_id: int, refresh: bool = False):
    """
    Belirli bir marka icin model listesi.
    Cache'den okur, refresh=true ile yeniler.
    """
    # Cache gecerli mi kontrol et (24 saat)
    if not refresh and is_cache_valid("models", make_id=make_id, max_age_hours=24):
        cached = get_cached_models(make_id)
        if cached:
            print(f"Cache'den okundu: {len(cached)} model (make_id={make_id})")
            return cached

    # Cache gecersiz veya bos - turbo.az'dan cek
    try:
        models = await get_models_async(make_id)
        if models:
            save_cached_models(make_id, models)
        return models
    except Exception as e:
        # Hata durumunda cache'den dondur (eski olsa bile)
        cached = get_cached_models(make_id)
        if cached:
            return cached
        return {"error": str(e), "message": "Model listesi alinamadi ve cache bos"}


@app.get("/api/filter-info")
async def get_filter_info(
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
):
    """
    Filtreye gore toplam sayfa ve tahmini arac sayisi.
    Hybrid mod: Önce hızlı mod dener, bloklanırsa yavaş moda geçer.
    """
    from concurrent.futures import ThreadPoolExecutor
    import asyncio

    mode_used = "fast"

    # Önce hızlı mod dene
    fast_scraper = FastTurboScraper()
    fast_available = await fast_scraper.test_connection()

    if fast_available:
        total_pages = await fast_scraper.get_total_pages(
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
        await fast_scraper.close_session()
    else:
        await fast_scraper.close_session()
        mode_used = "slow"

        # Yavaş mod (Chrome)
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            scraper = TurboAzScraper()

            def _get_total_pages():
                scraper.create_session()
                try:
                    return scraper.get_total_pages(
                        make_id=make_id,
                        model_id=model_id,
                        min_price=min_price,
                        max_price=max_price,
                        min_year=min_year,
                        max_year=max_year,
                    )
                finally:
                    scraper.close_session()

            total_pages = await loop.run_in_executor(pool, _get_total_pages)

    return {
        "total_pages": total_pages,
        "estimated_cars": total_pages * 36,
        "mode": mode_used,
    }


@app.post("/api/scrape-filtered")
async def trigger_filtered_scrape(
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    max_pages: int = Query(50, ge=1, le=500),
    with_details: bool = True,
):
    """
    Filtrelenmiş scraping tetikle (session takipli).
    Hybrid mod: Önce hızlı mod (aiohttp) dener, bloklanırsa yavaş moda (Chrome) geçer.
    """
    import time as time_module

    start_time = time_module.time()
    mode_used = "fast"

    # Önce hızlı mod dene
    fast_scraper = FastTurboScraper()
    fast_available = await fast_scraper.test_connection()

    if fast_available:
        print("Hybrid: Hızlı mod kullanılıyor (aiohttp)")

        # Toplam sayfa sayısını al
        total_pages = await fast_scraper.get_total_pages(
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )

        pages_to_scrape = min(total_pages, max_pages)

        # Hızlı scraping
        cars = await fast_scraper.run(
            pages=pages_to_scrape,
            with_details=with_details,
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )

        await fast_scraper.close_session()

        result = {
            "cars": cars,
            "total_pages": total_pages,
            "scraped_pages": pages_to_scrape,
        }
    else:
        print("Hybrid: Hızlı mod bloklandı, yavaş moda geçiliyor (Chrome)")
        await fast_scraper.close_session()
        mode_used = "slow"

        # Yavaş mod (Chrome)
        result = await run_filtered_scraper_async(
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
            max_pages=max_pages,
            with_details=with_details,
        )

    elapsed = time_module.time() - start_time

    # Filtreleri kaydet
    filters = {
        "make_id": make_id,
        "model_id": model_id,
        "min_price": min_price,
        "max_price": max_price,
        "min_year": min_year,
        "max_year": max_year,
        "max_pages": max_pages,
        "mode": mode_used,
    }

    # Session ile kaydet
    stats = save_cars_batch_with_session(result["cars"], filters=filters)

    return {
        "status": "completed",
        "mode": mode_used,
        "session_id": stats["session_id"],
        "total_pages": result["total_pages"],
        "scraped_pages": result["scraped_pages"],
        "total_scraped": stats["total"],
        "new_cars": stats["new_cars"],
        "updated_cars": stats["updated_cars"],
        "price_changes": stats["price_changes"],
        "elapsed_seconds": round(elapsed, 2),
    }


@app.post("/api/scrape-fast")
async def scrape_fast(
    pages: int = Query(5, ge=1, le=100),
    with_details: bool = True,
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
):
    """
    Hızlı scraping (aiohttp) - VPN ile çalışır.
    Cloudflare engellenirse hata döner.
    """
    import time as time_module

    start_time = time_module.time()

    # Test bağlantı
    fast_scraper = FastTurboScraper()
    if not await fast_scraper.test_connection():
        await fast_scraper.close_session()
        return {
            "status": "error",
            "error": "Cloudflare engeli - VPN kullanın veya /api/scrape-filtered endpoint'ini deneyin",
        }
    await fast_scraper.close_session()

    # Hızlı scraping
    cars = await run_scraper_fast_async(
        pages=pages,
        with_details=with_details,
        make_id=make_id,
        model_id=model_id,
        min_price=min_price,
        max_price=max_price,
        min_year=min_year,
        max_year=max_year,
    )

    elapsed = time_module.time() - start_time

    # Filtreleri kaydet
    filters = {
        "make_id": make_id,
        "model_id": model_id,
        "min_price": min_price,
        "max_price": max_price,
        "min_year": min_year,
        "max_year": max_year,
        "pages": pages,
        "mode": "fast",
    }

    # Session ile kaydet
    stats = save_cars_batch_with_session(cars, filters=filters)

    return {
        "status": "completed",
        "mode": "fast",
        "session_id": stats["session_id"],
        "total_scraped": stats["total"],
        "new_cars": stats["new_cars"],
        "updated_cars": stats["updated_cars"],
        "price_changes": stats["price_changes"],
        "elapsed_seconds": round(elapsed, 2),
    }


@app.get("/api/scraper/test")
async def test_scraper_connection():
    """
    Hangi scraper modunun kullanılabilir olduğunu test et.
    """
    fast_scraper = FastTurboScraper()
    fast_ok = await fast_scraper.test_connection()
    await fast_scraper.close_session()

    return {
        "fast_mode": fast_ok,
        "slow_mode": True,  # Chrome her zaman çalışır
        "recommendation": "fast" if fast_ok else "slow",
        "message": "VPN aktif - hızlı mod kullanılabilir" if fast_ok else "VPN yok - yavaş mod kullanılacak",
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


@app.get("/api/scrape-sessions/{session_id}/brands")
async def list_session_brands(session_id: int):
    """Bir session'daki marka dagilimini getir."""
    return get_session_brands(session_id)


# ==================== VERI YONETIMI ENDPOINT'LERI ====================


@app.post("/api/data/clear-old")
async def clear_old_data_endpoint(
    keep_days: int = Query(30, ge=1, le=365),
):
    """
    Eski verileri temizle.
    keep_days: Son X gun icinde guncellenmemis araclari sil.
    """
    result = clear_old_data(keep_days)
    return {
        "status": "completed",
        "deleted_cars": result["deleted_cars"],
        "deleted_sessions": result["deleted_sessions"],
        "message": f"{result['deleted_cars']} eski arac silindi",
    }


@app.post("/api/data/reset")
async def reset_data_endpoint(confirm: str = Query(..., pattern="^RESET$")):
    """
    TUM verileri sil (tam sifirlama).
    confirm parametresi 'RESET' olmali.
    """
    result = reset_database()
    return {
        "status": "completed",
        "deleted_cars": result["deleted_cars"],
        "deleted_sessions": result["deleted_sessions"],
        "deleted_history": result["deleted_history"],
        "message": "Veritabani tamamen sifirlandi",
    }


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
    max_pages: int = Query(50, ge=1, le=500),
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
    max_pages: Optional[int] = Query(None, ge=1, le=500),
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


# ==================== CACHE YONETIMI ====================


@app.get("/api/cache/stats")
async def get_cache_statistics():
    """Cache istatistiklerini getir."""
    return get_cache_stats()


@app.post("/api/cache/refresh")
async def refresh_cache(cache_type: str = Query("all", regex="^(all|makes|models)$")):
    """
    Cache'i yenile.
    cache_type: 'all', 'makes', veya 'models'
    """
    result = {"refreshed": [], "errors": []}

    if cache_type in ("all", "makes"):
        try:
            makes = await get_makes_async()
            if makes:
                count = save_cached_makes(makes)
                result["refreshed"].append({"type": "makes", "count": count})
            else:
                result["errors"].append({"type": "makes", "error": "Marka listesi bos"})
        except Exception as e:
            result["errors"].append({"type": "makes", "error": str(e)})

    # Models icin tum markalari gez (sadece cache_type="all" veya "models" ise)
    if cache_type in ("all", "models"):
        cached_makes = get_cached_makes()
        if cached_makes:
            models_count = 0
            for make in cached_makes[:50]:  # Ilk 50 marka ile sinirla
                try:
                    models = await get_models_async(make["id"])
                    if models:
                        save_cached_models(make["id"], models)
                        models_count += len(models)
                except Exception:
                    pass  # Hatalari sessizce gec
            result["refreshed"].append({"type": "models", "count": models_count})
        else:
            result["errors"].append({"type": "models", "error": "Marka cache'i bos, once markalari yenileyin"})

    return result


@app.post("/api/cache/clear")
async def clear_cache_data(cache_type: Optional[str] = None):
    """
    Cache'i temizle.
    cache_type: None (tumu), 'makes', veya 'models'
    """
    return clear_cache(cache_type)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
