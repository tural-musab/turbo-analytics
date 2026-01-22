"""
Turbo.az Analytics - FastAPI Backend
REST API for frontend dashboard
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from database import (
    get_stats,
    get_top_viewed,
    init_database,
    save_cars_batch,
    search_cars,
)
from scraper import TurboAzScraper


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama yasam dongusu."""
    init_database()
    print("Veritabani hazir.")
    yield


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
    limit: int = Query(50, ge=1, le=200),
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
    """Manuel scraping tetikle."""
    scraper = TurboAzScraper()
    cars = await scraper.run(pages=pages, with_details=True)

    saved = save_cars_batch(cars)

    return {
        "status": "completed",
        "total_scraped": len(cars),
        "saved_to_db": saved,
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
    """Filtrelenmiş scraping tetikle."""
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

    saved = save_cars_batch(result["cars"])

    return {
        "status": "completed",
        "total_pages": result["total_pages"],
        "scraped_pages": result["scraped_pages"],
        "total_scraped": result["total_cars"],
        "saved_to_db": saved,
        "elapsed_seconds": result["elapsed"],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
