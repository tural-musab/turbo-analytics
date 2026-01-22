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
    """Marka listesi."""
    stats = get_stats()
    return stats.get("top_brands", [])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
