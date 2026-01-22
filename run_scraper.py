#!/usr/bin/env python3
"""
Turbo.az Analytics - Scraper Runner
Veritabanini baslat ve scraper'i calistir
"""

import asyncio
import sys
from pathlib import Path

# Backend modullerini import et
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from database import init_database, save_cars_batch
from scraper import TurboAzScraper


async def main():
    """Ana fonksiyon."""
    print("=" * 50)
    print("Turbo.az Analytics - Scraper")
    print("=" * 50)

    # Veritabanini baslat
    print("\n[1/3] Veritabani hazirlaniyor...")
    init_database()

    # Scraper'i calistir
    print("\n[2/3] Scraper baslatiliyor...")
    scraper = TurboAzScraper()

    # Varsayilan olarak 5 sayfa, detaylarla birlikte
    pages = 5
    if len(sys.argv) > 1:
        try:
            pages = int(sys.argv[1])
        except ValueError:
            print(f"Gecersiz sayfa sayisi: {sys.argv[1]}, varsayilan 5 kullaniliyor")

    cars = await scraper.run(pages=pages, with_details=True)

    # Veritabanina kaydet
    print("\n[3/3] Veriler kaydediliyor...")
    saved = save_cars_batch(cars)
    print(f"Kaydedilen arac sayisi: {saved}")

    # Ozet
    print("\n" + "=" * 50)
    print("OZET")
    print("=" * 50)
    print(f"Toplam taranan arac: {len(cars)}")
    print(f"Veritabanina kaydedilen: {saved}")

    if cars:
        # En cok goruntulenenler
        sorted_cars = sorted(cars, key=lambda x: x.get("views", 0), reverse=True)
        print("\nEn cok goruntulenen 5 arac:")
        print("-" * 50)
        for i, car in enumerate(sorted_cars[:5], 1):
            print(f"{i}. {car['name'][:35]:35} | {car.get('views', 0):>6} baxis")

    print("\n" + "=" * 50)
    print("Tamamlandi!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
