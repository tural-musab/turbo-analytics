"""
Turbo.az Analytics - Web Scraper
Async scraping with aiohttp + BeautifulSoup
"""

import asyncio
import re
import time
from typing import Dict, List, Optional, Tuple, Union

import aiohttp
from bs4 import BeautifulSoup


class TurboAzScraper:
    BASE_URL = "https://turbo.az"
    CONCURRENT_REQUESTS = 20
    REQUEST_DELAY = 0.1

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        self.total_scraped = 0
        self.errors = 0

    async def create_session(self) -> None:
        """HTTP session olustur."""
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=self.CONCURRENT_REQUESTS)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "az,en;q=0.5",
            },
        )

    async def close_session(self) -> None:
        """Session kapat."""
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_page(self, url: str) -> Optional[str]:
        """Sayfa icerigini cek."""
        if not self.session:
            raise RuntimeError("Session is not initialized")

        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                print(f"HTTP {response.status}: {url}")
                return None
        except Exception as exc:
            self.errors += 1
            print(f"Fetch error: {url} - {exc}")
            return None
        finally:
            if self.REQUEST_DELAY:
                await asyncio.sleep(self.REQUEST_DELAY)

    @staticmethod
    def parse_price(price_text: str) -> Tuple[int, str]:
        """Fiyati parse et."""
        price_text = price_text.strip()

        if "₼" in price_text or "AZN" in price_text:
            currency = "AZN"
        elif "$" in price_text or "USD" in price_text:
            currency = "USD"
        elif "€" in price_text or "EUR" in price_text:
            currency = "EUR"
        else:
            currency = "AZN"

        numbers = re.findall(r"\d+", price_text.replace(" ", ""))
        price = int("".join(numbers)) if numbers else 0
        return price, currency

    @staticmethod
    def parse_attributes(attr_text: str) -> Dict[str, Optional[Union[int, str]]]:
        """Ozellikleri parse et (2024, 2.3 L, 0 km)."""
        parts = attr_text.split(",")
        result: Dict[str, Optional[int | str]] = {
            "year": None,
            "engine": None,
            "mileage": None,
        }

        for part in parts:
            part = part.strip()
            if re.match(r"^\d{4}$", part):
                result["year"] = int(part)
            elif "L" in part:
                result["engine"] = part
            elif "km" in part.lower():
                km = re.findall(r"\d+", part.replace(" ", ""))
                result["mileage"] = int("".join(km)) if km else 0

        return result

    @staticmethod
    def parse_location_date(text: str) -> Dict[str, Optional[str]]:
        """Konum ve tarihi parse et."""
        parts = text.split(",")
        return {
            "city": parts[0].strip() if parts else "Unknown",
            "date": parts[1].strip() if len(parts) > 1 else None,
        }

    async def parse_listing_page(self, page_num: int) -> List[Dict]:
        """Liste sayfasindan araclari cek."""
        url = f"{self.BASE_URL}/autos?page={page_num}"
        html = await self.fetch_page(url)

        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        cars: List[Dict] = []

        for item in soup.select(".products-i"):
            try:
                link = item.select_one(".products-i__link")
                if not link:
                    continue

                href = link.get("href", "")
                turbo_id = href.split("/")[2].split("-")[0] if "/autos/" in href else None

                if not turbo_id:
                    continue

                name_el = item.select_one(".products-i__name")
                price_el = item.select_one(".products-i__price")
                attr_el = item.select_one(".products-i__attributes")
                datetime_el = item.select_one(".products-i__datetime")

                name = name_el.text.strip() if name_el else "Unknown"
                price_text = price_el.text.strip() if price_el else "0"
                attr_text = attr_el.text.strip() if attr_el else ""
                loc_text = datetime_el.text.strip() if datetime_el else ""

                price, currency = self.parse_price(price_text)
                attributes = self.parse_attributes(attr_text)
                location = self.parse_location_date(loc_text)

                name_parts = name.split(" ", 1)
                brand = name_parts[0] if name_parts else "Unknown"
                model = name_parts[1] if len(name_parts) > 1 else ""

                classes = item.get("class", [])
                is_vip = "vipped" in classes
                is_premium = "featured" in classes

                cars.append(
                    {
                        "turbo_id": turbo_id,
                        "url": self.BASE_URL + href,
                        "name": name,
                        "brand": brand,
                        "model": model,
                        "price": price,
                        "currency": currency,
                        "year": attributes["year"],
                        "engine": attributes["engine"],
                        "mileage": attributes["mileage"],
                        "city": location["city"],
                        "is_vip": is_vip,
                        "is_premium": is_premium,
                        "is_new": attributes["mileage"] == 0,
                        "views": 0,
                    }
                )
            except Exception as exc:
                print(f"Parse error: {exc}")
                continue

        return cars

    async def parse_detail_page(self, car: Dict) -> Dict:
        """Detay sayfasindan baxis sayisini cek."""
        html = await self.fetch_page(car["url"])

        if not html:
            return car

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text()
        views_match = re.search(r"Baxışların sayı[:\s]*(\d[\d\s]*)", text)
        if views_match:
            car["views"] = int(views_match.group(1).replace(" ", ""))

        self.total_scraped += 1
        return car

    async def scrape_pages(self, start_page: int = 1, end_page: int = 10) -> List[Dict]:
        """Belirtilen sayfa araligini scrape et."""
        print(f"Sayfa {start_page}-{end_page} taraniyor...")

        tasks = [self.parse_listing_page(i) for i in range(start_page, end_page + 1)]
        pages_results = await asyncio.gather(*tasks)

        all_cars: List[Dict] = []
        for cars in pages_results:
            all_cars.extend(cars)

        print(f"{len(all_cars)} arac bulundu")
        return all_cars

    async def scrape_with_details(self, cars: List[Dict], batch_size: int = 50) -> List[Dict]:
        """Araclarin detay sayfalarini cek (baxis sayisi icin)."""
        print(f"{len(cars)} arac icin detaylar cekiliyor...")

        detailed_cars: List[Dict] = []

        for i in range(0, len(cars), batch_size):
            batch = cars[i : i + batch_size]
            tasks = [self.parse_detail_page(car) for car in batch]
            results = await asyncio.gather(*tasks)
            detailed_cars.extend(results)

            progress = min(i + batch_size, len(cars))
            print(f"Ilerleme: {progress}/{len(cars)} ({progress * 100 // len(cars)}%)")

            await asyncio.sleep(1)

        return detailed_cars

    async def run(self, pages: int = 5, with_details: bool = True) -> List[Dict]:
        """Ana scraping fonksiyonu."""
        start_time = time.time()

        print("Turbo.az Scraper baslatiliyor...")
        print(f"Hedef: {pages} sayfa")
        print("-" * 40)

        await self.create_session()

        try:
            cars = await self.scrape_pages(1, pages)

            if with_details and cars:
                cars = await self.scrape_with_details(cars)

            elapsed = time.time() - start_time
            print("-" * 40)
            print("Tamamlandi!")
            print(f"Toplam arac: {len(cars)}")
            print(f"Hatalar: {self.errors}")
            print(f"Sure: {elapsed:.1f} saniye")

            return cars
        finally:
            await self.close_session()


async def main() -> None:
    scraper = TurboAzScraper()
    cars = await scraper.run(pages=3, with_details=True)

    sorted_cars = sorted(cars, key=lambda x: x["views"], reverse=True)

    print("\nEn cok baxis alan ilk 10:")
    print("-" * 60)
    for i, car in enumerate(sorted_cars[:10], 1):
        name = car["name"][:30]
        views = car["views"]
        price = car["price"]
        currency = car["currency"]
        print(f"{i:2}. {name:30} | {views:>6} baxis | {price:>8} {currency}")


if __name__ == "__main__":
    asyncio.run(main())
