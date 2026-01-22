"""
Turbo.az Analytics - Web Scraper
Async scraping with aiohttp + BeautifulSoup
Filtre destekli versiyon
"""

import asyncio
import random
import re
import time
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode

import aiohttp
from bs4 import BeautifulSoup


class TurboAzScraper:
    BASE_URL = "https://turbo.az"
    CONCURRENT_REQUESTS = 5
    REQUEST_DELAY = 0.5

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        self.total_scraped = 0
        self.errors = 0
        self._makes_cache: Optional[List[Dict]] = None

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
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "az-AZ,az;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            },
        )

    async def close_session(self) -> None:
        """Session kapat."""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_makes(self) -> List[Dict]:
        """Marka listesini cek (ID ve isim)."""
        if self._makes_cache:
            return self._makes_cache

        url = f"{self.BASE_URL}/autos"
        html = await self.fetch_page(url)

        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        makes: List[Dict] = []

        select = soup.select_one('select[name="q[make][]"]')
        if select:
            for option in select.select("option[value]"):
                value = option.get("value", "")
                if value and value.isdigit():
                    makes.append({
                        "id": int(value),
                        "name": option.text.strip()
                    })

        self._makes_cache = makes
        return makes

    async def get_models(self, make_id: int) -> List[Dict]:
        """Belirli bir marka icin model listesini cek."""
        url = f"{self.BASE_URL}/autos?q%5Bmake%5D%5B%5D={make_id}"
        html = await self.fetch_page(url)

        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        models: List[Dict] = []

        select = soup.select_one('select[name="q[model][]"]')
        if select:
            # CSS selector'da class rakamla baslamaz, manuel filtre
            make_id_str = str(make_id)
            for option in select.select("option[value]"):
                classes = option.get("class", [])
                if make_id_str not in classes:
                    continue
                value = option.get("value", "")
                count = option.get("data-count", "0")
                if value and value.isdigit():
                    models.append({
                        "id": int(value),
                        "name": option.text.strip(),
                        "count": int(count) if count.isdigit() else 0
                    })

        # Arac sayisina gore sirala
        models.sort(key=lambda x: x["count"], reverse=True)
        return models

    def build_filter_url(
        self,
        page: int = 1,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> str:
        """Filtre parametreleriyle URL olustur."""
        params: List[Tuple[str, str]] = []
        params.append(("page", str(page)))

        if make_id:
            params.append(("q[make][]", str(make_id)))
        if model_id:
            params.append(("q[model][]", str(model_id)))
        if min_price:
            params.append(("q[price_from]", str(min_price)))
        if max_price:
            params.append(("q[price_to]", str(max_price)))
        if min_year:
            params.append(("q[year_from]", str(min_year)))
        if max_year:
            params.append(("q[year_to]", str(max_year)))

        return f"{self.BASE_URL}/autos?{urlencode(params)}"

    async def get_total_pages(
        self,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> int:
        """Filtreye gore toplam sayfa sayisini bul."""
        url = self.build_filter_url(
            page=1,
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
        html = await self.fetch_page(url)

        if not html:
            return 0

        soup = BeautifulSoup(html, "lxml")

        # Son sayfa numarasini bul
        pagination = soup.select('a[href*="page="]')
        max_page = 1
        for link in pagination:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        return max_page

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

    async def parse_listing_page(
        self,
        page_num: int,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> List[Dict]:
        """Liste sayfasindan araclari cek (filtre destekli)."""
        url = self.build_filter_url(
            page=page_num,
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
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

    async def scrape_pages(
        self,
        start_page: int = 1,
        end_page: int = 10,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> List[Dict]:
        """Belirtilen sayfa araligini scrape et (filtre destekli)."""
        print(f"Sayfa {start_page}-{end_page} taraniyor...")

        tasks = [
            self.parse_listing_page(
                i,
                make_id=make_id,
                model_id=model_id,
                min_price=min_price,
                max_price=max_price,
                min_year=min_year,
                max_year=max_year,
            )
            for i in range(start_page, end_page + 1)
        ]
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

    async def run_filtered(
        self,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
        max_pages: int = 50,
        with_details: bool = True,
    ) -> Dict:
        """Filtrelenmiş scraping fonksiyonu."""
        start_time = time.time()

        filters = []
        if make_id:
            filters.append(f"make_id={make_id}")
        if model_id:
            filters.append(f"model_id={model_id}")
        if min_price:
            filters.append(f"min_price={min_price}")
        if max_price:
            filters.append(f"max_price={max_price}")
        if min_year:
            filters.append(f"min_year={min_year}")
        if max_year:
            filters.append(f"max_year={max_year}")

        print("Turbo.az Filtered Scraper baslatiliyor...")
        print(f"Filtreler: {', '.join(filters) if filters else 'Yok'}")
        print("-" * 40)

        await self.create_session()

        try:
            # Toplam sayfa sayisini bul
            total_pages = await self.get_total_pages(
                make_id=make_id,
                model_id=model_id,
                min_price=min_price,
                max_price=max_price,
                min_year=min_year,
                max_year=max_year,
            )

            pages_to_scrape = min(total_pages, max_pages)
            print(f"Toplam sayfa: {total_pages}, Taranacak: {pages_to_scrape}")

            if pages_to_scrape == 0:
                return {
                    "cars": [],
                    "total_pages": 0,
                    "scraped_pages": 0,
                    "total_cars": 0,
                    "elapsed": 0,
                }

            # Sayfalari tara
            cars = await self.scrape_pages(
                start_page=1,
                end_page=pages_to_scrape,
                make_id=make_id,
                model_id=model_id,
                min_price=min_price,
                max_price=max_price,
                min_year=min_year,
                max_year=max_year,
            )

            if with_details and cars:
                cars = await self.scrape_with_details(cars)

            elapsed = time.time() - start_time
            print("-" * 40)
            print("Tamamlandi!")
            print(f"Toplam arac: {len(cars)}")
            print(f"Hatalar: {self.errors}")
            print(f"Sure: {elapsed:.1f} saniye")

            return {
                "cars": cars,
                "total_pages": total_pages,
                "scraped_pages": pages_to_scrape,
                "total_cars": len(cars),
                "elapsed": round(elapsed, 1),
            }
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
