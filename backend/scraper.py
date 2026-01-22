"""
Turbo.az Analytics - Web Scraper
undetected-chromedriver ile Cloudflare bypass
"""

import asyncio
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class TurboAzScraper:
    BASE_URL = "https://turbo.az"
    REQUEST_DELAY = 1.0

    def __init__(self, headless: bool = False) -> None:
        self._driver: Optional[uc.Chrome] = None
        self._headless = headless
        self.total_scraped = 0
        self.errors = 0
        self._makes_cache: Optional[List[Dict]] = None

    def create_session(self) -> None:
        """Chrome browser oluştur (undetected-chromedriver ile)."""
        options = uc.ChromeOptions()

        if self._headless:
            options.add_argument("--headless=new")

        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        print("Chrome browser başlatılıyor...")
        self._driver = uc.Chrome(options=options)

        # İlk sayfayı ziyaret et
        print(f"Navigating to: {self.BASE_URL}")
        self._driver.get(self.BASE_URL)

        # Sayfanın yüklenmesini bekle
        time.sleep(5)

        print(f"Page title: {self._driver.title}")
        print("Browser hazır.")

    def close_session(self) -> None:
        """Browser kapat."""
        if self._driver:
            self._driver.quit()
            self._driver = None

    def fetch_page(self, url: str) -> Optional[str]:
        """Sayfa içeriğini çek."""
        if not self._driver:
            raise RuntimeError("Browser is not initialized")

        delay = self.REQUEST_DELAY + random.uniform(0.3, 1.0)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._driver.get(url)
                time.sleep(delay)

                # Sayfanın yüklenmesini bekle
                WebDriverWait(self._driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                html = self._driver.page_source

                # İçerik gerçek mi kontrol et
                if "products-i" in html or 'name="q[make]"' in html:
                    return html
                elif "Just a moment" in html or "challenge" in html.lower():
                    print(f"Cloudflare challenge (attempt {attempt + 1}/{max_retries}): {url}")
                    if attempt < max_retries - 1:
                        time.sleep(10)
                        continue
                    return None
                else:
                    return html

            except Exception as exc:
                self.errors += 1
                print(f"Fetch error (attempt {attempt + 1}): {url} - {exc}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                return None

        return None

    def get_makes(self) -> List[Dict]:
        """Marka listesini çek (ID ve isim)."""
        if self._makes_cache:
            return self._makes_cache

        url = f"{self.BASE_URL}/autos"
        html = self.fetch_page(url)

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

    def get_models(self, make_id: int) -> List[Dict]:
        """Belirli bir marka için model listesini çek."""
        url = f"{self.BASE_URL}/autos?q%5Bmake%5D%5B%5D={make_id}"
        html = self.fetch_page(url)

        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        models: List[Dict] = []

        select = soup.select_one('select[name="q[model][]"]')
        if select:
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
        """Filtre parametreleriyle URL oluştur."""
        params: List[str] = []
        params.append(f"page={page}")

        if make_id:
            params.append(f"q[make][]={make_id}")
        if model_id:
            params.append(f"q[model][]={model_id}")
        if min_price:
            params.append(f"q[price_from]={min_price}")
        if max_price:
            params.append(f"q[price_to]={max_price}")
        if min_year:
            params.append(f"q[year_from]={min_year}")
        if max_year:
            params.append(f"q[year_to]={max_year}")

        return f"{self.BASE_URL}/autos?{'&'.join(params)}"

    def get_total_pages(
        self,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> int:
        """Filtreye göre toplam sayfa sayısını bul."""
        url = self.build_filter_url(
            page=1,
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
        html = self.fetch_page(url)

        if not html:
            return 0

        soup = BeautifulSoup(html, "lxml")

        pagination = soup.select('a[href*="page="]')
        max_page = 1
        for link in pagination:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        return max_page

    @staticmethod
    def parse_price(price_text: str) -> Tuple[int, str]:
        """Fiyatı parse et."""
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
        """Özellikleri parse et (2024, 2.3 L, 0 km)."""
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

    def parse_listing_page(
        self,
        page_num: int,
        make_id: Optional[int] = None,
        model_id: Optional[int] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> List[Dict]:
        """Liste sayfasından araçları çek (filtre destekli)."""
        url = self.build_filter_url(
            page=page_num,
            make_id=make_id,
            model_id=model_id,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
        )
        html = self.fetch_page(url)

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

    def parse_detail_page(self, car: Dict) -> Dict:
        """Detay sayfasından bakış sayısını çek."""
        html = self.fetch_page(car["url"])

        if not html:
            return car

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text()
        views_match = re.search(r"Baxışların sayı[:\s]*(\d[\d\s]*)", text)
        if views_match:
            car["views"] = int(views_match.group(1).replace(" ", ""))

        self.total_scraped += 1
        return car

    def scrape_pages(
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
        """Belirtilen sayfa aralığını scrape et."""
        print(f"Sayfa {start_page}-{end_page} taranıyor...")

        all_cars: List[Dict] = []

        for page_num in range(start_page, end_page + 1):
            cars = self.parse_listing_page(
                page_num,
                make_id=make_id,
                model_id=model_id,
                min_price=min_price,
                max_price=max_price,
                min_year=min_year,
                max_year=max_year,
            )
            all_cars.extend(cars)

            progress = page_num - start_page + 1
            total = end_page - start_page + 1
            print(f"Sayfa ilerleme: {progress}/{total} ({len(all_cars)} araç)")

            # Her sayfa arasında bekle
            time.sleep(random.uniform(1.0, 2.0))

        print(f"{len(all_cars)} araç bulundu")
        return all_cars

    def scrape_with_details(self, cars: List[Dict], batch_size: int = 5) -> List[Dict]:
        """Araçların detay sayfalarını çek."""
        print(f"{len(cars)} araç için detaylar çekiliyor...")

        detailed_cars: List[Dict] = []

        for i, car in enumerate(cars):
            detailed_car = self.parse_detail_page(car)
            detailed_cars.append(detailed_car)

            progress = i + 1
            if progress % 10 == 0 or progress == len(cars):
                print(f"İlerleme: {progress}/{len(cars)} ({progress * 100 // len(cars)}%)")

        return detailed_cars

    def run(self, pages: int = 5, with_details: bool = True) -> List[Dict]:
        """Ana scraping fonksiyonu."""
        start_time = time.time()

        print("Turbo.az Scraper başlatılıyor (undetected-chromedriver)...")
        print(f"Hedef: {pages} sayfa")
        print("-" * 40)

        self.create_session()

        try:
            cars = self.scrape_pages(1, pages)

            if with_details and cars:
                cars = self.scrape_with_details(cars)

            elapsed = time.time() - start_time
            print("-" * 40)
            print("Tamamlandı!")
            print(f"Toplam araç: {len(cars)}")
            print(f"Hatalar: {self.errors}")
            print(f"Süre: {elapsed:.1f} saniye")

            return cars
        finally:
            self.close_session()

    def run_filtered(
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

        print("Turbo.az Filtered Scraper başlatılıyor (undetected-chromedriver)...")
        print(f"Filtreler: {', '.join(filters) if filters else 'Yok'}")
        print("-" * 40)

        self.create_session()

        try:
            # Toplam sayfa sayısını bul
            total_pages = self.get_total_pages(
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

            # Sayfaları tara
            cars = self.scrape_pages(
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
                cars = self.scrape_with_details(cars)

            elapsed = time.time() - start_time
            print("-" * 40)
            print("Tamamlandı!")
            print(f"Toplam araç: {len(cars)}")
            print(f"Hatalar: {self.errors}")
            print(f"Süre: {elapsed:.1f} saniye")

            return {
                "cars": cars,
                "total_pages": total_pages,
                "scraped_pages": pages_to_scrape,
                "total_cars": len(cars),
                "elapsed": round(elapsed, 1),
            }
        finally:
            self.close_session()


# Async wrapper for API compatibility
async def run_scraper_async(
    pages: int = 5,
    with_details: bool = True,
    headless: bool = False,
) -> List[Dict]:
    """Async wrapper for scraper (runs in thread pool)."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        scraper = TurboAzScraper(headless=headless)
        result = await loop.run_in_executor(
            pool, lambda: scraper.run(pages=pages, with_details=with_details)
        )
    return result


async def run_filtered_scraper_async(
    make_id: Optional[int] = None,
    model_id: Optional[int] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    max_pages: int = 50,
    with_details: bool = True,
    headless: bool = False,
) -> Dict:
    """Async wrapper for filtered scraper (runs in thread pool)."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        scraper = TurboAzScraper(headless=headless)
        result = await loop.run_in_executor(
            pool,
            lambda: scraper.run_filtered(
                make_id=make_id,
                model_id=model_id,
                min_price=min_price,
                max_price=max_price,
                min_year=min_year,
                max_year=max_year,
                max_pages=max_pages,
                with_details=with_details,
            ),
        )
    return result


async def get_makes_async(headless: bool = False) -> List[Dict]:
    """Async wrapper for get_makes."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        scraper = TurboAzScraper(headless=headless)

        def _get_makes():
            scraper.create_session()
            try:
                return scraper.get_makes()
            finally:
                scraper.close_session()

        result = await loop.run_in_executor(pool, _get_makes)
    return result


async def get_models_async(make_id: int, headless: bool = False) -> List[Dict]:
    """Async wrapper for get_models."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        scraper = TurboAzScraper(headless=headless)

        def _get_models():
            scraper.create_session()
            try:
                return scraper.get_models(make_id)
            finally:
                scraper.close_session()

        result = await loop.run_in_executor(pool, _get_models)
    return result


def main() -> None:
    scraper = TurboAzScraper()
    cars = scraper.run(pages=3, with_details=True)

    sorted_cars = sorted(cars, key=lambda x: x["views"], reverse=True)

    print("\nEn çok bakış alan ilk 10:")
    print("-" * 60)
    for i, car in enumerate(sorted_cars[:10], 1):
        name = car["name"][:30]
        views = car["views"]
        price = car["price"]
        currency = car["currency"]
        print(f"{i:2}. {name:30} | {views:>6} bakış | {price:>8} {currency}")


if __name__ == "__main__":
    main()
