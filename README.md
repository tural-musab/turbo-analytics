# Turbo.az Analytics

Azerbaycan otomobil pazari (turbo.az) icin analiz platformu. En cok goruntulenen araclari, fiyat trendlerini ve marka dagilimlarini takip edin.

## Ozellikler

- Turbo.az'dan async web scraping
- En cok goruntulenen araclarin listesi
- Marka ve fiyat dagilim grafikleri
- Filtreleme ve arama
- REST API
- Modern React dashboard

## Proje Yapisi

```
turbo-analytics/
├── backend/
│   ├── api.py           # FastAPI backend
│   ├── database.py      # SQLite veritabani
│   ├── scraper.py       # Async web scraper
│   └── requirements.txt # Python bagimliliklari
├── frontend/
│   ├── index.html       # Ana HTML
│   ├── app.js           # React uygulamasi
│   └── styles.css       # CSS stilleri
├── run_scraper.py       # Scraper calistirici
├── CLAUDE.md            # Claude Code rehberi
└── README.md            # Bu dosya
```

## Kurulum

### 1. Virtual Environment Olustur

```bash
cd backend

# Virtual environment olustur
python3 -m venv venv

# Aktive et (macOS/Linux)
source venv/bin/activate

# Aktive et (Windows)
venv\Scripts\activate
```

> **Not:** Terminal satırının başında `(venv)` görürseniz aktif demektir.

### 2. Python Bagimliliklari

```bash
# venv aktif iken
pip install -r requirements.txt
```

### 3. Veritabanini Baslat

```bash
python database.py
```

## Kullanim

### Scraper'i Calistir

```bash
# Varsayilan 5 sayfa
python run_scraper.py

# Ozel sayfa sayisi
python run_scraper.py 10
```

### API'yi Baslat

```bash
cd backend
python api.py
```

API <http://localhost:8000> adresinde calisacak.

### Frontend

Frontend'i kullanmak icin:

1. API'nin calistigidan emin olun
2. `frontend/index.html` dosyasini bir web sunucusuyla servis edin

Basit yontem:

```bash
cd frontend
python -m http.server 3000
```

Sonra <http://localhost:3000> adresini ziyaret edin.

## API Endpoint'leri

| Endpoint | Method | Aciklama |
|----------|--------|----------|
| `/` | GET | API durumu |
| `/api/stats` | GET | Genel istatistikler |
| `/api/top-viewed` | GET | En cok goruntulenen araclar |
| `/api/cars` | GET | Arac listesi (filtreleme destekli) |
| `/api/brands` | GET | Marka listesi |
| `/api/scrape` | POST | Manuel scraping tetikle |

### Ornekler

```bash
# Istatistikler
curl http://localhost:8000/api/stats

# En cok goruntulenen 20 arac
curl http://localhost:8000/api/top-viewed?limit=20

# Mercedes araclari filtrele
curl "http://localhost:8000/api/cars?brand=Mercedes&sort_by=price_desc"

# Scraping baslat
curl -X POST "http://localhost:8000/api/scrape?pages=3"
```

## Teknolojiler

### Backend

- Python 3.10+
- FastAPI
- undetected-chromedriver (Cloudflare bypass)
- Selenium (browser automation)
- BeautifulSoup4 (HTML parsing)
- SQLite (veritabani)

### Frontend

- React 18
- Tailwind CSS
- Chart.js
- Vanilla JavaScript (Babel ile JSX)

## Notlar

- Scraper, turbo.az'in sunucularina asiri yuklenmeyi onlemek icin rate limiting kullanir
- Detay sayfalari paralel olarak cekilir (varsayilan 20 concurrent request)
- Veriler SQLite veritabaninda saklanir

## Cloudflare Bypass

Turbo.az, Cloudflare bot korumasi kullanmaktadir. Bu koruma, standart HTTP isteklerini engellemektedir.

### Denenen Yaklasimlar

| Yontem                   | Sonuc        | Aciklama                                                  |
|--------------------------|--------------|-----------------------------------------------------------|
| aiohttp + headers        | ❌ Basarisiz | User-Agent rotasyonu ve gelismis headers ile denendi      |
| curl_cffi                | ❌ Basarisiz | TLS fingerprint impersonation ile Chrome/Firefox taklidi  |
| Playwright (headless)    | ❌ Basarisiz | Headless Chrome ve Firefox ile denendi                    |
| Playwright (visible)     | ❌ Basarisiz | Gorunur tarayici ile bile engellendi                      |
| undetected-chromedriver  | ✅ Basarili  | Cloudflare bypass icin ozel yapilmis Chrome driver        |

### Cozum

`undetected-chromedriver` kullanilarak Cloudflare korumasi basariyla gecilmistir. Bu kutuphane:

- Gercek Chrome tarayicisi kullanir
- Anti-bot detection patch'leri uygulanmistir
- Selenium WebDriver uyumludur
- Hem headless hem de visible modda calisir

## Lisans

MIT
