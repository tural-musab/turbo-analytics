# Turbo.az Analytics - Claude Code Rehberi

Bu dosya, Claude Code'un bu projede nasil calisacagini tanimlar.

## Proje Ozeti

Turbo.az (Azerbaycan araba pazari) icin web scraping ve analiz platformu.

## Teknoloji Stack

### Backend
- **Dil:** Python 3.10+
- **Framework:** FastAPI
- **Async HTTP:** aiohttp
- **HTML Parsing:** BeautifulSoup4 + lxml
- **Veritabani:** SQLite

### Frontend
- **Framework:** React 18 (CDN uzerinden)
- **CSS:** Tailwind CSS
- **Grafikler:** Chart.js
- **Transpiler:** Babel (browser icinde JSX)

## Proje Yapisi

```
turbo-analytics/
├── backend/
│   ├── api.py           # FastAPI REST API
│   ├── database.py      # SQLite islemleri
│   ├── scraper.py       # Async web scraper
│   └── requirements.txt
├── frontend/
│   ├── index.html       # Ana sayfa
│   ├── app.js           # React komponetleri
│   └── styles.css       # Ozel stiller
├── run_scraper.py       # CLI scraper
├── CLAUDE.md            # Bu dosya
└── README.md
```

## Kod Standartlari

### Python
- Type hints kullan
- Docstring'ler Turkce olabilir
- async/await kullan (blocking IO'dan kacin)
- f-string tercih et
- PEP 8 uyumlu

### JavaScript/React
- Functional components kullan
- Hooks (useState, useEffect, useRef)
- const/let kullan (var yok)
- Tailwind CSS class'lari tercih et

## Veritabani Semasi

### cars tablosu
| Kolon | Tip | Aciklama |
|-------|-----|----------|
| id | INTEGER | Primary key |
| turbo_id | TEXT | Turbo.az'daki ilan ID'si (UNIQUE) |
| url | TEXT | Ilan URL'i |
| name | TEXT | Arac adi (marka + model) |
| brand | TEXT | Marka |
| model | TEXT | Model |
| price | INTEGER | Fiyat |
| currency | TEXT | Para birimi (AZN/USD/EUR) |
| year | INTEGER | Uretim yili |
| engine | TEXT | Motor hacmi |
| mileage | INTEGER | Kilometre |
| city | TEXT | Sehir |
| views | INTEGER | Goruntulenme sayisi |
| is_new | BOOLEAN | Sifir km mi |
| is_vip | BOOLEAN | VIP ilan mi |
| is_premium | BOOLEAN | Premium ilan mi |
| created_at | TIMESTAMP | Kayit tarihi |
| updated_at | TIMESTAMP | Guncelleme tarihi |

## API Endpoint'leri

| Endpoint | Method | Aciklama |
|----------|--------|----------|
| GET / | API durumu |
| GET /api/stats | Genel istatistikler |
| GET /api/top-viewed?limit=N | En cok goruntulenenler |
| GET /api/cars?brand=X&min_price=Y | Filtreleme |
| GET /api/brands | Marka listesi |
| POST /api/scrape?pages=N | Scraping tetikle |

## Scraper Notlari

### Turbo.az HTML Selektorleri
- `.products-i` - Arac karti container
- `.products-i__link` - Detay sayfasi linki
- `.products-i__name` - Arac adi
- `.products-i__price` - Fiyat
- `.products-i__attributes` - Yil, Motor, KM
- `.products-i__datetime` - Sehir, Tarih

### Rate Limiting
- CONCURRENT_REQUESTS: 20
- REQUEST_DELAY: 0.1 saniye
- Batch size: 50 (detay sayfalari icin)

## Gelistirme Komutlari

```bash
# Backend baslat
cd backend && python api.py

# Frontend baslat (development)
cd frontend && python -m http.server 3000

# Scraper calistir
python run_scraper.py 5

# Veritabanini sifirla
rm backend/turbo_analytics.db && python backend/database.py
```

## Onemli Notlar

1. **Encoding:** Azerbaycan Turkcesi karakterleri icin UTF-8 kullan
2. **Async:** Scraper tamamen async, blocking call'lardan kacin
3. **Error Handling:** Scraper hatalarini logla ama devam et
4. **CORS:** API tum origin'lere acik (development icin)

## Gelecek Gelistirmeler

- [ ] Fiyat gecmisi takibi
- [ ] Email/Push bildirimleri
- [ ] Favoriler sistemi
- [ ] Karsilastirma ozelligi
- [ ] Daha fazla filtre secenegi
- [ ] Export (CSV, Excel)
