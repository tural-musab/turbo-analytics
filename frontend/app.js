/**
 * Turbo.az Analytics - React Dashboard
 */

const { useState, useEffect, useRef } = React;

// API Base URL
const API_URL = 'http://localhost:8000/api';

// Format numbers
const formatNumber = (num) => {
    if (!num) return '0';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
};

// Format price
const formatPrice = (price, currency) => {
    return `${formatNumber(price)} ${currency || 'AZN'}`;
};

// Stat Card Component
const StatCard = ({ title, value, icon, color }) => (
    <div className={`stat-card rounded-2xl p-6 ${color}`}>
        <div className="flex items-center justify-between">
            <div>
                <p className="text-gray-400 text-sm mb-1">{title}</p>
                <p className="text-3xl font-bold">{value}</p>
            </div>
            <div className="text-4xl opacity-50">{icon}</div>
        </div>
    </div>
);

// Car Card Component
const CarCard = ({ car, rank }) => (
    <div
        className="glass rounded-xl p-4 hover:scale-105 transition-transform cursor-pointer"
        onClick={() => window.open(car.url, '_blank')}
    >
        <div className="flex items-start gap-4">
            <div
                className={`text-2xl font-bold w-10 h-10 rounded-full flex items-center justify-center
                    ${rank <= 3 ? 'bg-yellow-500' : 'bg-gray-600'}`}
            >
                {rank}
            </div>
            <div className="flex-1">
                <h3 className="font-semibold text-lg">{car.name}</h3>
                <div className="flex items-center gap-2 mt-1 text-sm text-gray-400">
                    <span>{car.year}</span>
                    <span>-</span>
                    <span>{car.engine}</span>
                    <span>-</span>
                    <span>{car.city}</span>
                </div>
                <div className="flex items-center justify-between mt-3">
                    <span className="text-xl font-bold text-green-400">
                        {formatPrice(car.price, car.currency)}
                    </span>
                    <span className="flex items-center gap-1 text-blue-400">
                        <span>Views:</span>
                        <span className="font-semibold">{formatNumber(car.views)}</span>
                    </span>
                </div>
            </div>
        </div>
    </div>
);

// Brand Chart Component
const BrandChart = ({ data }) => {
    const chartRef = useRef(null);
    const chartInstance = useRef(null);

    useEffect(() => {
        if (chartInstance.current) {
            chartInstance.current.destroy();
        }

        if (chartRef.current && data.length > 0) {
            const ctx = chartRef.current.getContext('2d');
            chartInstance.current = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: data.map((d) => d.brand),
                    datasets: [
                        {
                            data: data.map((d) => d.count),
                            backgroundColor: [
                                '#3B82F6',
                                '#10B981',
                                '#F59E0B',
                                '#EF4444',
                                '#8B5CF6',
                                '#EC4899',
                                '#06B6D4',
                                '#F97316',
                                '#14B8A6',
                                '#6366F1',
                            ],
                            borderWidth: 0,
                        },
                    ],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: { color: '#fff', padding: 15 },
                        },
                    },
                },
            });
        }

        return () => {
            if (chartInstance.current) {
                chartInstance.current.destroy();
            }
        };
    }, [data]);

    return <canvas ref={chartRef} />;
};

// Main App
const App = () => {
    const [stats, setStats] = useState(null);
    const [topViewed, setTopViewed] = useState([]);
    const [loading, setLoading] = useState(true);
    const [scraping, setScraping] = useState(false);
    const [error, setError] = useState(null);

    // Fetch data
    const fetchData = async () => {
        try {
            setLoading(true);
            const [statsRes, topRes] = await Promise.all([
                fetch(`${API_URL}/stats`),
                fetch(`${API_URL}/top-viewed?limit=10`),
            ]);

            if (!statsRes.ok || !topRes.ok) throw new Error('API error');

            setStats(await statsRes.json());
            setTopViewed(await topRes.json());
            setError(null);
        } catch (err) {
            setError('API baglantisi kurulamadi. Backend calisiyor mu?');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    // Trigger scrape
    const triggerScrape = async () => {
        if (scraping) return;

        setScraping(true);
        try {
            const res = await fetch(`${API_URL}/scrape?pages=5`, { method: 'POST' });
            const data = await res.json();
            alert(`Scraping tamamlandi!\n${data.total_scraped} arac cekildi.`);
            fetchData();
        } catch (err) {
            alert('Scraping basarisiz!');
        } finally {
            setScraping(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);

    // Loading state
    if (loading) {
        return (
            <div className="min-h-screen flex items-center justify-center">
                <div className="text-center">
                    <div className="text-6xl mb-4 loading">...</div>
                    <p className="text-xl text-gray-400">Veriler yukleniyor...</p>
                </div>
            </div>
        );
    }

    // Error state
    if (error) {
        return (
            <div className="min-h-screen flex items-center justify-center">
                <div className="text-center glass rounded-2xl p-8 max-w-md">
                    <div className="text-6xl mb-4">!</div>
                    <h2 className="text-xl font-bold mb-2">Baglanti Hatasi</h2>
                    <p className="text-gray-400 mb-4">{error}</p>
                    <p className="text-sm text-gray-500 mb-4">
                        Backend'i baslatmak icin:
                        <br />
                        <code className="bg-gray-800 px-2 py-1 rounded">python api.py</code>
                    </p>
                    <button
                        onClick={fetchData}
                        className="bg-blue-500 hover:bg-blue-600 px-6 py-2 rounded-lg"
                    >
                        Tekrar Dene
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen p-6">
            {/* Header */}
            <header className="flex items-center justify-between mb-8">
                <div>
                    <h1 className="text-3xl font-bold flex items-center gap-3">
                        Turbo.az Analytics
                    </h1>
                    <p className="text-gray-400 mt-1">
                        Azerbaycan Otomobil Pazari Analiz Platformu
                    </p>
                </div>
                <button
                    onClick={triggerScrape}
                    disabled={scraping}
                    className={`px-6 py-3 rounded-xl font-semibold flex items-center gap-2
                        ${
                            scraping
                                ? 'bg-gray-600 cursor-not-allowed'
                                : 'bg-blue-500 hover:bg-blue-600 glow'
                        }`}
                >
                    {scraping ? (
                        <>
                            <span className="loading">...</span>
                            Cekiliyor...
                        </>
                    ) : (
                        <>
                            <span>~</span>
                            Verileri Guncelle
                        </>
                    )}
                </button>
            </header>

            {/* Stats Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                <StatCard
                    title="Toplam Arac"
                    value={formatNumber(stats?.total_cars || 0)}
                    icon="[C]"
                    color="border-l-4 border-blue-500"
                />
                <StatCard
                    title="Toplam Goruntulenme"
                    value={formatNumber(stats?.total_views || 0)}
                    icon="[V]"
                    color="border-l-4 border-green-500"
                />
                <StatCard
                    title="Ortalama Fiyat"
                    value={`${formatNumber(Math.round(stats?.avg_price_azn || 0))} AZN`}
                    icon="[$]"
                    color="border-l-4 border-yellow-500"
                />
                <StatCard
                    title="Marka Sayisi"
                    value={stats?.top_brands?.length || 0}
                    icon="[B]"
                    color="border-l-4 border-purple-500"
                />
            </div>

            {/* Main Content */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Top Viewed */}
                <div className="lg:col-span-2">
                    <div className="glass rounded-2xl p-6">
                        <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                            <span>[TOP]</span>
                            En Cok Goruntulenen Araclar
                        </h2>
                        <div className="space-y-3">
                            {topViewed.map((car, index) => (
                                <CarCard key={car.turbo_id} car={car} rank={index + 1} />
                            ))}
                            {topViewed.length === 0 && (
                                <p className="text-gray-400 text-center py-8">
                                    Henuz veri yok. "Verileri Guncelle" butonuna tiklayin.
                                </p>
                            )}
                        </div>
                    </div>
                </div>

                {/* Brand Distribution */}
                <div>
                    <div className="glass rounded-2xl p-6 h-full">
                        <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                            <span>[CHART]</span>
                            Marka Dagilimi
                        </h2>
                        <div className="h-80">
                            {stats?.top_brands?.length > 0 ? (
                                <BrandChart data={stats.top_brands} />
                            ) : (
                                <p className="text-gray-400 text-center py-8">
                                    Veri bekleniyor...
                                </p>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* Footer */}
            <footer className="mt-8 text-center text-gray-500 text-sm">
                <p>Turbo.az Analytics - Azerbaycan Otomobil Pazari Analiz Platformu</p>
            </footer>
        </div>
    );
};

// Render
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
