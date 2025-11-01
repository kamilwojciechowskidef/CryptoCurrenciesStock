# etl/fetch_data.py
from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timezone
import time

# 1️⃣ Wczytaj zmienne środowiskowe
load_dotenv("etl/.env", encoding="utf-8")

API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

if not API_KEY:
    raise ValueError("❌ Brak COINGECKO_API_KEY w pliku .env")

# 2️⃣ Nagłówki (dla DEMO API)
headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY
}

# 3️⃣ Lista coinów (możesz dopasować do swojego projektu)
DEFAULT_COIN_IDS = [
    "bitcoin",
    "ethereum",
    "solana",
    "dogecoin",
    "tron",
    "ethena",
    "arbitrum",
    "optimism",
    "wormhole",
]

# 4️⃣ Funkcja do pobrania danych historycznych (ostatni rok)
def fetch_data(coin_ids=None, days_back=365):
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    all_rows = []
    for idx, coin in enumerate(coin_ids, start=1):
        try:
            url = f"{COINGECKO_BASE}/coins/{coin}/market_chart"
            params = {"vs_currency": "usd", "days": days_back, "interval": "daily"}

            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 401:
                raise RuntimeError(
                    "❌ CoinGecko 401 Unauthorized – sprawdź COINGECKO_API_KEY w .env."
                )
            if r.status_code == 429:
                print("⚠️ Zbyt wiele zapytań – czekam 15 sekund...")
                time.sleep(15)
                continue

            r.raise_for_status()
            data = r.json()
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            n = min(len(prices), len(volumes))

            for i in range(n):
                ts, price = prices[i]
                _, volume = volumes[i]
                ts_iso = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                all_rows.append({
                    "id": coin,
                    "symbol": coin[:3].upper(),
                    "name": coin.capitalize(),
                    "current_price": price,
                    "total_volume": volume,
                    "last_updated": ts_iso,
                })

            print(f"[{idx}/{len(coin_ids)}] ✅ {coin}: {n} punktów")
            time.sleep(1.2)

        except Exception as e:
            print(f"[{idx}/{len(coin_ids)}] ❌ Błąd dla {coin}: {e}")

    print(f"\n📊 Łącznie pobrano: {len(all_rows)} rekordów")
    return all_rows


# 5️⃣ Test ręczny
if __name__ == "__main__":
    print("🔗 Test połączenia z CoinGecko API...")
    ping = requests.get(f"{COINGECKO_BASE}/ping", headers=headers)
    print("Status:", ping.status_code)
    print("Odpowiedź:", ping.text)

    print("\n📥 Pobieranie danych...")
    data = fetch_data(days_back=365)
    print("Przykładowy rekord:", data[0] if data else "Brak danych")
