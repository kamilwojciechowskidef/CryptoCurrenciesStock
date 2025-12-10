import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from db.db import insert_data

# === Wczytaj konfigurację ===
load_dotenv("etl/.env", encoding="utf-8")

COINGECKO_BASE = "https://api.coingecko.com/api/v3/"
API_KEY = os.getenv("COINGECKO_API_KEY")

headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY,  # działa również dla kont demo
}

# Domyślne ID kryptowalut
DEFAULT_COIN_IDS = [
    "bitcoin", "ethereum", "solana", "dogecoin",
    "tron", "ethena", "arbitrum", "optimism", "wormhole"
]


def fetch_data(coin_ids=None, days_back=365):
    """
    Pobiera dane z CoinGecko dla wybranych kryptowalut
    i wysyła je bezpośrednio do bazy Supabase przez REST API.
    """
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    all_rows = []

    for idx, coin in enumerate(coin_ids, start=1):
        try:
            # === endpoint CoinGecko ===
            url = f"{COINGECKO_BASE}/coins/{coin}/market_chart"
            params = {"vs_currency": "usd", "days": days_back, "interval": "daily"}

            r = requests.get(url, headers=headers, params=params, timeout=30)

            # Obsługa błędów API
            if r.status_code == 401:
                raise RuntimeError("❌ CoinGecko 401 Unauthorized – sprawdź COINGECKO_API_KEY w .env.")
            if r.status_code == 429:
                print("⚠️ Zbyt wiele zapytań – czekam 15 sekund...")
                time.sleep(15)
                continue

            r.raise_for_status()
            data = r.json()

            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            n = min(len(prices), len(volumes))
            market_cap = data.get("market_cap", {}).get("usd")
            high_24h = data.get("high_24h", {}).get("usd")
            low_24h = data.get("low_24h", {}).get("usd")
            change_24h = data.get("price_change_percentage_24h")

            # === Zbierz dane ===
            for i in range(n):
                ts, price = prices[i]
                _, volume = volumes[i]
                ts_iso = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()

                all_rows.append({
                "coin_id": coin,
                "symbol": coin[:3].upper(),
                "name": coin.capitalize(),

                "current_price": round(price, 6),
                "total_volume": round(volume, 2),
                "market_cap": market_cap,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "price_change_percentage_24h": change_24h,

                "date_": ts_iso
            })
            print(f"[{idx}/{len(coin_ids)}] ✅ {coin}: {n} punktów")
            time.sleep(1.2)  # ograniczenie zapytań (API limit)

        except Exception as e:
            print(f"[{idx}/{len(coin_ids)}] ❌ Błąd dla {coin}: {e}")

    # === Zapis do Supabase ===
    if all_rows:
        print(f"\n📊 Łącznie pobrano: {len(all_rows)} rekordów")
        insert_data(all_rows)
    else:
        print("⚠️ Brak danych do zapisu.")

    return all_rows


if __name__ == "__main__":
    fetch_data(days_back=30)
