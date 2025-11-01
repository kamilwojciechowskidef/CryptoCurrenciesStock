from dotenv import load_dotenv
import os
import requests

# 1. Wczytaj zmienne środowiskowe
load_dotenv("etl/.env", encoding="utf-8")

API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

if not API_KEY:
    raise ValueError("Brak COINGECKO_API_KEY w pliku .env")

# 2. Przygotuj nagłówki
headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY
}

# 3. Przykład prostego testu połączenia
r = requests.get(f"{COINGECKO_BASE}/ping", headers=headers)

print("Status:", r.status_code)
print("Odpowiedź:", r.text)
