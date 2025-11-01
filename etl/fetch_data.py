# etl/fetch_data.py
from __future__ import annotations

import os
import time
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Domyślna lista coinów (dopasowana do Twoich danych/raw_data)
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

# Bezpieczne odstępy, żeby nie wpaść w rate-limit (publiczne API)
REQUEST_SLEEP_SEC = 1.2

# Załaduj zmienne środowiskowe (m.in. COINGECKO_API_KEY)
load_dotenv()


# --------------------------- helpers ---------------------------

def _make_session() -> requests.Session:
    """Requests session z retry na błędach 5xx/429."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def _iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def _get_coins_meta(session: requests.Session, coin_ids: Iterable[str]) -> Dict[str, Tuple[str, str]]:
    """
    Pobierz (symbol, name) dla coin_id z /coins/markets jednym strzałem.
    Zwraca mapę: coin_id -> (symbol, name)
    """
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": len(list(coin_ids)) or 250,
        "page": 1,
        "sparkline": "false",
    }
    r = session.get(url, params=params, timeout=25)
    r.raise_for_status()
    out = {}
    for item in r.json():
        cid = item.get("id")
        sym = (item.get("symbol") or "").upper()
        name = item.get("name") or cid
        if cid:
            out[cid] = (sym, name)
    return out


def _fetch_market_chart_range(
    session: requests.Session,
    coin_id: str,
    days: int = 365,
    interval: str = "daily",
) -> Dict:
    """
    Pobiera dane historyczne (prices/total_volumes) w zadanym zakresie czasowym.
    Zwraca json z kluczami 'prices', 'total_volumes'.
    """
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_unix,
        "to": end_unix,
        "interval": interval,  # 'daily' → jeden punkt na dzień
    }
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _records_from_chart_json(
    coin_id: str,
    symbol: str,
    name: str,
    chart_json: Dict,
) -> List[Dict]:
    """
    Konwertuje odpowiedź market_chart/range na listę rekordów zgodnych ze schematem:
    coin_id, symbol, name, current_price, total_volume, last_updated
    """
    prices = chart_json.get("prices") or []
    vols = chart_json.get("total_volumes") or []
    n = min(len(prices), len(vols))

    out: List[Dict] = []
    for i in range(n):
        t_ms, price = prices[i]
        _, volume = vols[i]
        out.append({
            "id": coin_id,
            "symbol": symbol,
            "name": name,
            "current_price": price,
            "total_volume": volume,
            "last_updated": _iso_from_ms(t_ms),
        })
    return out


# --------------------------- public API ---------------------------

def fetch_data(
    coin_ids: Iterable[str] = None,
    start_date: str = "2020-01-01",
    end_date: str | None = None,
    interval: str = "daily",
) -> List[Dict]:
    """
    Backfill danych historycznych z CoinGecko od start_date do end_date (domyślnie teraz).
    - używa /coins/{id}/market_chart/range
    - agregacja: 'daily'
    - zwraca listę rekordów zgodnych ze schematem pipeline'u (transform/save)

    Przykład:
        data = fetch_data(DEFAULT_COIN_IDS, start_date="2020-01-01")
    """
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    # zakres czasu
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.now(timezone.utc) if end_date is None
        else datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    )
    start_unix = _to_unix(start_dt)
    end_unix = _to_unix(end_dt)

    sess = _make_session()
    meta = _get_coins_meta(sess, coin_ids)

    all_rows: List[Dict] = []
    coin_ids_list = list(coin_ids)
    for idx, cid in enumerate(coin_ids_list, start=1):
        sym, nm = meta.get(cid, (cid[:3].upper(), cid.capitalize()))

        # Pobranie zakresu – dla stabilności możemy pociąć na okna (np. roczne)
        # ale przy 'daily' od 2020 → OK w jednym żądaniu. Jeśli kiedykolwiek
        # dostaniesz 413/414, odkomentuj chunkowanie poniżej.

        chart = _fetch_market_chart_range(sess, cid, start_unix, end_unix, interval=interval)
        rows = _records_from_chart_json(cid, sym, nm, chart)
        all_rows.extend(rows)

        # prosty progress + throttling
        # (publiczne API CoinGecko bywa kapryśne)
        # print(f"[{idx}/{len(list(coin_ids))}] {cid}: {len(rows)} rows")
        time.sleep(REQUEST_SLEEP_SEC)

    return all_rows


# ------------------ (opcjonalny) chunking czasu ------------------
def fetch_data_chunked(
    coin_ids: Iterable[str],
    start_date: str = "2020-01-01",
    end_date: str | None = None,
    interval: str = "daily",
    chunk_days: int = 365,
) -> List[Dict]:
    """
    Wersja z cięciem zakresu na kawałki (np. roczne), na wypadek problemów z dużymi zakresami.
    """
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.now(timezone.utc) if end_date is None
        else datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    )

    sess = _make_session()
    meta = _get_coins_meta(sess, coin_ids)

    all_rows: List[Dict] = []
    for idx, cid in enumerate(coin_ids, start=1):
        sym, nm = meta.get(cid, (cid[:3].upper(), cid.capitalize()))

        # pocięcie na okna
        cur_start = start_dt
        while cur_start < end_dt:
            cur_end = min(cur_start + timedelta(days=chunk_days), end_dt)
            chart = _fetch_market_chart_range(sess, cid, _to_unix(cur_start), _to_unix(cur_end), interval=interval)
            rows = _records_from_chart_json(cid, sym, nm, chart)
            all_rows.extend(rows)
            cur_start = cur_end
            time.sleep(REQUEST_SLEEP_SEC)

        # print(f"[{idx}/{len(list(coin_ids))}] {cid}: {len([r for r in all_rows if r['id']==cid])} rows")

    return all_rows


# --------------------------- manual run ---------------------------
if __name__ == "__main__":
    data = fetch_data(DEFAULT_COIN_IDS, start_date="2020-01-01")
    print(f"Fetched rows: {len(data)}")
    # przykładowy pierwszy rekord
    if data:
        print(data[0])
