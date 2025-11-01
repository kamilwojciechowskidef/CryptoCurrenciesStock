# etl/fetch_data.py
from __future__ import annotations

import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------- CONFIG ----------------------------

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Domyślna lista coinów (dopasuj do swojego projektu)
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

# Bezpieczny odstęp między requestami (public API)
REQUEST_SLEEP_SEC = 1.2

# Załaduj zmienne środowiskowe (m.in. COINGECKO_API_KEY)
load_dotenv()


# -------------------------- HELPERS -----------------------------

def _make_session() -> requests.Session:
    """Requests session z retry i NAGŁÓWKAMI klucza CoinGecko."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    # Pobierz klucz z dowolnej zmiennej
    api_key = (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("COINGECKO_DEMO_API_KEY")
        or os.getenv("COINGECKO_PRO_API_KEY")
    )

    if not api_key:
        raise RuntimeError(
            "Brak klucza CoinGecko. Dodaj do .env np.:\n"
            "  COINGECKO_API_KEY=CG-xxxxxxxxxxxxxxxxxxxxxxxx\n"
        )

    # Ustaw WSZYSTKIE znane nagłówki na tę samą wartość – zadziała dla demo/pro/free
    s.headers.update({
        "accept": "application/json",
        "x-cg-api-key": api_key,
        "x-cg-demo-api-key": api_key,
        "x-cg-pro-api-key": api_key,
    })
    return s


def _to_unix(dt: datetime) -> int:
    """UTC datetime -> unix seconds."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _iso_from_ms(ms: int) -> str:
    """ms timestamp (UTC) -> ISO 8601 z timezone 'Z'."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _get_coins_meta(session: requests.Session, coin_ids: Iterable[str]) -> Dict[str, Tuple[str, str]]:
    """
    Stabilne pobieranie meta per-coin (symbol, name) przez /coins/{id}.
    Unika 400 z /coins/markets?ids=... i radzi sobie z pojedynczymi błędami.
    Zwraca: coin_id -> (SYMBOL_UPPER, name)
    """
    out: Dict[str, Tuple[str, str]] = {}
    for cid in list(coin_ids):
        try:
            url = f"{COINGECKO_BASE}/coins/{cid}"
            r = session.get(url, params={"localization": "false"}, timeout=25)
            if r.status_code == 401:
                raise RuntimeError(
                    "CoinGecko 401 Unauthorized – sprawdź COINGECKO_API_KEY w .env."
                )
            if r.status_code == 404:
                # nieznane ID – fallback
                out[cid] = (cid[:3].upper(), cid.capitalize())
                continue
            r.raise_for_status()
            data = r.json()
            sym = (data.get("symbol") or "").upper() or cid[:3].upper()
            name = data.get("name") or cid.capitalize()
            out[cid] = (sym, name)
        except requests.HTTPError:
            # jakikolwiek 4xx/5xx – fallback
            out[cid] = (cid[:3].upper(), cid.capitalize())
        except Exception:
            out[cid] = (cid[:3].upper(), cid.capitalize())
        time.sleep(REQUEST_SLEEP_SEC * 0.6)  # delikatne ograniczenie tempa
    return out



def _fetch_market_chart_range(
    session: requests.Session,
    coin_id: str,
    start_unix: int,
    end_unix: int,
    interval: str = "daily",
) -> Dict:
    """
    Pobiera dane historyczne w zadanym zakresie.
    Zwraca json z kluczami 'prices', 'total_volumes'.
    """
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_unix, "to": end_unix, "interval": interval}
    r = session.get(url, params=params, timeout=30)

    if r.status_code == 401:
        raise RuntimeError(
            "CoinGecko API zwróciło 401 Unauthorized. Dodaj klucz do .env:\n"
            "COINGECKO_API_KEY=...\n"
        )
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
    coin_id/id, symbol, name, current_price, total_volume, last_updated (ISO).
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


# --------------------------- PUBLIC API --------------------------

def fetch_data(
    coin_ids: Iterable[str] | None = None,
    days_back: int = 365,
    interval: str = "daily",
) -> List[Dict]:
    """
    Pobiera dane historyczne z ostatniego `days_back` dni (domyślnie 365).
    - endpoint: /coins/{id}/market_chart/range
    - agregacja: 'daily'
    - zwraca listę rekordów zgodnych z pipeline'em (transform/save)
    """
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    now_utc = datetime.now(timezone.utc)
    start_dt = now_utc - timedelta(days=days_back)
    start_unix = _to_unix(start_dt)
    end_unix = _to_unix(now_utc)

    sess = _make_session()
    meta = _get_coins_meta(sess, coin_ids)

    all_rows: List[Dict] = []
    coin_ids_list = list(coin_ids)
    for idx, cid in enumerate(coin_ids_list, start=1):
        sym, nm = meta.get(cid, (cid[:3].upper(), cid.capitalize()))

        chart = _fetch_market_chart_range(sess, cid, start_unix, end_unix, interval=interval)
        rows = _records_from_chart_json(cid, sym, nm, chart)
        all_rows.extend(rows)

        # prosty throttling/progress
        print(f"[{idx}/{len(coin_ids_list)}] {cid}: {len(rows)} dni")
        time.sleep(REQUEST_SLEEP_SEC)

    return all_rows


# --------------------------- MANUAL RUN --------------------------

if __name__ == "__main__":
    try:
        data = fetch_data(DEFAULT_COIN_IDS, days_back=365)
        print(f"Fetched rows: {len(data)}")
        if data:
            print("Sample:", data[0])
    except Exception as e:
        print("ERROR during fetch:", repr(e))
        raise
