# etl/fetch_data.py
from __future__ import annotations

import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv("etl/.env", encoding="utf-8")
API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_BASE = "$https://api.coingecko.com/api/v3/ping?x_{API_KEY}"

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

REQUEST_SLEEP_SEC = 1.2

# --- solidne ładowanie .env (etl/.env i ./.env) ---
env_here = Path(__file__).with_name(".env")
if env_here.exists():
    load_dotenv(env_here)
load_dotenv()  # katalog roboczy

def _make_session() -> requests.Session:
    """Session z retry + wszystkimi nagłówkami CG API key (free/demo/pro)."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    api_key = (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("COINGECKO_DEMO_API_KEY")
        or os.getenv("COINGECKO_PRO_API_KEY")
    )
    if not api_key:
        raise RuntimeError(
            "Brak klucza CoinGecko. Dodaj do .env:\n"
            "  COINGECKO_API_KEY=CG-xxxxxxxxxxxxxxxxxxxx\n"
        )

    s.headers.update({
        "accept": "application/json",
        "x-cg-api-key": api_key,
        "x-cg-demo-api-key": api_key,
        "x-cg-pro-api-key": api_key,
    })
    return s

def _iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def _get_coins_meta(session: requests.Session, coin_ids: Iterable[str]) -> Dict[str, Tuple[str, str]]:
    """
    Stabilne meta per-coin przez /coins/{id}. Zwraca: coin_id -> (SYMBOL_UPPER, name)
    """
    out: Dict[str, Tuple[str, str]] = {}
    for cid in list(coin_ids):
        try:
            url = f"{COINGECKO_BASE}/coins/{cid}"
            r = session.get(url, params={"localization": "false"}, timeout=25)
            if r.status_code == 401:
                raise RuntimeError("CoinGecko 401 Unauthorized – sprawdź COINGECKO_API_KEY w .env.")
            if r.status_code == 404:
                out[cid] = (cid[:3].upper(), cid.capitalize())
                continue
            r.raise_for_status()
            data = r.json()
            sym = (data.get("symbol") or "").upper() or cid[:3].upper()
            name = data.get("name") or cid.capitalize()
            out[cid] = (sym, name)
        except Exception:
            out[cid] = (cid[:3].upper(), cid.capitalize())
        time.sleep(REQUEST_SLEEP_SEC * 0.6)
    return out

def _fetch_market_chart_days(
    session: requests.Session,
    coin_id: str,
    days: int = 365,
    interval: str = "daily",
) -> Dict:
    """
    Stabilne pobieranie przez /coins/{id}/market_chart z fallbackami na 400:
      1) interval=daily
      2) bez interval
      3) stopniowe zmniejszanie days (365 -> 360 -> 350 -> ...).
    """
    days = max(1, min(int(days), 365))

    def _call(d: int, with_interval: bool) -> requests.Response:
        url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": d}
        if with_interval:
            params["interval"] = interval
        r = session.get(url, params=params, timeout=30)
        return r

    # 1) spróbuj z interval=daily
    r = _call(days, with_interval=True)
    if r.status_code == 401:
        raise RuntimeError("CoinGecko 401 Unauthorized – sprawdź COINGECKO_API_KEY w .env.")
    if r.ok:
        return r.json()

    # 2) na 400 spróbuj bez interval
    if r.status_code == 400:
        time.sleep(REQUEST_SLEEP_SEC)
        r2 = _call(days, with_interval=False)
        if r2.ok:
            return r2.json()

        # 3) jeśli dalej 400, zmniejszaj days (bez interval, potem z interval)
        d = days - 5
        while d >= 30:  # nie schodź poniżej miesiąca
            time.sleep(REQUEST_SLEEP_SEC)
            r3 = _call(d, with_interval=False)
            if r3.ok:
                return r3.json()
            # spróbuj jeszcze raz z interval=daily dla tego d
            time.sleep(REQUEST_SLEEP_SEC)
            r4 = _call(d, with_interval=True)
            if r4.ok:
                return r4.json()
            d -= 10  # krok w dół

        # jeśli nic nie zadziałało – podaj komunikat z ostatniej odpowiedzi
        r2.raise_for_status()

    # inne błędy niż 400/401
    r.raise_for_status()
    return r.json()


def _records_from_chart_json(
    coin_id: str,
    symbol: str,
    name: str,
    chart_json: Dict,
) -> List[Dict]:
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

def fetch_data(
    coin_ids: Iterable[str] | None = None,
    days_back: int = 360,
    interval: str = "daily",
) -> List[Dict]:
    """
    Pobiera historię z ostatniego `days_back` dni (max 365) przez /market_chart.
    Zwraca listę rekordów kompatybilnych z transform/save.
    """
    if coin_ids is None:
        coin_ids = DEFAULT_COIN_IDS

    sess = _make_session()
    meta = _get_coins_meta(sess, coin_ids)

    all_rows: List[Dict] = []
    coin_ids_list = list(coin_ids)
    for idx, cid in enumerate(coin_ids_list, start=1):
        sym, nm = meta.get(cid, (cid[:3].upper(), cid.capitalize()))

        chart = _fetch_market_chart_days(sess, cid, days=days_back, interval=interval)
        rows = _records_from_chart_json(cid, sym, nm, chart)
        all_rows.extend(rows)

        print(f"[{idx}/{len(coin_ids_list)}] {cid}: {len(rows)} punktów")
        time.sleep(REQUEST_SLEEP_SEC)

    return all_rows

if __name__ == "__main__":
    try:
        data = fetch_data(DEFAULT_COIN_IDS, days_back=360)
        print(f"Fetched rows: {len(data)}")
        if data:
            print("Sample:", data[0])
    except Exception as e:
        print("ERROR during fetch:", repr(e))
        raise
