import os
import requests
from dotenv import load_dotenv
import pandas as pd

# Wczytanie .env
load_dotenv("etl/.env", encoding="utf-8")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TABLE = os.getenv("DATA_TABLE", "crypto_prices")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# ==============================
#  INSERT
# ==============================
def insert_data(records):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    res = requests.post(url, headers=HEADERS, json=records)
    if res.status_code in (200, 201, 204):
        print(f"✅ Wstawiono {len(records)} rekordów.")
    else:
        print(f"⚠️ Błąd ({res.status_code}): {res.text}")


# ==============================
#  LIST COINS
# ==============================
def list_coins():
    """
    Zwraca listę unikalnych kryptowalut: coin_id + name.
    """
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    params = {
        "select": "coin_id,name",
        "order": "coin_id",
    }
    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()

    df = pd.DataFrame(res.json()).drop_duplicates(["coin_id"])
    return df


# ==============================
#  HISTORY FOR ONE COIN
# ==============================
def get_history(coin_id: str, start, end):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"

    params = {
        "select": "coin_id,name,current_price,total_volume,date_",
        "coin_id": f"eq.{coin_id}",
        "and": f"(date_.gte.{start.isoformat()},date_.lte.{end.isoformat()})",
        "order": "date_.asc"
    }

    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()

    df = pd.DataFrame(res.json())
    if df.empty:
        return df

    return df.rename(columns={
        "current_price": "price",
        "total_volume": "volume",
        "date_": "ts"
    })




# ==============================
#  HISTORY FOR ALL COINS
# ==============================
def get_history_all(start, end):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"

    params = {
        "select": "coin_id,name,current_price,total_volume,date_",
        "and": f"(date_.gte.{start.isoformat()},date_.lte.{end.isoformat()})",
        "order": "date_.asc"
    }

    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()

    df = pd.DataFrame(res.json())
    if df.empty:
        return df

    return df.rename(columns={
        "current_price": "price",
        "total_volume": "volume",
        "date_": "ts"
    })




# ==============================
#  CLEAR TABLE
# ==============================
def clear_table():
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?id=neq.0"
    res = requests.delete(url, headers=HEADERS)
    print("🗑️ Tabela wyczyszczona." if res.ok else f"⚠️ Błąd czyszczenia: {res.text}")
