# etl/db.py
from __future__ import annotations

import os
from typing import Iterable, List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from dotenv import load_dotenv


# ------------------------ CONFIG / ENGINE ------------------------

# Ładujemy .env (UTF-8, bez BOM). Jeśli trzymasz .env w innym miejscu, podmień ścieżkę.
load_dotenv("etl/.env", encoding="utf-8")

def _build_engine() -> Engine:
    """Tworzy silnik SQLAlchemy dla Postgresa (psycopg)."""
    db_url = URL.create(
        "postgresql+psycopg",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB"),
    )
    return create_engine(
        db_url,
        pool_pre_ping=True,   # odświeża połączenie, gdy leży
        future=True,
    )

# Singleton engine do użycia w całym module
engine: Engine = _build_engine()


# ------------------------ DDL (INIT) ------------------------

def init_table() -> None:
    """
    Tworzy tabelę na potrzeby CRYPTOETL, jeśli nie istnieje.
    Klucz główny: (coin_id, last_updated) — pozwala trzymać historię.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS crypto_prices (
        coin_id        TEXT        NOT NULL,
        symbol         TEXT        NOT NULL,
        name           TEXT        NOT NULL,
        current_price  NUMERIC     NOT NULL,
        total_volume   NUMERIC     NOT NULL,
        last_updated   TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (coin_id, last_updated)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ------------------------ WRITE API ------------------------

def save_data(rows: Iterable[Dict[str, Any]]) -> int:
    """
    Zapisuje rekordy do tabeli `crypto_prices`.
    Oczekiwany kształt elementu:
      {
        "id": "bitcoin",            # -> coin_id
        "symbol": "BTC",
        "name": "Bitcoin",
        "current_price": 109_000.0,
        "total_volume": 3_088_616_9744,
        "last_updated": "2025-11-01T17:36:20.872Z"  # ISO lub datetime
      }
    Konflikty (ten sam coin_id+last_updated) są pomijane.
    Zwraca liczbę wstawionych wierszy (bez pominiętych).
    """
    rows = list(rows)
    if not rows:
        return 0

    insert_sql = text("""
        INSERT INTO crypto_prices
            (coin_id, symbol, name, current_price, total_volume, last_updated)
        VALUES
            (:coin_id, :symbol, :name, :current_price, :total_volume, :last_updated)
        ON CONFLICT (coin_id, last_updated) DO NOTHING
    """)

    # mapowanie pól wejściowych -> kolumny
    payload = [{
        "coin_id": r.get("coin_id") or r.get("id"),
        "symbol": (r.get("symbol") or "").upper(),
        "name": r.get("name") or (r.get("coin_id") or r.get("id") or "unknown").capitalize(),
        "current_price": r.get("current_price"),
        "total_volume": r.get("total_volume"),
        "last_updated": r.get("last_updated"),  # ISO 8601 lub datetime; PG poradzi sobie
    } for r in rows]

    with engine.begin() as conn:
        result = conn.execute(insert_sql, payload)
        # Uwaga: przy DML executemany result.rowcount bywa -1 (nieznane) dla niektórych sterowników.
        # Tutaj psycopg zwykle zwraca faktyczną liczbę wstawień; jeśli nie, możesz policzyć ręcznie po SELECT.
        return result.rowcount if result.rowcount is not None else 0


# ------------------------ READ API (dla dashboardu) ------------------------

def list_coins() -> pd.DataFrame:
    """
    Zwraca listę unikalnych coinów (id/symbol/name) dostępnych w tabeli.
    """
    q = text("""
        SELECT DISTINCT coin_id, name, symbol
        FROM crypto_prices
        ORDER BY name
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn)


def get_history(coin_id: str, start, end) -> pd.DataFrame:
    """
    Zwraca historię dla jednego coina w [start, end) (półotwarty przedział).
    Kolumny: ts, price, volume
    """
    q = text("""
        SELECT
            last_updated AS ts,
            current_price AS price,
            total_volume  AS volume
        FROM crypto_prices
        WHERE coin_id = :cid
          AND last_updated >= :start
          AND last_updated <  :end
        ORDER BY last_updated
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"cid": coin_id, "start": start, "end": end})


def get_history_all(start, end) -> pd.DataFrame:
    """
    Zwraca historię dla wszystkich coinów w [start, end).
    Kolumny: coin_id, name, symbol, ts, price, volume
    """
    q = text("""
        SELECT
            coin_id, name, symbol,
            last_updated AS ts,
            current_price AS price,
            total_volume  AS volume
        FROM crypto_prices
        WHERE last_updated >= :start
          AND last_updated <  :end
        ORDER BY coin_id, last_updated
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"start": start, "end": end})


# ------------------------ TEST RĘCZNY ------------------------

if __name__ == "__main__":
    # Prosty smoke test
    init_table()
    print("DB connected. crypto_prices exists.")
    df = list_coins()
    print(f"Coins in DB: {len(df)}")
