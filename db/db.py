# db/db.py
from __future__ import annotations

import os
from typing import Iterable, Dict, Any, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
from dotenv import load_dotenv
import sys
sys.path.append(r"c:\Users\kamil\Desktop\CryptoCurrenciesStock")


# ---------------- ENV & ENGINE ----------------

load_dotenv("etl/.env", encoding="utf-8") 

def _make_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # podmień stary driver na psycopg v3 jeśli trzeba
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("psycopg2", "psycopg", 1)
        return create_engine(db_url, pool_pre_ping=True, future=True)

    return create_engine(
        URL.create(
            "postgresql+psycopg",
            username=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            database=os.getenv("POSTGRES_DB", "cryptoetl"),
        ),
        pool_pre_ping=True,
        future=True,
    )

engine: Engine = _make_engine()


# ---------------- SCHEMA ----------------

DDL = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    coin_id        TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    name           TEXT        NOT NULL,
    current_price  NUMERIC     NOT NULL,
    total_volume   NUMERIC     NOT NULL,
    last_updated   TIMESTAMPTZ NOT NULL
);

-- Unikalność potrzebna dla ON CONFLICT (kolumny muszą mieć UNIQUE/PK)
CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_prices_coin_ts
  ON crypto_prices (coin_id, last_updated);

-- Dodatkowe indeksy wspomagające zapytania
CREATE INDEX IF NOT EXISTS idx_crypto_prices_ts
  ON crypto_prices (last_updated);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_coin_ts
  ON crypto_prices (coin_id, last_updated);
"""

def init_table() -> None:
    with engine.begin() as conn:
        for stmt in DDL.strip().split(";\n"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def dedupe_and_enforce_unique() -> None:
    """Usuwa potencjalne duplikaty i upewnia się, że unikalny indeks istnieje."""
    with engine.begin() as conn:
        # 1) usuń duplikaty (zostaw 1 wiersz na (coin_id,last_updated))
        conn.execute(text("""
            WITH d AS (
              SELECT ctid, row_number() OVER (
                PARTITION BY coin_id, last_updated ORDER BY ctid
              ) AS rn
              FROM crypto_prices
            )
            DELETE FROM crypto_prices
            WHERE ctid IN (SELECT ctid FROM d WHERE rn > 1);
        """))
        # 2) dopilnuj unikalnego indeksu
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_prices_coin_ts
            ON crypto_prices (coin_id, last_updated);
        """))


# ---------------- WRITE API ----------------

_INSERT_SQL = text("""
    INSERT INTO crypto_prices
        (coin_id, symbol, name, current_price, total_volume, last_updated)
    VALUES
        (:coin_id, :symbol, :name, :current_price, :total_volume, :last_updated)
    ON CONFLICT (coin_id, last_updated) DO NOTHING
""")

def save_data(rows: Iterable[Dict[str, Any]], batch_size: int = 5000) -> int:
    """
    Zapis listy rekordów (takich jak zwracane przez fetch_data.py).
    Duplikaty (coin_id, last_updated) są pomijane przez ON CONFLICT DO NOTHING.
    Zwraca liczbę wierszy przekazanych do inserta (attempted).
    """
    buffer: List[Dict[str, Any]] = []
    attempted = 0
    with engine.begin() as conn:
        for r in rows:
            buffer.append({
                "coin_id": r.get("id") or r.get("coin_id"),
                "symbol": (r.get("symbol") or "").upper(),
                "name": r.get("name") or (r.get("id") or "unknown").capitalize(),
                "current_price": r.get("current_price"),
                "total_volume": r.get("total_volume"),
                "last_updated": r.get("last_updated"),  # ISO/TZ -> timestamptz
            })
            if len(buffer) >= batch_size:
                conn.execute(_INSERT_SQL, buffer)
                attempted += len(buffer)
                buffer.clear()
        if buffer:
            conn.execute(_INSERT_SQL, buffer)
            attempted += len(buffer)
    return attempted


def save_dataframe(df: pd.DataFrame, batch_size: int = 5000) -> int:
    """
    Zapis z DataFrame (kolumny: id/coin_id, symbol, name, current_price, total_volume, last_updated).
    """
    if df.empty:
        return 0
    df = df.rename(columns={"id": "coin_id"})
    cols = ["coin_id", "symbol", "name", "current_price", "total_volume", "last_updated"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Brak kolumn w DataFrame: {missing}")
    rows = df[cols].to_dict(orient="records")
    return save_data(rows, batch_size=batch_size)


# ---------------- READ API (dla dashboardu) ----------------

def list_coins() -> pd.DataFrame:
    q = text("""
        SELECT DISTINCT coin_id, symbol, name
        FROM crypto_prices
        ORDER BY name
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn)


def get_history(coin_id: str, start, end) -> pd.DataFrame:
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


def get_history_all(start, end, only_coins: Optional[list[str]] = None) -> pd.DataFrame:
    base = """
        SELECT
            coin_id, symbol, name,
            last_updated AS ts,
            current_price AS price,
            total_volume  AS volume
        FROM crypto_prices
        WHERE last_updated >= :start
          AND last_updated <  :end
    """
    params: Dict[str, Any] = {"start": start, "end": end}
    if only_coins:
        base += " AND coin_id = ANY(:ids)"
        params["ids"] = only_coins
    base += " ORDER BY coin_id, last_updated"
    with engine.begin() as conn:
        return pd.read_sql(text(base), conn, params=params)

def fix_unique_indexes() -> None:
    """Usuwa ewentualny zły indeks (symbol, ts) i tworzy właściwy (coin_id, ts)."""
    with engine.begin() as conn:
        # Drop błędnego indeksu, jeśli istnieje
        conn.execute(text("DROP INDEX IF EXISTS ux_crypto_prices_symbol_ts;"))
        # Tworzymy właściwy unikalny indeks
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_prices_coin_ts
            ON crypto_prices (coin_id, last_updated);
        """))

# ---------------- SMOKE TEST ----------------

if __name__ == "__main__":
    # proste uruchomienie ETL: utwórz tabelę -> dedupe -> fetch -> save
    from etl.fetch_data import fetch_data  # dopasuj ścieżkę jeśli masz inaczej

    init_table()
    fix_unique_indexes()      
    dedupe_and_enforce_unique()

    print("✅ Tabela gotowa.")

    data = fetch_data(days_back=365)
    print(f"Fetched {len(data)} rows.")

    n = save_data(data)
    print(f"Inserted (attempted): {n}")

    dfc = list_coins()
    print(f"Coins in DB: {len(dfc)}")
