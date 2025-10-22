# db.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# pewne ładowanie .env (jak w app.py)
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def init_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crypto_prices (
        id SERIAL PRIMARY KEY,
        coin_id TEXT,
        symbol TEXT,
        name TEXT,
        current_price NUMERIC,
        market_cap BIGINT,
        total_volume BIGINT,
        high_24h NUMERIC,
        low_24h NUMERIC,
        price_change_percentage_24h NUMERIC,
        last_updated TIMESTAMP
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_prices_symbol_ts
        ON crypto_prices(symbol, last_updated);
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_query))
        print("[INFO] Table checked/created.")

def save_to_db(df):
    df.to_sql("crypto_prices", engine, if_exists="append", index=False)
    print("[INFO] Data saved to PostgreSQL.")
