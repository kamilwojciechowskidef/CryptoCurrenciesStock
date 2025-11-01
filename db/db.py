import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path="etl/.env", encoding="utf-8")

url = URL.create(
    drivername="postgresql+psycopg",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
)

# kontrolnie – sprawdź typ i ewentualne nie-ASCII (powinno być pusto)
dsn = str(url)
print("TYPE:", type(dsn).__name__)
print("NON-ASCII:", [(i, ch, hex(ord(ch))) for i, ch in enumerate(dsn) if ord(ch) > 127])

engine = create_engine(url, pool_pre_ping=True, future=True)

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

def save_to_db(df: pd.DataFrame):
    # UPSERT po (symbol, last_updated)
    records = df.to_dict(orient="records")
    if not records:
        print("[INFO] Nothing to save.")
        return

    cols = [
        "coin_id","symbol","name","current_price","market_cap","total_volume",
        "high_24h","low_24h","price_change_percentage_24h","last_updated"
    ]

    with engine.begin() as conn:
        # budujemy insert przez tekstowy SQL z ON CONFLICT DO NOTHING (prosty i szybki)
        insert_sql = """
        INSERT INTO crypto_prices
        (coin_id, symbol, name, current_price, market_cap, total_volume,
         high_24h, low_24h, price_change_percentage_24h, last_updated)
        VALUES
        (:coin_id, :symbol, :name, :current_price, :market_cap, :total_volume,
         :high_24h, :low_24h, :price_change_percentage_24h, :last_updated)
        ON CONFLICT (symbol, last_updated) DO NOTHING;
        """
        conn.execute(text(insert_sql), records)
        print(f"[INFO] Upserted {len(records)} rows.")
        
