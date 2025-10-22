CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id TEXT,
    symbol TEXT,
    name TEXT,
    current_price NUMERIC,
    market_cap BIGINT,
    total_volume BIGINT,
    timestamp TIMESTAMP DEFAULT now()
);
