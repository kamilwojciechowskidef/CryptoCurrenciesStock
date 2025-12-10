DROP TABLE IF EXISTS crypto_prices;

CREATE TABLE crypto_prices (
  id SERIAL PRIMARY KEY,
  coin_id TEXT NOT NULL,
  symbol TEXT,
  name TEXT,
  current_price NUMERIC,
  market_cap NUMERIC,
  total_volume NUMERIC,
  high_24h NUMERIC,
  low_24h NUMERIC,
  price_change_percentage_24h NUMERIC,

  date_ TIMESTAMPTZ NOT NULL
);
