DROP TABLE crypto_prices
CREATE TABLE crypto_prices (
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
CREATE UNIQUE INDEX ux_crypto_prices_symbol_ts
  ON crypto_prices(symbol, last_updated);
