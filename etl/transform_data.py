import pandas as pd

def transform_crypto_data(raw_data):
    df = pd.DataFrame(raw_data)
    # CoinGecko zwraca m.in. 'id', 'symbol', 'name', 'last_updated' itd.
    df["coin_id"] = df["id"]  # mapujemy id -> coin_id dla spójności ze schematem
    selected_columns = [
        "coin_id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_percentage_24h",
        "last_updated",
    ]
    df = df[selected_columns]
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True).dt.tz_convert(None)
    return df
