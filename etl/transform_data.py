# transform_data.py
import pandas as pd

def _to_datetime_utc(series):
    return pd.to_datetime(series, utc=True, errors="coerce")

def history_postprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dla jednej kryptowaluty: rzutowania typów, sort, ret, MA7/MA30.
    Wejście kolumny: ts, price, volume
    """
    df = df.copy()
    df["ts"] = _to_datetime_utc(df["ts"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["ts", "price"]).sort_values("ts").reset_index(drop=True)
    df["ret"]  = df["price"].pct_change()
    df["ma7"]  = df["price"].rolling(7, min_periods=1).mean()
    df["ma30"] = df["price"].rolling(30, min_periods=1).mean()
    return df

def allcoins_postprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dla wszystkich kryptowalut w zakresie: rzutowania i porządkowanie.
    Wejście kolumny: coin_id, name, symbol, ts, price, volume
    """
    df = df.copy()
    df["ts"] = _to_datetime_utc(df["ts"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["ts", "price"]).sort_values(["coin_id", "ts"]).reset_index(drop=True)
    return df

def add_index_100(df: pd.DataFrame) -> pd.DataFrame:
    """Dodaje kolumnę price_norm = price / first(price in group) * 100 (per coin)."""
    df = df.copy()
    first = df.groupby("coin_id")["price"].transform("first")
    df["price_norm"] = (df["price"] / first) * 100
    return df

def aggregate_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Suma wolumenów per coin (z wypełnieniem NaN->0)."""
    vol = (df.groupby(["coin_id", "name"], as_index=False)["volume"].sum(min_count=1))
    vol["volume"] = vol["volume"].fillna(0)
    vol["label"] = vol["name"].fillna(vol["coin_id"])
    return vol.sort_values(by="volume", ascending=False)
