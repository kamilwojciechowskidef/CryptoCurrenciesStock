import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pathlib import Path

# Wczytaj .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Streamlit UI
st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("📊 CryptoCurrencies Dashboard")

# Wczytaj dane
@st.cache_data
def load_data():
    query = "SELECT * FROM crypto_prices ORDER BY last_updated DESC"
    return pd.read_sql(query, engine)


df = load_data()

if df.empty:
    st.warning("Brak danych do wyświetlenia.")
    st.stop()

# Interfejs: wybór kryptowaluty
coins = df["name"].unique().tolist()
selected_coin = st.selectbox("Wybierz kryptowalutę:", coins)

# Filtruj dane
filtered = df[df["name"] == selected_coin].sort_values("last_updated", ascending=False)


# Wyświetlenie statystyk
latest = filtered.iloc[0]
st.metric("Aktualna cena", f"${latest['current_price']}")
st.metric("Zmiana 24h", f"{latest['price_change_percentage_24h']}%")
st.metric("Market Cap", f"${latest['market_cap']}")



# Wykres cen (jeśli masz dane historyczne — np. z wielu dni)
if len(filtered) > 1:
    st.line_chart(filtered[["last_updated", "current_price"]].set_index("last_updated"))
