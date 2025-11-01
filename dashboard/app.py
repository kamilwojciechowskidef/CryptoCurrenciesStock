import streamlit as st
import pandas as pd
import sys
sys.path.append(r"c:\Users\kamil\Desktop\CryptoCurrenciesStock")

from db.db import engine

st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("CryptoCurrencies Dashboard")


@st.cache_data
def load_data():
    query = "SELECT * FROM crypto_prices ORDER BY last_updated DESC"
    return pd.read_sql(query, engine)


df = load_data()

if df.empty:
    st.warning("No data to display.")
    st.stop()

# Coin selector
coins = df["name"].unique().tolist()
selected_coin = st.selectbox("Select a cryptocurrency:", coins)

# Filter
filtered = df[df["name"] == selected_coin].sort_values("last_updated", ascending=False)

# Stats
latest = filtered.iloc[0]
st.metric("Current Price", f"${latest['current_price']}")
st.metric("24h Change", f"{latest['price_change_percentage_24h']}%")
st.metric("Market Cap", f"${latest['market_cap']}")

# Price chart (if multiple rows)
if len(filtered) > 1:
    st.line_chart(filtered[["last_updated", "current_price"]].set_index("last_updated"))

