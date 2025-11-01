import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import sys
sys.path.append(r"c:\Users\kamil\Desktop\CryptoCurrenciesStock")

from db.db import list_coins, get_history  # <--- użyj gotowych funkcji

st.set_page_config("CryptoCurrencies Dashboard", layout="wide")
st.title("CryptoCurrencies Dashboard")

# --- lista coinów z bazy ---
coins_df = list_coins()
if coins_df.empty:
    st.warning("Brak danych w bazie. Uruchom ETL.")
    st.stop()

# mapowanie: etykieta -> coin_id
coins_df["label"] = coins_df["name"].fillna(coins_df["coin_id"])
labels = coins_df["label"].tolist()
label_to_cid = dict(zip(coins_df["label"], coins_df["coin_id"]))

left, right = st.columns([2, 1])
with left:
    label = st.selectbox("Select a cryptocurrency:", labels, index=labels.index("Ethereum") if "Ethereum" in labels else 0)
    coin_id = label_to_cid[label]
with right:
    default_start = datetime.utcnow() - timedelta(days=30)
    dt_range = st.date_input("Date range", (default_start.date(), datetime.utcnow().date()))
    start_date, end_date = (dt_range if isinstance(dt_range, tuple) else (default_start.date(), dt_range))

@st.cache_data(show_spinner=False, ttl=300)
def load_history_cached(cid: str, start: datetime, end: datetime) -> pd.DataFrame:
    return get_history(cid, start, end + timedelta(days=1))

df = load_history_cached(coin_id,
                         datetime.combine(start_date, datetime.min.time()),
                         datetime.combine(end_date, datetime.min.time()))

if df.empty:
    st.warning("Brak danych w wybranym zakresie. Zmień daty albo uruchom ETL.")
    st.stop()

# przygotowanie kolumn po SELECT ... AS
df["ts"] = pd.to_datetime(df["ts"], utc=True)
df = df.sort_values("ts").reset_index(drop=True)
df["ret"] = df["price"].pct_change()
df["ma7"]  = df["price"].rolling(7, min_periods=1).mean()
df["ma30"] = df["price"].rolling(30, min_periods=1).mean()

# --- wykresy bez zmian ---
fig_price = go.Figure()
fig_price.add_trace(go.Scatter(x=df["ts"], y=df["price"], name="Price", mode="lines"))
fig_price.add_trace(go.Scatter(x=df["ts"], y=df["ma7"],  name="MA 7",  mode="lines"))
fig_price.add_trace(go.Scatter(x=df["ts"], y=df["ma30"], name="MA 30", mode="lines"))
fig_price.update_layout(title=f"{label} – Price & Moving Averages", xaxis_title="Time", yaxis_title="Price", hovermode="x unified")
fig_price.update_xaxes(rangeslider_visible=True)
st.plotly_chart(fig_price, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    fig_vol = px.bar(df, x="ts", y="volume", title=f"{label} – Volume")
    fig_vol.update_layout(hovermode="x unified", xaxis_title="Time", yaxis_title="Volume")
    st.plotly_chart(fig_vol, use_container_width=True)
with c2:
    fig_hist = px.histogram(df.dropna(subset=["ret"]), x="ret", nbins=50, title=f"{label} – Distribution of Returns")
    fig_hist.update_layout(xaxis_title="Return (pct)", yaxis_title="Count")
    st.plotly_chart(fig_hist, use_container_width=True)

k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Current Price", f"${df['price'].iloc[-1]:,.2f}", delta=f"{(df['ret'].iloc[-1] or 0)*100:.2f}%")
with k2:
    st.metric("7-day avg", f"${df['ma7'].iloc[-1]:,.2f}")
with k3:
    st.metric("30-day avg", f"${df['ma30'].iloc[-1]:,.2f}")
