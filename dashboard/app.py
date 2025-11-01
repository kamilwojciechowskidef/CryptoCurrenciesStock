import pandas as pd
import streamlit as st
from datetime import datetime, timezone, timedelta
import calendar
import plotly.express as px
import plotly.graph_objects as go
import sys
sys.path.append(r"c:\Users\kamil\Desktop\CryptoCurrenciesStock")

from db.db import list_coins, get_history, get_history_all

st.set_page_config("CryptoCurrencies Dashboard", layout="wide")
st.title("CryptoCurrencies Dashboard")

# ---------- Selekcja COINA ----------
coins = list_coins()
if coins.empty:
    st.warning("Brak danych w bazie. Uruchom ETL.")
    st.stop()

coins["label"] = coins["name"].fillna(coins["coin_id"])
label_to_cid = dict(zip(coins["label"], coins["coin_id"]))
label = st.selectbox("Select a cryptocurrency:", coins["label"].tolist(),
                     index=(coins["label"].tolist().index("Ethereum")
                            if "Ethereum" in coins["label"].tolist() else 0))
coin_id = label_to_cid[label]

# ---------- Selekcja zakresu: rok/miesiąc początek/koniec ----------
now = datetime.now(timezone.utc)
years = list(range(2020, now.year + 1))
months = list(range(1, 13))

c1, c2, c3, c4 = st.columns(4)
with c1:
    y_start = st.selectbox("Start year", years, index=years.index(now.year))
with c2:
    m_start = st.selectbox("Start month", months, index=0)
with c3:
    y_end = st.selectbox("End year", years, index=years.index(now.year))
with c4:
    m_end = st.selectbox("End month", months, index=now.month - 1)

# budujemy zakres [start, end_next_month)
start_dt = datetime(y_start, m_start, 1, tzinfo=timezone.utc)
last_day = calendar.monthrange(y_end, m_end)[1]
end_dt = datetime(y_end, m_end, last_day, 23, 59, 59, tzinfo=timezone.utc)
# dla SQL wygodniej mieć górną granicę otwartą:
end_next = (datetime(y_end, m_end, 1, tzinfo=timezone.utc) + timedelta(days=32)).replace(day=1)

# ---------- KPI dla wybranego COINA (Current, MA7, MA30) ----------
# bierzemy okno ~60 dni wstecz od "teraz", żeby policzyć średnie kroczące
recent_end = now
recent_start = now - timedelta(days=60)
df_one = get_history(coin_id, recent_start, recent_end)

k1, k2, k3 = st.columns(3)
if df_one.empty:
    with k1: st.metric("Current Price", "—")
    with k2: st.metric("7-day avg", "—")
    with k3: st.metric("30-day avg", "—")
else:
    df_one["ts"] = pd.to_datetime(df_one["ts"], utc=True)
    df_one = df_one.sort_values("ts").reset_index(drop=True)
    df_one["ma7"] = df_one["price"].rolling(7, min_periods=1).mean()
    df_one["ma30"] = df_one["price"].rolling(30, min_periods=1).mean()

    last_price = df_one["price"].iloc[-1]
    ma7 = df_one["ma7"].iloc[-1]
    ma30 = df_one["ma30"].iloc[-1]
    delta = (df_one["price"].pct_change().iloc[-1] or 0) * 100

    with k1:
        st.metric("Current Price", f"${last_price:,.2f}", delta=f"{delta:.2f}%")
    with k2:
        st.metric("7-day avg", f"${ma7:,.2f}")
    with k3:
        st.metric("30-day avg", f"${ma30:,.2f}")

# Dodatkowy wykres tylko dla wybranego coina (Price + MA)
if not df_one.empty:
    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["price"], name="Price", mode="lines"))
    fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["ma7"],  name="MA 7", mode="lines"))
    fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["ma30"], name="MA 30", mode="lines"))
    fig_price.update_layout(title=f"{label} — Price & Moving Averages",
                            xaxis_title="Time", yaxis_title="Price",
                            hovermode="x unified")
    fig_price.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig_price, use_container_width=True)

st.markdown("---")

# ---------- WYKRESY „WSZYSTKIE COINY” w zakresie rok/miesiąc ----------
df_all = get_history_all(start_dt, end_next)
if df_all.empty:
    st.warning("Brak danych w podanym zakresie.")
    st.stop()

df_all["ts"] = pd.to_datetime(df_all["ts"], utc=True)
df_all = df_all.sort_values(["coin_id", "ts"]).reset_index(drop=True)

# 1) Linie cen znormalizowane do 100 na starcie (porównanie trendu)
def normalize_group(g):
    first = g["price"].iloc[0]
    g["price_norm"] = (g["price"] / first) * 100 if first else None
    return g
df_norm = df_all.groupby("coin_id", group_keys=False).apply(normalize_group)

fig_norm = px.line(df_norm, x="ts", y="price_norm", color="name",
                   title="All coins — Price (indexed to 100 at period start)",
                   labels={"price_norm": "Index (100=start)", "ts": "Time"})
st.plotly_chart(fig_norm, use_container_width=True)

# 2) Suma wolumenu per coin (bar)
vol = (df_all.groupby(["coin_id", "name"], as_index=False)["volume"].sum()
       .sort_values("volume", ascending=False))
fig_vol = px.bar(vol, x="name", y="volume", title="All coins — Total Volume in Range")
fig_vol.update_layout(xaxis_title="Coin", yaxis_title="Volume")
st.plotly_chart(fig_vol, use_container_width=True)

# 3) Korelacje zwrotów między coinami (heatmapa)
ret = (df_all.sort_values(["coin_id", "ts"])
             .assign(ret=lambda d: d.groupby("coin_id")["price"].pct_change()))
pivot_ret = ret.pivot_table(index="ts", columns="name", values="ret")
corr = pivot_ret.corr(min_periods=10)  # wymagaj trochę danych
fig_corr = px.imshow(corr, text_auto=False, aspect="auto",
                     title="All coins — Correlation of Returns")
st.plotly_chart(fig_corr, use_container_width=True)
