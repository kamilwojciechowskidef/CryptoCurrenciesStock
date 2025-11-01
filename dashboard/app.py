import pandas as pd
import streamlit as st
from datetime import datetime, timezone, timedelta
import calendar
import plotly.express as px
import plotly.graph_objects as go
import sys
sys.path.append(r"c:\Users\kamil\Desktop\CryptoCurrenciesStock")

from etl.transform_data import history_postprocess,allcoins_postprocess,add_index_100,aggregate_volume,volume_with_share
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
df_one = history_postprocess(df_one)

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
    ma7  = df_one["ma7"].iloc[-1]
    ma30 = df_one["ma30"].iloc[-1]
    delta = (df_one["ret"].iloc[-1] or 0) * 100

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
df_all = allcoins_postprocess(df_all)

# 2) Suma wolumenu per coin (bar)
df_line = df_all.copy()
pts = df_line.groupby("coin_id")["ts"].count()
df_line = df_line[df_line["coin_id"].isin(pts[pts >= 2].index)]
if not df_line.empty:
    df_line = add_index_100(df_line)
    df_line["label"] = df_line["name"].fillna(df_line["coin_id"])
    fig_norm = px.line(df_line, x="ts", y="price_norm", color="label",
                       title="All coins — Price (indexed to 100 at period start)",
                       labels={"price_norm":"Index (100=start)","ts":"Time","label":"Coin"})
    st.plotly_chart(fig_norm, use_container_width=True)
else:
    st.info("Za mało punktów na linie indeksu w wybranym zakresie.")

# --- WOLUmen: stałe kolory + legenda po prawej ---
vol = aggregate_volume(df_all)  # kolumny: coin_id, name, volume, label

# paleta i mapowanie kolorów per coin (spójne, ale łatwe do rozbudowy)
base_palette = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]
labels_sorted = vol["label"].tolist()
color_map = {lab: base_palette[i % len(base_palette)] for i, lab in enumerate(labels_sorted)}

fig_vol = px.bar(
    vol,
    x="label",
    y="volume",
    color="label",
    color_discrete_map=color_map,
    title="All coins — Total Trading Volume in Range",
    labels={
        "label": "Crypto_currency",
        "volume": "Total Trading Volume (sum over selected period)",
        "color": "Crypto_currency",
    },
)

fig_vol.update_layout(
    legend_title_text="Crypto_currency",
    legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top"),
    margin=dict(r=160, t=60, b=60, l=60),
    height=600,          # <-- zwiększamy wysokość wykresu
    bargap=0.25,         # <-- lekki odstęp między słupkami
)

# Jeśli chcesz, możesz włączyć skalę logarytmiczną dla lepszej widoczności małych wolumenów:
# fig_vol.update_yaxes(type="log", title="Total Trading Volume (log scale)")

fig_vol.update_yaxes(
    tickformat="~s",
    title="Total Trading Volume (sum over selected period)"
)
fig_vol.update_traces(
    hovertemplate="<b>%{x}</b><br>Volume: %{y:,}<extra></extra>",
    width=0.5
)

st.plotly_chart(fig_vol, use_container_width=True)
# --- Wolumen + Udział %: dwa wykresy obok siebie, wspólne kolory ---
vol = volume_with_share(df_all)  # kolumny: label, volume, share (w %)

# stałe kolory per coin (możesz nadpisać wybrane poniżej)
base_palette = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]
labels_sorted = vol["label"].tolist()
color_map = {lab: base_palette[i % len(base_palette)] for i, lab in enumerate(labels_sorted)}
# (opcjonalnie firmowe kolory)
color_map.update({
    "Bitcoin":  "#F7931A",
    "Ethereum": "#627EEA",
    "Solana":   "#14F195",
})

c_vol, c_share = st.columns(2)

# 1) Wykres wolumenów (absolutny)
with c_vol:
    fig_vol = px.bar(
        vol,
        x="label",
        y="volume",
        color="label",
        color_discrete_map=color_map,
        title="All coins — Total Trading Volume in Range",
        labels={
            "label": "Crypto_currency",
            "volume": "Total Trading Volume (sum over selected period)",
            "color": "Crypto_currency",
        },
    )
    fig_vol.update_layout(
        legend_title_text="Crypto_currency",
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top"),
        margin=dict(r=160, t=60, b=60, l=60),
        height=600,
        bargap=0.25,
    )
    fig_vol.update_yaxes(tickformat="~s")
    fig_vol.update_traces(
        hovertemplate="<b>%{x}</b><br>Volume: %{y:,}<extra></extra>",
        width=0.5,
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# 2) Wykres udziałów procentowych
with c_share:
    fig_share = px.bar(
        vol,
        x="label",
        y="share",
        color="label",
        color_discrete_map=color_map,
        title="All coins — Share of Total Volume",
        labels={
            "label": "Crypto_currency",
            "share": "Share of Total Volume (%)",
            "color": "Crypto_currency",
        },
    )
    fig_share.update_layout(
        showlegend=False,  # legenda już jest po lewej
        margin=dict(t=60, b=60, l=60, r=40),
        height=600,
        bargap=0.25,
    )
    fig_share.update_yaxes(range=[0, 100], ticksuffix="%", title="Share of Total Volume (%)")
    fig_share.update_traces(
        texttemplate="%{y:.1f}%",
        textposition="outside",
        cliponaxis=False,
        hovertemplate="<b>%{x}</b><br>Share: %{y:.2f}%<extra></extra>",
        width=0.5,
    )
    st.plotly_chart(fig_share, use_container_width=True)

