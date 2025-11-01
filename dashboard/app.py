# dashboard/app.py
import calendar
from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import sys
sys.path.append("C:/Users/kamil/Desktop/CryptoCurrenciesStock")

# --- DB API (tylko odczyt) ---
from db.db import list_coins, get_history, get_history_all

# --- Transformacje (po stronie ETL/transform_data.py) ---
from etl.transform_data import (
    history_postprocess,
    allcoins_postprocess,
    add_index_100,
    volume_with_share,
)

# ---------------- UI CONFIG ----------------
st.set_page_config("CryptoCurrencies Dashboard", layout="wide")
st.title("CryptoCurrencies Dashboard")

# ---------------- SELEKCJA KRYPTOWALUT ----------------
coins = list_coins()
if coins.empty:
    st.warning("Brak danych w bazie. Uruchom ETL.")
    st.stop()

coins["label"] = coins["name"].fillna(coins["coin_id"])
label_to_cid = dict(zip(coins["label"], coins["coin_id"]))
labels_all = coins["label"].tolist()

sel = st.multiselect(
    "Select cryptocurrencies:",
    options=["All"] + labels_all,
    default=["All"],
)

if not sel:
    st.stop()

if "All" in sel:
    selected_labels = labels_all
else:
    selected_labels = sel

selected_ids = [label_to_cid[l] for l in selected_labels]

# ---------------- SELEKCJA ZAKRESU: ROK/MIESIĄC ----------------
now = datetime.now(timezone.utc)
years = list(range(2020, now.year + 1))
months = list(range(1, 13))

c_y1, c_m1, c_y2, c_m2 = st.columns(4)
with c_y1:
    y_start = st.selectbox("Start year", years, index=years.index(now.year))
with c_m1:
    m_start = st.selectbox("Start month", months, index=0)
with c_y2:
    y_end = st.selectbox("End year", years, index=years.index(now.year))
with c_m2:
    m_end = st.selectbox("End month", months, index=now.month - 1)

start_dt = datetime(y_start, m_start, 1, tzinfo=timezone.utc)
last_day = calendar.monthrange(y_end, m_end)[1]
# górna granica otwarta: pierwszy dzień następnego miesiąca
end_next = (datetime(y_end, m_end, 1, tzinfo=timezone.utc) + timedelta(days=32)).replace(day=1)

# ---------------- KPI ----------------
recent_start, recent_end = now - timedelta(days=60), now

if len(selected_ids) == 1:
    # Jedna kryptowaluta → trzy metryki + wykres Price&MA
    cid = selected_ids[0]
    lab = selected_labels[0]
    df_one = history_postprocess(get_history(cid, recent_start, recent_end))

    k1, k2, k3 = st.columns(3)
    if df_one.empty:
        with k1: st.metric(f"{lab} — Current Price", "—")
        with k2: st.metric("7-day avg", "—")
        with k3: st.metric("30-day avg", "—")
    else:
        last_price = df_one["price"].iloc[-1]
        ma7 = df_one["ma7"].iloc[-1]
        ma30 = df_one["ma30"].iloc[-1]
        delta = float((df_one["ret"].iloc[-1] or 0) * 100)

        with k1: st.metric(f"{lab} — Current Price", f"${last_price:,.2f}", delta=f"{delta:.2f}%")
        with k2: st.metric("7-day avg", f"${ma7:,.2f}")
        with k3: st.metric("30-day avg", f"${ma30:,.2f}")

        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["price"], name="Price", mode="lines"))
        fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["ma7"], name="MA 7", mode="lines"))
        fig_price.add_trace(go.Scatter(x=df_one["ts"], y=df_one["ma30"], name="MA 30", mode="lines"))
        fig_price.update_layout(
            title=f"{lab} — Price & Moving Averages",
            xaxis_title="Time",
            yaxis_title="Price",
            hovermode="x unified",
        )
        fig_price.update_xaxes(rangeslider_visible=True)
        st.plotly_chart(fig_price, use_container_width=True)

else:
    # Wiele kryptowalut → kafelki metryk dla każdej (Current/7d/30d)
    cols = st.columns(3)
    col_idx = 0
    for lab, cid in zip(selected_labels, selected_ids):
        dfi = history_postprocess(get_history(cid, recent_start, recent_end))
        if dfi.empty:
            cols[col_idx].metric(f"{lab} — Current Price", "—")
        else:
            last_price = dfi["price"].iloc[-1]
            ma7 = dfi["ma7"].iloc[-1]
            ma30 = dfi["ma30"].iloc[-1]
            delta = float((dfi["ret"].iloc[-1] or 0) * 100)
            cols[col_idx].metric(f"{lab}", f"${last_price:,.2f}", delta=f"{delta:.2f}%")
            cols[col_idx].markdown(f"**7d avg:** ${ma7:,.2f}  \n**30d avg:** ${ma30:,.2f}")
        col_idx = (col_idx + 1) % 3

st.markdown("---")

# ---------------- DANE ZBIORCZE DLA WYBRANYCH KRYPTOWALUT ----------------
df_all = allcoins_postprocess(get_history_all(start_dt, end_next))
df_all = df_all[df_all["coin_id"].isin(selected_ids)]
if df_all.empty:
    st.warning("Brak danych w wybranym zakresie / dla wybranych kryptowalut.")
    st.stop()

# ---------------- LINIE: INDEXED TO 100 ----------------
# bierzemy tylko serie z ≥ 2 punktami
pts = df_all.groupby("coin_id")["ts"].count()
df_line = df_all[df_all["coin_id"].isin(pts[pts >= 2].index)].copy()

if not df_line.empty:
    df_line = add_index_100(df_line)
    df_line["label"] = df_line["name"].fillna(df_line["coin_id"])
    fig_norm = px.line(
        df_line,
        x="ts",
        y="price_norm",
        color="label",
        title="Price (indexed to 100 at period start)",
        labels={"price_norm": "Index (100=start)", "ts": "Time", "label": "Crypto_currency"},
    )
    st.plotly_chart(fig_norm, use_container_width=True)
else:
    st.info("Za mało punktów na linie indeksu w wybranym zakresie.")

# ---------------- WOLUMEN + UDZIAŁ PROCENTOWY ----------------
vol = volume_with_share(df_all)  # kolumny: label, volume, share

# stałe kolory per coin
base_palette = ["#F7931A", "#627EEA", "#14F195", "#d62728", "#9467bd",
                "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
labels_sorted = vol["label"].tolist()
color_map = {lab: base_palette[i % len(base_palette)] for i, lab in enumerate(labels_sorted)}
# opcjonalnie doprecyzowanie „firmowych”
color_map.update({"Bitcoin": "#F7931A", "Ethereum": "#627EEA", "Solana": "#14F195"})

c_vol, c_share = st.columns(2)

with c_vol:
    fig_vol = px.bar(
        vol,
        x="label",
        y="volume",
        color="label",
        color_discrete_map=color_map,
        title="Total Trading Volume in Range",
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

with c_share:
    fig_share = px.bar(
        vol,
        x="label",
        y="share",
        color="label",
        color_discrete_map=color_map,
        title="Share of Total Volume",
        labels={
            "label": "Crypto_currency",
            "share": "Share of Total Volume (%)",
            "color": "Crypto_currency",
        },
    )
    fig_share.update_layout(
        showlegend=False,
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

# ---------------- KORELACJE ZWROTÓW ----------------
df_corr = df_all.copy()
pts = df_corr.groupby("coin_id")["ts"].count()
df_corr = df_corr[df_corr["coin_id"].isin(pts[pts >= 3].index)]
if not df_corr.empty:
    df_corr = df_corr.sort_values(["coin_id", "ts"]).copy()
    df_corr["ret"] = df_corr.groupby("coin_id")["price"].pct_change()
    pivot_ret = df_corr.pivot_table(
        index="ts",
        columns=df_corr["name"].fillna(df_corr["coin_id"]),
        values="ret",
    )
    if not pivot_ret.dropna(how="all").empty:
        corr = pivot_ret.corr(min_periods=10)
        fig_corr = px.imshow(
            corr,
            text_auto=False,
            aspect="auto",
            title="Correlation of Returns",
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("Brak wystarczających danych do korelacji.")
