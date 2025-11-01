# dashboard/app.py
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# --- ścieżki importów (dostosuj jeśli potrzebujesz) ---
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from db.db import list_coins, get_history, get_history_all  # noqa: E402

# =========================
#        Caching
# =========================
@st.cache_data(ttl=300)
def cached_list_coins() -> pd.DataFrame:
    return list_coins()

@st.cache_data(ttl=300, show_spinner=False)
def cached_get_history(cid: str, start: datetime, end: datetime) -> pd.DataFrame:
    return get_history(cid, start, end)

@st.cache_data(ttl=300, show_spinner=False)
def cached_get_history_all(start: datetime, end: datetime) -> pd.DataFrame:
    return get_history_all(start, end)

# =========================
#        Utils
# =========================
def first_day(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)

def next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)

def last_month_start(year: int, month: int) -> datetime:
    return first_day(*next_month(year, month))

def add_mas(df: pd.DataFrame, price_col: str = "price", windows=(7, 30)) -> pd.DataFrame:
    df = df.sort_values("ts").copy()
    for w in windows:
        df[f"MA{w}"] = df[price_col].rolling(window=w, min_periods=1).mean()
    return df

def index_to_100(df: pd.DataFrame, price_col: str = "price", group_col: str = "coin_id") -> pd.DataFrame:
    out = []
    for cid, g in df.sort_values("ts").groupby(group_col, sort=False):
        g = g.copy()
        base = g[price_col].iloc[0]
        g["price_norm"] = (g[price_col] / base) * 100.0 if base and base != 0 else np.nan
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else df.copy()

def nice_delta_pct(a: float, b: float) -> float:
    try:
        return (a / b - 1.0) * 100.0
    except Exception:
        return np.nan

# stałe kolory dla monet
base_palette = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf", "#4e79a7", "#f28e2b",
]
def build_color_map(names: list[str]) -> dict[str, str]:
    cmap = {}
    for i, name in enumerate(names):
        cmap[name] = base_palette[i % len(base_palette)]
    return cmap

# =========================
#        UI
# =========================
st.set_page_config(page_title="CryptoCurrencies Dashboard", layout="wide")
st.title("CryptoCurrencies Dashboard")

coins_df = cached_list_coins()
if coins_df.empty:
    st.warning("Brak danych w bazie. Uruchom ETL (fetch → save).")
    st.stop()

coins_df = coins_df.sort_values("name")
all_names = coins_df["name"].tolist()
name_to_id = dict(zip(coins_df["name"], coins_df["coin_id"]))
id_to_name = dict(zip(coins_df["coin_id"], coins_df["name"]))

# ---- filtry (multi-select i zakres dat rok/miesiąc) ----
col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns([2, 1, 1, 1, 1])

with col_f1:
    selection = st.multiselect(
        "Select cryptocurrencies",
        ["All"] + all_names,
        default=["All"],
        help="Możesz wybrać jedną, kilka lub All.",
    )

now = datetime.now(timezone.utc)
years = list(range(2020, now.year + 1))

with col_f2:
    start_year = st.selectbox("Start year", years, index=max(0, years.index(now.year) - 1))
with col_f3:
    start_month = st.selectbox("Start month", list(range(1, 13)), index=now.month - 1)

with col_f4:
    end_year = st.selectbox("End year", years, index=years.index(now.year))
with col_f5:
    end_month = st.selectbox("End month", list(range(1, 13)), index=now.month - 1)

start_dt = first_day(start_year, start_month)
end_dt = last_month_start(end_year, end_month)  # zakres [start_dt, end_dt)

# wybór ID monet
if "All" in selection or len(selection) == 0:
    selected_names = all_names
else:
    selected_names = selection
selected_ids = [name_to_id[n] for n in selected_names]

# kolorystyka dla zaznaczonych
color_map = build_color_map(selected_names)

# =========================
#    Dane do wykresów
# =========================
# 1) zbiorczo dla wszystkich wybranych monet
hist_all = cached_get_history_all(start_dt, end_dt)
hist_all = hist_all[hist_all["coin_id"].isin(selected_ids)].copy()

# Jeśli nie ma danych, kończymy
if hist_all.empty:
    st.info("Brak danych w wybranym zakresie.")
    st.stop()

# 2) KPI – osobno dla pojedynczej monety vs wielu
if len(selected_ids) == 1:
    cid = selected_ids[0]
    single = cached_get_history(cid, start_dt, end_dt)
    if single.empty:
        st.info("Brak danych dla wybranej monety.")
    else:
        single = single.rename(columns={"ts": "ts", "price": "price", "volume": "volume"})
        single = add_mas(single, "price", windows=(7, 30))
        latest = single.sort_values("ts").iloc[-1]
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        with kpi_col1:
            st.metric(label=f"{id_to_name[cid]} — Current Price", value=f"${latest['price']:,.2f}")
        with kpi_col2:
            st.metric(label="7-day avg", value=f"${single['MA7'].iloc[-1]:,.2f}")
        with kpi_col3:
            st.metric(label="30-day avg", value=f"${single['MA30'].iloc[-1]:,.2f}")

        # Price + MA
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=single["ts"], y=single["price"], mode="lines", name="Price",
            line=dict(width=2, color=color_map[id_to_name[cid]])
        ))
        fig.add_trace(go.Scatter(
            x=single["ts"], y=single["MA7"], mode="lines", name="MA 7",
            line=dict(width=1.5, dash="dash")
        ))
        fig.add_trace(go.Scatter(
            x=single["ts"], y=single["MA30"], mode="lines", name="MA 30",
            line=dict(width=1.5, dash="dot")
        ))
        fig.update_layout(
            title=f"{id_to_name[cid]} — Price & Moving Averages",
            xaxis_title="Time",
            yaxis_title="Price",
            height=380,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=60, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    # kafelki KPI dla wielu monet – bieżąca cena + zmiana % vs 7d i 30d
    tiles = []
    recent_start = now - timedelta(days=60)
    for cid in selected_ids:
        df = cached_get_history(cid, max(start_dt, recent_start), end_dt)
        if df.empty:
            continue
        df = df.sort_values("ts")
        cur = df["price"].iloc[-1]
        # 7d/30d
        df7 = df[df["ts"] >= (now - timedelta(days=7))]
        df30 = df[df["ts"] >= (now - timedelta(days=30))]
        avg7 = df7["price"].mean() if not df7.empty else np.nan
        avg30 = df30["price"].mean() if not df30.empty else np.nan
        tiles.append({
            "name": id_to_name[cid],
            "cur": cur,
            "d7": nice_delta_pct(cur, avg7) if not np.isnan(avg7) else np.nan,
            "d30": nice_delta_pct(cur, avg30) if not np.isnan(avg30) else np.nan
        })

    if tiles:
        st.subheader("Selected coins — quick view")
        # po trzy KPI w wierszu
        n = 3
        for i in range(0, len(tiles), n):
            cols = st.columns(n)
            for c, t in zip(cols, tiles[i:i+n]):
                with c:
                    st.metric(
                        label=f"{t['name']} — Current",
                        value=f"${t['cur']:,.2f}",
                        delta=f"{t['d7']:.2f}% vs 7d" if not np.isnan(t['d7']) else "—"
                    )
                    st.caption(f"Δ30d: {t['d30']:.2f}%") if not np.isnan(t['d30']) else st.caption("Δ30d: —")

# =========================
#   Wykresy dla wielu monet
# =========================
st.markdown("---")

# 1) Price indexed to 100 (od startu okresu)
st.subheader("All selected — Price (indexed to 100 at period start)")
norm = index_to_100(
    hist_all.rename(columns={"price": "price", "ts": "ts"}),
    price_col="price",
    group_col="coin_id",
)
norm["name"] = norm["coin_id"].map(id_to_name)

fig_norm = px.line(
    norm,
    x="ts", y="price_norm", color="name",
    color_discrete_map=color_map,
    labels={"ts": "Time", "price_norm": "Index (100=start)"},
    height=380,
)
fig_norm.update_layout(
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=20, r=20, t=60, b=20),
)
st.plotly_chart(fig_norm, use_container_width=True)

# 2) Total trading volume (bar)
st.subheader("All coins — Total Trading Volume in Range")
vol = (hist_all.groupby("coin_id", as_index=False)["volume"].sum()
       .assign(name=lambda d: d["coin_id"].map(id_to_name)))
vol = vol.sort_values("volume", ascending=False)

fig_bar = px.bar(
    vol,
    x="name", y="volume",
    color="name",
    color_discrete_map=color_map,
    labels={"name": "Crypto_currency", "volume": "Total trading volume (sum in selected period)"},
    height=420,
)
fig_bar.update_layout(
    showlegend=True,
    legend=dict(orientation="v", yanchor="top", y=1.0, xanchor="left", x=1.02),
    margin=dict(l=20, r=20, t=40, b=20),
)
st.plotly_chart(fig_bar, use_container_width=True)

# 3) Share of total volume (%)
st.subheader("All coins — Share of Total Volume")
total_vol = vol["volume"].sum()
vol["share"] = np.where(total_vol > 0, vol["volume"] / total_vol * 100.0, np.nan)

fig_share = px.bar(
    vol,
    x="name", y="share",
    color="name",
    color_discrete_map=color_map,
    labels={"name": "Crypto_currency", "share": "Share of volume, %"},
    height=420,
)
fig_share.update_layout(
    showlegend=True,
    legend=dict(orientation="v", yanchor="top", y=1.0, xanchor="left", x=1.02),
    margin=dict(l=20, r=20, t=40, b=20),
)
st.plotly_chart(fig_share, use_container_width=True)

# 4) Correlation heatmap (dzienne stopy zwrotu)
st.subheader("All coins — Correlation of daily returns")
# pivot: index=ts, columns=coin_id, values=price; returns=diff(pct_change)
pivot = hist_all.pivot_table(index="ts", columns="coin_id", values="price")
rets = pivot.sort_index().pct_change().dropna(how="all")
if rets.shape[0] >= 5 and rets.shape[1] >= 2:
    corr = rets.corr()
    corr.columns = [id_to_name.get(c, c) for c in corr.columns]
    corr.index = [id_to_name.get(i, i) for i in corr.index]
    fig_corr = px.imshow(
        corr, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", zmin=-1, zmax=1,
        labels=dict(color="corr")
    )
    fig_corr.update_layout(height=500, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.caption("Za mało punktów danych, aby policzyć korelacje.")

# =========================
#  Footer
# =========================
st.markdown("---")
st.caption(
    "Tip: użyj suwaków rok/miesiąc, aby zawęzić okres. "
    "Wybierz jedną monetę, aby zobaczyć wykres Price & MA(7/30) i KPI dla tej monety."
)
