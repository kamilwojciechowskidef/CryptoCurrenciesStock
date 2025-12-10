# dashboard/app.py
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# --- ścieżki importów ---
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from db.db import list_coins, get_history, get_history_all  # noqa: E402

# =========================
#        Cache
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
def ensure_ts_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Upewnia się, że kolumna 'ts' ma typ datetime64 i strefę UTC."""
    if "ts" not in df.columns:
        return df
    out = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(out["ts"]):
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce", utc=True)
    elif getattr(out["ts"].dt, "tz", None) is None:
        out["ts"] = out["ts"].dt.tz_localize("UTC")
    return out

def first_day(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)

def next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)

def last_month_start(year: int, month: int) -> datetime:
    return first_day(*next_month(year, month))

def add_mas(df: pd.DataFrame, price_col: str = "price", windows=(7, 30)) -> pd.DataFrame:
    df = df.sort_values("ts").copy()
    for w in windows:
        df[f"MA{w}"] = df[price_col].rolling(window=w, min_periods=1).mean()
    return df

def index_to_100(df: pd.DataFrame, price_col: str = "price", group_col: str = "coin_id") -> pd.DataFrame:
    out = []
    for _, g in df.sort_values("ts").groupby(group_col, sort=False):
        g = g.copy()
        base = g[price_col].iloc[0]
        g["price_norm"] = (g[price_col] / base) * 100.0 if pd.notna(base) and base != 0 else np.nan
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else df.copy()

def nice_delta_pct(cur: float, ref: float) -> float:
    try:
        return (cur / ref - 1.0) * 100.0
    except Exception:
        return np.nan

# stałe kolory dla monet
PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf", "#4e79a7", "#f28e2b",
]
def build_color_map(names: list[str]) -> dict[str, str]:
    return {name: PALETTE[i % len(PALETTE)] for i, name in enumerate(names)}

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

# ---- filtry ----
col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns([2, 1, 1, 1, 1])
now = datetime.now(timezone.utc)
one_year_ago = (datetime.now() - timedelta(days=365)).date()
with col_f1:

    # --- inicjalizacja stanu (tylko raz) ---
    if "selection" not in st.session_state:
        st.session_state.selection = ["All"]

    # --- komponent multiselect ---
    selection = st.multiselect(
        f"Wybierz kryptowalutę i zakres dat od {one_year_ago} do {now.date()}",
        ["All"] + all_names,
        default=st.session_state.selection,
        key="selection",
        help="Możesz wybrać jedną, kilka lub All."
    )

    # --- logika wymuszająca poprawne zachowanie ---
    if "All" in st.session_state.selection and len(st.session_state.selection) > 1:
        st.session_state.selection = ["All"]

    # poprawiamy selection używany dalej w kodzie
    selection = st.session_state.selection




years = list(range(now.year - 1, now.year + 1))

with col_f2:
    start_year = st.selectbox("Start year", years, index=max(0, years.index(now.year) - 1))
with col_f3:
    start_month = st.selectbox("Start month", list(range(1, 13)), index=now.month - 1)

with col_f4:
    end_year = st.selectbox("End year", years, index=years.index(now.year))
with col_f5:
    end_month = st.selectbox("End month", list(range(1, 13)), index=now.month - 1)

start_dt = first_day(start_year, start_month)
end_dt = last_month_start(end_year, end_month)  # [start_dt, end_dt)

if end_dt <= start_dt:
    st.warning("Zakres dat jest pusty. Upewnij się, że (rok,miesiąc) końcowy > (rok,miesiąc) początkowy.")
    st.stop()

# wybór monet
selected_names = all_names if "All" in selection or len(selection) == 0 else selection
selected_ids = [name_to_id[n] for n in selected_names]
color_map = build_color_map(selected_names)

# =========================
#    Dane do wykresów
# =========================
hist_all = ensure_ts_utc(cached_get_history_all(start_dt, end_dt))
hist_all = hist_all[hist_all["coin_id"].isin(selected_ids)].copy()

if hist_all.empty:
    st.info("Brak danych w zaznaczonym zakresie.")
    st.stop()

# =========================
#        KPI
# =========================
if len(selected_ids) == 1:
    # pojedyncza moneta
    cid = selected_ids[0]
    single = ensure_ts_utc(cached_get_history(cid, start_dt, end_dt))
    if single.empty:
        st.info("Brak danych dla wybranej kryptowaluty.")
    else:
        single = add_mas(single.rename(columns={"ts": "ts", "price": "price"}), "price", windows=(7, 30))
        latest = single.sort_values("ts").iloc[-1]
        k1, k2, k3 = st.columns(3)
        with k1:
            st.metric(label=f"{id_to_name[cid]} — Current Price", value=f"${latest['price']:,.2f}")
        with k2:
            st.metric(label="Śrtednia 7 dni", value=f"${single['MA7'].iloc[-1]:,.2f}")
        with k3:
            st.metric(label="Śrtednia 30 dni", value=f"${single['MA30'].iloc[-1]:,.2f}")
        st.caption(
            "Te trzy wskaźniki pokazują aktualną cenę kryptowaluty oraz jej średnią "
            "cenę z ostatnich 7 i 30 dni. To szybki sposób na ocenę, czy obecna wartość "
            "jest powyżej czy poniżej ostatnich trendów. Jeśli aktualna cena przekracza "
            "średnie kroczące, moneta zwykle znajduje się w fazie wzrostowej — jeśli jest "
            "niżej, może sygnalizować lokalną słabość rynku."
        )

        # Price + MA
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=single["ts"], y=single["price"], mode="lines", name="Cena",
            line=dict(width=2, color=color_map[id_to_name[cid]])
        ))
        fig.add_trace(go.Scatter(x=single["ts"], y=single["MA7"],  mode="lines", name="MA 7",
                                 line=dict(width=1.5, dash="dash")))
        fig.add_trace(go.Scatter(x=single["ts"], y=single["MA30"], mode="lines", name="MA 30",
                                 line=dict(width=1.5, dash="dot")))
        fig.update_layout(
            title=f"{id_to_name[cid]} — Cena i Średnie 7 i 30 dni",
            xaxis_title="Czas", yaxis_title="Cena",
            height=380, margin=dict(l=20, r=20, t=60, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True, key="price_ma")
else:
    # wiele monet → kafelki
    recent_start = now - timedelta(days=60)
    thr7 = pd.Timestamp(now - timedelta(days=7))
    thr30 = pd.Timestamp(now - timedelta(days=30))
    if thr7.tz is None:   thr7 = thr7.tz_localize("UTC")
    if thr30.tz is None:  thr30 = thr30.tz_localize("UTC")

    tiles = []
    for cid in selected_ids:
        df = ensure_ts_utc(cached_get_history(cid, max(start_dt, recent_start), end_dt))
        if df.empty:
            continue
        df = df.sort_values("ts")
        cur = df["price"].iloc[-1]
        avg7  = df.loc[df["ts"] >= thr7,  "price"].mean()
        avg30 = df.loc[df["ts"] >= thr30, "price"].mean()
        tiles.append({
            "name": id_to_name[cid],
            "cur": cur,
            "d7":  nice_delta_pct(cur, avg7)  if pd.notna(avg7)  else np.nan,
            "d30": nice_delta_pct(cur, avg30) if pd.notna(avg30) else np.nan,
        })

    if tiles:
        st.subheader("Wybrane kryptowaluty — podsumowanie")
        n = 3
        for i in range(0, len(tiles), n):
            cols = st.columns(n)
            for c, t in zip(cols, tiles[i:i+n]):
                with c:
                    st.metric(label=f"{t['name']} — Current", value=f"${t['cur']:,.2f}",
                              delta=(f"{t['d7']:.2f}% vs 7d" if pd.notna(t["d7"]) else "—"))
                    if pd.notna(t["d30"]):
                        st.caption(f"Δ30d: {t['d30']:.2f}%")
                    else:
                        st.caption("Δ30d: —")

# =========================
#   Wykresy dla wielu monet
# =========================
st.markdown("---")

st.caption(
    "Wykres pokazuje, jak zmieniały się ceny poszczególnych kryptowalut względem "
    "ich wartości początkowej w wybranym okresie. Każda moneta startuje z poziomu 100, "
    "co pozwala łatwo porównać tempo wzrostu lub spadków między projektami, niezależnie od ich ceny."
)

# 1) Indeks 100 na starcie okresu
st.subheader("Cena (indeksowana do 100% w okresie rozpoczęcia)")
norm = index_to_100(hist_all.rename(columns={"price": "price"}), price_col="price", group_col="coin_id")
norm["name"] = norm["coin_id"].map(id_to_name)

fig_norm = px.line(
    norm, x="ts", y="price_norm", color="name",
    color_discrete_map=color_map,
    labels={"ts": "Czas", "price_norm": "Indeks (100%=początkowy)"},
    height=380,
)
fig_norm.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                       margin=dict(l=20, r=20, t=60, b=20))
st.plotly_chart(fig_norm, use_container_width=True, key="indexed_prices")



# 2) Łączny wolumen (słupki)
st.subheader("Łączny wolumen w zakresie")
st.caption(
    "Suma całkowitego wolumenu handlu dla każdej kryptowaluty w wybranym okresie. "
    "Wysoki wolumen zwykle oznacza dużą aktywność inwestorów oraz lepszą płynność rynku."
)
vol = (hist_all.groupby("coin_id", as_index=False)["volume"].sum()
       .assign(name=lambda d: d["coin_id"].map(id_to_name)))
vol = vol.sort_values("volume", ascending=False)

fig_bar = px.bar(
    vol, x="name", y="volume", color="name",
    color_discrete_map=color_map,
    labels={"name": "Kryptowaluty", "volume": "Łączny wolumen (suma w zaznaczonym okresie)"},
    height=420,
)
fig_bar.update_layout(showlegend=True,
                      legend=dict(orientation="v", yanchor="top", y=1.0, xanchor="left", x=1.02),
                      margin=dict(l=20, r=20, t=40, b=20))
st.plotly_chart(fig_bar, use_container_width=True, key="total_volume")



# 3) Udział w wolumenie (%)
st.subheader("Udział w wolumenie (%)")
st.caption(
    "Udział danej kryptowaluty w całkowitym wolumenie transakcji. "
    "Pokazuje, które projekty dominowały w handlu oraz jak wygląda ich względna popularność."
)
total_vol = vol["volume"].sum()
vol["share"] = np.where(total_vol > 0, vol["volume"] / total_vol * 100.0, np.nan)

fig_share = px.bar(
    vol, x="name", y="share", color="name",
    color_discrete_map=color_map,
    labels={"name": "Kryptowaluty", "share": "Udział w wolumenie, %"},
    height=420,
)
fig_share.update_layout(showlegend=True,
                        legend=dict(orientation="v", yanchor="top", y=1.0, xanchor="left", x=1.02),
                        margin=dict(l=20, r=20, t=40, b=20))
st.plotly_chart(fig_share, use_container_width=True, key="share_volume")



# 4) Heatmapa korelacji (dzienne stopy zwrotu)
st.subheader("Korelacja dziennych zwrotów")
st.caption(
    "Mapa korelacji dziennych stóp zwrotu między kryptowalutami. "
    "Wartości bliskie 1 oznaczają, że monety poruszają się podobnie, "
    "a wartości ujemne sugerują przeciwny kierunek zmian. To przydatne narzędzie "
    "do oceny dywersyfikacji portfela."
)
pivot = hist_all.pivot_table(index="ts", columns="coin_id", values="price").copy()

# index → datetime UTC (bez błędów dla datetime64[ns, UTC])
if not pd.api.types.is_datetime64_any_dtype(pivot.index):
    pivot.index = pd.to_datetime(pivot.index, errors="coerce", utc=True)
elif getattr(pivot.index, "tz", None) is None:
    pivot.index = pd.to_datetime(pivot.index, utc=True)

rets = pivot.sort_index().pct_change().dropna(how="all")
if rets.shape[0] >= 5 and rets.shape[1] >= 2:
    corr = rets.corr()
    corr.columns = [id_to_name.get(c, c) for c in corr.columns]
    corr.index   = [id_to_name.get(i, i) for i in corr.index]

    fig_corr = px.imshow(
        corr, text_auto=".2f", aspect="auto",
        color_continuous_scale="RdYlGn", zmin=-1, zmax=1,
        labels=dict(color="corr"),
    )
    fig_corr.update_layout(height=500, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_corr, use_container_width=True, key="correlation")
else:
    st.caption("Brak danych dla obliczenia korelacji")


# ==============================
# 1️⃣ Cumulative Returns
# ==============================
st.subheader("Kumulowane zwroty")
st.caption(
    "Skumulowane stopy zwrotu pokazują, jaki procent zysku lub straty wygenerowałaby "
    "inwestycja w daną kryptowalutę na początku wybranego okresu. Idealne do porównania "
    "długoterminowej efektywności projektów."
)


# Oblicz dzienne zwroty i skumulowany zwrot
returns = hist_all.pivot(index="ts", columns="coin_id", values="price").pct_change()
cum_returns = (1 + returns).cumprod() - 1
cum_returns = cum_returns.reset_index().melt(id_vars="ts", var_name="coin_id", value_name="cum_return")

fig_cum = px.line(
    cum_returns,
    x="ts",
    y="cum_return",
    color="coin_id",
    title="Cumulative Returns Over Time",
    labels={"cum_return": "Kumulowany zwrot", "ts": "Data", "coin": "Kryptowaluta"},
)

fig_cum.update_layout(
    height=500,
    margin=dict(l=20, r=20, t=40, b=40),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig_cum, use_container_width=True, key="cumulative_returns")

# ==============================
# 2️⃣ Risk–Return Scatter
# ==============================
st.subheader("Ryzyko vs Zwrot")

# policz średni zwrot i odchylenie standardowe
risk_return = (
    returns.agg(["mean", "std"])
    .T.reset_index()
    .rename(columns={"index": "coin_id", "mean": "avg_return", "std": "volatility"})
)

fig_risk = px.scatter(
    risk_return,
    x="volatility",
    y="avg_return",
    color="coin_id",
    size="volatility",
    hover_name="coin_id",
    title="Średni dzienny zwrot vs ryzyko",
    labels={
        "volatility": "Ryzyko",
        "avg_return": "Średni dzienny zwrot",
        "coin_id": "Kryptowaluta",
    },
    trendline="ols",
)

fig_risk.update_traces(marker=dict(opacity=0.8, line=dict(width=1, color="white")))
fig_risk.update_layout(
    height=500,
    margin=dict(l=20, r=20, t=40, b=40),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig_risk, use_container_width=True, key="risk_return")

# ==============================
# 3️⃣ Market Share Evolution
# ==============================
# ==============================
# 3️⃣ Market Share Evolution (wolumen jako proxy)
# ==============================
st.subheader("Ewolucja udziału w rynku")
st.caption(
    "Zmiany udziału rynkowego kryptowalut w czasie, mierzone udziałem w wolumenie transakcji. "
    "Rosnący udział sugeruje zwiększone zainteresowanie inwestorów, a spadający – osłabienie "
    "aktywności handlowej danego projektu."
)


# Użyj wolumenu zamiast kapitalizacji rynkowej (jeśli brak kolumny 'market_cap')
market_share = hist_all.copy()
market_share["share"] = market_share["volume"] / market_share.groupby("ts")["volume"].transform("sum")


fig_share_evo = px.area(
    market_share,
    x="ts",
    y="share",
    color="coin_id",
    title="Evolucja udziału w rynku",
    labels={"share": "Udział w rynku (%)", "ts": "Data", "coin_id": "Kryptowaluta"},
    groupnorm="fraction",
)

fig_share_evo.update_layout(
    height=500,
    margin=dict(l=20, r=20, t=40, b=40),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig_share_evo, use_container_width=True, key="market_share_evo")


# =========================
#  Footer
# =========================
st.markdown("---")
st.caption(
    "Ten dashboard zawiera podsumowanie wydajności kryptowalut. "
    "Użyj filtrów roku/miesiąca, aby zdefiniować okres analizy i wybierz jedną lub więcej kryptowalut. "
    "Wybieranie jednej kryptowaluty pokaże szczegółowe metryki, w tym cenę i średnie 7 i 30 dni."
)

