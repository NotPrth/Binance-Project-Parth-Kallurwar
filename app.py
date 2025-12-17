import time
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from ws_client import BinanceWSClient
from datastore import read_ticks
from analytics_engine import spread_z, rolling_corr, adf

SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

st.set_page_config("Binance WS Pair Analytics", layout="wide")
st.title("Binance Futures WebSocket Pair Analytics")

if "ws" not in st.session_state:
    st.session_state.ws = BinanceWSClient(SYMBOLS)
    st.session_state.ws.start()

with st.sidebar:
    s1 = st.selectbox("Symbol A", SYMBOLS, 0)
    s2 = st.selectbox("Symbol B", SYMBOLS, 1)
    window = st.slider("Rolling Window", 20, 200, 60)
    auto = st.checkbox("Auto refresh (3s)", True)

# IMPORTANT: lowercase symbols for DB match
rows = read_ticks([s1.lower(), s2.lower()])

if not rows:
    st.info("Waiting for live WebSocket data...")
    st.stop()

frame = pd.DataFrame(
    [(r.symbol, r.ts, r.price) for r in rows],
    columns=["symbol", "ts", "price"],
)

pivot = (
    frame
    .pivot(index="ts", columns="symbol", values="price")
    .sort_index()
)

if s1.lower() not in pivot or s2.lower() not in pivot:
    st.warning("Insufficient synchronized data")
    st.stop()

p1 = pivot[s1.lower()]
p2 = pivot[s2.lower()]

fig = go.Figure()
fig.add_trace(go.Scatter(x=pivot.index, y=p1, name=s1))
fig.add_trace(go.Scatter(x=pivot.index, y=p2, name=s2))
st.plotly_chart(fig, use_container_width=True)

spread, z = spread_z(p1, p2, window)
corr = rolling_corr(p1, p2, window)

c1, c2 = st.columns(2)
c1.line_chart(spread)
c2.line_chart(z)

st.subheader("Rolling Correlation")
st.line_chart(corr)

stat, pval = adf(spread)
st.metric("ADF p-value", f"{pval:.4f}" if pval else "N/A")

if auto:
    time.sleep(3)
    st.rerun()
