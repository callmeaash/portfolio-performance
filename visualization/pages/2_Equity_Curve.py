import streamlit as st
import plotly.express as px
from db import query

st.set_page_config(page_title='Equity Curve', layout='wide')
st.title("Equity Curve & Drawdown")

df = query("""
    select date, cumulative_pnl, drawdown_pct, daily_pnl
    from marts.mart_daily_pnl
    order by date
""")

fig1 = px.line(df, x="date", y="cumulative_pnl",
               title="Cumulative PnL", template="plotly_dark")
st.plotly_chart(fig1, use_container_width=True)

fig2 = px.area(df, x="date", y="drawdown_pct",
               title="Drawdown %", template="plotly_dark",
               color_discrete_sequence=["red"])
st.plotly_chart(fig2, use_container_width=True)