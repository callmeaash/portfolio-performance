import streamlit as st
import plotly.express as px
from db import query


st.set_page_config(page_title='Portfolio Performance', layout='wide')
st.title('Portfolio Performance Dashboard')

df = query('select * from marts.mart_portfolio_summary')

col1, col2, col3, col4 = st.columns(4)

col1.metric('Total Trades', df["total_trades"][0])
col2.metric("Total Wins",  df["total_wins"][0])
col3.metric("Total Losses",  df["total_losses"][0])
col4.metric("Win Rate", f"{df['win_rate_pct'][0]}%")

col5, col6, col7, col8 = st.columns(4)
col5.metric("Total PnL", f"Rs. {df['total_pnl'][0]:,.0f}")
col6.metric("Profit Factor",  df["profit_factor"][0])
col7.metric("Stocks Traded", df["symbols_traded"][0])
col8.metric("Average Holding Days", df["avg_holding_days"][0])


st.divider()

col6, col7, col8 = st.columns(3)
col6.metric("Max Drawdown",   f"{df['max_drawdown_pct'][0]}%")
col7.metric("Expectancy",     f"{df['expectancy_pct'][0]}%")
col8.metric("Calmar Ratio",     f"{df['calmer_ratio'][0]:.2f}")
