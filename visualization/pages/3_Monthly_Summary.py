import streamlit as st
import plotly.express as px
from db import query

st.set_page_config(page_title='Monthly Performance', layout='wide')

st.title("Monthly Performance")

df = query("""
    select month, total_pnl, win_rate_pct,
           month_result, cumulative_pnl
    from marts.mart_monthly_performance
    order by month
""")

fig = px.bar(df, x="month", y="total_pnl",
             color="month_result",
             color_discrete_map={"GREEN": "#00C896", "RED": "#E63946"},
             title="Monthly PnL",
             template="plotly_dark")
st.plotly_chart(fig, use_container_width=True)

st.dataframe(df, use_container_width=True)