import streamlit as st
import plotly.express as px
from db import query

st.set_page_config(page_title='Symbol Performance', layout='wide')

st.title("Symbol Performance")

df = query('select * from marts.mart_symbol_performance')

clean_df = df[['symbol', 'total_pnl', 'performance_tier']]
tier_colors = {
    "STAR":           "#00C896",
    "PROFITABLE":     "#4C9BE8",
    "UNDERPERFORMER": "#F4A261",
    "AVOID":          "#E63946"
}

fig = px.bar(clean_df, x="symbol", y="total_pnl",
             color="performance_tier",
             color_discrete_map=tier_colors,
             title="Total PnL by Symbol",
             template="plotly_dark")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Detailed Symbol Performance")
st.dataframe(df, use_container_width=True)