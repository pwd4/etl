import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os

st.title("📈 Currency Forecast Dashboard")

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


@st.cache_data
def load_df(query):
    return pd.read_sql(query, engine)


st.header("💱 Курс USD/RUB (фактический)")

try:
    df_usd = load_df("SELECT date, value FROM currency_rates WHERE code = 'USD' ORDER BY date")
    fig = px.line(df_usd, x="date", y="value", title="USD/RUB")
    st.plotly_chart(fig)
except Exception as e:
    st.error(f"Ошибка загрузки данных: {e}")


st.header("🛢 Цена нефти Brent")

try:
    df_oil = load_df("SELECT date, price FROM oil_prices ORDER BY date")
    fig = px.line(df_oil, x="date", y="price", title="Brent Oil Price")
    st.plotly_chart(fig)
except Exception as e:
    st.error(f"Ошибка загрузки данных: {e}")
