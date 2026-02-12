import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Data Quality Dashboard - Fintech", layout="wide")
st.title("📊 Data Quality Dashboard — Fintech Pipeline")

# Connexion
con = duckdb.connect('data/fintech.duckdb', read_only=True)

# --- Score global ---
quality = con.execute("SELECT * FROM analytics.mart_data_quality_summary").fetchdf()

col1, col2, col3, col4 = st.columns(4)
with col1:
    score = quality['data_quality_score'].iloc[0]
    st.metric("Data Quality Score", f"{score}%", 
              delta="Bon" if score > 95 else "À surveiller")
with col2:
    st.metric("Total Records", f"{quality['total_records'].iloc[0]:,}")
with col3:
    st.metric("Montants manquants", f"{quality['pct_missing_amounts'].iloc[0]}%")
with col4:
    st.metric("Transactions suspectes", f"{quality['pct_suspicious'].iloc[0]}%")

st.divider()

# --- Qualité par catégorie ---
st.subheader("Qualité par catégorie de transaction")
by_cat = con.execute("SELECT * FROM analytics.mart_quality_by_category").fetchdf()

fig = px.bar(by_cat, x='category', y=['missing_amounts', 'suspicious_amounts', 'fraud_count'],
             barmode='group', title='Anomalies par catégorie')
st.plotly_chart(fig, use_container_width=True)

# --- Distribution des montants ---
st.subheader("Distribution des montants")
amounts = con.execute("""
    SELECT amount FROM analytics.stg_transactions 
    WHERE amount IS NOT NULL AND abs(amount) < 10000
""").fetchdf()

fig2 = px.histogram(amounts, x='amount', nbins=50, title='Distribution des montants')
st.plotly_chart(fig2, use_container_width=True)

# --- Transactions par statut ---
col_left, col_right = st.columns(2)
with col_left:
    statuses = con.execute("""
        SELECT status, count(*) as count 
        FROM analytics.stg_transactions GROUP BY status
    """).fetchdf()
    fig3 = px.pie(statuses, values='count', names='status', title='Répartition par statut')
    st.plotly_chart(fig3)

with col_right:
    countries = con.execute("""
        SELECT country, count(*) as count 
        FROM analytics.stg_transactions GROUP BY country ORDER BY count DESC
    """).fetchdf()
    fig4 = px.bar(countries, x='country', y='count', title='Transactions par pays')
    st.plotly_chart(fig4)

con.close()
