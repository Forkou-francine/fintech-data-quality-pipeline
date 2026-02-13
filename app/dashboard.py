import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import json
import os

# --- Configuration de la page ---
# Ça doit être la première commande Streamlit dans le fichier
st.set_page_config(
    page_title="Data Quality Dashboard - Fintech",
    page_icon="📊",
    layout="wide"        # Utilise toute la largeur de l'écran
)

# --- Titre ---
st.title("📊 Data Quality Dashboard")
st.markdown("*Pipeline de qualité des données pour une fintech*")
st.divider()  # Ligne de séparation horizontale

# --- Connexion à DuckDB ---
# read_only=True parce que le dashboard ne modifie rien
con = duckdb.connect('data/fintech.duckdb', read_only=True)


# ============================================
# SECTION 1 : Métriques principales (en haut)
# ============================================
st.subheader("🎯 Score global de qualité")

# Récupère le résumé calculé par dbt
quality = con.execute("SELECT * FROM fintech.mart_data_quality_summary").fetchdf()

# Affiche 4 métriques côte à côte
# st.columns(4) crée 4 colonnes de largeur égale
col1, col2, col3, col4 = st.columns(4)

with col1:
    score = quality['data_quality_score'].iloc[0]
    # st.metric affiche un gros chiffre avec un label
    st.metric(
        label="Data Quality Score",
        value=f"{score}%",
        delta="Bon" if score > 95 else "À surveiller",
        delta_color="normal" if score > 95 else "inverse"
    )

with col2:
    total = quality['total_records'].iloc[0]
    st.metric(label="Total transactions", value=f"{total:,}")

with col3:
    missing = quality['pct_missing_amounts'].iloc[0]
    st.metric(label="Montants manquants", value=f"{missing}%")

with col4:
    suspicious = quality['pct_suspicious'].iloc[0]
    st.metric(label="Transactions suspectes", value=f"{suspicious}%")


# ============================================
# SECTION 2 : Résultat Great Expectations
# ============================================
st.divider()
st.subheader("🔍 Résultat des validations Great Expectations")

ge_file = 'data/processed/ge_validation_result.json'
if os.path.exists(ge_file):
    with open(ge_file, 'r') as f:
        ge_result = json.load(f)
    
    col_ge1, col_ge2, col_ge3 = st.columns(3)
    with col_ge1:
        st.metric("Règles testées", ge_result['total_rules'])
    with col_ge2:
        st.metric("Règles OK", ge_result['passed'])
    with col_ge3:
        st.metric("Règles échouées", ge_result['failed'])
    
    if ge_result['success']:
        st.success("✅ Toutes les validations passent !")
    else:
        st.warning(f"⚠️ {ge_result['failed']} validation(s) en échec")
else:
    st.info("ℹ️ Aucun résultat Great Expectations trouvé. Lance d'abord le script de validation.")


# ============================================
# SECTION 3 : Qualité par catégorie
# ============================================
st.divider()
st.subheader("📂 Qualité par catégorie de transaction")

by_cat = con.execute("SELECT * FROM fintech.mart_quality_by_category").fetchdf()

# Graphique en barres groupées
fig_cat = px.bar(
    by_cat,
    x='category',
    y=['missing_amounts', 'suspicious_amounts', 'fraud_count'],
    barmode='group',
    title='Anomalies détectées par catégorie',
    labels={
        'value': 'Nombre',
        'category': 'Catégorie',
        'variable': 'Type d\'anomalie'
    },
    color_discrete_map={
        'missing_amounts': '#FF6B6B',
        'suspicious_amounts': '#FFA500',
        'fraud_count': '#FF0000'
    }
)
st.plotly_chart(fig_cat, use_container_width=True)


# ============================================
# SECTION 4 : Distribution des montants
# ============================================
st.divider()
st.subheader("💰 Distribution des montants")

# On exclut les montants extrêmes pour avoir un histogramme lisible
amounts = con.execute("""
    SELECT amount 
    FROM fintech.stg_transactions 
    WHERE amount IS NOT NULL AND abs(amount) < 15000
""").fetchdf()

fig_hist = px.histogram(
    amounts,
    x='amount',
    nbins=50,
    title='Distribution des montants (hors valeurs extrêmes)',
    labels={'amount': 'Montant (€)', 'count': 'Nombre de transactions'}
)
fig_hist.update_traces(marker_color='#4ECDC4')
st.plotly_chart(fig_hist, use_container_width=True)


# ============================================
# SECTION 5 : Répartitions (2 graphiques côte à côte)
# ============================================
st.divider()
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📋 Répartition par statut")
    statuses = con.execute("""
        SELECT status, count(*) as count 
        FROM fintech.stg_transactions 
        GROUP BY status
    """).fetchdf()
    
    fig_pie = px.pie(
        statuses,
        values='count',
        names='status',
        title='Statut des transactions',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    st.subheader("🌍 Transactions par pays")
    countries = con.execute("""
        SELECT country, count(*) as count 
        FROM fintech.stg_transactions 
        GROUP BY country 
        ORDER BY count DESC
    """).fetchdf()
    
    fig_country = px.bar(
        countries,
        x='country',
        y='count',
        title='Volume par pays',
        labels={'country': 'Pays', 'count': 'Nombre'},
        color='count',
        color_continuous_scale='Teal'
    )
    st.plotly_chart(fig_country, use_container_width=True)


# ============================================
# SECTION 6 : Évolution temporelle
# ============================================
st.divider()
st.subheader("📅 Volume de transactions par mois")

monthly = con.execute("""
    SELECT 
        date_trunc('month', transaction_date) as month,
        count(*) as nb_transactions,
        sum(case when is_fraud then 1 else 0 end) as nb_frauds
    FROM fintech.int_transactions_enriched
    WHERE transaction_date IS NOT NULL
    GROUP BY 1
    ORDER BY 1
""").fetchdf()

fig_time = px.line(
    monthly,
    x='month',
    y='nb_transactions',
    title='Évolution mensuelle du volume',
    labels={'month': 'Mois', 'nb_transactions': 'Nombre de transactions'}
)
fig_time.update_traces(line_color='#2C3E50', line_width=2)
st.plotly_chart(fig_time, use_container_width=True)


# --- Fermeture de la connexion ---
con.close()

# --- Footer ---
st.divider()
st.caption("Pipeline : Python → DuckDB → dbt → Great Expectations → Streamlit")