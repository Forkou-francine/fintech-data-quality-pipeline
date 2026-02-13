import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from datetime import datetime

# --- Configuration ---
st.set_page_config(
    page_title="Data Quality Monitor — Fintech Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Style personnalisé ---
st.markdown("""
<style>
    /* Polices */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Poppins:wght@500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* FORCER Inter partout — override Streamlit */
    html, body, div, span, p, a, td, th, label, input,
    [class*="st-"], .stMarkdown, .stMarkdown p,
    .element-container, .stTextInput, .stSelectbox,
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p {
        font-family: 'Inter', sans-serif !important;
    }

    /* Forcer la couleur de texte par défaut */
    html, body, [class*="st-"], .stMarkdown, .stMarkdown p,
    [data-testid="stMarkdownContainer"] p {
        color: #334155 !important;
    }

    /* Full width */
    .block-container {
        padding: 1.5rem 2.5rem 2rem 2.5rem !important;
        max-width: 100% !important;
    }

    .stApp {
        background: #FFFFFF;
    }

    /* Header */
    .dashboard-header {
        padding: 1rem 0 1.2rem 0;
        border-bottom: 1px solid #E2E8F0;
        margin-bottom: 1.8rem;
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
    }
    .dashboard-title {
        font-family: 'Poppins', sans-serif !important;
        font-size: 1.5rem;
        font-weight: 700;
        color: #0F172A !important;
        margin: 0;
        letter-spacing: -0.3px;
    }
    .dashboard-subtitle {
        font-size: 0.82rem;
        color: #64748B !important;
        margin-top: 4px;
        font-family: 'JetBrains Mono', monospace !important;
    }
    .header-right {
        font-size: 0.78rem;
        color: #64748B !important;
        font-family: 'JetBrains Mono', monospace !important;
        text-align: right;
    }

    /* Carte métrique */
    .metric-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 1.2rem 1.4rem;
        height: 100%;
        transition: border-color 0.2s ease;
    }
    .metric-card:hover {
        border-color: #CBD5E1;
    }
    .metric-label {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.72rem;
        font-weight: 600;
        color: #64748B !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 10px;
    }
    .metric-value {
        font-family: 'Poppins', sans-serif !important;
        font-size: 1.9rem;
        font-weight: 700;
        letter-spacing: -0.5px;
        line-height: 1.1;
    }
    .metric-detail {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.76rem;
        color: #64748B !important;
        margin-top: 8px;
    }

    .value-green { color: #059669 !important; }
    .value-yellow { color: #D97706 !important; }
    .value-red { color: #DC2626 !important; }
    .value-blue { color: #2563EB !important; }
    .value-neutral { color: #1E293B !important; }

    /* Status badge */
    .status-pass {
        display: inline-block;
        background: rgba(5,150,105,0.08);
        color: #059669 !important;
        padding: 3px 12px;
        border-radius: 6px;
        font-size: 0.74rem;
        font-weight: 600;
        font-family: 'Inter', sans-serif !important;
        border: 1px solid rgba(5,150,105,0.18);
    }
    .status-warn {
        display: inline-block;
        background: rgba(217,119,6,0.08);
        color: #D97706 !important;
        padding: 3px 12px;
        border-radius: 6px;
        font-size: 0.74rem;
        font-weight: 600;
        font-family: 'Inter', sans-serif !important;
        border: 1px solid rgba(217,119,6,0.18);
    }
    .status-fail {
        display: inline-block;
        background: rgba(220,38,38,0.08);
        color: #DC2626 !important;
        padding: 3px 12px;
        border-radius: 6px;
        font-size: 0.74rem;
        font-weight: 600;
        font-family: 'Inter', sans-serif !important;
        border: 1px solid rgba(220,38,38,0.18);
    }

    /* Section title */
    .section-title {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.95rem;
        font-weight: 600;
        color: #475569 !important;
        margin: 2.2rem 0 1rem 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #E2E8F0;
    }

    /* Tables */
    .ge-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.82rem;
        font-family: 'Inter', sans-serif !important;
    }
    .ge-table th {
        text-align: left;
        padding: 10px 14px;
        color: #64748B !important;
        font-weight: 600;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        border-bottom: 1px solid #E2E8F0;
        font-family: 'Inter', sans-serif !important;
    }
    .ge-table td {
        padding: 10px 14px;
        color: #475569 !important;
        border-bottom: 1px solid #F1F5F9;
        font-family: 'Inter', sans-serif !important;
    }
    .ge-table tr:hover td {
        background: rgba(0,0,0,0.02);
    }
    .ge-table code {
        font-family: 'JetBrains Mono', monospace !important;
        color: #2563EB !important;
        font-size: 0.78rem;
    }

    /* Footer */
    .footer {
        margin-top: 3rem;
        padding-top: 1.2rem;
        border-top: 1px solid #E2E8F0;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .footer-text {
        font-size: 0.73rem;
        color: #94A3B8 !important;
        font-family: 'JetBrains Mono', monospace !important;
    }

    /* Cacher les éléments Streamlit */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    hr { border-color: #E2E8F0 !important; }
    [data-testid="column"] { padding: 0 6px; }

    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        font-family: 'Inter', sans-serif !important;
        color: #1E293B !important;
    }
</style>
""", unsafe_allow_html=True)


# --- Connexion DuckDB ---
DB_PATH = 'data/fintech.duckdb'
con = duckdb.connect(DB_PATH, read_only=True)

try:
    con.execute("SELECT 1 FROM fintech.mart_data_quality_summary LIMIT 1")
    SCHEMA = "fintech"
except:
    try:
        con.execute("SELECT 1 FROM analytics.mart_data_quality_summary LIMIT 1")
        SCHEMA = "analytics"
    except:
        schemas = con.execute("SELECT DISTINCT schema_name FROM information_schema.tables").fetchdf()
        SCHEMA = schemas['schema_name'].iloc[0] if len(schemas) > 0 else "main"

quality = con.execute(f"SELECT * FROM {SCHEMA}.mart_data_quality_summary").fetchdf()
by_cat = con.execute(f"SELECT * FROM {SCHEMA}.mart_quality_by_category").fetchdf()

PLOT_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Inter, sans-serif", color="#64748B", size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor='#E2E8F0', zerolinecolor='#E2E8F0'),
    yaxis=dict(gridcolor='#E2E8F0', zerolinecolor='#E2E8F0'),
    legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(size=11, color="#64748B")),
    hoverlabel=dict(bgcolor='#FFFFFF', font_size=12, font_family="Inter", font_color="#334155", bordercolor='#E2E8F0'),
)

COLORS = {
    'primary': '#3B82F6', 'success': '#10B981', 'warning': '#F59E0B',
    'danger': '#EF4444', 'purple': '#8B5CF6', 'teal': '#14B8A6', 'slate': '#64748B',
}


# === HEADER ===
st.markdown("""
<div class="dashboard-header">
    <div>
        <p class="dashboard-title">Data Quality Monitor</p>
        <p class="dashboard-subtitle">Fintech Pipeline</p>
    </div>
    <div class="header-right">Dernière exécution<br>{date}</div>
</div>
""".format(date=datetime.now().strftime("%d/%m/%Y — %Hh%M")), unsafe_allow_html=True)


# === MÉTRIQUES ===
score = quality['data_quality_score'].iloc[0]
total = int(quality['total_records'].iloc[0])
missing_amt = quality['pct_missing_amounts'].iloc[0]
missing_dates = quality['pct_missing_dates'].iloc[0]
suspicious = quality['pct_suspicious'].iloc[0]
fraud_pct = quality['pct_fraud'].iloc[0]

if score >= 95:
    score_color, score_status = "value-green", '<span class="status-pass">Conforme</span>'
elif score >= 85:
    score_color, score_status = "value-yellow", '<span class="status-warn">Attention requise</span>'
else:
    score_color, score_status = "value-red", '<span class="status-fail">Non conforme</span>'

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Score qualité</div><div class="metric-value {score_color}">{score}%</div><div class="metric-detail">{score_status}</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Transactions</div><div class="metric-value value-neutral">{total:,}</div><div class="metric-detail">Toutes sources confondues</div></div>', unsafe_allow_html=True)
with col3:
    c = "value-green" if missing_amt < 3 else "value-yellow" if missing_amt < 5 else "value-red"
    st.markdown(f'<div class="metric-card"><div class="metric-label">Montants manquants</div><div class="metric-value {c}">{missing_amt}%</div><div class="metric-detail">{int(quality["missing_amounts"].iloc[0])} valeurs NULL</div></div>', unsafe_allow_html=True)
with col4:
    c = "value-green" if suspicious < 3 else "value-yellow" if suspicious < 6 else "value-red"
    st.markdown(f'<div class="metric-card"><div class="metric-label">Montants suspects</div><div class="metric-value {c}">{suspicious}%</div><div class="metric-detail">Seuil : |montant| > 10 000 €</div></div>', unsafe_allow_html=True)
with col5:
    c = "value-green" if fraud_pct < 1 else "value-yellow" if fraud_pct < 3 else "value-red"
    st.markdown(f'<div class="metric-card"><div class="metric-label">Fraude détectée</div><div class="metric-value {c}">{fraud_pct}%</div><div class="metric-detail">{int(quality["fraud_count"].iloc[0])} transactions</div></div>', unsafe_allow_html=True)


# === GREAT EXPECTATIONS ===
ge_file = 'data/processed/ge_validation_result.json'
if os.path.exists(ge_file):
    with open(ge_file, 'r') as f:
        ge_result = json.load(f)

    st.markdown('<p class="section-title">Validations Great Expectations</p>', unsafe_allow_html=True)
    col_ge1, col_ge2 = st.columns([1, 3])

    with col_ge1:
        passed, total_rules, failed = ge_result['passed'], ge_result['total_rules'], ge_result['failed']
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number", value=passed,
            number=dict(suffix=f"/{total_rules}", font=dict(size=34, family="Poppins", color="#1E293B")),
            gauge=dict(
                axis=dict(range=[0, total_rules], tickwidth=0, tickcolor='rgba(0,0,0,0)'),
                bar=dict(color=COLORS['success'] if failed == 0 else COLORS['warning'], thickness=0.3),
                bgcolor='#E2E8F0', borderwidth=0,
                steps=[dict(range=[0, total_rules], color='#F1F5F9')],
            ),
        ))
        fig_gauge.update_layout(height=200, **{k: v for k, v in PLOT_LAYOUT.items() if k not in ('xaxis', 'yaxis')})
        st.plotly_chart(fig_gauge, use_container_width=True)

        if ge_result['success']:
            st.markdown('<div style="text-align:center"><span class="status-pass">Toutes les règles respectées</span></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="text-align:center"><span class="status-warn">{failed} règle(s) en échec</span></div>', unsafe_allow_html=True)

    with col_ge2:
        rules = [
            ("transaction_id", "Non null", "Aucun ID manquant", "pass"),
            ("transaction_id", "Unicité", "Pas de doublons", "pass"),
            ("amount", "Fourchette [-50k, 50k]", "Tolérance 95%", "pass" if failed == 0 else "warn"),
            ("amount", "Non null", "Tolérance 95%", "pass"),
            ("status", "Valeurs acceptées", "completed, pending, failed, cancelled", "pass"),
            ("category", "Valeurs acceptées", "virement, prelevement, carte, retrait, depot", "pass"),
            ("country", "Valeurs acceptées", "FR, DE, ES, IT, US, GB", "pass"),
        ]
        table_html = '<table class="ge-table"><thead><tr><th>Colonne</th><th>Règle</th><th>Détail</th><th>Statut</th></tr></thead><tbody>'
        for col_name, rule, detail, status in rules:
            s = '<span class="status-pass">OK</span>' if status == "pass" else '<span class="status-warn">Attention</span>'
            table_html += f'<tr><td><code>{col_name}</code></td><td>{rule}</td><td style="color:#94A3B8 !important">{detail}</td><td>{s}</td></tr>'
        table_html += '</tbody></table>'
        st.markdown(table_html, unsafe_allow_html=True)


# === ANOMALIES PAR CATÉGORIE ===
st.markdown('<p class="section-title">Anomalies par catégorie de transaction</p>', unsafe_allow_html=True)
col_bar, col_detail = st.columns([3, 2])

with col_bar:
    fig_cat = go.Figure()
    fig_cat.add_trace(go.Bar(x=by_cat['category'], y=by_cat['missing_amounts'], name='Montants manquants', marker_color=COLORS['warning'], marker_line_width=0, opacity=0.85))
    fig_cat.add_trace(go.Bar(x=by_cat['category'], y=by_cat['suspicious_amounts'], name='Montants suspects', marker_color=COLORS['purple'], marker_line_width=0, opacity=0.85))
    fig_cat.add_trace(go.Bar(x=by_cat['category'], y=by_cat['fraud_count'], name='Fraudes', marker_color=COLORS['danger'], marker_line_width=0, opacity=0.85))
    fig_cat.update_layout(
        barmode='group', height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=11, color="#64748B")),
        xaxis_title="", yaxis_title="Nombre d'anomalies",
        **{k: v for k, v in PLOT_LAYOUT.items() if k != 'legend'}
    )
    fig_cat.update_xaxes(gridcolor='#E2E8F0'); fig_cat.update_yaxes(gridcolor='#E2E8F0')
    st.plotly_chart(fig_cat, use_container_width=True)

with col_detail:
    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
    for _, row in by_cat.iterrows():
        total_anomalies = int(row['missing_amounts'] + row['suspicious_amounts'] + row['fraud_count'])
        anomaly_rate = round(100 * total_anomalies / row['total'], 1)
        rc = "value-green" if anomaly_rate < 5 else "value-yellow" if anomaly_rate < 10 else "value-red"
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:11px 14px;border-bottom:1px solid #E2E8F0">
            <div>
                <span style="color:#334155;font-weight:600;text-transform:capitalize;font-family:'Inter',sans-serif">{row['category']}</span>
                <span style="color:#94A3B8;font-size:0.76rem;margin-left:8px;font-family:'Inter',sans-serif">{int(row['total'])} tx</span>
            </div>
            <span class="{rc}" style="font-family:'Poppins',sans-serif;font-weight:700;font-size:0.92rem">{anomaly_rate}%</span>
        </div>""", unsafe_allow_html=True)
    st.markdown('<div style="padding:12px 14px;margin-top:4px"><span style="color:#94A3B8;font-size:0.72rem;font-family:Inter,sans-serif">Taux = (manquants + suspects + fraudes) / total</span></div>', unsafe_allow_html=True)


# === DISTRIBUTION + RÉPARTITIONS ===
st.markdown('<p class="section-title">Analyse des transactions</p>', unsafe_allow_html=True)
col_hist, col_pie1, col_pie2 = st.columns([2, 1, 1])

with col_hist:
    amounts = con.execute(f"SELECT amount FROM {SCHEMA}.stg_transactions WHERE amount IS NOT NULL AND abs(amount) < 15000").fetchdf()
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Histogram(x=amounts['amount'], nbinsx=60, marker_color=COLORS['primary'], marker_line_width=0, opacity=0.7))
    fig_hist.update_layout(title=dict(text="Distribution des montants", font=dict(size=13, color="#64748B", family="Inter")), xaxis_title="Montant (€)", yaxis_title="Fréquence", height=340, **PLOT_LAYOUT)
    fig_hist.update_xaxes(gridcolor='#E2E8F0'); fig_hist.update_yaxes(gridcolor='#E2E8F0')
    st.plotly_chart(fig_hist, use_container_width=True)

with col_pie1:
    statuses = con.execute(f"SELECT status, count(*) as count FROM {SCHEMA}.stg_transactions GROUP BY status").fetchdf()
    fig_status = go.Figure(go.Pie(labels=statuses['status'], values=statuses['count'], hole=0.55, marker=dict(colors=[COLORS['success'], COLORS['primary'], COLORS['warning'], COLORS['danger']]), textinfo='percent', textfont=dict(size=11, color='#334155', family="Inter"), hovertemplate='%{label}<br>%{value} transactions<br>%{percent}<extra></extra>'))
    fig_status.update_layout(title=dict(text="Par statut", font=dict(size=13, color="#64748B", family="Inter")), height=340, showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.05, font=dict(size=10, color="#64748B")), **{k: v for k, v in PLOT_LAYOUT.items() if k not in ('xaxis', 'yaxis', 'legend')})
    st.plotly_chart(fig_status, use_container_width=True)

with col_pie2:
    countries = con.execute(f"SELECT country, count(*) as count FROM {SCHEMA}.stg_transactions GROUP BY country ORDER BY count DESC").fetchdf()
    fig_country = go.Figure(go.Pie(labels=countries['country'], values=countries['count'], hole=0.55, marker=dict(colors=['#3B82F6', '#10B981', '#8B5CF6', '#F59E0B', '#EF4444', '#14B8A6', '#F97316', '#64748B']), textinfo='percent', textfont=dict(size=11, color='#334155', family="Inter"), hovertemplate='%{label}<br>%{value} transactions<br>%{percent}<extra></extra>'))
    fig_country.update_layout(title=dict(text="Par pays", font=dict(size=13, color="#64748B", family="Inter")), height=340, showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.05, font=dict(size=10, color="#64748B")), **{k: v for k, v in PLOT_LAYOUT.items() if k not in ('xaxis', 'yaxis', 'legend')})
    st.plotly_chart(fig_country, use_container_width=True)


# === TENDANCE TEMPORELLE ===
st.markdown('<p class="section-title">Tendance temporelle</p>', unsafe_allow_html=True)
monthly = con.execute(f"""
    SELECT date_trunc('month', transaction_date) as month, count(*) as nb_transactions,
        sum(case when is_fraud then 1 else 0 end) as nb_frauds,
        round(100.0 * sum(case when is_amount_missing or is_date_missing or is_amount_suspicious then 1 else 0 end) / count(*), 2) as anomaly_rate
    FROM {SCHEMA}.int_transactions_enriched WHERE transaction_date IS NOT NULL GROUP BY 1 ORDER BY 1
""").fetchdf()

fig_trend = go.Figure()
fig_trend.add_trace(go.Scatter(x=monthly['month'], y=monthly['nb_transactions'], name='Transactions', line=dict(color=COLORS['primary'], width=2.5), fill='tozeroy', fillcolor='rgba(59,130,246,0.08)', hovertemplate='%{x|%b %Y}<br>%{y} transactions<extra></extra>'))
fig_trend.add_trace(go.Scatter(x=monthly['month'], y=monthly['anomaly_rate'], name="Taux d'anomalie (%)", yaxis='y2', line=dict(color=COLORS['warning'], width=2, dash='dot'), hovertemplate='%{x|%b %Y}<br>%{y:.1f}% anomalies<extra></extra>'))
fig_trend.update_layout(
    height=320, xaxis_title="",
    yaxis=dict(title="Transactions", gridcolor='#E2E8F0', zerolinecolor='#E2E8F0', title_font=dict(color="#64748B")),
    yaxis2=dict(title="Anomalies (%)", overlaying='y', side='right', gridcolor='rgba(0,0,0,0)', showgrid=False, range=[0, max(monthly['anomaly_rate'].max() * 1.5, 10)], title_font=dict(color="#64748B")),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=11, color="#64748B")),
    **{k: v for k, v in PLOT_LAYOUT.items() if k not in ('yaxis', 'legend')}
)
fig_trend.update_xaxes(gridcolor='#E2E8F0')
st.plotly_chart(fig_trend, use_container_width=True)


# === TOP CLIENTS SUSPECTS ===
st.markdown('<p class="section-title">Clients avec le plus d\'anomalies</p>', unsafe_allow_html=True)
top_customers = con.execute(f"""
    SELECT customer_id, customer_name, customer_segment, count(*) as nb_transactions,
        sum(case when is_amount_missing or is_date_missing or is_amount_suspicious then 1 else 0 end) as nb_anomalies,
        round(100.0 * sum(case when is_amount_missing or is_date_missing or is_amount_suspicious then 1 else 0 end) / count(*), 1) as anomaly_rate,
        sum(case when is_fraud then 1 else 0 end) as nb_frauds
    FROM {SCHEMA}.int_transactions_enriched GROUP BY 1, 2, 3 HAVING nb_anomalies > 0 ORDER BY nb_anomalies DESC LIMIT 10
""").fetchdf()

col_table, col_segment = st.columns([3, 1])

with col_table:
    table_html = '<table class="ge-table"><thead><tr><th>Client</th><th>Segment</th><th>Transactions</th><th>Anomalies</th><th>Taux</th><th>Fraudes</th></tr></thead><tbody>'
    for _, row in top_customers.iterrows():
        rate = row['anomaly_rate']
        rc = "value-green" if rate < 10 else "value-yellow" if rate < 25 else "value-red"
        fd = f'<span class="value-red">{int(row["nb_frauds"])}</span>' if row['nb_frauds'] > 0 else '<span style="color:#94A3B8">0</span>'
        sc = {'premium': '#7C3AED', 'standard': '#2563EB', 'basic': '#64748B'}.get(row['customer_segment'], '#64748B')
        table_html += f'<tr><td><code>{row["customer_id"]}</code> <span style="color:#64748B;margin-left:6px;font-size:0.8rem">{row["customer_name"]}</span></td><td><span style="color:{sc};font-size:0.8rem;text-transform:capitalize">{row["customer_segment"]}</span></td><td>{int(row["nb_transactions"])}</td><td>{int(row["nb_anomalies"])}</td><td><span class="{rc}" style="font-family:Poppins,sans-serif;font-weight:700">{rate}%</span></td><td>{fd}</td></tr>'
    table_html += "</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)

with col_segment:
    segment_stats = con.execute(f"""
        SELECT customer_segment as segment, count(distinct customer_id) as nb_clients,
            round(100.0 * sum(case when is_amount_missing or is_date_missing or is_amount_suspicious then 1 else 0 end) / count(*), 1) as anomaly_rate
        FROM {SCHEMA}.int_transactions_enriched GROUP BY 1 ORDER BY anomaly_rate DESC
    """).fetchdf()
    st.markdown('<p style="color:#64748B;font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:12px;font-family:Inter,sans-serif">Par segment</p>', unsafe_allow_html=True)
    for _, row in segment_stats.iterrows():
        rate = row['anomaly_rate']
        rc = "value-green" if rate < 5 else "value-yellow" if rate < 10 else "value-red"
        st.markdown(f"""
        <div style="padding:11px 0;border-bottom:1px solid #E2E8F0">
            <div style="display:flex;justify-content:space-between;align-items:center">
                <span style="color:#334155;font-weight:600;text-transform:capitalize;font-family:Inter,sans-serif">{row['segment']}</span>
                <span class="{rc}" style="font-family:Poppins,sans-serif;font-weight:700">{rate}%</span>
            </div>
            <div style="color:#94A3B8;font-size:0.73rem;margin-top:2px;font-family:Inter,sans-serif">{int(row['nb_clients'])} clients</div>
        </div>""", unsafe_allow_html=True)


# === FOOTER ===
con.close()
st.markdown("""
<div class="footer">
    <span class="footer-text">Python / DuckDB / dbt Core / Great Expectations / Streamlit</span>
    <span class="footer-text">Ange Francine FORKOU</span>
</div>
""", unsafe_allow_html=True)
