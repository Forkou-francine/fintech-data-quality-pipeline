# 📊 Pipeline de qualité des données — Fintech

Un pipeline end-to-end qui ingère des transactions financières simulées,
les transforme avec dbt, valide leur qualité avec Great Expectations,
orchestre le tout avec Airflow, et expose un dashboard de monitoring.

## 🏗️ Architecture

<!-- Colle ici une capture d'écran de ton schéma ou du DAG Airflow -->
```text
Faker (Python) → DuckDB → dbt → Great Expectations → Streamlit
                                        ↑
                                   Airflow (daily)
```

## 🛠️ Stack technique

- **Ingestion** : Python + Faker (données synthétiques)
- **Stockage** : DuckDB
- **Transformation** : dbt Core (staging → intermediate → marts)
- **Qualité** : Great Expectations (7 règles) + dbt tests
- **Orchestration** : Apache Airflow
- **Visualisation** : Streamlit + Plotly

## 🚀 Installation et lancement

### Pré-requis
- Python 3.11+
- Docker Desktop (pour Airflow)

### Lancer le pipeline manuellement
```bash
# 1. Cloner le repo
git clone https://github.com/ton-username/fintech-data-quality-pipeline.git
cd fintech-data-quality-pipeline

# 2. Créer et activer l'environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# .\venv\Scripts\Activate  # Windows

# 3. Installer les dépendances
pip install duckdb dbt-duckdb pandas faker streamlit plotly great-expectations

# 4. Générer les données
python scripts/generate_data.py

# 5. Exécuter dbt
cd dbt_project && dbt run && dbt test && cd ..

# 6. Lancer la validation
python scripts/run_ge_validation.py

# 7. Voir le dashboard
streamlit run app/dashboard.py
```

### Lancer avec Airflow (orchestration automatique)
```bash
docker-compose up airflow-init
docker-compose up airflow-webserver airflow-scheduler
# Ouvrir http://localhost:8080 (admin/admin)
```

## 📈 Dashboard

<!-- INSÈRE ICI TES CAPTURES D'ÉCRAN -->

Le dashboard affiche :
- Score global de qualité des données
- Résultats des validations Great Expectations
- Anomalies par catégorie de transaction
- Distribution des montants
- Répartition par statut et par pays
- Évolution temporelle du volume

## 🧪 Data Quality : ce qui est vérifié

### Tests dbt
- Unicité des IDs (transactions, clients)
- Pas de NULL sur les champs critiques
- Valeurs acceptées pour statut et catégorie

### Great Expectations
- transaction_id : non null + unique
- amount : entre -50k et 50k (tolérance 5%)
- amount : non null à 95%+
- status : valeurs connues uniquement
- category : valeurs connues uniquement
- country : codes pays valides

## 📁 Structure du projet
```
├── dags/                        # DAG Airflow
│   └── fintech_pipeline_dag.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/             # Nettoyage
│   │   ├── intermediate/        # Logique métier
│   │   └── marts/               # Tables finales
│   └── dbt_project.yml
├── scripts/
│   ├── generate_data.py         # Génération de données
│   └── run_ge_validation.py     # Validation GE
├── app/
│   └── dashboard.py             # Dashboard Streamlit
├── docs/
│   └── architecture.md
├── docker-compose.yml           # Airflow
└── README.md
```

## 💡 Ce que j'ai appris

- Structurer un projet dbt avec les conventions staging/intermediate/marts
- Écrire des tests de data quality déclaratifs (dbt) et programmatiques (GE)
- Orchestrer un pipeline avec Airflow via Docker
- Concevoir un dashboard de monitoring orienté data quality

## 👤 Auteure

**Ange Francine FORKOU** — Data Engineer & BI Developer