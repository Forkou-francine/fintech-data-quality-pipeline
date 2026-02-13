"""
DAG Airflow : Pipeline de qualité des données Fintech
Exécute quotidiennement :
  1. Génération de données synthétiques
  2. Transformation dbt (staging → intermediate → marts)
  3. Tests dbt
  4. Validation Great Expectations
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- Configuration par défaut de chaque tâche ---
default_args = {
    'owner': 'ange',                          # Qui a créé ce DAG
    'depends_on_past': False,                  # Chaque exécution est indépendante
    'email_on_failure': False,                 # Pas d'email si ça plante
    'retries': 1,                              # Si une tâche échoue, réessayer 1 fois
    'retry_delay': timedelta(minutes=5),       # Attendre 5 min avant de réessayer
}

# --- Définition du DAG ---
# "with DAG(...) as dag" crée le contexte dans lequel on définit nos tâches
with DAG(
    dag_id='fintech_data_quality_pipeline',    # Nom unique du DAG
    default_args=default_args,
    description='Pipeline de qualité des données fintech',
    schedule_interval='@daily',                 # S'exécute une fois par jour
    start_date=datetime(2025, 1, 1),           # Date de début (historique)
    catchup=False,                              # Ne pas rattraper les jours passés
    tags=['fintech', 'data-quality'],          # Tags pour filtrer dans l'UI
) as dag:

    # --- TÂCHE 1 : Générer les données ---
    # BashOperator = exécute une commande bash (terminal)
    generate_data = BashOperator(
        task_id='generate_synthetic_data',
        bash_command='cd /opt/airflow && python scripts/generate_data.py',
    )

    # --- TÂCHE 2 : Exécuter dbt run ---
    # Ça lance tous les modèles SQL (staging → intermediate → marts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt run --profiles-dir /opt/airflow/dbt_project'
        ),
    )

    # --- TÂCHE 3 : Exécuter dbt test ---
    # Vérifie les règles définies dans les fichiers .yml
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt test --profiles-dir /opt/airflow/dbt_project'
        ),
    )

    # --- TÂCHE 4 : Validation Great Expectations ---
    ge_validation = BashOperator(
        task_id='great_expectations_validation',
        bash_command='cd /opt/airflow && python scripts/run_ge_validation.py',
    )

    # --- ORDRE D'EXÉCUTION ---
    # >> signifie "puis". Ça crée le graphe :
    # generate_data → dbt_run → dbt_test → ge_validation
    generate_data >> dbt_run >> dbt_test >> ge_validation