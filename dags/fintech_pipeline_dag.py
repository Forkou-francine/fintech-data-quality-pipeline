from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ange',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fintech_data_quality_pipeline',
    default_args=default_args,
    description='Pipeline de qualité des données fintech',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fintech', 'data-quality'],
) as dag:

    # Task 1 : Générer / Ingérer les données
    generate_data = BashOperator(
        task_id='generate_synthetic_data',
        bash_command='cd /opt/airflow && python scripts/generate_data.py',
    )

    # Task 2 : Exécuter dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    # Task 3 : Exécuter dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    )

    # Task 4 : Great Expectations validation
    run_ge_validation = BashOperator(
        task_id='great_expectations_validation',
        bash_command='cd /opt/airflow && python scripts/run_ge_checkpoint.py',
    )

    # Dépendances
    generate_data >> dbt_run >> dbt_test >> run_ge_validation