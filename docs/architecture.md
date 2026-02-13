# Architecture du pipeline

## Vue d'ensemble
```text
┌─────────────────┐     ┌─────────────┐     ┌──────────────────────┐
│  Python/Faker   │────▶│  DuckDB     │────▶│  dbt Core            │
│  (génération)   │     │  (raw)      │     │  staging             │
└─────────────────┘     └─────────────┘     │  intermediate        │
                                            │  marts               │
                                            └──────────┬───────────┘
                                                       │
                              ┌─────────────────────────┼──────────────────┐
                              │                         │                  │
                              ▼                         ▼                  ▼
                    ┌──────────────────┐   ┌───────────────────┐  ┌──────────────┐
                    │ Great            │   │ dbt tests         │  │ Streamlit    │
                    │ Expectations     │   │ (unique, not_null │  │ Dashboard    │
                    │ (7 règles)       │   │  accepted_values) │  │ (6 vues)     │
                    └──────────────────┘   └───────────────────┘  └──────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Airflow          │
                    │ (orchestration   │
                    │  quotidienne)    │
                    └──────────────────┘
```

## Couches dbt

| Couche       | Rôle                           | Exemple                          |
|-------------|--------------------------------|----------------------------------|
| staging     | Nettoyage, typage, normalisation | stg_transactions, stg_customers |
| intermediate | Jointures, flags de qualité    | int_transactions_enriched        |
| marts       | Agrégations finales pour la BI | mart_data_quality_summary        |

## Technologies

| Outil              | Rôle                | Version |
|-------------------|---------------------|---------|
| Python            | Ingestion           | 3.11    |
| DuckDB            | Stockage            | 1.x     |
| dbt Core          | Transformation      | 1.7+    |
| Great Expectations | Data Quality       | 1.x     |
| Apache Airflow    | Orchestration       | 2.8     |
| Streamlit         | Visualisation       | 1.x     |