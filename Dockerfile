FROM python:3.11-slim

RUN pip install --no-cache-dir \
    duckdb \
    dbt-duckdb \
    great-expectations \
    pandas \
    faker \
    streamlit \
    requests

WORKDIR /app