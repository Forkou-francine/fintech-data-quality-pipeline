import pandas as pd
from faker import Faker
import random
import duckdb
from datetime import datetime, timedelta

fake = Faker('fr_FR')

def generate_transactions(n=10000):
    """Génère des transactions financières réalistes."""
    categories = ['virement', 'prélèvement', 'carte', 'retrait', 'dépôt']
    statuses = ['completed', 'pending', 'failed', 'cancelled']
    
    transactions = []
    for i in range(n):
        date = fake.date_time_between(start_date='-1y', end_date='now')
        amount = round(random.uniform(-5000, 10000), 2)
        
        # Introduire volontairement des anomalies pour tester la data quality
        if random.random() < 0.02:  # 2% de données problématiques
            amount = None  # montant manquant
        if random.random() < 0.01:
            date = None  # date manquante
        if random.random() < 0.03:
            amount = round(random.uniform(50000, 100000), 2)  # montant anormal
            
        transactions.append({
            'transaction_id': f'TXN-{i:06d}',
            'customer_id': f'CUST-{random.randint(1, 500):04d}',
            'transaction_date': date,
            'amount': amount,
            'category': random.choice(categories),
            'status': random.choice(statuses),
            'merchant': fake.company(),
            'country': random.choice(['FR', 'FR', 'FR', 'DE', 'ES', 'IT', 'US', 'GB']),
            'is_fraud': random.random() < 0.005  # 0.5% de fraude
        })
    
    return pd.DataFrame(transactions)

def generate_customers(n=500):
    """Génère une table clients."""
    customers = []
    for i in range(1, n + 1):
        customers.append({
            'customer_id': f'CUST-{i:04d}',
            'name': fake.name(),
            'email': fake.email(),
            'registration_date': fake.date_between(start_date='-3y', end_date='-30d'),
            'segment': random.choice(['premium', 'standard', 'basic']),
            'country': random.choice(['FR', 'DE', 'ES', 'IT'])
        })
    return pd.DataFrame(customers)

if __name__ == '__main__':
    # Générer les données
    df_transactions = generate_transactions(10000)
    df_customers = generate_customers(500)
    
    # Sauvegarder en CSV (source brute)
    df_transactions.to_csv('data/raw/transactions.csv', index=False)
    df_customers.to_csv('data/raw/customers.csv', index=False)
    
    # Charger dans DuckDB
    con = duckdb.connect('data/fintech.duckdb')
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.transactions AS SELECT * FROM df_transactions")
    con.execute("CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM df_customers")
    
    print(f"✅ {len(df_transactions)} transactions générées")
    print(f"✅ {len(df_customers)} clients générés")
    print(f"📊 Anomalies volontaires :")
    print(f"   - Montants NULL : {df_transactions['amount'].isna().sum()}")
    print(f"   - Dates NULL : {df_transactions['transaction_date'].isna().sum()}")
    print(f"   - Fraudes : {df_transactions['is_fraud'].sum()}")
    
    con.close()