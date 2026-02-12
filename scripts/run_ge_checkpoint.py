import great_expectations as gx
import duckdb
import pandas as pd

def run_validation():
    # Connexion à DuckDB
    con = duckdb.connect('data/fintech.duckdb')
    df = con.execute("SELECT * FROM analytics.int_transactions_enriched").fetchdf()
    con.close()
    
    # Créer le contexte GE
    context = gx.get_context()
    
    # Définir la source de données
    datasource = context.data_sources.add_pandas("transactions_source")
    data_asset = datasource.add_dataframe_asset("transactions")
    batch = data_asset.add_batch_definition_whole_dataframe("full_batch")
    
    # Créer une suite d'expectations
    suite = context.suites.add(
        gx.ExpectationSuite(name="transactions_quality_suite")
    )
    
    # Définir les expectations
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount", min_value=-50000, max_value=50000, mostly=0.95
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=['completed', 'pending', 'failed', 'cancelled']
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="amount", mostly=0.95
        )
    )
    
    # Exécuter la validation
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="transactions_validation",
            data=batch,
            suite=suite
        )
    )
    
    result = validation_definition.run(batch_parameters={"dataframe": df})
    
    if not result.success:
        print("⚠️ Certaines validations ont échoué !")
        for r in result.results:
            if not r.success:
                print(f"  ❌ {r.expectation_config.type}: {r.result}")
    else:
        print("✅ Toutes les validations passent !")
    
    return result.success

if __name__ == '__main__':
    run_validation()