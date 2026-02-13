import great_expectations as gx
import duckdb
import pandas as pd
import json
import os

def run_validation():
    """
    Lance les validations Great Expectations sur nos données transformées.
    On vérifie les données APRÈS transformation dbt (pas les données brutes).
    """
    
    print("🔄 Connexion à DuckDB...")
    con = duckdb.connect('data/fintech.duckdb', read_only=True)
    
    # On récupère les données de la table enrichie (celle créée par dbt)
    df = con.execute("SELECT * FROM fintech.int_transactions_enriched").fetchdf()
    con.close()
    print(f"   → {len(df)} lignes chargées")
    
    # --- Créer le contexte Great Expectations ---
    # Le contexte, c'est l'objet principal qui gère tout dans GE
    print("🔄 Initialisation de Great Expectations...")
    context = gx.get_context()
    
    # --- Connecter nos données ---
    # On dit à GE : "voilà un DataFrame pandas à valider"
    data_source = context.data_sources.add_pandas("fintech_source")
    data_asset = data_source.add_dataframe_asset("transactions_enriched")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
    
    # --- Définir les règles de qualité (= expectations) ---
    print("🔄 Définition des règles de qualité...")
    suite = context.suites.add(
        gx.ExpectationSuite(name="transactions_quality_suite")
    )
    
    # RÈGLE 1 : transaction_id ne doit JAMAIS être null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id")
    )
    
    # RÈGLE 2 : transaction_id doit être unique (pas de doublons)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
    )
    
    # RÈGLE 3 : les montants doivent être entre -50 000 et 50 000
    #           "mostly=0.95" signifie qu'on tolère 5% d'exceptions
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount",
            min_value=-50000,
            max_value=50000,
            mostly=0.95
        )
    )
    
    # RÈGLE 4 : le statut doit être une valeur connue
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=['completed', 'pending', 'failed', 'cancelled']
        )
    )
    
    # RÈGLE 5 : au moins 95% des montants doivent être remplis
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="amount",
            mostly=0.95
        )
    )
    
    # RÈGLE 6 : la catégorie doit être une valeur connue
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="category",
            value_set=['virement', 'prelevement', 'carte', 'retrait', 'depot']
        )
    )
    
    # RÈGLE 7 : le pays doit être un code de 2 lettres connu
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="country",
            value_set=['FR', 'DE', 'ES', 'IT', 'US', 'GB']
        )
    )
    
    # --- Lancer la validation ---
    print("🔄 Exécution des validations...")
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="transactions_validation",
            data=batch_definition,
            suite=suite
        )
    )
    
    result = validation_definition.run(batch_parameters={"dataframe": df})
    
    # --- Afficher les résultats ---
    print("\n" + "=" * 60)
    print("📊 RAPPORT DE QUALITÉ DES DONNÉES")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for r in result.results:
        # Récupérer le nom de la règle et la colonne
        expectation_type = r.expectation_config.type
        column = r.expectation_config.kwargs.get("column", "N/A")
        
        if r.success:
            print(f"  ✅ {expectation_type} ({column})")
            passed += 1
        else:
            print(f"  ❌ {expectation_type} ({column})")
            # Afficher le détail de l'échec
            if hasattr(r.result, 'unexpected_count'):
                print(f"     → {r.result['unexpected_count']} valeurs non conformes")
            failed += 1
    
    print(f"\n📈 Résultat : {passed} règles OK / {passed + failed} total")
    
    if result.success:
        print("🎉 TOUTES LES VALIDATIONS PASSENT !")
    else:
        print("⚠️  CERTAINES VALIDATIONS ONT ÉCHOUÉ — voir détails ci-dessus")
    
    # --- Sauvegarder le résultat en JSON (utile pour le dashboard plus tard) ---
    summary = {
        "validation_date": str(pd.Timestamp.now()),
        "total_rules": passed + failed,
        "passed": passed,
        "failed": failed,
        "success": result.success
    }
    
    os.makedirs('data/processed', exist_ok=True)
    with open('data/processed/ge_validation_result.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n💾 Résultat sauvegardé dans data/processed/ge_validation_result.json")
    
    return result.success


if __name__ == '__main__':
    run_validation()