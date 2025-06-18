from neo4j import GraphDatabase
import pandas as pd
from scipy.stats import kruskal
from statsmodels.stats.multitest import multipletests
import scikit_posthocs as sp
import os

# Configuration
CONFIG = {
    "neo4j_uri": "bolt://localhost:7687",
    "neo4j_user": "neo4j",
    "neo4j_password": "password",
    "neo4j_db": "database1",
    "export_dir": "visualisations",
    "min_samples": 5,
    "airline_id": 22536055  # American Airlines
}

os.makedirs(CONFIG['export_dir'], exist_ok=True)

def fetch_data(iso_start, iso_end):
    """Fetch conversation data from Neo4j"""
    try:
        driver = GraphDatabase.driver(
            CONFIG["neo4j_uri"],
            auth=(CONFIG["neo4j_user"], CONFIG["neo4j_password"])
        )

        query = """
        MATCH (c:Conversation)
        WHERE c.top_label IS NOT NULL 
          AND c.start_sentiment IS NOT NULL 
          AND c.end_sentiment IS NOT NULL
          AND c.airlineId IS NOT NULL
          AND c.start <= datetime($iso_end) 
          AND c.end >= datetime($iso_start)
        RETURN 
          c.top_label AS issue,
          toFloat(c.start_sentiment) AS start,
          toFloat(c.end_sentiment) AS end,
          toInteger(c.airlineId) AS airline_id
        """

        with driver.session(database=CONFIG["neo4j_db"]) as session:
            result = session.run(query,iso_start=iso_start, iso_end=iso_end)
            df = pd.DataFrame([dict(record) for record in result])

        return df

    except Exception as e:
        print(f"Database error: {e}")
        return pd.DataFrame()

    finally:
        if 'driver' in locals():
            driver.close()

def analyze_american_airlines_only(df):
    """Kruskal-Wallis test across issue types for American Airlines only"""
    df['change'] = df['end'] - df['start']

    # Filter for American Airlines only
    aa_df = df[df['airline_id'] == CONFIG['airline_id']].copy()

    # Filter issues with enough data
    filtered = aa_df.groupby('issue').filter(lambda x: len(x) >= CONFIG['min_samples'])
    grouped = filtered.groupby('issue')['change'].apply(list)

    if len(grouped) < 2:
        print("Not enough issues with sufficient data.")
        return pd.DataFrame()

    # Kruskal-Wallis test
    h_stat, p_val = kruskal(*grouped.values)

    result = {
        'kruskal_h': h_stat,
        'kruskal_p': p_val,
        'n_issues': len(grouped)
    }

    # Save result summary
    results_df = pd.DataFrame([result])
    results_path = os.path.join(CONFIG['export_dir'], 'kruskal_american_airlines.csv')
    results_df.to_csv(results_path, index=False)
    print(f"Kruskal-Wallis H={h_stat:.3f}, p={p_val:.4f} on {len(grouped)} issues.")
    print(f"Summary saved to: {results_path}")

    #Post-hoc Dunn test
    posthoc_df = sp.posthoc_dunn(filtered, val_col='change', group_col='issue', p_adjust='holm')
    posthoc_path = os.path.join(CONFIG['export_dir'], 'posthoc_american_airlines.csv')
    posthoc_df.to_csv(posthoc_path)
    print(f"Post-hoc results saved to: {posthoc_path}")

    return results_df

def main(start,end):
    print("Fetching data from Neo4j...")
    df = fetch_data(start,end)
    if df.empty:
        print("No data fetched. Exiting.")
        return

    print("Running Kruskal-Wallis analysis for American Airlines only...")
    results = analyze_american_airlines_only(df)

    if not results.empty:
        print("Analysis done")
    else:
        print("Error")


