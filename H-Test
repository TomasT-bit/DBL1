from neo4j import GraphDatabase
import pandas as pd
from scipy.stats import kruskal
from statsmodels.stats.multitest import multipletests
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Configuration
CONFIG = {
    "neo4j_uri": "bolt://localhost:7687",
    "neo4j_user": "neo4j",
    "neo4j_password": "password",
    "neo4j_db": "twitter",
    "export_dir": "results_kruskal_only",
    "min_samples": 5,
    "airline_id": 22536055  # American Airlines
}

os.makedirs(CONFIG['export_dir'], exist_ok=True)

def fetch_data():
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
        RETURN 
          c.top_label AS issue,
          toFloat(c.start_sentiment) AS start,
          toFloat(c.end_sentiment) AS end,
          toInteger(c.airlineId) AS airline_id
        """
        
        with driver.session(database=CONFIG["neo4j_db"]) as session:
            result = session.run(query)
            df = pd.DataFrame([dict(record) for record in result])
        
        return df
    
    except Exception as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    
    finally:
        if 'driver' in locals():
            driver.close()

def analyze_kruskal(df):
    """Perform Kruskal-Wallis test on American Airlines vs Other Airlines per issue"""
    if df.empty:
        print("No data to analyze.")
        return pd.DataFrame(), df

    df['change'] = df['end'] - df['start']
    df['group'] = df['airline_id'].apply(
        lambda x: 'American Airlines' if x == CONFIG['airline_id'] else 'Other Airlines'
    )

    results = []

    for issue in df['issue'].unique():
        subset = df[df['issue'] == issue]
        groups = subset.groupby('group')

        group_values = [group['change'].values for name, group in groups if len(group) >= CONFIG['min_samples']]

        if len(group_values) == 2:
            try:
                h_stat, p_val = kruskal(*group_values)
                results.append({
                    'issue': issue,
                    'kruskal_h': h_stat,
                    'kruskal_p': p_val,
                })
            except Exception as e:
                print(f"Kruskal-Wallis error for {issue}: {e}")

    kruskal_df = pd.DataFrame(results)

    if not kruskal_df.empty:
        corrected = multipletests(kruskal_df['kruskal_p'], method='holm')
        kruskal_df['kruskal_p_adj'] = corrected[1]
        kruskal_df['kruskal_significant'] = kruskal_df['kruskal_p_adj'] < 0.05

    return kruskal_df, df

def visualize_kruskal(kruskal_df, df):
    """Visualize significant issues with Kruskal-Wallis test"""
    viz_dir = os.path.join(CONFIG['export_dir'], 'plots')
    os.makedirs(viz_dir, exist_ok=True)

    sig_issues = kruskal_df[kruskal_df['kruskal_significant']]

    for _, row in sig_issues.iterrows():
        issue = row['issue']
        subset = df[df['issue'] == issue]

        plt.figure(figsize=(10, 6))
        sns.boxplot(data=subset, x='group', y='change', palette='Set2')
        plt.title(f"{issue}\nKruskal-Wallis p={row['kruskal_p_adj']:.3g}")
        plt.xlabel("Airline Group")
        plt.ylabel("Sentiment Change")
        plt.tight_layout()

        safe_name = "".join(c for c in issue if c.isalnum() or c in " _-")
        plt.savefig(os.path.join(viz_dir, f"{safe_name[:50]}.png"), dpi=150)
        plt.close()

    if not sig_issues.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=sig_issues.sort_values('kruskal_p_adj'),
            x='kruskal_h', y='issue', palette='coolwarm'
        )
        plt.title("Significant Issues (Kruskal-Wallis)")
        plt.xlabel("Kruskal-Wallis H Statistic")
        plt.ylabel("Issue")
        plt.tight_layout()
        plt.savefig(os.path.join(CONFIG['export_dir'], 'kruskal_summary.png'), dpi=150)
        plt.close()

def main():
    print("Running Kruskal-Wallis analysis...")
    
    df = fetch_data()
    kruskal_df, df_cleaned = analyze_kruskal(df)

    if not kruskal_df.empty:
        kruskal_df.to_csv(os.path.join(CONFIG['export_dir'], 'kruskal_results.csv'), index=False)
        df_cleaned.to_csv(os.path.join(CONFIG['export_dir'], 'cleaned_data.csv'), index=False)
        visualize_kruskal(kruskal_df, df_cleaned)

        print(f"\n✅ Kruskal-Wallis test complete. Results saved to {CONFIG['export_dir']}")
        print(f"🔍 Significant issues: {kruskal_df['kruskal_significant'].sum()} out of {len(kruskal_df)}")
    else:
        print("No significant Kruskal-Wallis results.")

if __name__ == "__main__":
    main()
