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
    "neo4j_db": "twitterconversations",
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

def analyze_kruskal(df):
    """Perform Kruskal-Wallis test on American Airlines vs Other Airlines per issue,
    plus aggregate all isssues"""
    if df.empty:
        print("No data to analyze.")
        return pd.DataFrame(), df

    df['change'] = df['end'] - df['start']
    df['group'] = df['airline_id'].apply(
        lambda x: 'American Airlines' if x == CONFIG['airline_id'] else 'Other Airlines'
    )

    results = []

    # 1. Analyze each individual issue 
    unique_issues = [issue for issue in df['issue'].unique()]
    for issue in unique_issues:
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

    # 2. Aggregate all issues except "Other" into one combined "Other" group for analysis
    other_subset = df[~df['issue'].isin(unique_issues)]
    if not other_subset.empty:
        groups = other_subset.groupby('group')
        group_values = [group['change'].values for name, group in groups if len(group) >= CONFIG['min_samples']]

        if len(group_values) == 2:
            try:
                h_stat, p_val = kruskal(*group_values)
                results.append({
                    'issue': 'Other (combined)',
                    'kruskal_h': h_stat,
                    'kruskal_p': p_val,
                })
            except Exception as e:
                print(f"Kruskal-Wallis error for combined Other: {e}")

    kruskal_df = pd.DataFrame(results)

    if not kruskal_df.empty:
        corrected = multipletests(kruskal_df['kruskal_p'], method='holm')
        kruskal_df['kruskal_p_adj'] = corrected[1]
        kruskal_df['kruskal_significant'] = kruskal_df['kruskal_p_adj'] < 0.05

    return kruskal_df, df

def visualize_kruskal(kruskal_df, df):

    sig_issues = kruskal_df[kruskal_df['kruskal_significant']]
    sig_issues = sig_issues[~sig_issues['issue'].str.contains('Other', case=False)]

    if sig_issues.empty:
        print("No significant issues to plot.")
        return

    filtered_df = df[df['issue'].isin(sig_issues['issue'])]

    # Set the order of 'group' so American Airlines comes first
    filtered_df['group'] = pd.Categorical(filtered_df['group'],
                                          categories=['American Airlines', 'Other Airlines'],
                                          ordered=True)

    plt.figure(figsize=(14, 7))
    ax = sns.violinplot(data=filtered_df, x='issue', y='change', hue='group', palette='Set2', split=False)

    plt.xlabel("Issue")
    plt.ylabel("Sentiment Change")
    plt.title("Sentiment Change by Issue and Airline Group (Significant Issues)")

    ax.set_ylim(-2.5, 2.5)

    ymin, ymax = ax.get_ylim()
    yrange = ymax - ymin

    for i, issue in enumerate(sig_issues['issue']):
        pval = sig_issues[sig_issues['issue'] == issue]['kruskal_p_adj'].values[0]
        ax.text(i, ymin - 0.07 * yrange, f"p = {pval:.3g}",
                ha='center', va='top', fontsize=10, color='black')

    # Move legend outside the plot (right side)
    plt.legend(title='Airline Group', loc='center left', bbox_to_anchor=(1, 0.5))

    plt.tight_layout()
    plt.savefig(os.path.join(CONFIG["export_dir"], "Issues violin.pdf"))
    plt.show
    plt.close()


def main(start,end):
    print("Running Kruskal-Wallis analysis...")
    
    df = fetch_data(start,end)
    kruskal_df, df_cleaned = analyze_kruskal(df)

    if not kruskal_df.empty:
        visualize_kruskal(kruskal_df, df_cleaned)

        print(f"Kruskal-Wallis test complete. Results saved to {CONFIG['export_dir']}")
        print(f"Significant issues: {kruskal_df['kruskal_significant'].sum()} out of {len(kruskal_df)}")
    else:
        print("No significant Kruskal-Wallis results.")
