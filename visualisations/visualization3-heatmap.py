import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO
from neo4j import GraphDatabase
from matplotlib.colors import LinearSegmentedColormap

# Neo4j connection
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(user, password), database="databasefinal")

def run_query(tx, query):
    return [record.data() for record in tx.run(query)]

get_data ='''
MATCH (t:Tweet)
Return t.sentiment_expected_value as sentiment_score, t.created_at as date , t.sentiment_label as sentiment_label'''

# Function to run the query
def run_query(tx, query):
    return [record.data() for record in tx.run(query)]

# Run the query and store the result in a DataFrame
with driver.session() as session:
    results = session.execute_read(run_query, get_data)

driver.close()

# Convert to DataFrame
df = pd.DataFrame(results)

# Convert sentiment_expected_value to float (if needed)
df["sentiment_score"] = df["sentiment_score"].astype(float)
scores = df["sentiment_score"].values

pivot_df = df.pivot_table(index='sentiment_label', columns='date', values='sentiment_score', aggfunc='mean')

# Define a diverging colormap for sentiment
cmap = LinearSegmentedColormap.from_list("red_green", ["red", "white", "green"])

plt.figure(figsize=(12, 6))
sns.heatmap(pivot_df, cmap=cmap, center=0, annot=True, fmt=".2f",
            cbar_kws={"label": "Average Sentiment Score"}, linewidths=0.5)

plt.title("Average Sentiment Score by Date and Sentiment Label")
plt.xlabel("Date")
plt.ylabel("Sentiment Label")
plt.tight_layout()
plt.show()


