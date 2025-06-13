from neo4j import GraphDatabase
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DB = "final"
AMERICAN_AIR_ID = 22536055
EXPORT_DIR = "export"

os.makedirs(EXPORT_DIR, exist_ok=True)

query = """
MATCH (c:Conversation)
WHERE c.top_label IS NOT NULL 
  AND c.start_sentiment IS NOT NULL 
  AND c.end_sentiment IS NOT NULL 
  AND c.airlineId IS NOT NULL
RETURN 
  c.top_label AS top_label,
  c.start_sentiment AS start_sentiment,
  c.end_sentiment AS end_sentiment,
  c.airlineId AS airlineId
"""

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
    database=NEO4J_DB
)

def fetch_data(tx):
    return tx.run(query).data()

with driver.session() as session:
    records = session.execute_read(fetch_data)

df = pd.DataFrame(records)

df["start_sentiment"] = pd.to_numeric(df["start_sentiment"], errors="coerce")
df["end_sentiment"] = pd.to_numeric(df["end_sentiment"], errors="coerce")
df = df.dropna(subset=["start_sentiment", "end_sentiment", "top_label", "airlineId"])
df["sentiment_change"] = df["end_sentiment"] - df["start_sentiment"]
df["group"] = df["airlineId"].apply(lambda x: "AmericanAir" if int(x) == AMERICAN_AIR_ID else "Others")

plt.figure(figsize=(18, 9))
sns.boxplot(
    data=df,
    x="top_label",
    y="sentiment_change",
    hue="group",
    palette="PuOr",
    fliersize=2
)
plt.title("Distribution of Sentiment Change per Complaint Category")
plt.xlabel("Complaint Category")
plt.ylabel("Sentiment Change")
plt.axhline(0, color="black", linestyle="--", linewidth=0.8)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(f"{EXPORT_DIR}/boxplot.png")
plt.show()
