import pandas as pd
from transformers import pipeline
from neo4j import GraphDatabase
from tqdm import tqdm
import torch


CSV_FILE = r"C:\Users\o0dan\OneDrive\Desktop\DBL data challanges\conversations_from_neo4j.csv"
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(user, password), database="databaseconversation")

CANDIDATE_LABELS = [
    "delayed flight",
    "lost baggage",
    "poor customer service",
    "ticket issue",
    "other",
    "cancelled flight",
    "uncomfortable flight",
    "trouble with refunds",
    "discrimination"
]

# Load and preprocess CSV
print(" Loading CSV")
df = pd.read_csv(CSV_FILE)

print("Filtering tweets")
df_first_part = df[df["relationship.positionType"] == 1]

print("Grouping tweets by conversation_node.id")
grouped = df_first_part.groupby("conversation_node.id")["connected_node.text"] \
    .apply(lambda texts: " ".join(texts)).reset_index()
grouped.rename(columns={"connected_node.text": "combined_text"}, inplace=True)

print(f"Total conversations to classify: {len(grouped)}")


#Load zero-shot classifier

device = 0 if torch.cuda.is_available() else -1
print(f"🤖 Loading zero-shot classifier on device {device}...")
zero_shot_classifier = pipeline(
    "zero-shot-classification",
    model="joeddav/xlm-roberta-large-xnli",
    device=device,
    use_fast=False,
    batch_size=16
)

# Classify and get top label
print("Running zero-shot classification")
top_labels = []
batch_size = 16

for i in tqdm(range(0, len(grouped), batch_size), desc="Classifying", unit="batch"):
    batch_texts = grouped["combined_text"].iloc[i:i+batch_size].tolist()
    results = zero_shot_classifier(batch_texts, candidate_labels=CANDIDATE_LABELS, multi_label=False)
    if isinstance(results, dict):
        results = [results]
    top_labels.extend([res["labels"][0] for res in results])

grouped["top_label"] = top_labels

# Write classification back to Neo4j
print("Writing classification results back to Neo4j")

update_query = """
MATCH (c:Conversation)
WHERE id(c) = $conversation_node_id
SET c.top_label = $top_label
"""

with driver.session() as session:
    for _, row in tqdm(grouped.iterrows(), total=len(grouped), desc="Updating Neo4j", unit="conversation"):
        session.run(update_query, {
            "conversation_node_id": int(row["conversation_node.id"]),
            "top_label": row["top_label"]
        })
driver.close()

print("All done! Classification labels saved to Neo4j.")
