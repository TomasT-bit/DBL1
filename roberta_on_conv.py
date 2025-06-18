import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F
from tqdm import tqdm
import time
from neo4j import GraphDatabase
from transformers import pipeline
from datasets import Dataset
import math


device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device set to use {device}")

#Loads sentiment analysis model
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name).to(device)
model.eval()

#Loads the zero-shot classifier
zero_shot_classifier = pipeline(
    "zero-shot-classification",
    model="joeddav/xlm-roberta-large-xnli",
    device=0 ,
    use_fast=False,
    batch_size=16
)
candidate_labels = ["delayed flight", "lost baggage", "poor customer service", "ticket issue", "other", "cancelled flight", "uncomfortable flight", "trouble with refunds"]


#Connecting to Neo4j
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(user, password), database="databaseconversation")

print("Loading data from CSVs")
csv_path = r"C:\Users\o0dan\OneDrive\Desktop\DBL data challanges\conversations_from_neo4j.csv"
df = pd.read_csv(csv_path)

#We only take the start and end of conversations
df = df[df["relationship.positionType"].isin([1, 2])].copy()

grouped_df = (
    df.groupby(["conversation_node.id", "relationship.positionType"])
    .agg({"connected_node.text": lambda texts: " ".join(str(t) for t in texts)})
    .reset_index()
)
grouped_df.columns = ["conversation_id", "position_type", "text"]

print("Starting preprocessing")
grouped_df["clean_text"] = (
    grouped_df["text"]
    .fillna("")
    .str.replace(r'@\S+', '@user', regex=True)
    .str.replace(r'http\S+', 'http', regex=True)
)

texts = grouped_df["clean_text"].tolist()
expected_values = []
batch_size = 512


start_time = time.time()
print("Starting sentiment analysis")

with torch.no_grad():
    for i in tqdm(range(0, len(texts), batch_size), desc="Processing"):
        batch_texts = texts[i:i+batch_size]
        encoded = tokenizer(batch_texts, padding=True, truncation=True, max_length=128, return_tensors="pt")
        encoded = {k: v.to(device) for k, v in encoded.items()}

        outputs = model(**encoded)
        probs = F.softmax(outputs.logits, dim=1)

        for p in probs:
            prob_dict = {model.config.id2label[j]: p[j].item() for j in range(len(p))}
            expected_value = round(prob_dict["positive"] - prob_dict["negative"], 4)
            expected_values.append(expected_value)


grouped_df["sentiment_expected_value"] = expected_values


hf_dataset = Dataset.from_pandas(grouped_df[["clean_text"]])

#Applying zero-shot in batches
def classify_batch(batch):
    texts = batch["clean_text"]
    if isinstance(texts, str):
        texts = [texts]
    results = zero_shot_classifier(texts, candidate_labels, multi_label=False)
    if isinstance(results, dict):
        results = [results]
    return {"predicted_category": [r["labels"][0] for r in results]}


#Updating Neo4j with the new attributes
def update_conversations_batch(tx, batch_data):
    query = """
        UNWIND $rows AS row
        MATCH (c:Conversation)
        WHERE id(c) = row.conv_id
        WITH c, row
        SET c.start_sentiment = CASE WHEN row.position_type = 1 THEN row.sentiment ELSE c.start_sentiment END,
            c.end_sentiment = CASE WHEN row.position_type = 2 THEN row.sentiment ELSE c.end_sentiment END
        RETURN count(*) AS updated
"""

    tx.run(query, rows=batch_data)

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

print("Updating Neo4j in batches")
updates = grouped_df[["conversation_id", "position_type", "sentiment_expected_value"]].rename(
    columns={
        "conversation_id": "conv_id",
        "position_type": "position_type",
        "sentiment_expected_value": "sentiment"
    }
)
batch_size = 1000

with driver.session() as session:
    for batch in tqdm(chunks(updates.to_dict("records"), batch_size), total=math.ceil(len(updates) / batch_size)):
        session.execute_write(update_conversations_batch, batch)

print(f"Done! Processed {len(grouped_df)} conversations in {round(time.time() - start_time, 2)} seconds.")