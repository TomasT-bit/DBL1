import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F
from tqdm import tqdm
from neo4j import GraphDatabase

# --- Load model & tokenizer ---
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
model.eval()

print(f"📦 Model loaded on device: {device.upper()}")

# --- Preprocessing function ---
def preprocess(text):
    if pd.isna(text):
        return ""
    words = text.split(" ")
    words = ['@user' if w.startswith('@') else 'http' if w.startswith('http') else w for w in words]
    return " ".join(words)

# --- Neo4j connection ---
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(user, password))

def fetch_tweets(tx):
    query = "MATCH (t:Tweet) RETURN id(t) as id, t.text as text"
    return list(tx.run(query))

def update_sentiment(tx, tweet_id, result):
    query = """
    MATCH (t)
    WHERE id(t) = $id
    SET t.sentiment_positive = $pos,
        t.sentiment_neutral = $neu,
        t.sentiment_negative = $neg,
        t.sentiment_expected_value = $ev,
        t.sentiment_label = $label
    """
    tx.run(query, id=tweet_id,
           pos=result["positive"],
           neu=result["neutral"],
           neg=result["negative"],
           ev=result["expected_value"],
           label=result["label"])

# --- Run Everything ---
batch_size = 512

with driver.session(database = "databasetest") as session:
    # Step 1: Fetch data
    tweets = session.execute_read(fetch_tweets)
    texts = [preprocess(t["text"]) for t in tweets]

    # Step 2: Sentiment Analysis
    all_results = []

    with torch.no_grad():
        for i in tqdm(range(0, len(texts), batch_size), desc="🔍 Processing"):
            batch_texts = texts[i:i+batch_size]
            encoded = tokenizer(batch_texts, padding=True, truncation=True, max_length=128, return_tensors="pt")
            encoded = {k: v.to(device) for k, v in encoded.items()}

            outputs = model(**encoded)
            probs = F.softmax(outputs.logits, dim=1)

            for p in probs:
                prob_dict = {model.config.id2label[i]: round(p[i].item(), 4) for i in range(len(p))}
                expected_value = round(prob_dict["positive"] - prob_dict["negative"], 4)
                sentiment_label = max(prob_dict, key=prob_dict.get)
                all_results.append({
                    "expected_value": expected_value,
                    "label": sentiment_label
                })

    # Step 3: Update Neo4j
    for tweet, result in tqdm(zip(tweets, all_results), total=len(tweets), desc="📝 Updating Neo4j"):
        session.execute_write(update_sentiment, tweet["id"], result)

print("✅ Sentiment analysis and database update complete.")
