import pandas as pd
from transformers import pipeline
from tqdm import tqdm
import os
import torch
import time

# --- Setup ---
# Set CUDA device
#os.environ["CUDA_VISIBLE_DEVICES"] = "0"

# Check if CUDA is available
#if torch.cuda.is_available():
 #   print(f"✅ CUDA is available. Using GPU: {torch.cuda.get_device_name(0)}")
#else:
 #   print("❌ CUDA is not available. Using CPU.")

# --- Load Sentiment Pipeline ---
print("🔁 Loading sentiment model...")
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    truncation=True,
    max_length=128,
    batch_size=32,  
    device=0 
)

# --- Preprocessing ---
def preprocess(text):
    new_text = []
    for t in str(text).split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

# --- Load Data ---
csv_path = r"C:\Users\o0dan\.Neo4jDesktop\relate-data\dbmss\dbms-1cfc0d33-1283-4972-bac5-2c9acd4a2855\import\tweets.csv"

print("📥 Loading data...")
df = pd.read_csv(csv_path)

# Filter English tweets
df = df[df["lang"] == "en"]

# Preprocess
df["clean_text"] = df["text"].apply(preprocess)

# --- Run Sentiment Analysis in Batches with Progress ---
print(f"🚀 Starting sentiment analysis on {len(df)} tweets...")
start_time = time.time()

results = []
batch_size = 32

for i in tqdm(range(0, len(df), batch_size), desc="🔍 Processing batches"):
    batch = df["clean_text"].iloc[i:i+batch_size].tolist()
    try:
        batch_results = sentiment_pipeline(batch)
        results.extend(batch_results)
    except Exception as e:
        print(f"❌ Batch {i}-{i+batch_size} failed: {e}")
        results.extend([{"label": "ERROR", "score": 0.0}] * len(batch))

# --- Save Results Back to CSV ---
df["sentiment_label"] = [r["label"] for r in results]
df["sentiment_score"] = [round(r["score"], 4) for r in results]

df.to_csv(csv_path, index=False)
print(f"✅ Done! Results saved to: {csv_path}")
print(f"⏱️ Total time: {round(time.time() - start_time, 2)} seconds")
