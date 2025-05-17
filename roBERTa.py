import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F
from tqdm import tqdm
import time

# --- Load model & tokenizer once ---
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to("cuda" if torch.cuda.is_available() else "cpu")
model.eval()

print(f"📦 Model loaded on device: {device.upper()}")

# --- Preprocessing ---
def preprocess(text):
    if pd.isna(text):
        return ""
    words = text.split(" ")
    words = ['@user' if w.startswith('@') else 'http' if w.startswith('http') else w for w in words]
    return " ".join(words)

# --- Load data ---
csv_path = r"C:\Users\o0dan\.Neo4jDesktop\relate-data\dbmss\dbms-1cfc0d33-1283-4972-bac5-2c9acd4a2855\import\tweets.csv"
df = pd.read_csv(csv_path)

df["clean_text"] = df["text"].apply(preprocess)

# --- Inference ---
batch_size = 512
texts = df["clean_text"].tolist()

sentiment_labels = []
sentiment_scores = []

start_time = time.time()
print(f"🚀 Starting sentiment analysis on {len(texts)} tweets...")

with torch.no_grad():
    for i in tqdm(range(0, len(texts), batch_size), desc="🔍 Processing"):
        batch_texts = texts[i:i+batch_size]
        encoded = tokenizer(batch_texts, padding=True, truncation=True, max_length=128, return_tensors="pt")
        encoded = {k: v.to(device) for k, v in encoded.items()}

        outputs = model(**encoded)
        probs = F.softmax(outputs.logits, dim=1)
        scores, preds = torch.max(probs, dim=1)

        for score, pred in zip(scores, preds):
            label = model.config.id2label[pred.item()]
            sentiment_labels.append(label)
            sentiment_scores.append(round(score.item(), 4))

# --- Save Results Back to CSV ---
df["sentiment_label"] = sentiment_labels
df["sentiment_score"] = sentiment_scores


# Drop intermediate column
df.drop(columns=["clean_text"], inplace=True)

df.to_csv(csv_path, index=False)
print(f"✅ Done! Results saved to: {csv_path}")
print(f"⏱️ Total time: {round(time.time() - start_time, 2)} seconds")

