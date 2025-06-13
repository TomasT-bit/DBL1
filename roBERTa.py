import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F
from tqdm import tqdm
import time

#Load model and tokenizer

model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to("cuda" if torch.cuda.is_available() else "cpu")
model.eval()

#Preprocessing the text
def preprocess(text):
    if pd.isna(text):
        return ""
    words = text.split(" ")
    words = ['@user' if w.startswith('@') else 'http' if w.startswith('http') else w for w in words]
    return " ".join(words)

#Loading the data from the csv
csv_path = r"C:\Users\o0dan\.Neo4jDesktop\relate-data\dbmss\dbms-1cfc0d33-1283-4972-bac5-2c9acd4a2855\import\tweets.csv"
df = pd.read_csv(csv_path)
df["clean_text"] = df["text"].apply(preprocess)

batch_size = 512
texts = df["clean_text"].tolist()

sentiment_labels = []
sentiment_scores = []
expected_values = []

start_time = time.time()

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
            label = max(prob_dict, key=prob_dict.get)
            expected_values.append(expected_value)
            sentiment_labels.append(label)

#Saves results Back to csv
df["sentiment_expected_value"] = expected_values
df["sentiment_label"] = sentiment_labels

# Drop clean_text because we dont use it in the database
df.drop(columns=["clean_text"], inplace=True)

df.to_csv(csv_path, index=False)

print(f"Total time: {round(time.time() - start_time, 2)} seconds")