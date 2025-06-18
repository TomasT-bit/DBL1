import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F

# Load pre-trained model and tokenizer from Hugging Face
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Set device to GPU if available (otherwise use CPU)
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
model.eval()

# Preprocessing tweets
def preprocess(text):
    words = text.split(" ")
    words = ['@user' if w.startswith('@') else 'http' if w.startswith('http') else w for w in words]
    return " ".join(words)

# Run sentiment analysis on a single tweet
def get_sentiment(text):
    text = preprocess(text)
    encoded = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=128).to(device)
    with torch.no_grad():
        output = model(**encoded)
        probs = F.softmax(output.logits, dim=1)[0]
        prob_dict = {model.config.id2label[i]: probs[i].item() for i in range(len(probs))}
        expected_value = round(prob_dict["positive"] - prob_dict["negative"], 4)
        label = max(prob_dict, key=prob_dict.get)
    return label, expected_value

# Run sentiment analysis on a list of tweets
def get_sentiment_batch(texts, batch_size=64):
    preprocessed = [preprocess(text) for text in texts]
    encoded = tokenizer(preprocessed, return_tensors="pt", truncation=True, padding=True, max_length=128).to(device)

    with torch.no_grad():
        outputs = model(**encoded)
        probs = F.softmax(outputs.logits, dim=1)

    results = []
    for prob in probs:
        prob_dict = {model.config.id2label[i]: prob[i].item() for i in range(len(prob))}
        expected_value = round(prob_dict["positive"] - prob_dict["negative"], 4)
        label = max(prob_dict, key=prob_dict.get)
        results.append((label, expected_value))
    return results
