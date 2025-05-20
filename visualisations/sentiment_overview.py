import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Load the data
df = pd.read_csv("import/tweets.csv")

# Step 2: Calculate mean sentiment score
mean_score = df["sentiment_expected_value"].mean()
print(f"Mean Sentiment Score: {round(mean_score, 3)}")

# Step 3: Calculate % of each sentiment label
sentiment_counts = df["sentiment_label"].value_counts(normalize=True) * 100
print(sentiment_counts)

# Step 4: Plot a bar chart
colors = {
    "positive": "#1b9e77",
    "neutral": "#999999",
    "negative": "#d95f02"
}
sentiment_counts = sentiment_counts.reindex(["positive", "neutral", "negative"])

sentiment_counts.plot(kind='bar', color=[colors[label] for label in sentiment_counts.index])
plt.title("Tweet Sentiment Distribution")
plt.ylabel("Percentage")
plt.xlabel("Sentiment")
plt.tight_layout()
plt.savefig("sentiment_bar_chart.png")
plt.show()
