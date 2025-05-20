import pandas as pd
import matplotlib.pyplot as plt

# Load the tweets
df = pd.read_csv("import/tweets.csv")

# Convert to float just in case
df["sentiment_expected_value"] = pd.to_numeric(df["sentiment_expected_value"], errors="coerce")

# Mean score (again)
mean_score = df["sentiment_expected_value"].mean()

# Plot histogram
plt.figure(figsize=(10, 5))
plt.hist(df["sentiment_expected_value"], bins=30, color="#0072B2", edgecolor="black", alpha=0.8)

# Add vertical line for the mean
plt.axvline(mean_score, color="#d95f02", linestyle="dashed", linewidth=2, label=f"Mean: {round(mean_score, 3)}")

# Labeling
plt.title("Distribution of Sentiment Scores")
plt.xlabel("Sentiment Score (-1 = Negative, +1 = Positive)")
plt.ylabel("Tweet Count")
plt.legend()
plt.tight_layout()

# Save figure
plt.savefig("sentiment_distribution_chart.png")
plt.show()
