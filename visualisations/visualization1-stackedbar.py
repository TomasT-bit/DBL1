import pandas as pd

data = {
    "Sentiment": ["Positive", "Negative", "Neutral"],
    "Original_count": [156868, 461431, 324395],
    "Original_score": [0.845, 0.811, 0.768],
    "Quoted_count": [50633, 92566, 59994],
    "Quoted_score": [0.849, 0.802, 0.7],
    "Replies_count": [322072, 529811, 592835],
    "Replies_score": [0.85, 0.775, 0.74]
}

df = pd.DataFrame(data)
print(df)
import matplotlib.pyplot as plt

# Pivot data for stacked bars
counts_df = df[["Sentiment", "Original_count", "Quoted_count", "Replies_count"]].set_index("Sentiment").T

counts_df.plot(kind='bar', stacked=True, color=['#4CAF50', '#F44336', '#9E9E9E'], figsize=(10, 6))
plt.ylabel("Tweet Count")
plt.title("Sentiment Distribution by Tweet Type")
plt.xticks(rotation=0)
plt.show()