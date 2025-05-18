import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Data
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
# Prepare data for pie charts
counts = df[["Original_count", "Quoted_count", "Replies_count"]].sum(axis=0)
labels = ["Original", "Quoted", "Replies"]

# Plot pie charts
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for i, (label, count) in enumerate(zip(labels, counts)):
    axes[i].pie(
        df[f"{label}_count"],
        labels=df["Sentiment"],
        autopct="%1.1f%%",
        colors=["#4CAF50", "#F44336", "#9E9E9E"],
        startangle=90,
    )
    axes[i].set_title(f"{label} Tweets")

plt.suptitle("Sentiment Distribution by Tweet Type", y=1.05)
plt.tight_layout()
plt.show()