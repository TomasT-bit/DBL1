import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Full dataset
data = {
    "Airline": ["KLM", "AirFrance", "British_Airways", "AmericanAir", "Lufthansa", 
                "easyJet", "RyanAir", "SingaporeAir", "Qantas", "EtihadAirways", "VirginAtlantic"],
    # Positive
    "Positive_original": [0.86, 0.859, 0.873, 0.862, 0.859, 0.853, 0.833, 0.867, 0.863, 0.85, 0.89],
    "Positive_quoted": [0.872, 0.86, 0.864, 0.837, 0.862, 0.846, 0.839, 0.879, 0.855, 0.883, 0.897],
    "Positive_replies": [0.848, 0.839, 0.85, 0.833, 0.851, 0.832, 0.836, 0.87, 0.85, 0.87, 0.89],
    # Negative
    "Negative_original": [0.815, 0.817, 0.816, 0.849, 0.827, 0.815, 0.818, 0.793, 0.811, 0.833, 0.8],
    "Negative_quoted": [0.826, 0.837, 0.823, 0.841, 0.827, 0.827, 0.847, 0.8, 0.818, 0.813, 0.815],
    "Negative_replies": [0.787, 0.797, 0.787, 0.806, 0.793, 0.782, 0.793, 0.769, 0.776, 0.796, 0.769],
    # Neutral
    "Neutral_original": [0.75, 0.767, 0.756, 0.723, 0.758, 0.752, 0.761, 0.778, 0.75, 0.785, 0.753],
    "Neutral_quoted": [0.717, 0.749, 0.735, 0.706, 0.746, 0.731, 0.738, 0.753, 0.735, 0.749, 0.736],
    "Neutral_replies": [0.732, 0.75, 0.732, 0.72, 0.738, 0.74, 0.742, 0.79, 0.726, 0.749, 0.743]
}
df = pd.DataFrame(data)

# Heatmap
plt.figure(figsize=(12,8))
sns.heatmap(df.set_index('Airline'), annot=True, cmap="RdYlGn", center=0.8, fmt=".3f",
            cbar_kws={'label': 'Sentiment Score'}, linewidths=0.5)
plt.title("All Sentiment Scores by Airline", pad=20)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Prepare positive scores
pos_df = df.melt(id_vars=['Airline'], 
                value_vars=['Positive_original','Positive_quoted','Positive_replies'],
                var_name='Metric', value_name='Score')

# Violin plot
plt.figure(figsize=(14,6))
sns.violinplot(x='Airline', y='Score', data=pos_df, inner='quartile', palette='Pastel1')
plt.title("Distribution of Positive Sentiment Scores")
plt.xticks(rotation=45)
plt.ylim(0.7, 1.0)
plt.grid(axis='y', alpha=0.3)
plt.show()
# KDE for negative original scores
plt.figure(figsize=(10,6))
sns.kdeplot(data=df[df['Airline'].isin(['KLM', 'AmericanAir'])], 
           x='Negative_original', hue='Airline',
           fill=True, alpha=0.3, palette=['#1f77b4', '#d62728'])
plt.title("Density of Negative Scores (Original Tweets)")
plt.xlabel("Score")
plt.axvline(x=df['Negative_original'].mean(), color='black', linestyle='--', label='Global Mean')
plt.legend()
plt.show()


