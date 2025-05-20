import pandas as pd
import matplotlib.pyplot as plt

# Load the data
df = pd.read_csv("import/tweets.csv")

# Convert the 'created_at' column to datetime
df["created_at"] = pd.to_datetime(df["created_at"], format="%a %b %d %H:%M:%S %z %Y", errors="coerce")

# Remove rows with missing dates
df = df.dropna(subset=["created_at"])

# Group by day and compute average sentiment
df["date"] = df["created_at"].dt.date
daily_avg = df.groupby("date")["sentiment_expected_value"].mean()

# Plot
plt.figure(figsize=(12, 5))
plt.plot(daily_avg.index, daily_avg.values, color="#E69F00", label="Daily Avg Sentiment")
plt.axhline(0, color="gray", linestyle="--", linewidth=1)

# Optionally add ±1 std deviation band
std = df["sentiment_expected_value"].std()
mean = df["sentiment_expected_value"].mean()
plt.fill_between(daily_avg.index, mean - std, mean + std, color="#0072B2", alpha=0.2, label="±1 Std Dev")

# Labels and styling
plt.title("Daily Average Sentiment Over Time")
plt.xlabel("Date")
plt.ylabel("Average Sentiment Score")
plt.legend()
plt.tight_layout()
plt.savefig("sentiment_trend_chart.png")
plt.show()

# Load and parse dates
df = pd.read_csv("import/tweets.csv")
df["created_at"] = pd.to_datetime(df["created_at"], format="%a %b %d %H:%M:%S %z %Y", errors="coerce")

# Drop missing dates
df = df.dropna(subset=["created_at"])

# ✅ CHECK: Date range
print("✅ Total rows with date:", len(df))
print("📅 Earliest tweet:", df["created_at"].min())
print("📅 Latest tweet:", df["created_at"].max())
