from neo4j import GraphDatabase
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from datetime import datetime
import Htest
import Htest2

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"             
NEO4J_DB = "twitterconversations"       
EXPORT_DIR= "visualisations"
os.makedirs(EXPORT_DIR, exist_ok=True)

START = "01/01/2000"  
END = "3/30/2020"    

# Convert to string
start_dt = datetime.strptime(START, "%m/%d/%Y")
end_dt = datetime.strptime(END, "%m/%d/%Y")

# iso time
iso_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
iso_end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ") 


# neo4j setup 
driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
    database=NEO4J_DB
)

# create indexes inside neo4j
def create_indexes():
    indexes = [
        # conversation indexes
        "CREATE INDEX conversation_start_idx IF NOT EXISTS FOR (c:Conversation) ON (c.start)",
        "CREATE INDEX conversation_end_idx IF NOT EXISTS FOR (c:Conversation) ON (c.end)",
        "CREATE INDEX conversation_id_idx IF NOT EXISTS FOR (c:Conversation) ON (c.id)",
        
        # Tweet indexes
        "CREATE INDEX tweet_id_idx IF NOT EXISTS FOR (t:Tweet) ON (t.id)",
        "CREATE INDEX tweet_createdat_idx IF NOT EXISTS FOR (t:Tweet) ON (t.createdAt)"]
    
    with driver.session() as session:
        for query in indexes:
            session.run(query)

#Fetch the sentiment for all tweets created within the time window 
def fetch_tweet_sentiments_by_time(iso_start, iso_end):
    query = """
    MATCH (t:Tweet)
    WHERE t.created_at >= datetime($iso_start) AND t.created_at <= datetime($iso_end)
      AND t.sentiment_expected_value IS NOT NULL
    RETURN toFloat(t.sentiment_expected_value) AS sentiment
    """
    with driver.session() as session:
        result = session.run(query, {"iso_start": iso_start, "iso_end": iso_end})
        return [record["sentiment"] for record in result if record["sentiment"] is not None]



# Fetch sentiment deltas for conversations active in the time window
def fetch_sentiment_deltas_by_time(iso_start, iso_end):
    query = """
        MATCH (c:Conversation)
        WHERE c.start <= datetime($iso_end) AND c.end >= datetime($iso_start)
            AND c.start_sentiment IS NOT NULL AND c.end_sentiment IS NOT NULL
            AND c.airlineId IS NOT NULL
        RETURN toFloat(c.end_sentiment) - toFloat(c.start_sentiment) AS delta,
            toInteger(c.airlineId) AS airlineId
        """
    with driver.session() as session:
        result = session.run(query, {"iso_start": iso_start, "iso_end": iso_end})
        return [(record["delta"], record["airlineId"]) for record in result if record["delta"] is not None]



# Plot histogram of sentiment changes 
def plot_sentiment_change_histogram(deltas, start_str, end_str):
    df = pd.DataFrame({"Sentiment Change": deltas})

    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x="Sentiment Change", bins=30, color="skyblue")
    plt.title(f"Histogram of Sentiment Change") 
    plt.xlabel("Sentiment Change (End - Start)")
    plt.ylabel("Number of Conversations")
    plt.tight_layout()
    plt.savefig(os.path.join(EXPORT_DIR, "sentiment_change_histogram_time_filtered.pdf"))
    


# Stacked percentage barchart for sentiment change
import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_sentiment_direction_stacked(deltas_with_ids, aa_airline_id=22536055):
    df = pd.DataFrame(deltas_with_ids, columns=["delta", "airlineId"])
    df["Group"] = df["airlineId"].apply(lambda x: "American Airlines" if x == aa_airline_id else "Competition")
    df["Sentiment Change"] = df["delta"].apply(lambda x: "Positive" if x >= 0 else "Negative")

    summary = df.groupby(["Group", "Sentiment Change"]).size().unstack(fill_value=0)
    percent_df = summary.div(summary.sum(axis=1), axis=0) * 100

    ax = percent_df.plot(
        kind="bar",
        stacked=True,
        color=["#0078D2", "#DA1A32"],
        figsize=(8, 6),
        rot=0
    )

    # Add percentage labels
    for i, group in enumerate(percent_df.index):
        cumulative = 0
        for sentiment in percent_df.columns:
            value = percent_df.loc[group, sentiment]
            if value > 0:
                ax.text(
                    i, cumulative + value / 2,
                    f"{value:.1f}%",
                    ha="center", va="center", color="white", fontsize=10, fontweight="bold"
                )
                cumulative += value

    ax.set_ylabel("Percentage of Conversations")
    ax.set_title("Positive vs Negative sentiment change")
    ax.legend(title="Sentiment Change")
    plt.tight_layout()
    plt.savefig(os.path.join(EXPORT_DIR, "sentiment_change_stacked_bar.pdf"))
    


   

# Plot sentiment distrubution 
def plot_tweet_sentiment_histogram(sentiments, start_str, end_str):
    df = pd.DataFrame({"sentiment_expected_value": sentiments})
    df["sentiment_expected_value"] = pd.to_numeric(df["sentiment_expected_value"], errors="coerce")
    
    mean_score = df["sentiment_expected_value"].mean()

    plt.figure(figsize=(10, 5))
    plt.hist(df["sentiment_expected_value"], bins=30, color="#0072B2", edgecolor="black", alpha=0.8)
    plt.axvline(mean_score, color="#d95f02", linestyle="dashed", linewidth=2, label=f"Mean: {round(mean_score, 3)}")

    plt.title(f"Distribution of Sentiment Scores")
    plt.xlabel("Sentiment Score (-1 = Negative, +1 = Positive)")
    plt.ylabel("Tweet Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(EXPORT_DIR, "sentiment_distribution_chart.pdf"))



if __name__ == "__main__":
    
    create_indexes() #CREATE indecies
    
    # create cistribution of sentiment scores
    tweet_sentiments = fetch_tweet_sentiments_by_time(iso_start, iso_end)
    print(f"Fetched {len(tweet_sentiments)} tweet sentiment scores between {START} and {END}.")
    if tweet_sentiments:
        plot_tweet_sentiment_histogram(tweet_sentiments, START, END)
    else:
        print("No tweets with sentiment scores in the given time frame.")

    # crete distribution of sentiment change 
    deltas = fetch_sentiment_deltas_by_time(iso_start, iso_end)
    print(f"Fetched {len(deltas)} sentiment delta values for conversations active between {START} and {END}.")
    if deltas:
        plot_sentiment_change_histogram([d[0] for d in deltas], START, END)
    else:
        print("No conversations with sentiment deltas in the given time frame.")

    # create percentage chart for sentiment change    
    deltas_with_ids = fetch_sentiment_deltas_by_time(iso_start, iso_end)
    print(f"Fetched {len(deltas_with_ids)} sentiment delta values with airline IDs.")
    if deltas_with_ids:
        plot_sentiment_direction_stacked(deltas_with_ids, aa_airline_id=22536055)
    else:
        print("No data to plot sentiment direction by airline.")

    # create Violin plots for AA vs others with Kruskal-Wallis
    Htest.main(iso_start,iso_end)

    # run Kruskal-Wallis
    Htest2.main(iso_start, iso_end)
    driver.close()

 