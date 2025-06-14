from neo4j import GraphDatabase
import seaborn as sns
import pandas as pd

# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password" 
DATABASE = "showing"       

driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))

def fetch_sentiment_deltas():
    query = """
    MATCH (c:Conversation)
    RETURN toFloat(c.end_sentiment) - toFloat(c.start_sentiment) AS delta
    """
    with driver.session(database=DATABASE) as session:
        result = session.run(query)
        return [record["delta"] for record in result if record["delta"] is not None]

def plot_bar_and_histogram(deltas):
    df = pd.DataFrame({"Sentiment Change": deltas})

    
    df = df[df["Sentiment Change"] != 0]

    df["Sentiment Type"] = df["Sentiment Change"].apply(lambda v: "Positive Change" if v > 0 else "Negative Change")


    bar_plot = sns.countplot(data=df, x="Sentiment Type", palette={"Positive Change": "green", "Negative Change": "red"})
    bar_plot.set_title("Number of Conversations by Sentiment Change Type")
    bar_plot.set_xlabel("Sentiment Change Type")
    bar_plot.set_ylabel("Number of Conversations")
    bar_plot.figure.tight_layout()
    bar_plot.figure.savefig("sentiment_change_bar_chart.png")
    bar_plot.figure.clf()  

    hist_plot = sns.histplot(data=df, x="Sentiment Change", bins=30, color="skyblue")
    hist_plot.set_title("Histogram of Sentiment Change (End - Start)")
    hist_plot.set_xlabel("Sentiment Change (End - Start)")
    hist_plot.set_ylabel("Number of Conversations")
    hist_plot.figure.tight_layout()
    hist_plot.figure.savefig("sentiment_change_histogram.png")
    hist_plot.figure.clf()

if __name__ == "__main__":
    deltas = fetch_sentiment_deltas()
    print(f"Fetched {len(deltas)} sentiment change values.")
    plot_bar_and_histogram(deltas)
    driver.close()