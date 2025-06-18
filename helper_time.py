import pandas as pd
from datetime import datetime
from neo4j import GraphDatabase

# === Configuration ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DB = "twitterconversations"


def convert_twitter_ts_vectorized(series):
    """
    Convert Twitter-style timestamps to ISO 8601 format 
    """
    def parse(ts):
        if ts is None:
            return None
        # If already in ISO format, return as-is
        if isinstance(ts, str) and ts[0:4].isdigit() and "T" in ts:
            return ts
        try:
            dt = datetime.strptime(ts, "%a %b %d %H:%M:%S %z %Y")
            return dt.isoformat()
        except Exception:
            return None

    return series.apply(parse)

def fetch_tweet_data(session):
    """
    Fetch tweet IDs and creation timestamps from Neo4j.
    """
    result = session.run("""
        MATCH (c:Conversation)-[:PART_OF]->(t:Tweet)
        RETURN t.tweetId AS tweetId, t.created_at AS created_at
    """)
    return pd.DataFrame(result.data())

def update_tweet_timestamps(session, df, batch_size=500):
    """
    Update the `created_at` field on Tweet nodes in Neo4j with datetime objects.
    """
    updates = df.dropna(subset=['iso'])

    for i in range(0, len(updates), batch_size):
        batch_df = updates.iloc[i:i+batch_size]
        batch = batch_df[['tweetId', 'iso']].to_dict("records")
        session.run("""
            UNWIND $batch AS row
            MATCH (t:Tweet {tweetId: row.tweetId})
            SET t.created_at = datetime(row.iso)
        """, {"batch": batch})

def set_conversation_bounds(session):
    """
    Set the start and end timestamp on each Conversation node based on its related Tweet nodes.
    """
    session.run("""
        MATCH (c:Conversation)-[:PART_OF]->(t:Tweet)
        WHERE t.created_at IS NOT NULL
        WITH c, min(t.created_at) AS start, max(t.created_at) AS end
        SET c.start = start, c.end = end
    """)

def main():
    print("Connecting.")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        with driver.session(database=NEO4J_DB) as session:
            #Make index 
            session.run("""
            CREATE INDEX tweetId_index IF NOT EXISTS
        FOR (t:Tweet) ON (t.tweetId) 
        """)
            print("Fetching tweets")
            df = fetch_tweet_data(session)
            print(f"Retrieved {len(df)} tweets")

            print("Converting tweet timestamps...")
            df['iso'] = convert_twitter_ts_vectorized(df['created_at'])
            converted = df['iso'].notnull().sum()
            print(f"Converted {converted}/{len(df)} timestamps successfully")

            print("Updating tweet nodes in Neo4j...")
            update_tweet_timestamps(session, df)

            print("Updating conversation start and end times...")
            set_conversation_bounds(session)

            print("Update done")
    finally:
        driver.close()
        print("Neo4j connection closed.")

if __name__ == "__main__":
    main()
