import pandas as pd
from datetime import datetime
from neo4j import GraphDatabase

# === Config ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DB = "twitterconversations"
OUTPUT_CSV = "conversation_nodes_extended.csv"

# === Timestamp Conversion Function ===
def convert_twitter_ts_vectorized(series):
    def parse(ts):
        if ts is None:
            return None
        if isinstance(ts, str) and ts[0:4].isdigit() and "T" in ts:
            return ts
        try:
            dt = datetime.strptime(ts, "%a %b %d %H:%M:%S %z %Y")
            return dt.isoformat()
        except Exception:
            return None
    return series.apply(parse)

# === Connect to Neo4j ===
print("🚀 Connecting to Neo4j...")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session(database=NEO4J_DB) as session:
    print("🔧 Step 0: Ensure index on Tweet.tweetId ...")
    session.run("""
        MATCH (c:Conversation)
RETURN 
    labels(c)[0] AS label,
    c.conversationId AS id,
    c.airlineId AS airlineId,
    c.start_sentiment AS start_sentiment,
    c.end_sentiment AS end_sentiment,
    c.top_label AS top_label,
    c.start AS start,
    c.end AS end
    """)

    print("🔄 Step 1: Fetching tweets in conversations...")
    result = session.run("""
        MATCH (c:Conversation)-[:PART_OF]->(t:Tweet)
        RETURN t.tweetId AS tweetId, t.created_at AS created_at
    """)
    df = pd.DataFrame(result.data())
    print(f"📥 Retrieved {len(df)} tweets")

    print(f"⚡ Step 2: Converting timestamps...")
    df['iso'] = convert_twitter_ts_vectorized(df['created_at'])
    converted = df['iso'].notnull().sum()
    print(f"✅ Converted {converted}/{len(df)} timestamps successfully")

    print("⚡ Step 3: Batch updating tweets in Neo4j...")
    batch_size = 500
    updates = df.dropna(subset=['iso'])

    for i in range(0, len(updates), batch_size):
        batch_df = updates.iloc[i:i+batch_size]
        print(f"⏳ Updating batch {i//batch_size + 1} ({len(batch_df)} tweets)...")
        batch = batch_df[['tweetId', 'iso']].to_dict("records")
        session.run("""
            UNWIND $batch AS row
            MATCH (t:Tweet {tweetId: row.tweetId})
            SET t.created_at = datetime(row.iso)
        """, {"batch": batch})

    print("🕒 Step 4: Setting Conversation start/end timestamps...")
    session.run("""
        MATCH (c:Conversation)-[:PART_OF]->(t:Tweet)
        WHERE t.created_at IS NOT NULL
        WITH c, min(t.created_at) AS start, max(t.created_at) AS end
        SET c.start = start, c.end = end
    """)
    print("✅ Conversation time bounds updated")


driver.close()
print("🔌 Neo4j connection closed")
