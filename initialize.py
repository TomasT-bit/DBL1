import os
import json
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor

# === Configuration ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Panterz07"
DATA_DIR = r"C:\Users\o0dan\OneDrive\Desktop\DBL data challanges\DBL1\data"
BATCH_SIZE = 1500         # Number of tweets per batch
CHUNK_SIZE = 20           # Number of files per thread chunk
MAX_WORKERS = 4           # Parallel threads (adjust up to 8 on Ryzen 7 if stable)

# === Neo4j Driver ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# === Cypher Queries ===
def create_tweet_batch(tx, tweets):
    tx.run("""
        UNWIND $tweets AS tweet
        MERGE (t:Tweet {id: tweet.id})
        ON CREATE SET t.created_at = tweet.created_at, t.text = tweet.text, t.source = tweet.source
    """, tweets=tweets)

def create_user_batch(tx, users):
    tx.run("""
        UNWIND $users AS user
        MERGE (u:User {id: user.id})
        ON CREATE SET u.name = user.name, u.screen_name = user.screen_name, u.location = user.location
    """, users=users)

def create_relationship_batch(tx, relationships):
    tx.run("""
        UNWIND $relationships AS rel
        MATCH (t:Tweet {id: rel[0]})
        MATCH (u:User {id: rel[1]})
        MERGE (u)-[:POSTED]->(t)
    """, relationships=relationships)

# === File Processor ===
def process_json_file(file_path, session):
    print(f"📥 Opening: {file_path}")
    tweets, users, relationships = [], [], []

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                tweet = json.loads(line)
                if 'id' in tweet and 'user' in tweet:
                    tweets.append(tweet)
                    users.append(tweet['user'])
                    relationships.append((tweet['id'], tweet['user']['id']))

                    if len(tweets) >= BATCH_SIZE:
                        session.execute_write(create_tweet_batch, tweets)
                        session.execute_write(create_user_batch, users)
                        session.execute_write(create_relationship_batch, relationships)
                        print(f"✅ Wrote batch of {len(tweets)} tweets from {file_path}")
                        tweets.clear(); users.clear(); relationships.clear()
            except json.JSONDecodeError as e:
                print(f"❌ JSON error in {file_path}: {e}")
                continue

    if tweets:
        session.execute_write(create_tweet_batch, tweets)
        session.execute_write(create_user_batch, users)
        session.execute_write(create_relationship_batch, relationships)
        print(f"✅ Final batch of {len(tweets)} tweets from {file_path}")

# === Chunk Processor ===
def process_file_chunk(file_chunk):
    with driver.session() as session:
        for file_path in file_chunk:
            process_json_file(file_path, session)

# === Main Controller ===
def process_all_files():
    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    chunks = [all_files[i:i + CHUNK_SIZE] for i in range(0, len(all_files), CHUNK_SIZE)]
    
    print(f"📂 {len(all_files)} JSON files found, split into {len(chunks)} chunks")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_file_chunk, chunks)

    print("🎉 All data successfully imported into Neo4j.")

# === Run the script ===
if __name__ == "__main__":
    process_all_files()
