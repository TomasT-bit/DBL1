import os
import json
from neo4j import GraphDatabase

# Set up local Neo4J
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Panterz07"
data_f = r"C:\Users\o0dan\OneDrive\Desktop\DBL data challanges\DBL1\data"  # Modify this path
BATCH_SIZE = 10  # Number of files per batch

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def process_json_file(file_path, session):
    with open(file_path, 'r', encoding='utf-8') as file:
        print(f"📥 Opened file: {file_path}")
        batch_tweets = []
        batch_users = []
        batch_relationships = []
        
        for line in file:
            try:
                tweet = json.loads(line)
                if 'id' in tweet and 'user' in tweet:
                    # Collect tweets, users, and relationships for batch processing
                    batch_tweets.append(tweet)
                    batch_users.append(tweet['user'])
                    batch_relationships.append((tweet['id'], tweet['user']['id']))
                    
                    # Process batch when it reaches the specified size
                    if len(batch_tweets) >= 1500:  # 100 tweets per batch for example
                        session.execute_write(create_tweet_batch, batch_tweets)
                        session.execute_write(create_user_batch, batch_users)
                        session.execute_write(create_relationship_batch, batch_relationships)
                        print(f"✅ Processed batch of {len(batch_tweets)} tweets")
                        batch_tweets.clear()  # Reset for the next batch
                        batch_users.clear()
                        batch_relationships.clear()
            
            except json.JSONDecodeError as e:
                print(f"❌ Error decoding line in {file_path}: {e}")
                continue
        
        # Process any remaining tweets in the last batch
        if batch_tweets:
            session.execute_write(create_tweet_batch, batch_tweets)
            session.execute_write(create_user_batch, batch_users)
            session.execute_write(create_relationship_batch, batch_relationships)
            print(f"✅ Processed final batch of {len(batch_tweets)} tweets")

    print(f"✅ Processed {file_path}")

def create_tweet_batch(tx, tweets):
    query = """
    UNWIND $tweets AS tweet
    CREATE (t:Tweet {id: tweet.id, created_at: tweet.created_at, text: tweet.text, source: tweet.source})
    """
    tx.run(query, tweets=tweets)

def create_user_batch(tx, users):
    query = """
    UNWIND $users AS user
    CREATE (u:User {id: user.id, name: user.name, screen_name: user.screen_name, location: user.location})
    """
    tx.run(query, users=users)

def create_relationship_batch(tx, relationships):
    query = """
    UNWIND $relationships AS relationship
    MATCH (t:Tweet {id: relationship[0]})
    MATCH (u:User {id: relationship[1]})
    CREATE (u)-[:POSTED]->(t)
    """
    tx.run(query, relationships=relationships)

# Process all JSON files in the directory in batches
def process_all_files_in_batches():
    all_files = [f for f in os.listdir(data_f) if f.endswith(".json")]
    num_files = len(all_files)
    
    print(f"Total files to process: {num_files}")
    
    for i in range(0, num_files, BATCH_SIZE):
        batch_files = all_files[i:i+BATCH_SIZE]
        print(f"📂 Processing batch {i//BATCH_SIZE + 1} of {len(all_files)//BATCH_SIZE + 1}")
        
        # Process each file in the current batch
        with driver.session() as session:
            for filename in batch_files:
                file_path = os.path.join(data_f, filename)
                process_json_file(file_path, session)
        
        print(f"✅ Completed batch {i//BATCH_SIZE + 1}")

# Run the import in batches
process_all_files_in_batches()
print("🎉 Database creation complete.")
