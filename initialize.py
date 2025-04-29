import os
import json
from neo4j import GraphDatabase

# Set up local Neo4J
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
data_f = "./data"  # data directory

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def process_json_file(file_path):
    with open(file_path, 'r') as file:
        print("opened file")
        with driver.session() as session:
            for line in file:
                tweet = json.loads(line)
                if 'id' in tweet and 'user' in tweet:
                    session.execute_write(create_tweet, tweet)
                    print ("creating tweet")
                    session.execute_write(create_user, tweet['user'])
                    print("creating user ")
                    session.execute_write(create_relationship, tweet['id'], tweet['user']['id'])
                    print("made relation")
    print(f"Processed {file_path}")


def create_tweet(tx, tweet):
    query = """
    CREATE (t:Tweet {id: $id, created_at: $created_at, text: $text, source: $source})
    """
    tx.run(query, id=tweet['id'], created_at=tweet['created_at'], text=tweet['text'], source=tweet['source'])

def create_user(tx, user):
    query = """
    CREATE (u:User {id: $id, name: $name, screen_name: $screen_name, location: $location})
    """
    tx.run(query, id=user['id'], name=user['name'], screen_name=user['screen_name'], location=user['location'])

#redesign
def create_relationship(tx, tweet_id, user_id):
    query = """
    MATCH (t:Tweet {id: $tweet_id})
    MATCH (u:User {id: $user_id})
    CREATE (u)-[:POSTED]->(t)
    """
    tx.run(query, tweet_id=tweet_id, user_id=user_id)


# Process
single_file_path = "./data/airlines-1558527599826.json"
process_json_file(single_file_path)

print("database created.")