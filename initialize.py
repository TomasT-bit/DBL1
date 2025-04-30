from neo4j import GraphDatabase
import time

# Set up local Neo4J
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Cypher queries for batch insertions using APOC
def import_users():
    query = """
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "file:///users.csv" AS row RETURN row',
        'CREATE (u:User {userId: row.`userId:ID(User)`, name: row.name, screen_name: row.screen_name})',
        {batchSize: 1000, parallel: true}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            print(f"Batch Info: {record['batches']}, Total: {record['total']}, Errors: {record['errorMessages']}")
        print("Users import complete.")

def import_tweets():
    query = """
    CALL apoc.load.csv('file:///tweets.csv', {skip: 0, separator: ','}) YIELD map AS row
    CREATE (t:Tweet {tweetId: row.tweetId, content: row.content, createdAt: row.createdAt})
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            print(f"Inserted Tweet: {record['tweetId']}")
        print("Tweets import complete.")

def import_posted():
    query = """
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "file:///posted.csv" AS row RETURN row',
        'MATCH (u:User {userId: row.`:START_ID(User)`}) MATCH (t:Tweet {tweetId: row.`:END_ID(Tweet)`}) CREATE (u)-[:POSTED]->(t)',
        {batchSize: 1000, parallel: true}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            print(f"Batch Info: {record['batches']}, Total: {record['total']}, Errors: {record['errorMessages']}")
        print("POSTED relationships import complete.")

def import_mentions():
    query = """
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "file:///mentions.csv" AS row RETURN row',
        'MATCH (t:Tweet {tweetId: row.`:START_ID(Tweet)`}) MATCH (u:User {screen_name: row.`:END_ID(User)`}) CREATE (t)-[:MENTIONED]->(u)',
        {batchSize: 1000, parallel: true}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            print(f"Batch Info: {record['batches']}, Total: {record['total']}, Errors: {record['errorMessages']}")
        print("MENTIONED relationships import complete.")

# Main function to run all imports
def run_inserts():
    start_time = time.time()

    import_users()
    import_tweets()
    import_posted()
    import_mentions()

    print(f"All insertions completed in {time.time() - start_time} seconds.")

# Run the insertions
run_inserts()

# Close the connection
driver.close()
