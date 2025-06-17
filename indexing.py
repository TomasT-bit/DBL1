from neo4j import GraphDatabase
import logging

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
DB_NAME = "twitter"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_indexes():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session(database=DB_NAME) as session:
        logger.info("Creating indexes if not exist...")

        session.run("""
        CREATE INDEX tweetId_index IF NOT EXISTS
        FOR (t:Tweet) ON (t.tweetId)
        """)
        logger.info("Index on :Tweet(tweetId) created or already exists.")

        session.run("""
        CREATE INDEX userId_index IF NOT EXISTS
        FOR (u:User) ON (u.userId)
        """)
        logger.info("Index on :User(userId) created or already exists.")

    driver.close()
    logger.info("Index creation complete.")

if __name__ == "__main__":
    create_indexes()
