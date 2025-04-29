from neo4j import GraphDatabase

# Set up local Neo4J
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def delete_all(tx):
    query = """
    MATCH (n)
    DETACH DELETE n
    """
    tx.run(query)

# Delete all nodes and relationships
with driver.session() as session:
    session.execute_write(delete_all)

print("Deleted.")
