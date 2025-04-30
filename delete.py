from neo4j import GraphDatabase

# Set up local Neo4J
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"


#TBh just remove the database and build a new one 

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Simple Cypher query to delete all nodes and relationships
DELETE_QUERY = "MATCH (n) DETACH DELETE n"

# Function to run the deletion
def clear_database():
    with driver.session() as session:
        result = session.run(DELETE_QUERY)
        print("All nodes and relationships deleted.")
        # Check how many nodes were deleted
        count = session.run("MATCH (n) RETURN count(n)").single()[0]
        print(f"Remaining nodes: {count}")

# Run the deletion process
clear_database()

# Close the connection
driver.close()
