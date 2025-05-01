from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Cypher query for batch deletion - so its done over time
DELETE_QUERY = """
CALL apoc.periodic.iterate(
  'MATCH (n) RETURN n LIMIT 100000',  // Fetches nodes in batches of 1000
  'DETACH DELETE n',               // Deletes each batch of nodes
  {batchSize:1000, parallel:true}  // Set batch size and parallelism
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""

# Function to check how many nodes are left in the database
def get_remaining_nodes(session):
    result = session.run("MATCH (n) RETURN count(n) AS remaining_nodes")
    return result.single()['remaining_nodes']
y
def clear_database():
    with driver.session() as session:
        remaining_nodes = get_remaining_nodes(session)
        print(f"Starting with {remaining_nodes} nodes.")
        
        while remaining_nodes > 0:
            print(f"Deleting nodes, {remaining_nodes} remaining...")
            result = session.run(DELETE_QUERY)
            
            for record in result:
                print(f"Batches processed: {record['batches']}, Total nodes deleted: {record['total']}")
                if record['errorMessages']:
                    print(f"Error messages: {record['errorMessages']}")
            
            remaining_nodes = get_remaining_nodes(session)
            print(f"Remaining nodes: {remaining_nodes}")
        
        print("Database cleared successfully.")

clear_database()

# Close connection
driver.close()
