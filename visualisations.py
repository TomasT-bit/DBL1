from neo4j import GraphDatabase
OUTPUT_DIR = "results" 

START=
END= 

#SET-UP
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
DB_NAME = "TwitterConvo"

 driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session(database=DB_NAME) as session: