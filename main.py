import os
from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
# Cypher query for batch deletion - so its done over time
Query1 =""""
MATCH (t:Tweet)
WHERE t.lang = 'en'
RETURN count(t) AS EnglishTweetCount
"""

count_eng=driver.session(Query1)