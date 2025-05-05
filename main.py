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

Query2 ="""
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"22536055"}) Limit 25
Return n1,n2
"""
count_eng=driver.session(Query1)