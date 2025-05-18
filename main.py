import os
from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Initialize driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
# Cypher query for batch deletion - so its done over time


#Conversations moddeled as weakly connected components, make sure that the previous projection was cleaned
def get_conversations():
    with driver.session(database="twitter") as session:
        # Ensure previous graph is dropped
        session.run("""
            CALL gds.graph.exists('convoGraph') YIELD exists
            WITH exists
            CALL apoc.do.when(
                exists,
                'CALL gds.graph.drop("convoGraph", false)',
                '',
                {}
            ) YIELD value
            RETURN 1
        """)

        # Create graph projection using Cypher
        session.run("""
            CALL gds.graph.project.cypher(
                'convoGraph',
                'MATCH (t:Tweet) RETURN id(t) AS id',
                'MATCH (a:Tweet)-[r:REPLIES|RETWEETS|QUOTES]->(b:Tweet)
                 RETURN id(a) AS source, id(b) AS target, "UNDIRECTED" AS orientation'
            )
        """)

        # Run WCC algorithm
        count_result = session.run("""
            CALL gds.wcc.stats('convoGraph')
            YIELD componentCount
        """)
        component_count = count_result.single()["componentCount"]

        # Stream WCC results
        result = session.run("""
            CALL gds.wcc.stream('convoGraph')
            YIELD nodeId, componentId
            RETURN componentId, collect(gds.util.asNode(nodeId).tweetId) AS tweets
            ORDER BY size(tweets) DESC
        """)

        conversations = []
        less = 0
        more = 0

        for record in result:
            tweets = record["tweets"]
            conversations.append({
                "componentId": record["componentId"],
                "tweets": tweets
            })
            if len(tweets)<2:
                print(tweets)
                less+=1
            if len(tweets) >= 2:
                more += 1
        
        return component_count, conversations, less , more

        # Drop projection
        session.run("CALL gds.graph.drop('convoGraph') YIELD graphName")

        return component_count, conversations, less_than_2


component_count, conversations, count1, count2 = get_conversations()
print(f"Total conversations: {component_count} \n ")
print(f"Conversation one node {count1}, {count2}")











































#count_eng=driver.session(NumberOfEnglishTweets)
