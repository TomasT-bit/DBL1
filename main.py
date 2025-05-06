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
    with driver.session(database="twitter") as session: #make sure to include this as name of ur database

        # Create projection
        session.run("""
            CALL gds.graph.project(
                'convoGraph',
                'Tweet',
                {
                    REPLIES: {orientation: 'UNDIRECTED'},
                    RETWEETS: {orientation: 'UNDIRECTED'},
                    QUOTES: {orientation: 'UNDIRECTED'}
                }
            )
        """)

        #number of conversations
        count_result = session.run("""
            CALL gds.wcc.stats('convoGraph')
            YIELD componentCount
        """)
        component_count = count_result.single()["componentCount"]
        print(f"Total number of conversations: {component_count}")

        # get hte component
        result = session.run("""
            CALL gds.wcc.stream('convoGraph')
            YIELD nodeId, componentId
            RETURN componentId, collect(gds.util.asNode(nodeId).tweetId) AS tweets
            ORDER BY size(tweets) DESC
        """)

        conversations = []
        for record in result:
            conversations.append({
                "componentId": record["componentId"],
                "tweets": record["tweets"]
            })

        # Cleanup
        session.run("CALL gds.graph.drop('convoGraph') YIELD graphName")

        return component_count, conversations




NumberOfEnglishTweets =""""
MATCH (t:Tweet)
WHERE t.lang = 'en'
RETURN count(t) AS EnglishTweetCount
"""

MentionedAmericanAir ="""
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"22536055"}) Limit 25
Return n1,n2
"""

PostedByAmericanAir ="""
MATCH (n2:User{userId:"22536055"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

PostedKLM ="""
MATCH (n2:User{userId:"56377143"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedKLM = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"56377143"}) Limit 25
Return n1,n2
"""
PostedAirFrance = """
MATCH (n2:User{userId:"106062176"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedAirFrance ="""
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"106062176"}) Limit 25
Return n1,n2
"""

PostedBritish_Airways = """
MATCH (n2:User{userId:"18332190"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedBritish_Airways ="""
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"18332190"}) Limit 25
Return n1,n2
"""

PostedLufthansa = """
MATCH (n2:User{userId:"124476322"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedLufthansa = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"124476322"}) Limit 25
Return n1,n2
"""

PostedAirBerlin = """
MATCH (n2:User{userId:"26223583"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedAirBerlin = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"26223583"}) Limit 25
Return n1,n2
"""

PostedAirBerlinAssist ="""
MATCH (n2:User{userId:"2182373406"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedAirBerlinAssist = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"2182373406"}) Limit 25
Return n1,n2
"""

PostedEasyJet = """
MATCH (n2:User{userId:"38676903"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedEasyJet = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"38676903"}) Limit 25
Return n1,n2
"""

PostedRyanAir = """
MATCH (n2:User{userId:"1542862735"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedRyanAir = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"1542862735"}) Limit 25
Return n1,n2
"""

PostedSingaporeAir = """
MATCH (n2:User{userId:"253340062"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedSingaporeAir = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"253340062"}) Limit 25
Return n1,n2
"""

PostedQantas = """
MATCH (n2:User{userId:"218730857"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedQantas = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"218730857"}) Limit 25
Return n1,n2
"""

PostedEtihadAirways = """
MATCH (n2:User{userId:"22536055"})-[r:POSTED]->(n1:Tweet) Limit 25
RETURN n1, n2
"""

MentionedEtihadAirways = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"45621423"}) Limit 25
Return n1,n2
"""

PostedVirginAtlantic = """
MATCH (n2:User{userId:"20626359"})-[r:POSTED]->(n1:Tweet)
RETURN n1, n2
Limit 50
"""

MentionedVirginAtlantic = """
MATCH (n1:Tweet)-[r:MENTIONED]->(n2:User{userId:"20626359"}) Limit 25
Return n1,n2
"""

PostedAmericanAirEnglishOnly = """
MATCH (u:User)-[:POSTED]->(t:Tweet)
MATCH (n2:User {userId: "22536055"})-[:POSTED]->(n1:Tweet)
WHERE t.lang = "en"
RETURN n1, n2, u, t
LIMIT 100
"""


component_count, conversations = get_conversations()
print(f"Total conversations: {component_count}")











































count_eng=driver.session(NumberOfEnglishTweets)
