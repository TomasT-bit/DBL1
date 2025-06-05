from neo4j import GraphDatabase
from collections import defaultdict
import csv
import os
from tqdm import tqdm


# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Airline user ID
airline_ids = [
    "56377143",
    "106062176",
    "18332190",
    "22536055",
    "124476322",
    "26223583",
    "2182373406",
    "38676903",
    "1542862735",
    "253340062",
    "218730857",
    "45621423",
    "20626359"
]

# Create writers once at the start, and append to them for each airline
def create_global_csv(conversations, airline_id, conv_writer, edge_writer):
    global conversation_counter

    for tweet_ids in conversations:
        conv_id = f"{airline_id}_{conversation_counter}"
        conversation_counter += 1

        # Write to conversations.csv
        conv_writer.writerow([conv_id, airline_id])

        # Write to conversation_edges.csv
        for tid in tweet_ids:
            edge_writer.writerow([conv_id, tid, "PART_OF"])


# Connect to Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def valid_conversation(conversation, airline_tweet_ids):
    """
    Keeps only airline tweets that have at least one non-airline tweet before and after them.
    Recursively removes invalid airline tweets until all remaining ones are valid.
    """
    n = len(conversation)
    keep = [True] * n
    changed = False

    for i, tid in enumerate(conversation):
        if tid in airline_tweet_ids:
            has_before = any(conversation[j] not in airline_tweet_ids for j in range(i - 1, -1, -1))
            has_after = any(conversation[j] not in airline_tweet_ids for j in range(i + 1, n))
            if not (has_before and has_after):
                keep[i] = False
                changed = True

    filtered = [tid for i, tid in enumerate(conversation) if keep[i]]

    if not any(tid in airline_tweet_ids for tid in filtered):
        return None

    if changed:
        return valid_conversation(filtered, airline_tweet_ids)

    return filtered


# Global conversation counter
conversation_counter = 1

def create_conversation_nodes(conversations, airline_id):
    global conversation_counter  # Access the global variable
    with driver.session(database="twitter") as session:
        for tweet_ids in conversations:
            conv_id = f"{airline_id}_{conversation_counter}"
            conversation_counter += 1  # Increment for next conversation

            # Create the CONVERSATION node
            session.run("""
            MERGE (c:CONVERSATION {conversationId: $conv_id, airlineId: $airline_id})
            """, conv_id=conv_id, airline_id=airline_id)

            # Link each Tweet to the CONVERSATION
            for tid in tweet_ids:
                session.run("""
                MATCH (t:Tweet {tweetId: $tid})
                MATCH (c:CONVERSATION {conversationId: $conv_id})
                MERGE (c)-[:PART_OF]->(t)
                """, tid=tid, conv_id=conv_id)




def get_graph(airline_id):
    with driver.session(database="twitter") as session:
                  
        # Create in-memory graph using only REPLIES
        session.run("""
        CALL gds.graph.project(
            'Graph',
            'Tweet',
            {
                REPLIES: {
                    type: 'REPLIES',
                    orientation: 'UNDIRECTED'
                }
            }
        )
        """)

        # Run WCC algorithm
        wcc_result = session.run("""
        CALL gds.wcc.stream('Graph')
        YIELD nodeId, componentId
        RETURN gds.util.asNode(nodeId).tweetId AS tweetId, componentId
        """)

        components = defaultdict(list)
        for record in wcc_result:
            tweet_id = record["tweetId"]
            component_id = record["componentId"]
            components[component_id].append(tweet_id)

        # Get airline tweets
        result = session.run("""
        MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet)
        RETURN t.tweetId AS tweetId
        """, airline_id=airline_id)
        airline_tweet_ids = set(str(record["tweetId"]) for record in result)

        matched_components = []
        for cid, tweet_list in components.items():
            tweet_list = list(dict.fromkeys(tweet_list))  # Ensure unique and ordered
            filtered = valid_conversation(tweet_list, airline_tweet_ids)
            if filtered:
                matched_components.append(filtered)

        print(f"Found {len(matched_components)} valid conversations for {airline_id}")

        # Drop the in-memory graph
        session.run("CALL gds.graph.drop('Graph') YIELD graphName")

    return matched_components

if __name__ == "__main__":
    output_dir = "import"
    os.makedirs(output_dir, exist_ok=True)

    # Paths to global files
    conv_csv_path = os.path.join(output_dir, "conversations.csv")
    edges_csv_path = os.path.join(output_dir, "conversation_edges.csv")

    with open(conv_csv_path, mode='w', newline='', encoding='utf-8') as conv_file, \
         open(edges_csv_path, mode='w', newline='', encoding='utf-8') as edge_file:

        conv_writer = csv.writer(conv_file)
        edge_writer = csv.writer(edge_file)

        # Write headers
        conv_writer.writerow([":ID(Conversation)", "airlineId"])
        edge_writer.writerow([":START_ID(Conversation)", ":END_ID(Tweet)", ":TYPE"])

        # Process each airline and write to the shared CSVs
        for airline_id in airline_ids:
            conversations = get_graph(airline_id)
            create_global_csv(conversations, airline_id, conv_writer, edge_writer)
            #create_conversation_nodes(conversations, airline_id)
