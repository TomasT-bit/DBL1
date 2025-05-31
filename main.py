from neo4j import GraphDatabase
from collections import defaultdict
import csv
import os
import logging

# Setup for log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Neo4j connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Airlines
airline_ids = [
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
]

conversation_counter = 1

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def annotate_positions(conversation, airline_tweet_ids):
    annotations = {}
    airline_indices = [i for i, tid in enumerate(conversation) if tid in airline_tweet_ids]

    if not airline_indices:
        return {tid: 0 for tid in conversation}

    first_airline_idx = airline_indices[0]
    last_airline_idx = airline_indices[-1]

    for i, tid in enumerate(conversation):
        if tid in airline_tweet_ids:
            annotations[tid] = 0
        elif i < first_airline_idx:
            annotations[tid] = 1
        elif i > last_airline_idx:
            annotations[tid] = 2
        else:
            annotations[tid] = 0
    return annotations

def get_graph(airline_id):
    with driver.session(database="twitter9") as session: #make sure to rename according to ur twitter name  
        try: #drop graph if previous cycle was terminated without properly closing
            session.run("CALL gds.graph.drop('Graph', false)")
        except:
            pass
        #create weakly connected componnets on replied relations and tweets 
        session.run("""
        CALL gds.graph.project(
            'Graph', 'Tweet', {
                REPLIES: {type: 'REPLIES', orientation: 'UNDIRECTED'}
            })
        """)

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

        result = session.run("""
        MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet)
        RETURN t.tweetId AS tweetId
        """, airline_id=airline_id)
        airline_tweet_ids = set(str(record["tweetId"]) for record in result)

        matched_components = []
        annotations_per_convo = []

        for tweet_list in components.values():
            if len(tweet_list) < 2:
                continue

            reply_tree_result = session.run("""
            UNWIND $tweet_ids AS tid
            MATCH (t:Tweet {tweetId: tid})
            OPTIONAL MATCH (t)<-[:REPLIES]-(child:Tweet)
            WHERE child.tweetId IN $tweet_ids
            RETURN t.tweetId AS parent, collect(child.tweetId) AS children
            """, tweet_ids=tweet_list)

            children_map = {record["parent"]: record["children"] for record in reply_tree_result}
            all_children = {c for clist in children_map.values() for c in clist}

            def dfs(node, visited, result):
                if node in visited:
                    return
                visited.add(node)
                for child in children_map.get(node, []):
                    dfs(child, visited, result)
                result.append(node)

            visited = set()
            ordered = []
            for root in set(tweet_list) - all_children:
                dfs(root, visited, ordered)

            ordered = list(reversed(ordered))  # chronological order from dfs, hence here we split it into linear trees 

            # Trim tweets
            start, end = 0, len(ordered)
            while start < end and ordered[start] in airline_tweet_ids:
                start += 1
            while end > start and ordered[end - 1] in airline_tweet_ids:
                end -= 1

            trimmed = ordered[start:end]

            # Only keep conversation if valid
            has_airline_inside = any(tid in airline_tweet_ids for tid in trimmed)
            if has_airline_inside and len(trimmed) >= 3:
                matched_components.append(trimmed)
                annotations = annotate_positions(trimmed, airline_tweet_ids)
                annotations_per_convo.append(annotations)

        session.run("CALL gds.graph.drop('Graph')")
        logger.info(f"Airline {airline_id}: {len(matched_components)} valid conversations found.")

    return matched_components, annotations_per_convo

#saving to csv 
def create_global_csv(conversations, annotations_list, airline_id, conv_writer, edge_writer):
    global conversation_counter
    for tweet_ids, annotations in zip(conversations, annotations_list):
        conv_id = f"{conversation_counter}"
        conversation_counter += 1
        conv_writer.writerow(["Conversation",conv_id, airline_id])
        for tid in tweet_ids:
            part_type = annotations.get(tid, 0)
            edge_writer.writerow([conv_id, tid, "PART_OF", part_type])

if __name__ == "__main__":
    output_dir = "import"
    os.makedirs(output_dir, exist_ok=True)

    conv_csv_path = os.path.join(output_dir, "conversations.csv")
    edges_csv_path = os.path.join(output_dir, "conversation_edges.csv")

    with open(conv_csv_path, mode='w', newline='', encoding='utf-8') as conv_file, \
         open(edges_csv_path, mode='w', newline='', encoding='utf-8') as edge_file:

        conv_writer = csv.writer(conv_file)
        edge_writer = csv.writer(edge_file)

        conv_writer.writerow([":LABEL",":ID(Conversation)", "airlineId"])
        edge_writer.writerow([":START_ID(Conversation)", ":END_ID(Tweet)", ":TYPE", "positionType:int"])

        for airline_id in airline_ids:
            conversations, annotations = get_graph(airline_id)
            create_global_csv(conversations, annotations, airline_id, conv_writer, edge_writer)
