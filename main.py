import os
import csv
import time
import logging
import cProfile
import pstats
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Process, Manager
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

# Config
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
DB_NAME = "twitter9"
MAX_WORKERS = 6  # Reduced from 13
LOG_EVERY_N = 100

airline_ids = [
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(processName)s - %(message)s"
)
logger = logging.getLogger(__name__)

def annotate_positions(conversation, airline_tweet_ids):
    annotations = {}
    airline_indices = [i for i, tid in enumerate(conversation) if tid in airline_tweet_ids]
    if not airline_indices:
        return {tid: 0 for tid in conversation}
    first, last = airline_indices[0], airline_indices[-1]
    for i, tid in enumerate(conversation):
        if tid in airline_tweet_ids:
            annotations[tid] = 0
        elif i < first:
            annotations[tid] = 1
        elif i > last:
            annotations[tid] = 2
        else:
            annotations[tid] = 0
    return annotations

def get_conversations(airline_id, queue):
    try:
        logger.info(f"START: Processing airline {airline_id}")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        graph_name = f"Graph_{airline_id}"

        with driver.session(database=DB_NAME) as session:
            try:
                session.run(f"CALL gds.graph.drop('{graph_name}', false) YIELD graphName")
            except:
                pass

            session.run(f"""
            CALL gds.graph.project(
                '{graph_name}', 'Tweet', {{
                    REPLIES: {{type: 'REPLIES', orientation: 'UNDIRECTED'}}
                }}
            )
            """)

            session.run(f"""
            CALL gds.wcc.write('{graph_name}', {{
                writeProperty: 'componentId'
            }})
            YIELD nodePropertiesWritten
            """)

            result = session.run("""
            MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet)
            RETURN t.tweetId AS tweetId
            """, airline_id=airline_id)
            airline_tweet_ids = set(str(record["tweetId"]) for record in result)

            wcc_result = session.run("""
            MATCH (t:Tweet)
            WHERE t.componentId IS NOT NULL
            RETURN t.tweetId AS tweetId, t.componentId AS componentId
            """)

            components = defaultdict(list)
            for record in wcc_result:
                components[record["componentId"]].append(record["tweetId"])

            for tweet_list in components.values():
                if len(tweet_list) < 2:
                    continue

                reply_tree_result = session.run("""
                UNWIND $tweet_ids AS tid
                MATCH (t:Tweet {tweetId: tid})
                OPTIONAL MATCH (t)<-[:REPLIES]-(child:Tweet)
                WITH t.tweetId AS parent, collect(DISTINCT child.tweetId) AS children
                RETURN parent, children
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

                visited, ordered = set(), []
                for root in set(tweet_list) - all_children:
                    dfs(root, visited, ordered)

                ordered = list(reversed(ordered))

                # Trim
                start, end = 0, len(ordered)
                while start < end and ordered[start] in airline_tweet_ids:
                    start += 1
                while end > start and ordered[end - 1] in airline_tweet_ids:
                    end -= 1

                trimmed = ordered[start:end]
                has_airline_inside = any(tid in airline_tweet_ids for tid in trimmed)

                if has_airline_inside and len(trimmed) >= 3:
                    annotations = annotate_positions(trimmed, airline_tweet_ids)
                    logger.info(f"Airline {airline_id}: putting conversation of size {len(trimmed)} into queue")
                    queue.put((airline_id, trimmed, annotations))

            session.run(f"CALL gds.graph.drop('{graph_name}', false) YIELD graphName")
            logger.info(f"END: Finished airline {airline_id}")
    except Exception as e:
        logger.error(f"FATAL ERROR in airline {airline_id}: {e}")

def retry_on_deadlock(func, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            return func()
        except TransientError as e:
            if "DeadlockDetected" in str(e):
                logger.warning(f"Deadlock detected. Retrying... (attempt {attempt + 1})")
                time.sleep(delay * (attempt + 1))
                continue
            raise
    raise RuntimeError("Max retries exceeded due to deadlocks.")

def get_conversations_with_retry(airline_id, queue):
    return retry_on_deadlock(lambda: get_conversations(airline_id, queue))

def csv_writer(queue):
    output_dir = "import"
    os.makedirs(output_dir, exist_ok=True)

    conv_csv_path = os.path.join(output_dir, "conversations.csv")
    edges_csv_path = os.path.join(output_dir, "conversation_edges.csv")

    conv_count = 0

    with open(conv_csv_path, mode='w', newline='', encoding='utf-8') as conv_file, \
         open(edges_csv_path, mode='w', newline='', encoding='utf-8') as edge_file:

        conv_writer = csv.writer(conv_file)
        edge_writer = csv.writer(edge_file)

        conv_writer.writerow([":LABEL", ":ID(Conversation)", "airlineId"])
        edge_writer.writerow([":START_ID(Conversation)", ":END_ID(Tweet)", ":TYPE", "positionType:int"])

        conv_id = 1
        while True:
            try:
                data = queue.get(timeout=10)
            except Exception:
                logger.warning("Queue timeout reached. Waiting for more data...")
                continue

            if data == "DONE":
                break

            airline_id, tweet_ids, annotations = data
            conv_writer.writerow(["Conversation", conv_id, airline_id])
            for tid in tweet_ids:
                part_type = annotations.get(tid, 0)
                edge_writer.writerow([conv_id, tid, "PART_OF", part_type])

            conv_file.flush()
            edge_file.flush()
            os.fsync(conv_file.fileno())
            os.fsync(edge_file.fileno())

            conv_count += 1
            if conv_count % LOG_EVERY_N == 0:
                logger.info(f"{conv_count} conversations written.")

            conv_id += 1

def parallel_extract(airline_ids, queue):
    logger.info(f"Launching {MAX_WORKERS} workers for {len(airline_ids)} airlines.")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_conversations_with_retry, aid, queue): aid
            for aid in airline_ids
        }
        for future in as_completed(futures):
            aid = futures[future]
            try:
                future.result()
                logger.info(f"SUCCESS: Airline {aid} completed.")
            except Exception as e:
                logger.error(f"ERROR: Airline {aid} failed with error: {e}")
    queue.put("DONE")

if __name__ == "__main__":
    profile_output = "profile_stats.prof"
    with Manager() as manager:
        queue = manager.Queue()
        writer_process = Process(target=csv_writer, args=(queue,))
        writer_process.start()

        with cProfile.Profile() as pr:
            parallel_extract(airline_ids, queue)
            pr.dump_stats(profile_output)

        writer_process.join(timeout=600)
        if writer_process.is_alive():
            logger.error("Writer process did not finish in time.")

        print(f"Profile saved to {profile_output}")
        stats = pstats.Stats(profile_output)
        stats.strip_dirs().sort_stats('cumtime').print_stats(20)
