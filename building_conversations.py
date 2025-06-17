import os
import csv
import time
import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Process, Manager
from neo4j import GraphDatabase
import pandas as pd
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(processName)s - %(message)s"
)
logger = logging.getLogger(__name__)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
DB_NAME = "twitter"
MAX_WORKERS = 6

airline_ids = [
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
]

candidate_labels = [
    "delayed flight", "lost baggage", "poor customer service", "ticket issue",
    "other", "cancelled flight", "uncomfortable flight", "trouble with refunds",
    "discrimination"
]

device = "cuda" if torch.cuda.is_available() else "cpu"
logger.info(f"Device set to use {device}")

# Load sentiment model
sentiment_model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(sentiment_model_name)
model = AutoModelForSequenceClassification.from_pretrained(sentiment_model_name).to(device)
model.eval()

# Use Facebook's bart-large-mnli for zero-shot classification (fast & reliable)
zero_shot_classifier = pipeline(
    "zero-shot-classification",
    model="facebook/bart-large-mnli",
    device=0 if device == "cuda" else -1,
    batch_size=16
)


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
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    graph_name = f"Graph_{airline_id}"

    with driver.session(database=DB_NAME) as session:
        try:
            session.run("CREATE INDEX tweet_id_index IF NOT EXISTS FOR (t:Tweet) ON (t.tweetId)")
            session.run("CREATE INDEX user_id_index IF NOT EXISTS FOR (u:User) ON (u.userId)")
        except Exception as e:
            logger.warning(f"Index creation issue: {e}")

        try:
            session.run(f"CALL gds.graph.drop('{graph_name}')")
        except Exception:
            pass  # graph may not exist, ignore

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
        """)

        result = session.run("""
        MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet)
        RETURN t.tweetId AS tweetId
        """, airline_id=airline_id)
        airline_tweet_ids = set(str(r["tweetId"]) for r in result)

        wcc_result = session.run("""
        MATCH (t:Tweet)
        WHERE exists(t.componentId)
        RETURN t.tweetId AS tweetId, t.componentId AS componentId
        """)
        components = defaultdict(list)
        for r in wcc_result:
            components[r["componentId"]].append(r["tweetId"])

        for tweet_list in components.values():
            if len(tweet_list) < 2:
                continue

            reply_tree_result = session.run("""
            UNWIND $tweet_ids AS tid
            MATCH (t:Tweet {tweetId: tid})
            OPTIONAL MATCH (t)<-[:REPLIES]-(child:Tweet)
            RETURN t.tweetId AS parent, collect(DISTINCT child.tweetId) AS children
            """, tweet_ids=tweet_list)

            children_map = {r["parent"]: r["children"] for r in reply_tree_result}
            all_children = {c for clist in children_map.values() for c in clist}

            def dfs(node, visited, result):
                if node in visited:
                    return
                visited.add(node)
                for child in children_map.get(node, []):
                    dfs(child, visited, result)
                result.append(node)

            visited, ordered = set(), []
            roots = set(tweet_list) - all_children
            for root in roots:
                dfs(root, visited, ordered)

            ordered = list(reversed(ordered))
            start, end = 0, len(ordered)
            while start < end and ordered[start] in airline_tweet_ids:
                start += 1
            while end > start and ordered[end - 1] in airline_tweet_ids:
                end -= 1

            trimmed = ordered[start:end]
            has_airline = any(tid in airline_tweet_ids for tid in trimmed)
            if has_airline and len(trimmed) >= 3:
                annotations = annotate_positions(trimmed, airline_tweet_ids)

                time_result = session.run("""
                MATCH (t:Tweet)
                WHERE t.tweetId IN [$start_tid, $end_tid]
                RETURN t.tweetId AS tweetId, t.created_at AS created_at
                """, start_tid=trimmed[0], end_tid=trimmed[-1])
                time_map = {r["tweetId"]: r["created_at"] for r in time_result}

                queue.put({
                    "airline_id": airline_id,
                    "tweet_ids": trimmed,
                    "annotations": annotations,
                    "start_time": time_map.get(trimmed[0], None),
                    "end_time": time_map.get(trimmed[-1], None)
                })

        try:
            session.run(f"CALL gds.graph.drop('{graph_name}')")
        except Exception:
            pass  # ignore drop errors

    driver.close()


def parallel_extract(queue):
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_conversations, aid, queue) for aid in airline_ids]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in get_conversations: {e}")
    queue.put("DONE")


def writer(queue):
    output_dir = "import"
    os.makedirs(output_dir, exist_ok=True)
    conv_file_path = os.path.join(output_dir, "conversations.csv")
    edge_file_path = os.path.join(output_dir, "conversation_edges.csv")

    with open(conv_file_path, "w", newline='', encoding="utf-8") as conv_file, \
         open(edge_file_path, "w", newline='', encoding="utf-8") as edge_file:

        conv_writer = csv.writer(conv_file)
        edge_writer = csv.writer(edge_file)
        conv_writer.writerow([":LABEL", ":ID(Conversation)", "airlineId", "start", "end"])
        edge_writer.writerow([":START_ID(Conversation)", ":END_ID(Tweet)", ":TYPE", "positionType:int"])

        conv_id = 1
        while True:
            data = queue.get()
            if data == "DONE":
                break

            conv_writer.writerow(["Conversation", f"c{conv_id}", data["airline_id"], data["start_time"], data["end_time"]])
            for tid in data["tweet_ids"]:
                edge_writer.writerow([f"c{conv_id}", tid, "PART_OF", data["annotations"].get(tid, 0)])

            conv_id += 1


def run_sentiment_and_zero_shot():
    csv_path = os.path.join("import", "conversations.csv")
    edge_path = os.path.join("import", "conversation_edges.csv")

    logger.info("Loading conversations CSV and edges")
    if not os.path.exists(edge_path):
        logger.error(f"Missing file: {edge_path}")
        return

    edge_df = pd.read_csv(edge_path)

    tweets_path = "tweets.csv"
    if not os.path.exists(tweets_path):
        logger.error(f"Missing tweets CSV file: {tweets_path}")
        return

    tweets_df = pd.read_csv(tweets_path)
    merged = edge_df.merge(tweets_df, left_on=":END_ID(Tweet)", right_on="tweetId", how="left")
    merged.fillna("", inplace=True)

    # Clean text for sentiment model
    merged["clean_text"] = merged["text"].str.replace(r'@\S+', '@user', regex=True)\
                                         .str.replace(r'http\S+', 'http', regex=True)

    texts = merged["clean_text"].tolist()

    sentiment_scores = []
    batch_size = 512

    logger.info("Starting sentiment analysis")
    with torch.no_grad():
        for i in tqdm(range(0, len(texts), batch_size)):
            batch_texts = texts[i:i+batch_size]
            encoded = tokenizer(batch_texts, padding=True, truncation=True, max_length=128, return_tensors="pt")
            encoded = {k: v.to(device) for k, v in encoded.items()}
            outputs = model(**encoded)
            probs = F.softmax(outputs.logits, dim=1)
            sentiment_scores.extend(probs.cpu().tolist())

    merged["sentiment_neg"] = [s[0] for s in sentiment_scores]
    merged["sentiment_neu"] = [s[1] for s in sentiment_scores]
    merged["sentiment_pos"] = [s[2] for s in sentiment_scores]

    logger.info("Starting zero-shot classification")
    # Run zero-shot classification in batches
    zero_shot_results = []
    chunk_size = 64
    for i in tqdm(range(0, len(texts), chunk_size)):
        batch = texts[i:i+chunk_size]
        results = zero_shot_classifier(batch, candidate_labels)
        if isinstance(results, dict):
            zero_shot_results.append(results)
        else:
            zero_shot_results.extend(results)

    # Extract scores and labels from zero-shot output
    merged["zero_shot_label"] = [r["labels"][0] for r in zero_shot_results]
    merged["zero_shot_score"] = [r["scores"][0] for r in zero_shot_results]

    merged.to_csv(os.path.join("import", "conversations_annotated.csv"), index=False)
    logger.info("Sentiment and zero-shot classification complete. Output saved.")


if __name__ == "__main__":
    manager = Manager()
    q = manager.Queue()

    writer_process = Process(target=writer, args=(q,))
    writer_process.start()

    parallel_extract(q)

    writer_process.join()

    run_sentiment_and_zero_shot()
