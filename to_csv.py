import threading
import queue
import os
import json
import csv
from roberta_sentiment import get_sentiment_batch

NUM_PRODUCERS = 6
BATCH_SIZE = 64

DATA_DIR = "data"
OUTPUT_DIR = "import"
os.makedirs(OUTPUT_DIR, exist_ok=True)

files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]
file_chunks = [files[i::NUM_PRODUCERS] for i in range(NUM_PRODUCERS)]

data_queue = queue.Queue(maxsize=1000)  # buffer size, tune as needed
stop_signal = object()  # special object to signal consumer to stop

# Shared sets for deduplication
user_ids = set()
tweet_ids = set()
posted_edges = set()
reply_edges = set()
screen_name_to_id = {}
lock = threading.Lock()

def get_full_text(tweet):
    if "retweeted_status" in tweet:
        retweeted = tweet["retweeted_status"]
        if "extended_tweet" in retweeted:
            return retweeted["extended_tweet"].get("full_text", "")
        return retweeted.get("full_text", retweeted.get("text", ""))
    else:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))

def classify_tweet_type(tweet):
    if "retweeted_status" in tweet:
        return 2
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        return 3
    elif tweet.get("in_reply_to_status_id") or tweet.get("in_reply_to_status_id_str") or tweet.get("in_reply_to_user_id"):
        return 4
    return 1

def producer(files_subset):
    tweet_batch = []
    def flush_batch(batch):
        texts = [entry["text"] for entry in batch]
        sentiments = get_sentiment_batch(texts)
        for entry, (label, expected_value) in zip(batch, sentiments):
            data_queue.put(("tweet", {
                "tid": entry["tid"],
                "text": entry["text"],
                "created_at": entry["created_at"],
                "lang": entry["lang"],
                "type": entry["type"],
                "sentiment_label": label,
                "sentiment_expected_value": expected_value,
            }))
        batch.clear()

    for file_path in files_subset:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                    if "delete" in tweet:
                        continue
                    if tweet.get("lang") != "en":
                        continue
                    created_at_str = tweet.get("created_at")
                    if not created_at_str:
                        continue

                    user = tweet.get("user", {})
                    uid = user.get("id_str")
                    tid = tweet.get("id_str")
                    if not uid or not tid:
                        continue

                    # Write users & user mentions with deduplication (lock protected)
                    with lock:
                        if uid not in user_ids:
                            data_queue.put(("user", {
                                "uid": uid,
                                "name": user.get("name", ""),
                                "screen_name": user.get("screen_name", ""),
                                "followers_count": user.get("followers_count", ""),
                                "verified": 1 if user.get("verified") else 0
                            }))
                            user_ids.add(uid)
                            screen_name_to_id[user.get("screen_name", "")] = uid

                        screen_name = user.get("screen_name", "")
                        if screen_name and screen_name not in screen_name_to_id:
                            screen_name_to_id[screen_name] = uid
                        """
                        for mention in tweet.get("entities", {}).get("user_mentions", []):
                            mention_id = mention.get("id_str")
                            mention_sn = mention.get("screen_name")
                            if mention_id and mention_sn:
                                if mention_id not in user_ids:
                                    data_queue.put(("user", {
                                        "uid": mention_id,
                                        "name": "",
                                        "screen_name": mention_sn,
                                        "followers_count": "",
                                        "verified": 0
                                    }))
                                    user_ids.add(mention_id)
                                if mention_sn not in screen_name_to_id:
                                    screen_name_to_id[mention_sn] = mention_id
                        """

                    # Collect tweets in batch
                    with lock:
                        if tid not in tweet_ids:
                            tweet_type = classify_tweet_type(tweet)
                            text = get_full_text(tweet)
                            tweet_batch.append({
                                "tid": tid,
                                "text": text,
                                "created_at": created_at_str,
                                "lang": tweet.get("lang", ""),
                                "type": tweet_type
                            })
                            tweet_ids.add(tid)

                    # Flush batch if big enough
                    if len(tweet_batch) >= BATCH_SIZE:
                        flush_batch(tweet_batch)

                    # POSTED edge
                    with lock:
                        posted = (uid, tid)
                        if posted not in posted_edges:
                            data_queue.put(("posted", (uid, tid)))
                            posted_edges.add(posted)

                    # REPLIES edge
                    replied_tid = tweet.get("in_reply_to_status_id_str")
                    with lock:
                        if tid and replied_tid and tid in tweet_ids and replied_tid in tweet_ids:
                            edge = (tid, replied_tid)
                            if edge not in reply_edges:
                                data_queue.put(("replies", edge))
                                reply_edges.add(edge)

                except Exception:
                    continue

    # Flush any remaining tweets
    if tweet_batch:
        flush_batch(tweet_batch)

def consumer():
    users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
    tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
    posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")
    replies_file = open(os.path.join(OUTPUT_DIR, "replies.csv"), "w", newline="", encoding="utf-8")

    users_writer = csv.writer(users_file)
    tweets_writer = csv.writer(tweets_file)
    posted_writer = csv.writer(posted_file)
    replies_writer = csv.writer(replies_file)

    users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
    tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "Type", "sentiment_label", "sentiment_expected_value"])
    posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])
    replies_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])

    while True:
        item = data_queue.get()
        if item is stop_signal:
            break
        kind, data = item
        if kind == "user":
            users_writer.writerow(["User", data["uid"], data["name"], data["screen_name"], data["followers_count"], data["verified"]])
        elif kind == "tweet":
            tweets_writer.writerow([
                "Tweet", data["tid"], data["text"], data["created_at"], data["lang"], data["type"],
                data["sentiment_label"], data["sentiment_expected_value"]
            ])
        elif kind == "posted":
            posted_writer.writerow([data[0], data[1], "POSTED"])
        elif kind == "replies":
            replies_writer.writerow([data[0], data[1], "REPLIES"])
        data_queue.task_done()

    users_file.close()
    tweets_file.close()
    posted_file.close()
    replies_file.close()

# Launch producers
producers = []
for i in range(NUM_PRODUCERS):
    t = threading.Thread(target=producer, args=(file_chunks[i],))
    t.start()
    producers.append(t)

# Launch consumer
consumer_thread = threading.Thread(target=consumer)
consumer_thread.start()

for t in producers:
    t.join()

# Signal consumer to stop and wait
data_queue.put(stop_signal)
consumer_thread.join()
