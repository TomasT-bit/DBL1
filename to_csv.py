import os
import json
import csv
import logging
from datetime import datetime
from roberta_sentiment import get_sentiment_batch  

logging.basicConfig(level=logging.CRITICAL) #debbuging

DATA_DIR = "data" #name of the directory with json files 
OUTPUT_DIR = "import"
os.makedirs(OUTPUT_DIR, exist_ok=True)


""" 
Defining sets in order to ensure Uniquness
"""
user_ids = set()
tweet_ids = set()
posted_edges = set()
screen_name_to_id = dict()

files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]

#Gets the correct text for each tweet type 
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


"""
Classifying a tweet based on its type 
"""
def classify_tweet_type(tweet):
    if "retweeted_status" in tweet:
        return 2 #Retweet
    elif tweet.get("is_quote_status") and "quoted_status" in tweet: #Quote
        return 3
    elif tweet.get("in_reply_to_status_id") or tweet.get("in_reply_to_status_id_str") or tweet.get("in_reply_to_user_id"):
        return 4 #Reply
    return 1 #Original

users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")

users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)

users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "Type", "sentiment_label", "sentiment_expected_value"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])

BATCH_SIZE = 64 #batch size for the roBERTA
tweet_batch = []

def flush_batch(batch):
    texts = [entry["text"] for entry in batch]
    sentiments = get_sentiment_batch(texts)
    for entry, (label, expected_value) in zip(batch, sentiments):
        tweets_writer.writerow([
            "Tweet",
            entry["tid"],
            entry["text"],
            entry["created_at"],
            entry["lang"],
            entry["type"],
            label,
            expected_value
        ])
        tweet_ids.add(entry["tid"])

for file_path in files:
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if list(tweet.keys())[0] == "delete":
                    continue

                created_at_str = tweet.get("created_at")
                if not created_at_str:
                    continue
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                if tweet.get("lang") != "en":
                    continue

                user = tweet.get("user", {})
                uid = user.get("id_str")
                tid = tweet.get("id_str")
                if not uid or not tid:
                    continue

                if uid not in user_ids:
                    users_writer.writerow([
                        "User",
                        uid,
                        user.get("name", ""),
                        user.get("screen_name", ""),
                        user.get("followers_count", ""),
                        1 if user.get("verified") else 0
                    ])
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid

                screen_name = user.get("screen_name", "")
                if screen_name and screen_name not in screen_name_to_id:
                    screen_name_to_id[screen_name] = uid

                entities = tweet.get("entities", {})
                user_mentions = entities.get("user_mentions", [])
                for mention in user_mentions:
                    mention_id = mention.get("id_str")
                    mention_screen_name = mention.get("screen_name")
                    if mention_id and mention_screen_name and mention_id not in user_ids:
                        users_writer.writerow(["User", mention_id, "", mention_screen_name, "", 0])
                        user_ids.add(mention_id)
                    if mention_screen_name not in screen_name_to_id:
                        screen_name_to_id[mention_screen_name] = mention_id

                if tid not in tweet_ids:
                    text = get_full_text(tweet)
                    tweet_type = classify_tweet_type(tweet)

                    tweet_batch.append({
                        "tid": tid,
                        "text": text,
                        "created_at": created_at_str,
                        "lang": tweet.get("lang", ""),
                        "type": tweet_type
                    })

                    if len(tweet_batch) >= BATCH_SIZE:
                        flush_batch(tweet_batch)
                        tweet_batch = []

                posted = (uid, tid)
                if posted not in posted_edges:
                    posted_writer.writerow([uid, tid, "POSTED"])
                    posted_edges.add(posted)

            except Exception:
                continue

if tweet_batch:
    flush_batch(tweet_batch)

users_file.close()
tweets_file.close()
posted_file.close()

# SECOND PASS: Create REPLIES relationships
reply_edges = set()
replies_file = open(os.path.join(OUTPUT_DIR, "replies.csv"), "w", newline="", encoding="utf-8")
replies_writer = csv.writer(replies_file)
replies_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])

with open(os.path.join(OUTPUT_DIR, "replies.csv"), "w", newline="", encoding="utf-8") as replies_file:
    replies_writer = csv.writer(replies_file)
    replies_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])

    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                    if list(tweet.keys())[0] == "delete":
                        continue

                    if tweet.get("lang") != "en":
                        continue

                    tid = tweet.get("id_str")
                    replied_tid = tweet.get("in_reply_to_status_id_str")

                    if tid and replied_tid and tid in tweet_ids and replied_tid in tweet_ids:
                        edge = (tid, replied_tid)
                        if edge not in reply_edges:
                            replies_writer.writerow([tid, replied_tid, "REPLIES"])
                            reply_edges.add(edge)

                except Exception:
                    continue
replies_file.close()