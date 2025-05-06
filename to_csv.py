import os
import json
import csv
import re
import logging
from tqdm import tqdm
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.CRITICAL)

# === Paths ===
DATA_DIR = "data"
OUTPUT_DIR = "import"

FILTER_START = datetime(2000, 1, 1)
FILTER_END = datetime(2026, 12, 31)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Global sets to track uniqueness ===
user_ids = set()
tweet_ids = set()
posted_edges = set()

raw_total = 0
valid_total = 0
original_total = 0

screen_name_to_id = dict()

mention_edges = set()
retweet_edges = set()
quoted_edges = set()
contain_edges = set()

hashtag_counter = Counter()

# === Regex ===
mention_structure = re.compile(r"@(\w+)")
hashtag_structure = re.compile(r"#(\w+)")

# === Helpers ===
def extract_mentions(text):
    return mention_structure.findall(text)

def extract_hashtag(text):
    return hashtag_structure.findall(text)

def get_full_text(tweet):
    if "retweeted_status" in tweet:
        return ""
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))
    else:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))

def get_favorite_count(tweet):
    if "retweeted_status" in tweet:
        return tweet["retweeted_status"].get("favorite_count", 0)
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        return tweet["quoted_status"].get("favorite_count", 0)
    return tweet.get("favorite_count", 0)

def classify_tweet_type(tweet):
    if "retweeted_status" in tweet:
        return 2
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        return 3
    return 1

# === Get all JSON files ===
files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]

# === First Pass: Nodes and Posted Relationships ===
users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
hashtag_file = open(os.path.join(OUTPUT_DIR, "hashtag.csv"), "w", newline="", encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")

users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
hashtag_writer = csv.writer(hashtag_file)
posted_writer = csv.writer(posted_file)

# Write headers
users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "favorite_count", "Type"])
hashtag_writer.writerow([":LABEL", ":ID(Hashtag)", "hashtag_text", "counter"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])

for file_path in tqdm(files, desc="First pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)

                raw_total += 1

                if list(tweet.keys())[0] == "delete":
                    continue

                created_at_str = tweet.get("created_at")
                if not created_at_str:
                    continue
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                if not (FILTER_START <= created_at.replace(tzinfo=None) <= FILTER_END):
                    continue

                user = tweet.get("user", {})
                uid = user.get("id_str")
                tid = tweet.get("id_str")

                if not uid or not tid:
                    continue

                valid_total += 1
                if "retweeted_status" in tweet or (tweet.get("is_quote_status") and "quoted_status" in tweet):
                    continue
                original_total += 1


                if uid not in user_ids:
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", ""),
                                            user.get("followers_count", ""), 1 if user.get("verified") else 0])
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid

                if tid not in tweet_ids:
                    text = get_full_text(tweet)
                    favorite_count = get_favorite_count(tweet)
                    tweet_type = classify_tweet_type(tweet)
                    tweets_writer.writerow(["Tweet", tid, text, created_at_str, tweet.get("lang", ""), favorite_count, tweet_type])
                    tweet_ids.add(tid)

                hashtags = extract_hashtag(get_full_text(tweet))
                hashtag_counter.update(hashtags)

                posted = (uid, tid)
                if posted not in posted_edges:
                    posted_writer.writerow([uid, tid, "POSTED"])
                    posted_edges.add(posted)

            except Exception as e:
                continue

# Write hashtags
for tag, count in hashtag_counter.items():
    hashtag_writer.writerow(["Hashtag", tag, tag, count])

# Close files
users_file.close()
tweets_file.close()
hashtag_file.close()
posted_file.close()

# === Second Pass: Relationships ===
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline="", encoding="utf-8")
retweets_file = open(os.path.join(OUTPUT_DIR, "retweets.csv"), "w", newline="", encoding="utf-8")
quotes_file = open(os.path.join(OUTPUT_DIR, "quoted.csv"), "w", newline="", encoding="utf-8")
contains_file = open(os.path.join(OUTPUT_DIR, "contain.csv"), "w", newline="", encoding="utf-8")

mentions_writer = csv.writer(mentions_file)
retweets_writer = csv.writer(retweets_file)
quotes_writer = csv.writer(quotes_file)
contains_writer = csv.writer(contains_file)

# Write headers
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
retweets_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])
quotes_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])
contains_writer.writerow([":START_ID(Tweet)", ":END_ID(Hashtag)", ":TYPE"])

for file_path in tqdm(files, desc="Second pass"):
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
                if not (FILTER_START <= created_at.replace(tzinfo=None) <= FILTER_END):
                    continue

                uid = tweet.get("user", {}).get("id_str")
                tid = tweet.get("id_str")
                if not tid:
                    continue

                # --- Mentions ---
                text = get_full_text(tweet)
                mentions = extract_mentions(text)
                for mentioned_screen_name in mentions:
                    mentioned_uid = screen_name_to_id.get(mentioned_screen_name)
                    if mentioned_uid:
                        edge = (tid, mentioned_uid)
                        if edge not in mention_edges:
                            mentions_writer.writerow([tid, mentioned_uid, "MENTIONED"])
                            mention_edges.add(edge)

                # --- Retweets ---
                if "retweeted_status" in tweet:
                    original_tweet = tweet["retweeted_status"]
                    original_tid = original_tweet.get("id_str")
                    if original_tid and tid and original_tid in tweet_ids:
                        edge = (tid, original_tid)
                        if edge not in retweet_edges:
                            retweets_writer.writerow([tid, original_tid, "RETWEETED"])
                            retweet_edges.add(edge)

                # --- Quotes ---
                if tweet.get("is_quote_status") and "quoted_status" in tweet:
                    quoted_tweet = tweet["quoted_status"]
                    quoted_tid = quoted_tweet.get("id_str")
                    if quoted_tid and tid and quoted_tid in tweet_ids:
                        edge = (tid, quoted_tid)
                        if edge not in quoted_edges:
                            quotes_writer.writerow([tid, quoted_tid, "QUOTED"])
                            quoted_edges.add(edge)

                # --- Hashtags Contain ---
                hashtags = extract_hashtag(text)
                for tag in hashtags:
                    edge = (tid, tag)
                    if edge not in contain_edges:
                        contains_writer.writerow([tid, tag, "CONTAINS"])
                        contain_edges.add(edge)

            except Exception as e:
                continue

# Close second pass files
mentions_file.close()
retweets_file.close()
quotes_file.close()
contains_file.close()

print("Number of raw tweets:", raw_total)
print("Number of valid tweets:", valid_total)
print("Number of original tweets:", original_total)

json_line_count = 0
for file in os.listdir("data"):
    if file.endswith(".json"):
        with open(os.path.join("data", file), "r", encoding="utf-8") as f:
            for line in f:
                json_line_count += 1
print("Tweets before cleaning:", json_line_count)

cleaned_lines = 0
with open("import/tweets.csv", "r", encoding="utf-8") as f:
    for line in f:
        cleaned_lines += 1
cleaned_lines -= 1
print("Tweets after cleaning:", cleaned_lines)
