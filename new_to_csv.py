import os
import json
import csv
import re
import logging
from tqdm import tqdm
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.CRITICAL)

"""
Conversation: exits reply into or reply out of 
"""
"""
NODES: 
users:  "userId:ID(User)", "name", "screen_name", "followers", "verified"])  DONE

tweets: "tweetId:ID(Tweet)", "text", "created_at", "lang", "Type"]
                                                                                        -1 Original
                                                                                        -2 Retweet
                                                                                        -3 Quote  DONE
hashtg: ":ID(Hashtag)", "Hashtag", "counter"]

RELATIONS: 
Posted: ":START_ID(User)", ":END_ID(Tweet)", ":TYPE"]) DONE
Mentions: ":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
Retweets: ":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"]) - from a tweet to the tweet it is retweeting
Quotes: ":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"]) - tweet quoting another tweet
Contains: from tweet to hashtag
"""

# Directory paths
DATA_DIR = "data"
OUTPUT_DIR = "import"

# Filtering on time 
FILTER_START = datetime(2000, 1, 1)
FILTER_END = datetime(2026, 12, 31)
os.makedirs(OUTPUT_DIR, exist_ok=True)

#keep unique users, tweets, posted 
user_ids = set()
tweet_ids = set()
posted_edges = set()

screen_name_to_id = dict()  # Dictionary of screen name and id

files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]

mention_structure = re.compile(r"@(\w+)")  # Structure of @ mentions
hashtag_structure = re.compile(r"#(\w+)")  # Structure of #hashtags

'''
Find all the mentioned users using the structure of mentions in the given text
- Found this better than using the mentioned field directly
'''
def extract_mentions(text):
    return mention_structure.findall(text)

def extract_hashtag(text):
    return hashtag_structure.findall(text)

"""
Returns the text the user wrote:
- For a normal tweet: returns the full text
- For a quote tweet: returns only the user's added comment
- For a retweet: returns an empty string
"""
def get_full_text(tweet):
    if "retweeted_status" in tweet:
        return ""  # Retweet has no user-written content
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))
    else:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))

"""
Classifies the tweet type:
- 1 for a normal tweet
- 2 for a retweet
- 3 for a quote tweet
"""
def classify_tweet_type(tweet):
    if "retweeted_status" in tweet:
        return 2  # Retweet
    elif tweet.get("is_quote_status") and "quoted_status" in tweet:
        return 3  # Quote tweet
    return 1  # Normal tweet

def get_favorite_count(tweet):
    if "favorite_count" in tweet:
        return tweet.get("favorite_count", 0)
    elif "extended_tweet" in tweet:
        return tweet["extended_tweet"].get("favorite_count", 0)
    else:
        return 0


# WE DEAL with the JSONs in passes to ensure population of variables for keeping unique ids and valid connections

# First pass: USERS, TWEETS, POSTED, HASHTAG

# Open output files
users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
hashtag_file = open(os.path.join(OUTPUT_DIR, "hashtag.csv"), "w", newline="", encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")
users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)
hashtag_writer = csv.writer(hashtag_file)

# counter for hashtags
hashtag_counter = Counter()

# Write headers
users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "Type"])
hashtag_writer.writerow([":LABEL", ":ID(Hashtag)", "hashtag_text", "counter"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])

for file_path in tqdm(files, desc="First pass"):
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

                user = tweet.get("user", {})
                uid = user.get("id_str")
                tid = tweet.get("id_str")

                if not uid or not tid:
                    continue

                if uid not in user_ids:
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", ""), user.get("followers_count", ""), 1 if user.get("verified") else 0])
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

            except Exception:
                continue

# Insert hashtags into hashtag.csv
for tag, count in hashtag_counter.items():
    hashtag_writer.writerow(["Hashtag", tag, tag, count])

# Closing files
users_file.close()
tweets_file.close()
posted_file.close()
hashtag_file.close()

# SECOND PASS: MENTIONS, RETWEETS, QUOTED, CONTAINS

mention_edges = set()
retweet_edges = set()
quoted_edges = set()
contain_edges = set()

# Opening output files
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

                if not uid or not tid:
                    continue

                # === Mentions
                text = get_full_text(tweet)
                mentions = extract_mentions(text)
                for mentioned_screen_name in mentions:
                    mentioned_uid = screen_name_to_id.get(mentioned_screen_name)
                    if mentioned_uid:
                        edge = (tid, mentioned_uid)
                        if edge not in mention_edges:
                            mentions_writer.writerow([tid, mentioned_uid, "MENTIONED"])
                            mention_edges.add(edge)

                # === Retweets
                if "retweeted_status" in tweet:
                    original_tweet = tweet["retweeted_status"]
                    original_tid = original_tweet.get("id_str")
                    if original_tid and original_tid in tweet_ids:
                        edge = (tid, original_tid)
                        if edge not in retweet_edges:
                            retweets_writer.writerow([tid, original_tid, "RETWEETED"])
                            retweet_edges.add(edge)

                # === Quotes
                if tweet.get("is_quote_status") and "quoted_status" in tweet:
                    quoted_tweet = tweet["quoted_status"]
                    quoted_tid = quoted_tweet.get("id_str")
                    if quoted_tid and quoted_tid in tweet_ids:
                        edge = (tid, quoted_tid)
                        if edge not in quoted_edges:
                            quotes_writer.writerow([tid, quoted_tid, "QUOTED"])
                            quoted_edges.add(edge)

                # === Contains hashtags
                hashtags = extract_hashtag(text)
                for hashtag in hashtags:
                    if hashtag in hashtag_counter:
                        edge = (tid, hashtag)
                        if edge not in contain_edges:
                            contains_writer.writerow([tid, hashtag, "CONTAINS"])
                            contain_edges.add(edge)

            except Exception:
                continue

# Closing second pass files
mentions_file.close()
retweets_file.close()
quotes_file.close()
contains_file.close()
