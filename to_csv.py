import os
import json
import csv
import re
import logging
from tqdm import tqdm
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.CRITICAL) #disregard non critical logs

# Directory paths
DATA_DIR = "data1"
OUTPUT_DIR = "import"
# Filtering on time 
FILTER_START = datetime(2000, 1, 1)
FILTER_END = datetime(2026, 12, 31)
os.makedirs(OUTPUT_DIR, exist_ok=True)

#keep unique users, tweets, posted 
user_ids = set()
tweet_ids = set()
posted_edges = set()

screen_name_to_id = dict()  # Dictionary of screen name and id, to ensure changed screen_names dont mess up the moddeling

files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]

mention_structure = re.compile(r"@(\w+)")  # Structure of @ mentions
hashtag_structure = re.compile(r"#(\w+)")  # Structure of # hashtags

'''
Find all the mentioned users using the structure of mentions in the given text
- Found this better than using the mentioned field directly
'''
def extract_mentions(text):
    return mention_structure.findall(text)

"""
Find all the mentioned users using the structure of mentions in the given text
"""
def extract_hashtag(text):
    return hashtag_structure.findall(text)

"""
Returns the text the user wrote:
- For a normal tweet: returns the full text
- For a quote tweet: returns only the user's added comment
- For a retweet: returns an empty string ??
"""
def get_full_text(tweet):
    if "retweeted_status" in tweet:
        return ""  # Retweet has no user-written content ??
    else:
        if "extended_tweet" in tweet:
            return tweet["extended_tweet"].get("full_text", "")
        return tweet.get("full_text", tweet.get("text", ""))

"""
Classifies the tweet type:
- 1 for a normal tweet
- 2 for a retweet
- 3 for a quote tweet
- 4 for reply 
- 0 outside of time period 
"""
def classify_tweet_type(tweet,created_at):
    if (FILTER_START <= created_at.replace(tzinfo=None) <= FILTER_END):
        if "retweeted_status" in tweet:
            return 2  # Retweet
        elif tweet.get("is_quote_status") and "quoted_status" in tweet:
            return 3  # Quote tweet
        elif tweet.get("in_reply_to_status_id") is not None or tweet.get("in_reply_to_status_id_str"):
            return 4  # Reply
        return 1  # Normal tweet
    else:
        return 0 #Outside of the given time period

# WE DEAL with the JSONs in passes to ensure population of variables for keeping unique ids and valid connections

# First pass we populate: USERS, TWEETS, POSTED, HASHTAG

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

# Write headers #disregard :LABEL md :TYPE 
users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "Type"])
hashtag_writer.writerow([":LABEL", ":ID(Hashtag)", "hashtag_text", "counter"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])

for file_path in tqdm(files, desc="First pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)

                if list(tweet.keys())[0] == "delete": #Skipping delete events in jsons
                    continue
                
                created_at_str = tweet.get("created_at")
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                if not created_at_str: #skip if it does not have created_at field
                    continue
                
                if tweet.get("lang") != "en":
                    continue  # Skip non-English tweets

                #Creating the objects
                user = tweet.get("user", {})
                uid = user.get("id_str")
                tid = tweet.get("id_str")
                created_at_str = tweet.get("created_at")

                if not uid or not tid: #empty, skip
                    continue
                
                #Create Users
                if uid not in user_ids: #uid not in the dictionary yet 
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", ""), user.get("followers_count", ""), 1 if user.get("verified") else 0])
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid #populating the dictionary with the first witness  

                screen_name = user.get("screen_name", "")
                if screen_name and screen_name not in screen_name_to_id:
                    screen_name_to_id[screen_name] = uid

                entities = tweet.get("entities", {})
                user_mentions = entities.get("user_mentions", [])
                for mention in user_mentions:
                    mention_id = mention.get("id_str")
                    mention_screen_name = mention.get("screen_name")
                    if mention_id and mention_screen_name:
                        if mention_id not in user_ids:
                            users_writer.writerow(["User", mention_id, "", mention_screen_name, "", 0])
                            user_ids.add(mention_id)
                        if mention_screen_name not in screen_name_to_id:
                            screen_name_to_id[mention_screen_name] = mention_id

                #Create Tweets
                if tid not in tweet_ids:
                    text = get_full_text(tweet)
                    tweet_type = classify_tweet_type(tweet,created_at)
                    tweets_writer.writerow(["Tweet", tid, text, created_at_str, tweet.get("lang", ""), tweet_type])
                    tweet_ids.add(tid)

                # Also add mentioned users from text that may not appear in user_mentions
                text = get_full_text(tweet)
                mentions_in_text = extract_mentions(text)
                for screen_name in mentions_in_text:
                    if screen_name not in screen_name_to_id:
                        continue  # No known ID for this screen_name
                    mention_id = screen_name_to_id[screen_name]
                    if mention_id not in user_ids:
                        users_writer.writerow(["User", mention_id, "", screen_name, "", 0])
                        user_ids.add(mention_id)

                #Keep updating hasthags
                hashtags = extract_hashtag(get_full_text(tweet))
                hashtag_counter.update(hashtags)

                #Posted Relation
                posted = (uid, tid)
                if posted not in posted_edges:
                    posted_writer.writerow([uid, tid, "POSTED"])
                    posted_edges.add(posted)

            except Exception:
                continue

# Insert hashtags into hashtag.csv
for tag, count in hashtag_counter.items():
    hashtag_writer.writerow(["Hashtag", tag, tag, count])

# Closing files, we keep tweets open to accomadate generated ones 
users_file.close()
posted_file.close()
hashtag_file.close()
tweets_file.close()

# SECOND PASS: MENTIONS, RETWEETS, QUOTED, CONTAINS, generated tweets

#Ensure uniquness
mention_edges = set()
retweet_edges = set()
quoted_edges = set()
contain_edges = set()
reply_edges = set()

# Opening output files
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline="", encoding="utf-8")
retweets_file = open(os.path.join(OUTPUT_DIR, "retweets.csv"), "w", newline="", encoding="utf-8")
quotes_file = open(os.path.join(OUTPUT_DIR, "quoted.csv"), "w", newline="", encoding="utf-8")
contains_file = open(os.path.join(OUTPUT_DIR, "contain.csv"), "w", newline="", encoding="utf-8")
mentions_writer = csv.writer(mentions_file)
retweets_writer = csv.writer(retweets_file)
quotes_writer = csv.writer(quotes_file)
contains_writer = csv.writer(contains_file)

replies_file = open(os.path.join(OUTPUT_DIR, "replies.csv"), "w", newline="", encoding="utf-8")
replies_writer = csv.writer(replies_file)

# Write headers
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
retweets_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])
quotes_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)",  ":TYPE"])
contains_writer.writerow([":START_ID(Tweet)", ":END_ID(Hashtag)", ":TYPE"])
replies_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])

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

                uid = tweet.get("user", {}).get("id_str")
                tid = tweet.get("id_str")

                if not uid or not tid:
                    continue

                # Mentions
                text = get_full_text(tweet)
                mentions = extract_mentions(text)
                for mentioned_screen_name in mentions:
                    mentioned_uid = screen_name_to_id.get(mentioned_screen_name)

                    # Critical fix: skip if not a known user ID
                    if not mentioned_uid or mentioned_uid not in user_ids:
                        continue
                    
                    edge = (tid, mentioned_uid)
                    if edge not in mention_edges:
                        mentions_writer.writerow([tid, mentioned_uid, "MENTIONED"])
                        mention_edges.add(edge)


                # Retweets
                if "retweeted_status" in tweet:
                    # It's a retweet
                    pointing_to = tweet["retweeted_status"]
                    original_tid = pointing_to.get("id_str")
                    if original_tid in tweet_ids:
                        edge = (tid, original_tid)
                        if edge not in retweet_edges:
                            retweets_writer.writerow([tid, original_tid, "RETWEETS"]) #Header Retweets
                            retweet_edges.add(edge)


                # === Quotes
                if "retweeted_status" not in tweet and tweet.get("is_quote_status") and "quoted_status" in tweet:
                    quoted_tweet = tweet["quoted_status"]
                    quoted_tid = tweet["quoted_status_id_str"]
                    if quoted_tid in tweet_ids:
                        edge = (tid, quoted_tid)
                        if edge not in quoted_edges:
                            quotes_writer.writerow([tid, quoted_tid, "QUOTES"]) #Header Quotes
                            quoted_edges.add(edge)

                    # === Replies
                if tweet.get("in_reply_to_status_id_str"):
                    replied_tid = tweet.get("in_reply_to_status_id_str")
                    if replied_tid in tweet_ids:
                        edge = (tid, replied_tid)
                        if edge not in reply_edges:
                            replies_writer.writerow([tid, replied_tid, "REPLIES"])
                            reply_edges.add(edge)

                # === Contains hashtags
                hashtags = extract_hashtag(text)
                for hashtag in hashtags:
                    if hashtag and hashtag in hashtag_counter:
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
replies_file.close()