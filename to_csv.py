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
users:  "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets: "tweetId:ID(Tweet)", "text", "created_at", "lang", "favorited_count"])
hashtag: ":ID(Hashtag)", "counter"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])


RELATIONS: 
Posted":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])
Mentioned: ":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
Reteeted: ":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])  
Quoted: ":START_ID(User)", ":END_ID(Tweet)", ":TYPE", "extra_text:STRING"])
Contains: ":START_ID(Tweet)", ":END_ID(Hashtag)", ":TYPE"])
"""

#Directory paths
DATA_DIR = "data1" 
OUTPUT_DIR = "import"

FILTER_START = datetime(2000, 1, 1)
FILTER_END = datetime(2026, 12, 31)
os.makedirs(OUTPUT_DIR, exist_ok=True)

#Variables to keep unique users, tweets, posted 
user_ids = set()
tweet_ids = set()
posted_edges = set()
duplicate_posted = 0
screen_name_to_id = dict() # dictonary of screen name and id ensures robustness against chaning names over time 


files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")] #makes path to all jsosn inside the directory

mention_structure = re.compile(r"@(\w+)") #Structure of @ ppl
hashtag_structure = re.compile(r"#(\w+)") #Structure of # txt

'''
Find all the mentioned users usign the structure of mentions in the given text
-Found this better than using the field mentioned directly
''' 
def extract_mentions(text):
    return mention_structure.findall(text)

def extract_hashtag(text):
    return hashtag_structure.findall(text)

'''
Find the full text of tweets
- For regular tweets:
     - If the tweet contains an "extended_tweet" field, the full text is obtained from the "full_text" field.
     - If the "extended_tweet" field is not present, the function falls back to the "text" field (for tweets without extended text).

- For retweets:
     - If the tweet is a retweet (i.e., it contains the "retweeted_status" field):
        - The function checks if the original tweet has an "extended_tweet". If so, it returns the "full_text" from the original tweet.
        - If there is no "extended_tweet", the function falls back to the "full_text" or "text" field from the retweeted tweet.

'''
def get_full_text(tweet):
    if "retweeted_status" in tweet:
        rt_status = tweet["retweeted_status"]
        if "extended_tweet" in rt_status:
            return rt_status["extended_tweet"]["full_text"]
        return rt_status.get("full_text", rt_status.get("text", ""))
    elif "extended_tweet" in tweet:
        return tweet["extended_tweet"]["full_text"]
    return tweet.get("full_text", tweet.get("text", ""))

# DEAL with the jsosn in passes to ensure population of variables for keeping unique ids and valid connections

#First pass: USERS, TWEETS, POSTED,HASHTAG

#Open folders 
users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
hashtag_file = open(os.path.join(OUTPUT_DIR, "hashtag.csv"), "w", newline="", encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")
users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)
hashtag_writer= csv.writer(hashtag_file)

#counter for the hashtag
hashtag_counter = Counter()


#Initial write for labels 
users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers", "verified"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang", "favorite_count"]) 
hashtag_writer.writerow([":LABEL", ":ID(Hashtag)", "counter"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])


for file_path in tqdm(files, desc="First pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if list(tweet.keys())[0] == "delete": #dont consider delete events, we keep the nodes 
                    continue

                created_at_str = tweet.get("created_at")
                if not created_at_str:
                    continue
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y") #proper structure
                if not (FILTER_START <= created_at.replace(tzinfo=None) <= FILTER_END):
                    continue

                user = tweet.get("user", {})
                uid = user.get("id_str") #userid
                tid = tweet.get("id_str") #twitterid

                #empty 
                if not uid or not tid:
                    continue
                

                #uid is unique
                if uid not in user_ids:
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", ""),user.get("followers_count",""), 1 if user.get("verified") else 0])
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid

                #tid is unique
                if tid not in tweet_ids:
                    text = get_full_text(tweet)
                    hashtags= extract_hashtag(text)
                    favorite_count = tweet.get("favorite_count", 0)
                    print(favorite_count)
                    tweets_writer.writerow(["Tweet", tid, text, created_at_str, tweet.get("lang", ""), favorite_count])
                    tweet_ids.add(tid)
                # Counting hashtags 
                hashtags = extract_hashtag(text)
                hashtag_counter.update(hashtags)


                #Create posted, and if unique add 
                posted = (uid, tid)
                if posted not in posted_edges:
                    posted_writer.writerow([uid, tid, "POSTED"])
                    posted_edges.add(posted)
                else:
                    duplicate_posted += 1

            except Exception:
                continue

#Insert the tags into the hashtag.csv
for tag, count in hashtag_counter.items():
    hashtag_writer.writerow(["Hashtag", tag, count])


#Closing files
users_file.close()
tweets_file.close()
posted_file.close()
hashtag_file.close()

#Second pass taaking care of mentions and retweets 

mention_edges = set()
retweet_edges = set()
quoted_edges = set()
contain_edges = set()
mention_skipped = 0
retweet_skipped = 0
duplicate_mentions = 0
duplicate_retweets = 0
duplicate_quoted = 0
duplicate_contains = 0  # Track duplicate contains relationships

# Opening files
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline="", encoding="utf-8")
retweets_file = open(os.path.join(OUTPUT_DIR, "retweets.csv"), "w", newline="", encoding="utf-8")
mentions_writer = csv.writer(mentions_file)
retweets_writer = csv.writer(retweets_file)
quoted_file = open(os.path.join(OUTPUT_DIR, "quoted.csv"), "w", newline="", encoding="utf-8")
contain_file = open(os.path.join(OUTPUT_DIR, "contain.csv"), "w", newline="", encoding="utf-8")
quoted_writer = csv.writer(quoted_file)
contain_writer = csv.writer(contain_file)

# Labels
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
retweets_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])  
quoted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE", "extra_text:STRING"])
contain_writer.writerow([":START_ID(Tweet)", ":END_ID(Hashtag)", ":TYPE"])

for file_path in tqdm(files, desc="Second pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if list(tweet.keys())[0] == "delete": #dont consider delete events, we keep the nodes 
                    continue

                created_at_str = tweet.get("created_at")
                if not created_at_str:
                    continue
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y") #proper structure
                if not (FILTER_START <= created_at.replace(tzinfo=None) <= FILTER_END):
                    continue
                user = tweet.get("user", {})
                uid = user.get("id_str") #userid
                tid = tweet.get("id_str") #twitterid

                text = get_full_text(tweet)
                mentions = extract_mentions(text)
                for mentioned_screen_name in mentions:
                    mentioned_uid = screen_name_to_id.get(mentioned_screen_name)
                    if not mentioned_uid:
                        mention_skipped += 1
                        continue
                    
                    # Unique edge between tweet and mentioned user so mention relation
                    edge = (tid, mentioned_uid)
                    if edge not in mention_edges:
                        mentions_writer.writerow([tid, mentioned_uid, "MENTIONED"])
                        mention_edges.add(edge)
                    else:
                        #print(f"{edge} is a mentioned duplicate")
                        duplicate_mentions += 1

                # Handle retweet by associating the user with the original tweet, not creating a new tweet node
                if "retweeted_status" in tweet:
                    original_tid = tweet["retweeted_status"].get("id_str")
                    if original_tid and original_tid in tweet_ids:
                        edge = (tweet["user"]["id_str"], original_tid)  # Link user to original tweet
                        if edge not in retweet_edges:
                            retweets_writer.writerow([tweet["user"]["id_str"], original_tid, "RETWEETED"])  # User - Original Tweet
                            retweet_edges.add(edge)
                        else:
                            duplicate_retweets += 1
                    else:
                        retweet_skipped += 1

                    continue  # Skip this retweet from being processed as a new tweet

                # QUOTED relation with extra text
                if tweet.get("is_quote_status") and "quoted_status" in tweet:
                    quoted_tweet = tweet["quoted_status"]
                    quoted_tid = quoted_tweet.get("id_str")
                    uid = tweet.get("user", {}).get("id_str")

                # This is the additional comment text from the quoting user
                    quote_comment_text = get_full_text(tweet)

                    if uid and quoted_tid and quoted_tid in tweet_ids:
                        edge = (uid, quoted_tid)
                        if edge not in quoted_edges:
                            quoted_writer.writerow([uid, quoted_tid, "QUOTED", quote_comment_text])
                            quoted_edges.add(edge)

                # CONTAIN relation: tweet uses each hashtag
                hashtags = extract_hashtag(get_full_text(tweet))
                for tag in hashtags:
                    edge = (tid, tag)  # We are not changing tag 
                    if edge not in contain_edges:
                        contain_writer.writerow([tid, tag, "CONTAIN"])
                        contain_edges.add(edge)
                    else:
                        #print(f"{edge} is a mentioned duplicate")
                        duplicate_contains += 1

            except Exception:
                continue

mentions_file.close()
retweets_file.close()
quoted_file.close()
contain_file.close()

""""
print(f"Duplicate POSTED {duplicate_posted}")
print(f"Duplicate MENTIONED {duplicate_mentions}")
print(f"Duplicate RETWEETED {duplicate_retweets}")
print(f"Duplicate QUOTED {duplicate_quoted}")
print(f"Duplicate CONTAINS {duplicate_contains}")
print(f"Skipped {mention_skipped} mentions")
print(f"Skipped {retweet_skipped} retweets")
"""