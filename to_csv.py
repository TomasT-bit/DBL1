import os
import json
import csv
import re
import logging
from tqdm import tqdm
from datetime import datetime

logging.basicConfig(level=logging.CRITICAL)

#Directory paths
DATA_DIR = "data" 
OUTPUT_DIR = "import"

FILTER_START = datetime(2000, 1, 1)
FILTER_END = datetime(2026, 12, 31)
os.makedirs(OUTPUT_DIR, exist_ok=True)

#Variaables to keep unique users, tweets, posted 
user_ids = set()
tweet_ids = set()
posted_edges = set()
duplicate_posted = 0
screen_name_to_id = dict() # dictonary of screen name and id ensures robustness against chaning names over time 


files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")] #makes path to all jsosn inside the directory

mention_structure = re.compile(r"@(\w+)") #Structure of @ ppl 

'''
Find all the mentioned users usign the structure of mentions in the given text
'''
def extract_mentions(text):
    return mention_structure.findall(text)

'''
Find all the mentioned users usign the structure of mentions in the given text

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

#Open folders 
users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline="", encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline="", encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline="", encoding="utf-8")
users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)

#Initial write for labels 
users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name", "followers"])
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at", "lang"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])

for file_path in tqdm(files, desc="First pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if list(tweet.keys())[0] == "delete": #ont consider delete events, we keep the nodes 
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
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", ""), user.get("followers_count", 0)])
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid

                #tid is unique
                if tid not in tweet_ids:
                    text = get_full_text(tweet)
                    tweets_writer.writerow(["Tweet", tid, text, created_at_str, tweet.get("lang", "")])
                    tweet_ids.add(tid)

                #Create posted, and if unique add 
                posted = (uid, tid)
                if posted not in posted_edges:
                    posted_writer.writerow([uid, tid, "POSTED"])
                    posted_edges.add(posted)
                else:
                    duplicate_posted += 1

            except Exception:
                continue
#Closing files
users_file.close()
tweets_file.close()
posted_file.close()




#Secon pass taaking care of mentions and retweets 



mention_edges = set()
retweet_edges = set()
mention_skipped = 0
retweet_skipped = 0
duplicate_mentions = 0
duplicate_retweets = 0


#Opening
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline="", encoding="utf-8")
retweets_file = open(os.path.join(OUTPUT_DIR, "retweets.csv"), "w", newline="", encoding="utf-8")
mentions_writer = csv.writer(mentions_file)
retweets_writer = csv.writer(retweets_file)

#Laabels
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])
retweets_writer.writerow([":START_ID(Tweet)", ":END_ID(Tweet)", ":TYPE"])

for file_path in tqdm(files, desc="Second pass"):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if list(tweet.keys())[0] == "delete":
                    continue

                tid = tweet.get("id_str")
                if not tid:
                    continue

                text = get_full_text(tweet)
                mentions = extract_mentions(text)
                for mentioned_screen_name in mentions:
                    mentioned_uid = screen_name_to_id.get(mentioned_screen_name)
                    if not mentioned_uid:
                        mention_skipped += 1
                        continue
                    
                    #Unique edge beteen twitter aand mentioned user so mentioned relation
                    edge = (tid, mentioned_uid)
                    if edge not in mention_edges:
                        mentions_writer.writerow([tid, mentioned_uid, "MENTIONED"])
                        mention_edges.add(edge)
                    else:
                        duplicate_mentions += 1


                #Unique ege between tweet and other twweet, so retweeted relation 
                if "retweeted_status" in tweet:
                    original_tid = tweet["retweeted_status"].get("id_str")
                    if original_tid and original_tid in tweet_ids:
                        edge = (tid, original_tid)
                        if edge not in retweet_edges:
                            retweets_writer.writerow([tid, original_tid, "RETWEETED"])
                            retweet_edges.add(edge)
                        else:
                            duplicate_retweets += 1
                    else:
                        retweet_skipped += 1

            except Exception:
                continue

mentions_file.close()
retweets_file.close()

print(f"Duplicate POSTED {duplicate_posted}")
print(f"Duplicate MENTIONED {duplicate_mentions}")
print(f"Duplicate RETWEETED {duplicate_retweets}")
print(f"Skipped {mention_skipped} mentions")
print(f"Skipped {retweet_skipped} retweets")
