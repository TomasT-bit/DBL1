import os
import json
import csv
import re
import logging
from tqdm import tqdm
from datetime import datetime

# Directory paths
DATA_DIR = "data"
OUTPUT_DIR = "import"
os.makedirs(OUTPUT_DIR, exist_ok=True)

#Unique ids - for duplicates
user_ids = set()
tweet_ids = set()
screen_name_to_id = {}

#Start and end dates for data
#Filter_start() =
#Filter_end() =

# the filtered csv files r put in the directory "import" in the DBL1 folder, make sure to create it before running
users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline='', encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline='', encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline='', encoding="utf-8")
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline='', encoding="utf-8")

users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)
mentions_writer = csv.writer(mentions_file)

users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name"])  # Add label column for Users
tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at"])  # Add label column for Tweets
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])  # No label
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])  # No label

def extract_mentions(text): #should we specify whats a mention and whtats a RT? 
    return re.findall(r"@(\w+)", text)

#Sometimes very large id turn into scientific notiation, this fixes it.
def format_id(id_value):
    return str(id_value) 

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                user = tweet.get("user", {})
                
                # Skip if tweet ID or user ID is missing - should be done differently ?
                if not tweet.get("id") or not user.get("id"):
                    continue

                uid = format_id(user["id"])
                tid = format_id(tweet["id"])

                #Create user id 
                if uid not in user_ids:
                    users_writer.writerow(["User", uid, user.get("name", ""), user.get("screen_name", "")]) 
                    user_ids.add(uid)
                    screen_name_to_id[user.get("screen_name", "")] = uid

                #Create tweet id 
                if tid not in tweet_ids:
                    tweets_writer.writerow(["Tweet", tid, tweet.get("text", ""), tweet.get("created_at", "")]) 
                    tweet_ids.add(tid)

                #Create posted rel
                posted_writer.writerow([uid, tid, "POSTED"])

                #Creaate mentions
                mentions = extract_mentions(tweet.get("text", ""))
                for mentioned_screen_name in mentions:
                    mentioned_user_id = screen_name_to_id.get(mentioned_screen_name)
                    if mentioned_user_id:
                        mentions_writer.writerow([tid, mentioned_user_id, "MENTIONED"])
            except Exception as e:
                logging.warning(f"Error processing line in {file_path}: {e}")

def process():
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]
    for fpath in tqdm(files, desc="Processing files"):
        process_file(fpath)


if __name__ == "__main__":
    process()
    # Close all files
    users_file.close()
    tweets_file.close()
    posted_file.close()
    mentions_file.close()


#TO DO HERE:
#Filter eronous data + rationale
#ADD from to functionality 
