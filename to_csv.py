import os
import json
import csv
import re
import logging
from tqdm import tqdm

DATA_DIR = "data"
OUTPUT_DIR = "cvs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

user_ids = set()
tweet_ids = set()

users_file = open(os.path.join(OUTPUT_DIR, "users.csv"), "w", newline='', encoding="utf-8")
tweets_file = open(os.path.join(OUTPUT_DIR, "tweets.csv"), "w", newline='', encoding="utf-8")
posted_file = open(os.path.join(OUTPUT_DIR, "posted.csv"), "w", newline='', encoding="utf-8")
mentions_file = open(os.path.join(OUTPUT_DIR, "mentions.csv"), "w", newline='', encoding="utf-8")

users_writer = csv.writer(users_file)
tweets_writer = csv.writer(tweets_file)
posted_writer = csv.writer(posted_file)
mentions_writer = csv.writer(mentions_file)

# Write headers
users_writer.writerow(["userId:ID(User)", "name", "screen_name"])
tweets_writer.writerow(["tweetId:ID(Tweet)", "text", "created_at"])
posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])
mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])

def extract_mentions(text):
    return re.findall(r"@(\w+)", text)

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                user = tweet.get("user", {})
                if not tweet.get("id") or not user.get("id"):
                    continue

                uid = str(user["id"])
                tid = str(tweet["id"])

                # users
                if uid not in user_ids:
                    users_writer.writerow([uid, user.get("name", ""), user.get("screen_name", "")])
                    user_ids.add(uid)

                # tweets
                if tid not in tweet_ids:
                    tweets_writer.writerow([tid, tweet.get("text", ""), tweet.get("created_at", "")])
                    tweet_ids.add(tid)

                # user posted tweet
                posted_writer.writerow([uid, tid, "POSTED"])

                # tweet mentioned user sceen name is currently id need to change this 
                mentions = extract_mentions(tweet.get("text", ""))
                for mentioned_screen_name in mentions:
                    mentions_writer.writerow([tid, mentioned_screen_name, "MENTIONED"])
            except Exception as e:
                logging.warning(f"Error in {file_path}: {e}")

def process():
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]
    for fpath in tqdm(files, desc="Processing files"):
        process_file(fpath)

if __name__ == "__main__":  
    os.makedirs("cvs")
    process()
    users_file.close()
    tweets_file.close()
    posted_file.close()
    mentions_file.close()
