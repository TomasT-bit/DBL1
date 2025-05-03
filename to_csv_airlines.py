import os
import json
import csv
import re
import logging
import shutil
from tqdm import tqdm

# Define constants
TARGET_AIRLINE = "American Air"
DATA_DIR = "data"
OUTPUT_DIR = "import"
TARGET_DIR = os.path.join(OUTPUT_DIR, "american_air")
OTHERS_DIR = os.path.join(OUTPUT_DIR, "others")

def format_id(id_val):
    return str(id_val)

def extract_mentions(text):
    return re.findall(r"@(\w+)", text)

def mentions_american_air(text):
    return "american air" in text.lower()

def init_writer(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    users = open(os.path.join(output_dir, "users.csv"), "w", newline='', encoding="utf-8")
    tweets = open(os.path.join(output_dir, "tweets.csv"), "w", newline='', encoding="utf-8")
    posted = open(os.path.join(output_dir, "posted.csv"), "w", newline='', encoding="utf-8")
    mentions = open(os.path.join(output_dir, "mentions.csv"), "w", newline='', encoding="utf-8")

    users_writer = csv.writer(users)
    tweets_writer = csv.writer(tweets)
    posted_writer = csv.writer(posted)
    mentions_writer = csv.writer(mentions)

    users_writer.writerow([":LABEL", "userId:ID(User)", "name", "screen_name"])
    tweets_writer.writerow([":LABEL", "tweetId:ID(Tweet)", "text", "created_at"])
    posted_writer.writerow([":START_ID(User)", ":END_ID(Tweet)", ":TYPE"])
    mentions_writer.writerow([":START_ID(Tweet)", ":END_ID(User)", ":TYPE"])

    return {
        "files": [users, tweets, posted, mentions],
        "writers": {
            "users": users_writer,
            "tweets": tweets_writer,
            "posted": posted_writer,
            "mentions": mentions_writer
        },
        "user_ids": set(),
        "tweet_ids": set(),
        "screen_name_to_id": {}
    }

def close_writer(writer):
    for f in writer["files"]:
        f.close()

def process_tweet(tweet, writer):
    user = tweet.get("user", {})
    if not tweet.get("id") or not user.get("id"):
        return

    uid = format_id(user["id"])
    tid = format_id(tweet["id"])

    # Write user
    if uid not in writer["user_ids"]:
        writer["writers"]["users"].writerow(["User", uid, user.get("name", ""), user.get("screen_name", "")])
        writer["user_ids"].add(uid)
        writer["screen_name_to_id"][user.get("screen_name", "")] = uid

    # Write tweet
    if tid not in writer["tweet_ids"]:
        writer["writers"]["tweets"].writerow(["Tweet", tid, tweet.get("text", ""), tweet.get("created_at", "")])
        writer["tweet_ids"].add(tid)

    # Write POSTED relationship
    writer["writers"]["posted"].writerow([uid, tid, "POSTED"])

    # Write MENTIONS relationships
    mentions = extract_mentions(tweet.get("text", ""))
    for screen_name in mentions:
        mentioned_uid = writer["screen_name_to_id"].get(screen_name)
        if mentioned_uid:
            writer["writers"]["mentions"].writerow([tid, mentioned_uid, "MENTIONED"])

def process_file(file_path, target_writer, others_writer):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                text = tweet.get("text", "")

                if mentions_american_air(text):
                    process_tweet(tweet, target_writer)
                else:
                    process_tweet(tweet, others_writer)

            except Exception as e:
                logging.warning(f"Failed to process tweet: {e}")

def process_all():
    # Clean previous output
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)

    # Initialize writers
    target_writer = init_writer(TARGET_DIR)
    others_writer = init_writer(OTHERS_DIR)

    # Process files
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".json")]
    for path in tqdm(files, desc="Processing files"):
        process_file(path, target_writer, others_writer)

    close_writer(target_writer)
    close_writer(others_writer)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_all()
