# DBL DATA CHALLANGE 

Welcome to the DBL Data Challenge repository! This project contains the initial processing of json files into csv with fields relevant for analysis that are later loaded into neo4j, as well as neo4j cyphers that are used for data processing and analysis of the twitter data.

## Table of Contents

- [Project Overview](#introduction)
- [Setup](#setup)
- [Sentimement Analysis Usage](#sentimement)

## Introduction 
This repository is intended to help with analyzing benefit of the presence of airline twitter teams on the social media platoform. It is designed in such a way that it accounts for both the benefit of the social media teams activity in regards to posting content as well the support role the teams often plays. 

We convert the jsons into csvs to mke use of admin import command made by neo4j for faster database building

We believe that the role of airline's twitter team is to create positive effect on the platform by usage of posts, as well to provide support regarding issues, hence: 
Firstly we track the whole flow of a posts made by a airlines twitter team, and analyze the sentiment. Secondly we compare the sentiment of tweet that the airline is replying to (defined as support action) and sentiment after ((Define length)). Since we use neo4j we make use of common graph algorithms

Modeling: 

    Nodes: 
        1. USERS:  "userId", "name", "screen_name", "followers", "verified"
        2. TWEETS: "tweetId", "text", "created_at", "lang", "Type" 
         Type used to distinquish between original tweet (1) ,retweet (2),quote tweet(3), and generated one for the sake of connection(0)
        3. HASHTAG "Name", "Count"

    Relations: 
        1. Posted: ":START_ID(User)", ":END_ID(Tweet)"
        2. Mentions: ":START_ID(Tweet)", ":END_ID(User)"
        3. Retweets: ":START_ID(Tweet)", ":END_ID(Tweet)" 
        4. Quotes: ":START_ID(Tweet)", ":END_ID(Tweet)"
        5. Contains:":START_ID(Tweet)", ":END_ID(Hashtag)"

## Setup
To use the scripts in this repository, you need to have latest Python and Jav installed. Clone the repository and install the required dependencies.

```bash
git clone https://github.com/TomasT-bit/DBL1
cd DBL-Data-Challange
pip install -r requirements.txt

```
In addition: 
1. Install neo4j desktop
2. Move Data folder into DBL1 call it "data"
3. Have running Neo4j dbms with password "password"
5. in config file of neo4j change according to below
```bash
dbms.security.allow_csv_import_from_file_urls=true is uncommented 
dbms.directories.import=import ?
dbms.security.procedures.unrestricted=apoc.*,gds.*
dbms.security.procedures.allowlist=apoc.*,gds.* is uncommentedd
dbms.directories.plugins=plugins ?

server.memory.pagecache.size=2G
dbms.memory.heap.initial_size=3G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G
```
6. move the cvs files to import/ in neo4j
7. Install APOC plugin and Graph Data Science Library 
8. Run in powershell(adapt filepath to your neo4j-admin.ps1):
```bash
PowerShell -File "C:\Users\20241225\Desktop\DBL1\Neo\relate-data\dbmss\dbms-9079e945-2bb0-4856-b164-8cefb28053e3\bin\neo4j-admin.ps1" `
    database import full twitter9 `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --nodes="import\hashtag.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\mentions.csv" `
    --relationships="import\retweets.csv" `
    --relationships="import\quoted.csv" `
    --relationships="import\contain.csv" `
    --relationships="import\replies.csv"
```
9. Create new database in the neo4j project called twitter

## Sentimement
