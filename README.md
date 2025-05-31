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

Firstly we track the whole flow of a posts made by a airlines twitter team, and analyze the sentiment. Sometimes there are relations of a node to a node outside of the scope of either the collected data or the given time period, note that this need not to be accounted for since we either mine weakly connected components starting from tweet made from an ariline (the whole sentiment) / or a weakly connected components that have a reply from an airline, here we consider the change of the sentiment before and the sentiment after the said reply 

### Modeling: 
Nodes: 
1. USERS:  "userId", "name", "screen_name", "followers", "verified"
2. TWEETS: "tweetId", "text", "created_at", "lang", "Type" 
Type used to distinguish between original tweet (1) ,retweet (2),quote tweet(3), reply(4) and generated one for the sake of connection(0)
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
cd DBL1
pip install -r requirements.txt

```
In addition: 
1. Install neo4j desktop
2. Move Data folder into DBL1 call it "data"
3. Have running Neo4j dbms with password "password"
5. in config file of neo4j change according to below
```bash
dbms.security.allow_csv_import_from_file_urls=true is uncommented 
dbms.security.procedures.allowlist=apoc.*,gds.* is uncommentedd

server.memory.pagecache.size=2G
dbms.memory.heap.initial_size=3G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G
```
6. move the cvs files to import/ in neo4j
7. Install APOC plugin and Graph Data Science Library 
8. Run in powershell(adapt filepath to your neo4j-admin.ps1):
```bash
& "C:\Users\20241225\.Neo4jDesktop\relate-data\dbmss\dbms-304bd8b7-f0ea-4110-baaa-b3109d3ce4c4\bin\neo4j-admin.ps1" database import full twitter9 `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\replies.csv"

9. Create new database in the neo4j project called twitter

## Sentimement
