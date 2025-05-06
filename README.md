# DBL1
#SETUP
Install neo4j desktop
1. Move Data folder into DBL1 call it "data"
2. Running python to_csv.py selects the relations and entities to be moddeled in neo4j and puts it in created folder "cvs"
3. Have running Neo4j dbms with password "password"
4. make sure to pip install neo4j
5. in config file of neo4j change according to below
6. move the cvs files to import/ in neo4j


Rationale: 
jsons take way too long neo4j has its own method for cvs. 

Running: 
python to_csv.py - makes csv from jsons
initialize.py - puts the data into neo4j 
delete.py - clears the database

in neo4j.conf make sure that !dbms.security.allow_csv_import_from_file_urls=true is uncommented 
                            !dbms.directories.import=import ?
                            !dbms.security.procedures.unrestricted=apoc.*,gds.*
                            !dbms.security.procedures.allowlist=apoc.*,gds.* is uncomented
                            dbms.directories.plugins=plugins ?


                            #dbms.memory.heap.initial_size=1G
                            #dbms.memory.heap.max_size=4G
                            #dbms.memory.pagecache.size=2G
!Download APOC on the database and restart ! 	[\["5.24.0"\]](https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases)


then adapt this to your paths 
PowerShell -File "C:\Users\20231225\Desktop\DBL1\Neo\relate-data\dbmss\dbms-7be25c3d-fff2-4069-abc4-d50d6e8c5425\bin\neo4j-admin.ps1" database import full twitter3 --overwrite-destination=true --multiline-fields=true --verbose --nodes="import\users.csv" --nodes="import\tweets.csv" --nodes="import\hashtag.csv" --relationships="import\posted.csv" --relationships="import\mentions.csv" --relationships="import\retweets.csv" --relationships="import\quoted.csv" --relationships="import\contain.csv"


After create database name in neo4j desktop

Definitions:    
Conversations: reply to eo at least once (consider hamiltonian/eularian cycles in future)

Modeling: 
    Mentions: Tweet id, mentioned User id - directed edge between tweet and user
    Tweets: tweet id, text, time created -node of user 
    Posted: user id, tweet id -directed edge between user and tweet
    Users: user id, name, screen name - node of user


    Removed attributes: 
        source - https, following or not, os type 
        truncated\
        in_replies

        geo location, coordinates, place, contributors, is quote

        quote count, reply count, retweet count, favorite count 

        favorited, retweeted, filter_level,lang,timestamp_ms


#TO DO


When running to_csv.py bunch of errors regarding missing entries 
so far ignoring failrues in missing values etc, make sure filtreing is good 
include filtering based on date 
Other features that must be included, look through the data description / better moddeling
neo4j has in built online feature look into it if you want 
