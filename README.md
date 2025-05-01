# DBL1
#SETUP
Install neo4j desktop
1. Move Data folder into DBL1 call it "Data"
2. Running python to_csv.py selects the relations and entities to be moddeled in neo4j and puts it in created folder "cvs"
3. Have running Neo4j dbms with password "password"
4. make sure to pip install neo4j
5. in config file of neo4j change dbms.memory.transaction.total.max=1G
6. move the cvs files to import/ in neo4j
7. use "   powershell -File "C:\Users\20241225\Desktop\DBL1\Neo\relate-data\dbmss\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\bin\neo4j-admin.ps1" `
database import full neo4j `
--overwrite-destination=true `
--multiline-fields=true `
--nodes="import\users.csv" `
--nodes="import\tweets.csv" `
--relationships="import\posted.csv" `
--relationships="import\mentions.csv"
 

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


Definitions:    
Conversations: reply to eo at least once (consider hamiltonian/eularian cycles in future)

Modeling: 
    Mentions: User id, mentioned User id - for edges
    Tweets: tweet id, text, time created
    Posted: user id, tweet id 
    Users: user id, name, screen name



#TO DO
When running to_csv.py bunch of errors regarding missing entries 
so far ignoring failrues in missing values etc, make sure filtreing is good 
include filtering based on date 
Other features that must be included, look through the data description / better moddeling
neo4j has in built online feature look into it if you want 
