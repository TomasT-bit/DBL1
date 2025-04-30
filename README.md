# DBL1
#SETUP
Install neo4j desktop
1. Move Data folder into DBL1 call it "Data"
2. Running python to_csv.py selects the relations and entities to be moddeled in neo4j and puts it in created folder "cvs"
3. Have running Neo4j dbms with password "password"
4. make sure to pip install neo4j
5. in config file of neo4j change dbms.memory.transaction.total.max=1G
6. move the cvs files to import/ in neo4j

Rationale: 
jsons take way too long neo4j has its own method for cvs. 

Running: 
python to_csv.py - makes csv from jsons
initialize.py - puts the data into neo4j 
delete.py - clears the database

in neo4j.conf make sure that !dbms.security.allow_csv_import_from_file_urls=true is uncommented 
                            !dbms.directories.import=import
                            !dbms.security.procedures.unrestricted=apoc.*
                            !dbms.security.procedures.allowlist=apoc.* is uncomented

                            #dbms.memory.heap.initial_size=1G
                            #dbms.memory.heap.max_size=4G
                            #dbms.memory.pagecache.size=2G
!Download APOC on the database and restart !


Definitions:    
Conversations: reply to eo at least once (consider hamiltonian/eularian cycles in future)

Modeling: 
    Mentions: User id, mentioned User id - for edges
    Tweets: tweet id, text, time created
    Posted: user id, tweet id 
    Users: user id, name, screen name



#TO DO
When running to_csv.py bunch of errors regarding missing entries 
Other features that must be included, look through the data description 
neo4j has in built online feature look into it if you want 
