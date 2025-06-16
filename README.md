# DBL DATA CHALLANGE 
Welcome to the DBL Data Challenge repository! This project contains the initial processing of json files into csv with fields relevant for analysis that are later loaded into neo4j, as well as neo4j cyphers that are used for data processing and analysis of the twitter data.


# Tweet Processing with Sentiment Analysis

## Setup

1. Clone the repository
```bash
git clone https://github.com/yourname/yourproject.git
cd yourproject

Create a virtual environment
python -m venv venv

Activate the virtual environment
venv\Scripts\activate


Install dependencies

bash
Copy
Edit
pip install -r requirements.txt


Run the script

## Table of Contents

- [Project Overview](#introduction)
- [Setup](#setup)
- [Sentimement Analysis Usage](#sentimement)

## Introduction 
This repository is intended to help with analyzing benefit of the presence of airline twitter teams on the social media platoform. It is designed such that it accounts for the larger context of a support queue, not limiting the sentiment to be analyzed on a tweet between two users. 

We convert jsons in the data/ to csv posted,replies,tweets,users and these are then imported into neo4j, through powershell comand. Here it then gets put into valid conversations. These conversations are then saved along the difference in the end sentiment and start sentiment of the conversation., and type of issue the conversation is reffering to. 

Finally we provide insight into the data by: 

### Modeling: 
Nodes: 
1. USERS:  :LABEL,userId:ID(User),name,screen_name,followers,verified
2. TWEETS: :LABEL,tweetId:ID(Tweet),text,created_at,lang,Type,sentiment_label,sentiment_expected_value 
3. CONVERSATIONS: 

Relations: 
1. Posted: ":START_ID(User)", ":END_ID(Tweet)"
2. REPLIES: 
3. PART_OF:

## Setup
To use the scripts in this repository, you need to Clone the repository and have the latest python and NEO4J Dekstop app. 

```bash
git clone https://github.com/TomasT-bit/DBL1
cd DBL1
```

Create a virtual environment

```bash
python -m venv venv
```

Activate the virtual environment
```bash
venv\Scripts\activate
```

Install dependencies
```bash
pip install -r requirements.txt
```


Start new databse in Neo4j, download plugins APOC and GDSL and locate its config thorugh the Neo4j Dekstop, in it the following need to be 
1. Move Data folder into DBL1 call it "data"
2. Have running Neo4j dbms with password "password"
3. in config file of neo4j change according to below
Uncommented:
```bash
dbms.security.allow_csv_import_from_file_urls=true
dbms.security.procedures.allowlist=apoc.*,gds.* 
```
Values Changed:
```bash
server.memory.pagecache.size=2G
dbms.memory.heap.initial_size=3G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G
```

Finally move the jsons into the data/ folder and run to_csv.py, resulting 4 csv move into import/ into the neo4j database, after which in powershell run adapted to the location admin.ps1 
```bash
& PowerShell -File "</admin.ps1>" `
 3cbf770abb38727bebbffd270fae3b36ab77aae7
    database import full twitter `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\replies.csv" `
```

9. Inside the neo4j project create new database called twitter and start the project
 
10. Run building_conversations.py, add the newly created 2 csv into the import folder of neo4j and finally run powershell command
```bash
$ PowerShell -File "</admin.ps1>" `
 3cbf770abb38727bebbffd270fae3b36ab77aae7
    database import full twitterConversations `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\replies.csv" `
     --nodes="import\conversations.csv" `
    --relationships="import\conversation_edges.csv"
```
11. Inside the neo4j project create new database called twitter and start the project

12. Now analysis is done on the moddeled conversations with analysis.py



## Sentimement

In order to perform sentiment analysis for this project we have used a pre-trained model from Hugging Face. "cardiffnlp/twitter-roberta-base-sentiment-latest" classifies each tweet as positive, neutral or negative based on the probability that the tweet fits that label.We went one step further and instead of just using the probabilities we calculated the expected value for each tweet following the formula: positive_propability * 1 + neutral_probability * 0 + negative_probability * -1. This gives us an interval between -1 and 1 where all the tweets are represented. Since the model is pre-trained on tweets already, it is pretty certain  for most tweets and we found that most tweets fall on the extremes of the interval such as close to 1 or -1 and at 0. 

Input : csv file with all the tweets, most important the text attribute.

Output : Same csv file but with 2 more columns, expected_value and sentiment_label

How it works:

1. We load the model and its tokenizer from Hugging Face
2. We preprocess the tweets to remove any links and replacing the mentions(@) in the tweet with @user.( make sure to change the path for the csv file)
3. Tokenize and batch the text from the tweets using the torch library on the GPU(preferably)
4. Get the softamax probabiltiies for each of the labels(positive, negative and neutral)
5. Compute the expected value
6. Return the expected value and the label with the highest probability
7. Save back to CSV

After the model is done we can distribute the csv file among us and import it into Neo4j

"C++"
# c++ compiler (MSVC)
1. Go to "https://visualstudio.microsoft.com/visual-cpp-build-tools/"
2. Click “Download Build Tools” and install it
3. During installation, make sure these are checked:
MSVC v143 - VS 2022 C++ x64/x86 build tools
Windows 10 SDK
CMake tools for Windows

# using a simple C++ HTTP library (cpr)
1. Go to "https://www.7-zip.org/" and dl it and install it
2. Click on "Environment Variables..."
3. select "path" in "System variables" and then click "edit"
4. Click on "new" and then add this address "C:\Program Files\7-Zip"
5. Open PowerShell and run "cd C:\Users\bitas\OneDrive\Documents\vcpkg
.\vcpkg.exe install cpr" # for HTTP requests"
6. in powershell run ".\vcpkg.exe install nlohmann-json" #to parse JSON tweet files

# switching to MSVC (Because MSVC is no compatible with vcpkg)
1. open Powershell on desktop and run this "cd "C:\Users\bitas\OneDrive\Documents\vcpkg"
>> .\vcpkg.exe install openssl:x64-windows"
2. Open "x64 Native Tools Command Prompt for VS 2022":
3. cd C:\Users\bitas\OneDrive\Documents\DBL1
4. cl /std:c++17 main.cpp /EHsc ^
More? /I"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows\include" ^
More? /link /LIBPATH:"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows\lib" ^
More? cpr.lib libssl.lib libcrypto.lib ws2_32.lib crypt32.lib user32.lib zlib.lib

# fix the main.cpp problem:
1. Open the Command Palette "ctrl+ shift+ p"
2. Open "C/C++: Edit Configurations (JSON)"
3. in "includePath" add this path "C:/Users/bitas/OneDrive/Documents/vcpkg/installed/x64-windows/include"
