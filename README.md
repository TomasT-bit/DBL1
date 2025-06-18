# DBL Data Challenge

Welcome to the **DBL Data Challenge1** repository!

This project processes Twitter JSONS into CSV files for graph modeling in Neo4j. It incorporates NEO4j. sentiment analysis, classification of issues and provides analytical insights based on time and sentiment changes.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Setup](#setup)
- [Neo4j Configuration](#configuration)
- [Importing to Neo4j](#importing-to-neo4j)
- [Sentiment Analysis](#sentiment-analysis)
- [Scripts Overview](#scripts-overview)

---

## Project Overview

This repository supports the analysis of comparision of airline support teams engaging with users on Twitter in order to resolve an issue. The conversations are moddeled as trees with airline tweet in it and user tweets at the start and end, we do not restrict this only to one user to capture the greater effect the twitter teams reponse has.

### Workflow Summary

1. Convert raw Twitter JSON files in the `data/` folder into CSVs: `tweets`, `users`, `posted`, and `replies`, this is done with cleaning in mind.
2. Import the CSVs into a Neo4j graph database using PowerShell and the `admin.ps1` script, as described below.
3. Model conversations and classify each by the Type of issue discussed
4. Perform analysis on the moddeled conversations

---

## Setup

```bash
# Clone the repository
git clone https://github.com/yourname/yourproject.git
cd yourproject
mkdir data

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create new project inside Neo4j
Download the APOC, Graph Data Science library inside Neo4j (on the side of the created project) 
Edit the Neo4j configuration file (`neo4j.conf`) and apply the following settings:

```bash
dbms.security.allow_csv_import_from_file_urls=true
dbms.security.procedures.allowlist=apoc.*,gds.*

server.memory.pagecache.size=2G
dbms.memory.heap.initial_size=3G
dbms.memory.heap.max_size=4G
```

## Importing to Neo4j

### Step-by-Step

1. **Prepare Raw Data**  
   Move your raw Twitter JSON files into the `data/` directory of the project.

2. **Convert JSON to CSV**  
   Run the following script to convert the data into CSV format:
   ```bash
   python scripts/to_csv.py
   ``` 
3.  **Import created csv into Neo4j**
    Move the generated files into /import in the Neo4j project directory 
    Run this command inside the terminal, where "<path-to-admin.ps1>" is your path to the admin.ps1
    ```bash
    PowerShell -File "<path-to-admin.ps1>" `
    database import full twitter `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\replies.csv" 
    ```
4. **Import create Database**
    Create new database called "twitter" inside Neo4j with password "password"

5. **Create Conversations**
    Run the following script 
   ```bash
   python building_conversations.py
   ``` 
6. **Run sentimnet on conversations**
    ????

7. **Clasify the type of issues in conversations** 
    ???

8.**Reimport the new files into Neo4j** 
Move the newly created files into Neo4j project directory and run
```bash 
PowerShell -File "<path-to-admin.ps1>" `
    database import full twitterconversations `
    --overwrite-destination=true `
    --multiline-fields=true `
    --verbose `
    --nodes="import\users.csv" `
    --nodes="import\tweets.csv" `
    --nodes="import\conversations.csv" `
    --relationships="import\posted.csv" `
    --relationships="import\replies.csv" `
    --relationships="import\conversation_edges.csv"
```
After create new databse inside the Neo4j project called twitterconversations
8.**Add start and end time to the conversations** 
 Run the following script 
   ```bash
   python helper_time.py
   ``` 
9. **Run visualisations** 
Run the following script 
   ```bash
   python visualisations.py
   ``` 


## Sentiment-analysis
In order to perform sentiment analysis for this project we have used a pre-trained model from Hugging Face. "cardiffnlp/twitter-roberta-base-sentiment-latest" classifies each tweet as positive, neutral or negative based on the probability that the tweet fits that label.We went one step further and instead of just using the probabilities we calculated the expected value for each tweet following the formula: positive_propability * 1 + neutral_probability * 0 + negative_probability * -1. This gives us an interval between -1 and 1 where all the tweets are represented. Since the model is pre-trained on tweets already, it is pretty certain  for most tweets and we found that most tweets fall on the extremes of the interval such as close to 1 or -1 and at 0. 

## Input:
csv file with all the tweets, most important the text attribute.

## Output:
Same csv file but with 2 more columns, expected_value and sentiment_label

## How it works:
1. We load the model and its tokenizer from Hugging Face
2. We preprocess the tweets to remove any links and replacing the mentions(@) in the tweet with @user.( make sure to change the path for the csv file)
3. Tokenize and batch the text from the tweets using the torch library on the GPU(preferably)
4. Get the softamax probabiltiies for each of the labels(positive, negative and neutral)
5. Compute the expected value
6. Return the expected value and the label with the highest probability
7. Save back to CSV

After the model is done we can distribute the csv file among us and import it into Neo4j



## Scripts Overview

| Script Name              | Description                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| `to_csv.py`              | Converts raw Twitter JSON files into structured CSVs (`users`, `tweets`, `posted`, `replies`). |
| `roberta_sentiment.py`   | Performs sentiment analysis on individual tweets using the RoBERTa model.   |
| `building_conversations.py` | Constructs conversations by recursively linking replies and calculates sentiment shifts. |
| `helper_time.py`         | Provides time-based utilities and functions to support temporal analysis.   |
| `roberta_on_conv.py`     | Runs sentiment analysis specifically on the start and end tweets of each conversation. |
| `classifier.py`          | Classifies conversations based on predefined issue types (e.g., complaint, praise). |
| `visualisations.py`      | Generates visual representations and plots of the analysis results.         |
| `Htest.py`               | Experimental script for hypothesis testing and statistical analysis.        |
| `Htest2.py`              | Extended or alternative version of `Htest.py` for validating insights.      |
