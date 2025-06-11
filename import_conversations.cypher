
// === Import Conversation Nodes ===
LOAD CSV WITH HEADERS FROM 'file:///conversations.csv' AS row
MERGE (c:Conversation {id: row[":ID(Conversation)"]})
SET c.airlineId = row.airlineId;

// === Import PART_OF Relationships ===
LOAD CSV WITH HEADERS FROM 'file:///conversation_edges.csv' AS row
MATCH (t:Tweet {tweetId: row[":END_ID(Tweet)"]})
MATCH (c:Conversation {id: row[":START_ID(Conversation)"]})
MERGE (t)-[:PART_OF {positionType: toInteger(row["positionType:int"])}]->(c);

// === Optional: Import Conversation Stats (Size) ===
LOAD CSV WITH HEADERS FROM 'file:///conversation_stats.csv' AS row
MATCH (c:Conversation {id: row[":ID(Conversation)"]})
SET c.size = toInteger(row.size);
