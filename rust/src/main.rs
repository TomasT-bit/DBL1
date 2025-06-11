use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::IndexSet;
use neo4rs::*;

static CONVERSATION_COUNTER: AtomicUsize = AtomicUsize::new(1);

const AIRLINE_IDS: &[&str] = &[
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
];

fn config() -> ConfigBuilder {
    ConfigBuilder::default()
        .uri("localhost:7687")
        .user("neo4j")
        .password("password")
        .db("twitter")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config().build()?;
    let graph = Arc::new(Graph::connect(config).await?);

    let mut tasks = FuturesUnordered::new();

    for &airline_id in AIRLINE_IDS {
        let graph = graph.clone();
        tasks.push(tokio::spawn(async move {
            process_airline(airline_id, graph).await
        }));
    }

    while let Some(result) = tasks.next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Error processing airline: {}", e),
            Err(e) => eprintln!("Task join error: {}", e),
        }
    }

    Ok(())
}

async fn process_airline(
    airline_id: &str,
    graph: Arc<Graph>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let graph_name = format!("ReplyGraph_{}", airline_id);

    // Step 1: Project the full graph using Cypher
    let projection_query = format!(
        "CALL gds.graph.project.cypher(
            '{}',
            'MATCH (t:Tweet) RETURN id(t) AS id',
            'MATCH (a:Tweet)-[r:REPLIES]-(b:Tweet) RETURN id(a) AS source, id(b) AS target, type(r) AS type'
        )",
        graph_name
    );
    graph.run(query(projection_query.as_str())).await?;
    println!("Graph projection complete for airline {}", airline_id);

    // Step 2: Run WCC
    let mut components: HashMap<i64, Vec<String>> = HashMap::new();
    let wcc_query = format!(
        "CALL gds.wcc.stream('{}')
         YIELD nodeId, componentId
         RETURN gds.util.asNode(nodeId).tweetId AS tweetId, componentId",
        graph_name
    );
    let mut result = graph.execute(query(wcc_query.as_str())).await?;

    while let Ok(Some(row)) = result.next().await {
        let tweet_id: String = row.get("tweetId")?;
        let component_id: i64 = row.get("componentId")?;
        components.entry(component_id).or_default().push(tweet_id);
    }

    // Step 3: Get airline tweet IDs
    let mut airline_tweet_ids = HashSet::new();
    let mut result = graph.execute(
        query("MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet) RETURN t.tweetId AS tweetId")
            .param("airline_id", airline_id),
    ).await?;

    while let Ok(Some(row)) = result.next().await {
        let tweet_id: String = row.get("tweetId")?;
        airline_tweet_ids.insert(tweet_id);
    }

    // Step 4: Filter valid conversations
    let mut valid_conversations = vec![];

    for tweets in components.values() {
        let ordered: Vec<String> = IndexSet::<_>::from_iter(tweets.iter().cloned()).into_iter().collect();
        if let Some(valid) = valid_conversation(&ordered, &airline_tweet_ids) {
            valid_conversations.push(valid);
        }
    }

    println!("Found {} valid conversations for airline {}", valid_conversations.len(), airline_id);

    // Step 5: Create CONVERSATION nodes and link tweets
    for tweet_ids in valid_conversations {
        let conv_id = format!("{}_{}", airline_id, CONVERSATION_COUNTER.fetch_add(1, Ordering::SeqCst));

        // Create the CONVERSATION node
        graph
            .run(
                query("MERGE (c:CONVERSATION {conversationId: $conv_id, airlineId: $airline_id})")
                    .param("conv_id", conv_id.clone())
                    .param("airline_id", airline_id),
            )
            .await?;

        // Link each Tweet to the CONVERSATION using batched Cypher query
        graph
            .run(
                query(
                    "UNWIND $tweet_ids AS tid
                     MATCH (t:Tweet {tweetId: tid})
                     MATCH (c:CONVERSATION {conversationId: $conv_id})
                     MERGE (c)-[:PART_OF]->(t)"
                )
                .param("tweet_ids", tweet_ids.clone())
                .param("conv_id", conv_id.clone()),
            )
            .await?;
    }

    // Step 6: Drop the GDS graph
    let drop_query = format!("CALL gds.graph.drop('{}') YIELD graphName", graph_name);
    graph.run(query(drop_query.as_str())).await?;
    println!("Graph dropped for airline {}", airline_id);

    Ok(())
}

fn valid_conversation(conversation: &[String], airline_tweet_ids: &HashSet<String>) -> Option<Vec<String>> {
    let mut keep = vec![true; conversation.len()];
    let mut changed = false;

    for (i, tid) in conversation.iter().enumerate() {
        if airline_tweet_ids.contains(tid) {
            let has_before = (0..i).any(|j| !airline_tweet_ids.contains(&conversation[j]));
            let has_after = (i + 1..conversation.len()).any(|j| !airline_tweet_ids.contains(&conversation[j]));
            if !(has_before && has_after) {
                keep[i] = false;
                changed = true;
            }
        }
    }

    let filtered: Vec<String> = conversation
        .iter()
        .enumerate()
        .filter_map(|(i, tid)| if keep[i] { Some(tid.clone()) } else { None })
        .collect();

    if !filtered.iter().any(|tid| airline_tweet_ids.contains(tid)) {
        return None;
    }

    if changed {
        return valid_conversation(&filtered, airline_tweet_ids);
    }

    Some(filtered)
}
