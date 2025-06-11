#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <mutex>
#include <sstream>

using json = nlohmann::json;
using namespace std::chrono;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

std::mutex query_mutex;

// Creates a JSON payload with the query and parameters
json post_query(const string& query, const json& params = json::object()) {
    // 1. Construct the Neo4j transaction payload
    json payload = {
        {"statements", {{{"statement", query}, {"parameters", params}}}}
    };
    // 2. Send HTTP POST request to Neo4j
    cpr::Response r = cpr::Post(
        cpr::Url{"http://localhost:7474/db/twitter9/tx/commit"},
        cpr::Authentication("neo4j", "password", cpr::AuthMode::BASIC),
        cpr::Header{{"Content-Type", "application/json"}},
        cpr::Body{payload.dump()}
    );
    // 3. Error handling
    if (r.status_code != 200) {
        std::cerr << "Query failed: " << r.status_code << "\n" << r.text << "\n";
    }
    // 4. Return parsed JSON response
    return json::parse(r.text);
}

unordered_set<string> fetch_airline_tweet_ids(const string& airline_id) {
    // Query to get all tweets posted by a specific airline user // Query: Find all tweets posted by this airline
    string query = "MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet) RETURN t.tweetId AS tweetId";
    // Execute query with parameter
    json result = post_query(query, { {"airline_id", airline_id} });
    // Store results in a hash set (for fast lookups)
    unordered_set<string> ids;
    for (const auto& row : result["results"][0]["data"]) {
        ids.insert(row["row"][0].get<string>());
    }
    return ids;
}

unordered_map<string, unordered_map<string, vector<string>>> fetch_component_children_map() {
    // Query to get all tweets with componentId and their replies
    string query = R"(
        MATCH (t:Tweet)
        WHERE t.componentId IS NOT NULL
        OPTIONAL MATCH (t)<-[:REPLIES]-(child:Tweet)
        RETURN t.tweetId AS parent, t.componentId AS componentId, collect(DISTINCT child.tweetId) AS children
    )";
    json result = post_query(query);
    // Organize data by component ID, then parent tweet // Structure: { componentId → { parentTweet → [childTweets] } }
    unordered_map<string, unordered_map<string, vector<string>>> grouped;
    for (const auto& row : result["results"][0]["data"]) {
        string parent = row["row"][0].get<string>();
        string component_id = std::to_string(row["row"][1].get<int>());
        vector<string> children;
        // Collect all replies (children)
        for (const auto& child : row["row"][2]) {
            if (!child.is_null()) children.push_back(child.get<string>());
        }
        grouped[component_id][parent] = children;
    }
    return grouped;
}

// dfs() – Depth-First Search for Conversation Ordering
void dfs(const string& node, const unordered_map<string, vector<string>>& children_map,
         unordered_map<string, bool>& visited, vector<string>& result) {
    if (visited[node]) return; // Skip if already visited
    visited[node] = true; // Mark as visited
    // Recursively visit all children
    auto it = children_map.find(node);
    if (it != children_map.end()) {
        for (const auto& child : it->second) {
            dfs(child, children_map, visited, result);
        }
    }
    // Post-order: Add node after children are processed
    result.push_back(node);
}

unordered_map<string, int> annotate_positions(const vector<string>& convo, const unordered_set<string>& airline_ids) {
    unordered_map<string, int> annotations;
    vector<int> positions;
    // Find positions of airline tweets
    for (int i = 0; i < convo.size(); ++i) {
        if (airline_ids.count(convo[i])) positions.push_back(i);
    }
    // If no airline tweets, mark all as type 0
    if (positions.empty()) {
        for (const auto& tid : convo) annotations[tid] = 0;
        return annotations;
    }
    // Classify tweets based on position relative to airline tweets
    int first = positions.front(), last = positions.back();
    for (int i = 0; i < convo.size(); ++i) {
        if (airline_ids.count(convo[i])) annotations[convo[i]] = 0; // Airline tweet
        else if (i < first) annotations[convo[i]] = 1; // Before first airline tweet
        else if (i > last) annotations[convo[i]] = 2; // After last airline tweet
        else annotations[convo[i]] = 0; // Between airline tweets
    }
    return annotations;
}

void send_batch_to_neo4j(const vector<std::tuple<int, string, vector<string>, unordered_map<string, int>>>& batch) {
    std::ostringstream query;
    // For each conversation in the batch:
    for (const auto& [conv_id, airline_id, trimmed, annotations] : batch) {
        // 1. Create a Conversation node
        query << "CREATE (:Conversation {id: " << conv_id << ", airlineId: '" << airline_id << "'})\n";
        // 2. Match all Tweet nodes in the conversation
        for (size_t i = 0; i < trimmed.size(); ++i) {
            query << "MATCH (t" << i << ":Tweet {tweetId: '" << trimmed[i] << "'})\n";
        }
        // 3. Link Conversation to Tweets with positionType
        for (size_t i = 0; i < trimmed.size(); ++i) {
            query << "MATCH (c:Conversation {id: " << conv_id << "})\n";
            query << "MERGE (c)-[:PART_OF {positionType: " << annotations.at(trimmed[i]) << "}]->(t" << i << ")\n";
        }
    }
    // 4. Execute the batch query (thread-safe)
    std::lock_guard<std::mutex> lock(query_mutex);
    post_query(query.str());
}

void handle_airline(const string& airline_id, int& global_conv_id) {
    std::cout << "==> Handling airline: " << airline_id << std::endl;
    // 1. Create a Neo4j in-memory graph for WCC (Weakly Connected Components)
    string graph_name = "Graph_" + airline_id;
    post_query("CALL gds.graph.drop('" + graph_name + "', false) YIELD graphName");
    post_query("CALL gds.graph.project('" + graph_name + "', 'Tweet', { REPLIES: {type: 'REPLIES', orientation: 'UNDIRECTED'} })");
    post_query("CALL gds.wcc.write('" + graph_name + "', { writeProperty: 'componentId' })");

    // 2. Fetch airline tweets & conversation structures
    auto airline_tweet_ids = fetch_airline_tweet_ids(airline_id);
    auto all_components = fetch_component_children_map();

    vector<std::tuple<int, string, vector<string>, unordered_map<string, int>>> batch;

    // 3. Process each conversation component
    for (const auto& [comp_id, children_map] : all_components) {
        if (children_map.size() < 3) continue;
        // 4. Collect all children tweets
        unordered_set<string> all_children;
        for (const auto& [p, kids] : children_map) all_children.insert(kids.begin(), kids.end());
        // 5. Perform DFS to order tweets chronologically
        unordered_map<string, bool> visited;
        vector<string> ordered;
        for (const auto& [t, _] : children_map) {
            if (!all_children.count(t)) dfs(t, children_map, visited, ordered);
        }
        std::reverse(ordered.begin(), ordered.end());
        // 6. Trim non-airline tweets from start/end
        int start = 0, end = ordered.size();
        while (start < end && airline_tweet_ids.count(ordered[start])) start++;
        while (end > start && airline_tweet_ids.count(ordered[end - 1])) end--;

        vector<string> trimmed(ordered.begin() + start, ordered.begin() + end);
        // 7. Skip if no airline tweets or too small
        bool has_airline = std::any_of(trimmed.begin(), trimmed.end(), [&](const string& t) {
            return airline_tweet_ids.count(t);
        });
        if (!has_airline || trimmed.size() < 3) continue;
        // 8. Annotate tweet positions
        auto annotations = annotate_positions(trimmed, airline_tweet_ids);
        // 9. Add to batch
        batch.emplace_back(global_conv_id++, airline_id, trimmed, annotations);
        // 10. Send batch if it reaches size 50
        if (batch.size() >= 50) {
            send_batch_to_neo4j(batch);
            batch.clear();
        }
    }
    // 11. Send remaining items in batch
    if (!batch.empty()) send_batch_to_neo4j(batch);
    // 12. Clean up graph
    post_query("CALL gds.graph.drop('" + graph_name + "', false) YIELD graphName");
    std::cout << "==> Finished airline: " << airline_id << std::endl;
}

int main() {
    auto start = high_resolution_clock::now();
    // List of airline user IDs
    vector<string> airline_ids = {
        "56377143", "106062176", "18332190", "22536055", "124476322",
        "26223583", "2182373406", "38676903", "1542862735", "253340062",
        "218730857", "45621423", "20626359"
    };
    vector<std::thread> threads;
    int conv_id = 1; // Global conversation counter
    // Process airlines in parallel (max 4 threads)
    for (const auto& airline_id : airline_ids) {
        threads.emplace_back(handle_airline, airline_id, std::ref(conv_id));
        if (threads.size() >= 4) {
            // Join remaining threads
            for (auto& t : threads) t.join();
            // Print execution time
            threads.clear();
        }
    }
    for (auto& t : threads) t.join();

    auto end = high_resolution_clock::now();
    std::cout << "DONE in " << duration_cast<seconds>(end - start).count() << " seconds.\n";
    return 0;
}

// change the memory settings in neo4j:
/* dbms.memory.heap.initial_size=4G
dbms.memory.heap.max_size=6G
dbms.memory.pagecache.size=2G */

// To compile, run the following line in "x64 Native Tools Command Prompt for VS 2022":

/* cd C:\Users\bitas\OneDrive\Documents\DBL1 */

/* cl /std:c++17 fully_automated.cpp /EHsc ^
/I"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\include" ^
/link /LIBPATH:"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\lib" ^
cpr.lib libcurl.lib libssl.lib libcrypto.lib zlib.lib ^
ws2_32.lib crypt32.lib user32.lib advapi32.lib */

//and then:

/* .\fully_automated.exe */
