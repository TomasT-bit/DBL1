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
using namespace std::string_literals;
using namespace std::chrono;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

std::mutex query_mutex;

json post_query(const string& query, const json& params = json::object()) {
    json payload = {
        {"statements", {{{"statement", query}, {"parameters", params}}}}
    };
    cpr::Response r = cpr::Post(
        cpr::Url{"http://localhost:7474/db/twitter9/tx/commit"},
        cpr::Authentication("neo4j", "password", cpr::AuthMode::BASIC),
        cpr::Header{{"Content-Type", "application/json"}},
        cpr::Body{payload.dump()}
    );
    if (r.status_code != 200) {
        std::cerr << "Query failed: " << r.status_code << "\n" << r.text << "\n";
    }
    return json::parse(r.text);
}

unordered_set<string> fetch_airline_tweet_ids(const string& airline_id) {
    string query = "MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet) RETURN t.tweetId AS tweetId";
    json result = post_query(query, { {"airline_id", airline_id} });
    unordered_set<string> ids;
    for (const auto& row : result["results"][0]["data"]) {
        ids.insert(row["row"][0].get<string>());
    }
    return ids;
}

unordered_map<string, unordered_map<string, vector<string>>> fetch_component_children_map() {
    string query = R"(
        MATCH (t:Tweet) WHERE exists(t.componentId) AND t.componentId IS NOT NULL
        OPTIONAL MATCH (t)<-[:REPLIES]-(child:Tweet)
        RETURN t.tweetId AS parent, t.componentId AS componentId, collect(DISTINCT child.tweetId) AS children
    )";
    json result = post_query(query);
    unordered_map<string, unordered_map<string, vector<string>>> grouped;
    for (const auto& row : result["results"][0]["data"]) {
        string parent = row["row"][0].get<string>();
        string component_id = std::to_string(row["row"][1].get<int>());
        vector<string> children;
        for (const auto& child : row["row"][2]) {
            if (!child.is_null()) children.push_back(child.get<string>());
        }
        grouped[component_id][parent] = children;
    }
    return grouped;
}

void dfs(const string& node, const unordered_map<string, vector<string>>& children_map,
         unordered_map<string, bool>& visited, vector<string>& result) {
    if (visited[node]) return;
    visited[node] = true;
    auto it = children_map.find(node);
    if (it != children_map.end()) {
        for (const auto& child : it->second) {
            dfs(child, children_map, visited, result);
        }
    }
    result.push_back(node);
}

unordered_map<string, int> annotate_positions(const vector<string>& convo, const unordered_set<string>& airline_ids) {
    unordered_map<string, int> annotations;
    vector<int> positions;
    for (int i = 0; i < convo.size(); ++i) {
        if (airline_ids.count(convo[i])) positions.push_back(i);
    }
    if (positions.empty()) {
        for (const auto& tid : convo) annotations[tid] = 0;
        return annotations;
    }
    int first = positions.front(), last = positions.back();
    for (int i = 0; i < convo.size(); ++i) {
        if (airline_ids.count(convo[i])) annotations[convo[i]] = 0;
        else if (i < first) annotations[convo[i]] = 1;
        else if (i > last) annotations[convo[i]] = 2;
        else annotations[convo[i]] = 0;
    }
    return annotations;
}

void handle_airline(const string& airline_id, int& global_conv_id) {
    std::cout << "==> Handling airline: " << airline_id << std::endl;

    string graph_name = "Graph_" + airline_id;
    post_query("CALL gds.graph.drop('" + graph_name + "', false) YIELD graphName");
    post_query("CALL gds.graph.project('" + graph_name + "', 'Tweet', { REPLIES: {type: 'REPLIES', orientation: 'UNDIRECTED'} })");
    post_query("CALL gds.wcc.write('" + graph_name + "', { writeProperty: 'componentId' })");

    auto airline_tweet_ids = fetch_airline_tweet_ids(airline_id);
    std::cout << "   Airline tweets fetched: " << airline_tweet_ids.size() << std::endl;

    auto all_components = fetch_component_children_map();
    std::cout << "   Components found: " << all_components.size() << std::endl;

    for (const auto& [comp_id, children_map] : all_components) {
        if (children_map.size() < 3) continue;

        unordered_set<string> all_children;
        for (const auto& [p, kids] : children_map) all_children.insert(kids.begin(), kids.end());

        unordered_map<string, bool> visited;
        vector<string> ordered;
        for (const auto& [t, _] : children_map) {
            if (!all_children.count(t)) dfs(t, children_map, visited, ordered);
        }
        std::reverse(ordered.begin(), ordered.end());

        int start = 0, end = ordered.size();
        while (start < end && airline_tweet_ids.count(ordered[start])) start++;
        while (end > start && airline_tweet_ids.count(ordered[end - 1])) end--;

        vector<string> trimmed(ordered.begin() + start, ordered.begin() + end);
        bool has_airline = std::any_of(trimmed.begin(), trimmed.end(), [&](const string& t) {
            return airline_tweet_ids.count(t);
        });
        if (!has_airline || trimmed.size() < 3) continue;

        auto annotations = annotate_positions(trimmed, airline_tweet_ids);

        std::ostringstream query;
        query << "CREATE (:Conversation {id: " << global_conv_id << ", airlineId: '" << airline_id << "'})\n";
        for (size_t i = 0; i < trimmed.size(); ++i) {
            query << "MATCH (t" << i << ":Tweet {tweetId: '" << trimmed[i] << "'})\n";
        }
        for (size_t i = 0; i < trimmed.size(); ++i) {
            query << "MATCH (c:Conversation {id: " << global_conv_id << "})\n";
            query << "MERGE (c)-[:PART_OF {positionType: " << annotations[trimmed[i]] << "}]->(t" << i << ")\n";
        }

        {
            std::lock_guard<std::mutex> lock(query_mutex);
            post_query(query.str());
        }

        global_conv_id++;
    }

    post_query("CALL gds.graph.drop('" + graph_name + "', false) YIELD graphName");
    std::cout << "==> Finished airline: " << airline_id << std::endl;
}

int main() {
    auto start = high_resolution_clock::now();
    vector<string> airline_ids = {
        "56377143", "106062176", "18332190", "22536055", "124476322",
        "26223583", "2182373406", "38676903", "1542862735", "253340062",
        "218730857", "45621423", "20626359"
    };
    vector<std::thread> threads;
    int conv_id = 1;

    for (const auto& airline_id : airline_ids) {
        threads.emplace_back(handle_airline, airline_id, std::ref(conv_id));
        if (threads.size() >= 4) {
            for (auto& t : threads) t.join();
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


// run on neo4j to load csv files:
/* LOAD CSV WITH HEADERS FROM 'file:///conversations.csv' AS row
MERGE (:Conversation {id: toInteger(row.`:ID(Conversation)`), airlineId: row.airlineId}); */

/* LOAD CSV WITH HEADERS FROM 'file:///conversation_edges.csv' AS row
MATCH (c:Conversation {id: toInteger(row.`:START_ID(Conversation)`)})
MATCH (t:Tweet {tweetId: row.`:END_ID(Tweet)`})
MERGE (c)-[:PART_OF {positionType: toInteger(row.positionType)}]->(t); */

/* LOAD CSV WITH HEADERS FROM 'file:///conversation_stats.csv' AS row
MATCH (c:Conversation {id: toInteger(row.`:ID(Conversation)`)})
SET c.size = toInteger(row.size); */
