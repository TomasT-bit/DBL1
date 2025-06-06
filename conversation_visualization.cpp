#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <filesystem>
#include <curl/curl.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;

const std::string NEO4J_URL = "http://localhost:7474/db/twitter/tx/commit";
const std::string USERNAME = "neo4j";
const std::string PASSWORD = "password";

const std::vector<std::string> airline_ids = {
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
};

int conversation_counter = 1;

// CURL write callback
size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    std::string* str = static_cast<std::string*>(userdata);
    str->append(ptr, size * nmemb);
    return size * nmemb;
}

// Function to send a Cypher query via Neo4j REST API
json run_query(const std::string& query, const json& params = {}) {
    CURL* curl = curl_easy_init();
    std::string response_data;

    if (curl) {
        json body = {
            {"statements", {{
                {"statement", query},
                {"parameters", params}
            }}}
        };

        std::string auth = USERNAME + ":" + PASSWORD;
        std::string body_str = body.dump();

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl, CURLOPT_URL, NEO4J_URL.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_str.c_str());
        curl_easy_setopt(curl, CURLOPT_USERPWD, auth.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_data);

        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cerr << "CURL Error: " << curl_easy_strerror(res) << "\n";
        }

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }

    return json::parse(response_data);
}

// Function to write CSV lines
void write_csv_headers(std::ofstream& conv_file, std::ofstream& edge_file) {
    conv_file << ":ID(Conversation),airlineId\n";
    edge_file << ":START_ID(Conversation),:END_ID(Tweet),:TYPE\n";
}

void write_conversations_csv(const std::vector<std::vector<std::string>>& conversations, const std::string& airline_id,
                             std::ofstream& conv_file, std::ofstream& edge_file) {
    for (const auto& conv : conversations) {
        std::string conv_id = airline_id + "_" + std::to_string(conversation_counter++);
        conv_file << conv_id << "," << airline_id << "\n";
        for (const auto& tid : conv) {
            edge_file << conv_id << "," << tid << ",PART_OF\n";
        }
    }
}

// Retrieve and filter WCC components from Neo4j
std::vector<std::vector<std::string>> get_conversations(const std::string& airline_id) {
    std::vector<std::vector<std::string>> matched;

    // Project graph
    run_query(R"(
        CALL gds.graph.project(
            'Graph',
            'Tweet',
            {
                REPLIES: {
                    type: 'REPLIES',
                    orientation: 'UNDIRECTED'
                }
            }
        )
    )");

    // WCC algorithm
    json wcc_res = run_query(R"(
        CALL gds.wcc.stream('Graph')
        YIELD nodeId, componentId
        RETURN gds.util.asNode(nodeId).tweetId AS tweetId, componentId
    )");

    std::map<int, std::vector<std::string>> components;
    for (const auto& record : wcc_res["results"][0]["data"]) {
        auto row = record["row"];
        std::string tweetId = row[0];
        int componentId = row[1];
        components[componentId].push_back(tweetId);
    }

    // Get tweets from this airline
    json tweet_res = run_query(R"(
        MATCH (u:User {userId: $airline_id})-[:POSTED]->(t:Tweet)
        RETURN t.tweetId AS tweetId
    )", {{"airline_id", airline_id}});

    std::set<std::string> airline_tweet_ids;
    for (const auto& record : tweet_res["results"][0]["data"]) {
        airline_tweet_ids.insert(record["row"][0]);
    }

    // Filter components
    for (auto& [cid, tweets] : components) {
        std::set<std::string> unique;
        std::vector<std::string> filtered;
        for (const auto& tid : tweets) {
            if (unique.insert(tid).second) filtered.push_back(tid);
        }

        // Filter invalid conversations
        bool has_valid = false;
        for (size_t i = 0; i < filtered.size(); ++i) {
            if (airline_tweet_ids.count(filtered[i])) {
                bool before = false, after = false;
                for (size_t j = 0; j < i; ++j)
                    if (!airline_tweet_ids.count(filtered[j])) before = true;
                for (size_t j = i + 1; j < filtered.size(); ++j)
                    if (!airline_tweet_ids.count(filtered[j])) after = true;
                if (before && after) {
                    has_valid = true;
                    break;
                }
            }
        }

        if (has_valid) {
            matched.push_back(filtered);
        }
    }

    // Drop graph
    run_query("CALL gds.graph.drop('Graph') YIELD graphName");

    std::cout << "Found " << matched.size() << " valid conversations for " << airline_id << "\n";
    return matched;
}

int main() {
    fs::create_directory("import");
    std::ofstream conv_file("import/conversations.csv");
    std::ofstream edge_file("import/conversation_edges.csv");

    write_csv_headers(conv_file, edge_file);

    for (const auto& airline_id : airline_ids) {
        auto conversations = get_conversations(airline_id);
        write_conversations_csv(conversations, airline_id, conv_file, edge_file);
    }

    conv_file.close();
    edge_file.close();

    return 0;
}


/* cd C:\Users\bitas\OneDrive\Documents\DBL1 */

/* cl /std:c++17 conversation_visualization.cpp /EHsc ^
/I"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\include" ^
/link /LIBPATH:"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\lib" ^
cpr.lib libcurl.lib libssl.lib libcrypto.lib zlib.lib ^
ws2_32.lib crypt32.lib user32.lib advapi32.lib */

//and then:

/* .\conversation_visualization.exe */