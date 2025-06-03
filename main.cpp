#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <string>
#include <filesystem>

using json = nlohmann::json;

int main() {
    std::cout << "Reading tweet..." << std::endl;

    std::string file_path = "data_testing/sample_tweet.json";
    std::cout << "Looking for file at: " << file_path << std::endl;
    std::cout << "Current working directory: \"" << std::filesystem::current_path().string() << "\"" << std::endl;

    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open file!" << std::endl;
        return 1;
    }

    std::string line;
    std::getline(file, line);
    json tweet = json::parse(line);

    std::string tweet_id = std::to_string(tweet["id"].get<long long>());
    std::string tweet_text = tweet["text"].get<std::string>();

    std::cout << "Parsed tweet ID: " << tweet_id << std::endl;
    std::cout << "Parsed text: " << tweet_text << std::endl;

    // Cypher query
    std::string cypher_query =
        "MERGE (t:Tweet {id: \"" + tweet_id + "\"}) "
        "SET t.text = " + json(tweet_text).dump();
    std::cout << "Cypher query: " << cypher_query << std::endl;

    json payload = {
        {"statements", {{
            {"statement", cypher_query}
        }}}
    };

    std::cout << "Sending to Neo4j..." << std::endl;

    std::string url = "http://localhost:7474/db/neo4j/tx/commit";
    cpr::Response r = cpr::Post(
        cpr::Url{url},
        cpr::Authentication{"neo4j", "username", cpr::AuthMode::BASIC},
        cpr::Header{{"Content-Type", "application/json"}},
        cpr::Body{payload.dump()}
    );

    if (r.status_code == 0) {
        std::cerr << "Failed to connect to Neo4j." << std::endl;
    } else {
        std::cout << "Status Code: " << r.status_code << std::endl;
        std::cout << "Response Body:\n" << r.text << std::endl;
    }

    return 0;
}

// To compile, run the following line in "x64 Native Tools Command Prompt for VS 2022":

/* cd C:\Users\bitas\OneDrive\Documents\DBL1 */

/* cl /std:c++17 main.cpp /EHsc ^
/I"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\include" ^
/link /LIBPATH:"C:\Users\bitas\OneDrive\Documents\vcpkg\installed\x64-windows-static\lib" ^
cpr.lib libcurl.lib libssl.lib libcrypto.lib zlib.lib ^
ws2_32.lib crypt32.lib user32.lib advapi32.lib */

//and then:

/* .\main.exe */


