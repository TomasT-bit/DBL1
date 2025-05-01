import os
from neo4j import GraphDatabase

def import_airlines():
    # Get the current working directory (the directory where this script is located)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    
    # Define the relative path to the JSON file from the current working directory
    json_file_path = os.path.join(current_dir, "DBL1", "data", "airlines-1558527599826.json")
    
    # Convert the relative Windows path to the Neo4j file URL format
    file_url = f"file:///{json_file_path.replace('\\', '/')}"
    
    # Connect to Neo4j
    uri = "bolt://localhost:7687"  # Update if necessary
    username = "neo4j"
    password = "password"  # Replace with your Neo4j password

    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        # Using APOC to import JSON
        query = f"""
        CALL apoc.periodic.iterate(
            'CALL apoc.load.json("{file_url}") YIELD value
             RETURN value',
            'WITH value AS airline
             MERGE (a:Airline {{id: airline.id}})
             SET a.name = airline.name, a.country = airline.country, a.code = airline.code', 
            {{batchSize:1000, parallel:true}})
        """
        session.run(query)
        print("Import completed successfully.")

import_airlines()
