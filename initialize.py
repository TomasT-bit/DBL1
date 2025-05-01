import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Paths to your CSV files
users_csv = "C:\\Users\\20231225\\Desktop\\DBL1\\Neo\\relate-data\\dbmss\\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\\import\\users.csv"
tweets_csv = "C:\\Users\\20231225\\Desktop\\DBL1\\Neo\\relate-data\\dbmss\\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\\import\\tweets.csv"
relationships_csv = "C:\\Users\\20231225\\Desktop\\DBL1\\Neo\\relate-data\\dbmss\\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\\import\\relationships.csv"
mentions_csv = "C:\\Users\\20231225\\Desktop\\DBL1\\Neo\\relate-data\\dbmss\\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\\import\\mentions.csv"

# Full path to neo4j-admin executable
neo4j_admin_executable = "C:\\Users\\20231225\\Desktop\\DBL1\\Neo\\relate-data\\dbmss\\dbms-a8ab2966-095b-4dcc-ae66-ad0708e5ee24\\bin\\neo4j-admin.bat"

# Neo4j database name
database_name = "neo4j"

def run_import_command():
    try:
        # Run the import command
        logging.info("Running import command...")
        import_command = [
            neo4j_admin_executable, "database", "import",
            f"--database={database_name}",
            f"--nodes={users_csv}",
            f"--nodes={tweets_csv}",
            f"--relationships={relationships_csv}",
            f"--relationships={mentions_csv}"
        ]
        subprocess.run(import_command, check=True, shell=True)
        
        logging.info("Import completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    run_import_command()
