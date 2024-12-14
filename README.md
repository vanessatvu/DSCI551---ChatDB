# DSCI551---ChatDB
To run the project, ensure you have the following dependencies installed:

pymysql==1.1.0
pandas==1.5.3
spacy==3.6.0

1. You can install these dependencies using the provided requirements.txt file:
pip install -r requirements.txt

2. Set Up Databases:
MySQL: Ensure your MySQL database is running and configured with the necessary tables and data.
MongoDB: Set up your MongoDB database with the required collections.

3. Run the Main Script
Execute the main.py file to start the application:
python main.py

## File Descriptions

main.py
- Entry point for the application
- Prompts the user to select a database system (MongoDB or MySQL).
- Redirects the user to the appropriate interface (mongo_main.py or sqlmain.py) based on their selection.
  
mongo_config.py
- Contains configuration settings for MongoDB.
- Utility function to establish a connection to the MongoDB database.
- Defines key metrics and filters used for query generation.
  
mongo_main.py
- Implements the MongoDB interface for the application.
- Handles user commands for tasks like uploading datasets, exploring collections, generating sample queries, executing user queries, and deleting or switching datasets.
- Interacts with ChatDBMongo to manage backend operations and database queries.
  
mongo_query_generator.py
- Contains the logic for parsing natural language queries into MongoDB queries.
- Defines query templates for various operations such as aggregations, filtering, grouping, and top results.
- Uses tools like spaCy for tokenization and regex for pattern matching.
  
mongo_sample_queries.py
- Generates and displays sample queries to help users understand how to interact with the database.
- Provides predefined query examples covering operations like filtering, grouping, and finding averages or totals.
- Useful for testing and demonstrating the capabilities of the application.

sqlconfig.py
- Contains configuration settings for the MySQL database
- Utility function to establish a connection to the MySQL database.

sqlmain.py
- Implements the MySQL interface for the application.
- Handles user commands for tasks like uploading datasets, exploring tables, generating sample queries, and executing SQL queries.
- Interacts with the ChatDB class to perform backend database operations and manage data.

sqlquery_generator.py
- Responsible for parsing natural language queries into MySQL-compatible SQL queries.
- Defines query templates for tasks such as selecting, filtering, grouping, and aggregating data.
- Uses regex and pattern matching to convert user input into structured SQL queries.
  
sqlsample_queries.py
- Generates and displays sample SQL queries to help users understand the syntax and capabilities of the system.
- Includes examples for filtering data, calculating totals, and finding averages.

*** We also uploaded 2 of our 3 datasets since the 3rd one was too large to upload to GitHub ***
