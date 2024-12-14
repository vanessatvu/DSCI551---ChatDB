#sqlmain.py

import pymysql
import pandas as pd  # Importing pandas
import mysql.connector  # Importing mysql.connector
from sqlconfig import Config, DatabaseConfig
from sqlquery_generator import QueryGenerator
from sqlsample_queries import SampleQueryGenerator
import os

class ChatDB:
    def __init__(self):
        # Connect to MySQL database
        self.connection = pymysql.connect(
            host=DatabaseConfig.HOST,
            user=DatabaseConfig.USER,
            password=DatabaseConfig.PASSWORD,
            database=DatabaseConfig.DATABASE
        )
        self.cursor = self.connection.cursor()
        self.query_generator = QueryGenerator()
        self.sample_query_generator = SampleQueryGenerator(
            Config.VALID_METRICS["online_sales"], Config.VALID_GROUPS["online_sales"]
        )
        self.selected_table = None

    def upload_dataset(self, dataset_path, table_name):
        """Upload a dataset to MySQL."""
        if not os.path.exists(dataset_path):
            print(f"Error: The file at '{dataset_path}' does not exist.")
            return None
        if not dataset_path.endswith('.csv'):
            print("Error: Please provide a valid CSV file.")
            return None

        try:
            print(f"Reading the dataset from {dataset_path}...")
            df = pd.read_csv(dataset_path)
            print(f"Dataset has {len(df)} rows and {len(df.columns)} columns.")

            # Establish the database connection
            connection = mysql.connector.connect(
                host=DatabaseConfig.HOST,
                user=DatabaseConfig.USER,
                password=DatabaseConfig.PASSWORD,
                database=DatabaseConfig.DATABASE
            )

            if connection.is_connected():
                print("Successfully connected to the MySQL database.")
                cursor = connection.cursor()

                # Dynamically create the table based on the DataFrame
                print(f"Creating table '{table_name}'...")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                columns = ", ".join([f"{col} TEXT" for col in df.columns])
                cursor.execute(f"CREATE TABLE {table_name} ({columns})")

                # Insert data into the newly created table
                print("Inserting data into the table...")
                for _, row in df.iterrows():
                    # Escape single quotes by replacing them with double single quotes
                    values = ", ".join(["'{}'".format(str(value).replace("'", "''")) for value in row.values])
                    cursor.execute(f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({values})")

                connection.commit()
                print(f"Dataset successfully uploaded to MySQL as table '{table_name}'.")

                return table_name  # Return the table name
            else:
                print("Failed to connect to MySQL.")
                return None
        except mysql.connector.Error as e:
            print(f"Error uploading dataset: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None
        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed.")

    def explore_tables(self):
        """Display available tables and allow user to select a table."""
        try:
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            if tables:
                print("\nAvailable Tables:")
                for idx, table in enumerate(tables, 1):
                    # Access the first item of the tuple, which is the table name
                    table_name = table[0]
                    print(f"{idx}. {table_name}")
                # Allow user to select a table with number validation
                while True:
                    try:
                        selection = int(input("Enter the number of the table you want to explore: "))
                        if selection < 1 or selection > len(tables):
                            print(f"Please enter a number between 1 and {len(tables)}.")
                        else:
                            self.selected_table = tables[selection-1][0]  # Get table name from tuple
                            self.describe_table(self.selected_table)
                            break
                    except ValueError:
                        print("Invalid input. Please enter a valid number.")
            else:
                print("No tables found.")
        except Exception as e:
            print(f"Error fetching tables: {e}")


    def describe_table(self, table: str):
        """Display table schema and sample data."""
        self.cursor.execute(f"DESCRIBE {table};")
        schema = self.cursor.fetchall()
        print(f"\n### {table.upper()} ###")
        print("Schema:")
        for col in schema:
            print(f"- {col[0]} ({col[1]})")
        self.cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
        data = self.cursor.fetchall()
        if data:
            print("\nSample Data:")
            for row in data:
                print(row)

    def show_sample_queries(self):
        """Generate and display sample queries for the selected table."""
        queries = self.sample_query_generator.generate_sample_queries(5)
        print("\nSample Queries:")
        for idx, query in enumerate(queries, start=1):
            print(f"{idx}. {query}")

    def process_query(self, query: str):
        """Parse and execute a natural language query."""
        query_type, params = self.query_generator.parse_query(query)
        if query_type:
            groups = params["groups"]
            try:
                sql = params["template"].format(
                    table=self.selected_table,
                    metric=groups[0],
                    group_by=groups[1],
                    filter_column=groups[2] if len(groups) > 2 else "",
                    operator=groups[3] if len(groups) > 3 else "",
                    value=groups[4] if len(groups) > 4 else "",
                    n=groups[0] if query_type == "top_n" else ""
                )
                print("\nExecuting SQL:")
                print(sql)
                self.cursor.execute(sql)
                print("\nResults:")
                for row in self.cursor.fetchall():
                    print(row)
            except Exception as e:
                print(f"Error executing query: {e}")
        else:
            print("Query not recognized.")

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.connection.close()

def main():
    chatdb = ChatDB()
    print("Welcome to ChatDB! Type 'exit' to quit.")
    tables = chatdb.explore_tables()
    while True:
        print("\nCommands: upload dataset, explore, sample queries, query, exit")
        cmd = input("Enter a command: ").strip().lower()
        if cmd == "exit":
            break
        elif cmd == "upload dataset":
            dataset_path = input("Enter the full path of the dataset file (CSV format): ")
            table_name = input("Enter a name for the new table: ")
            chatdb.upload_dataset(dataset_path, table_name)
        elif cmd == "explore":
            table_idx = int(input(f"Select a table by number (1-{len(tables)}): ")) - 1
            chatdb.selected_table = tables[table_idx]
            print(f"You selected: {chatdb.selected_table}")
            chatdb.describe_table(chatdb.selected_table)
        elif cmd == "sample queries":
            if chatdb.selected_table:
                chatdb.show_sample_queries()
            else:
                print("Please explore and select a table first.")
        elif cmd == "query":
            query = input("Enter your query: ")
            chatdb.process_query(query)
        else:
            print("Invalid command. Please try again.")
    chatdb.close()

if __name__ == "__main__":
    main()
