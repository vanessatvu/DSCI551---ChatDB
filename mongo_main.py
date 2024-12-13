from pymongo import MongoClient
import pandas as pd
import os
from mongo_config import MongoDBConfig, Config
from mongo_query_generator import QueryGenerator
from mongo_sample_queries import SampleQueryGenerator, display_sample_queries



class ChatDBMongo:
    def __init__(self):
        self.client = MongoClient(MongoDBConfig.HOST, MongoDBConfig.PORT)
        self.db = self.client[MongoDBConfig.DATABASE]
        self.query_generator = QueryGenerator(
            Config.VALID_TOTAL_METRICS["default"],
            Config.VALID_AVERAGE_METRICS["default"],
            Config.VALID_GROUPS["default"],
            Config.NUMERIC_FILTERS,  # pass NUMERIC_FILTERS as a list
            Config.STRING_FILTERS   # pass STRING_FILTERS as a list
        )
        self.sample_query_generator = SampleQueryGenerator(
            Config.VALID_TOTAL_METRICS["default"],
            Config.VALID_AVERAGE_METRICS["default"],
            Config.VALID_GROUPS["default"],
            Config.NUMERIC_FILTERS,  # pass NUMERIC_FILTERS as a list
            Config.STRING_FILTERS   # pass STRING_FILTERS as a list
        )
        self.selected_collection = None


    # uploading dataset to db
    def upload_dataset(self, file_path, collection_name):
        try:
            df = pd.read_csv(file_path)
            data = df.to_dict(orient="records")
            self.db[collection_name].insert_many(data)
            print(f"Dataset successfully uploaded to MongoDB as collection '{collection_name}'.")
        except Exception as e:
            print(f"Error uploading dataset to MongoDB: {e}")


    # func to list collections in db
    def list_collections(self):
        collections = self.db.list_collection_names()
        if not collections:
            print("No collections available.")
            return []
        print("\nAvailable Collections:")
        for idx, col in enumerate(collections, 1):
            print(f"{idx}. {col}")
        return collections


    # func to prompt user to select collection
    def select_collection(self):
        collections = self.list_collections()
        if collections:  # make sure collections exist
            while True:
                try:
                    collection_idx = int(input(f"Select a collection by number (1-{len(collections)}): ")) - 1
                    if 0 <= collection_idx < len(collections):
                        self.selected_collection = collections[collection_idx]
                        print(f"You selected: {self.selected_collection}")
                        return
                    else:
                        print(f"Invalid selection. Please choose a number between 1 and {len(collections)}.")
                except ValueError:
                    print("Invalid input. Please enter a valid number.")


    # schema & sample data for collection
    def describe_collection(self):
        if not self.selected_collection:
            print("Please explore data to select a collection first.") # prompt user to select collection first
            return
        print(f"\n### {self.selected_collection.upper()} ###")
        document = self.db[self.selected_collection].find_one()
        if not document:
            print(f"Collection {self.selected_collection} is empty.")
            return
        # show schema
        print("Schema of the Data:")
        for idx, key in enumerate(document.keys(), start=1):
            print(f"{idx}. {key}")

        # show sample data
        print("\nSample Data:")
        samples = self.db[self.selected_collection].find().limit(5)
        for idx, sample in enumerate(samples, start=1):
            print(f"{idx}. {sample}")


    # function for sample queries
    def show_sample_queries(self):
        if not self.selected_collection:
            print("Please explore data to select a collection first.")
            return
        queries = self.sample_query_generator.generate_sample_queries(5)
        display_sample_queries(queries)  # display function for formatted output


    # process & execute user query
    def process_query(self, query):
        if not self.selected_collection:
            print("Please explore data to select a collection first.")
            return

        # parse natural language query
        query_type, params = self.query_generator.parse_query(query)
        if not query_type:
            print("Query not recognized. Please try again")
            return

        try:
            # generate the MongoDB query
            mongo_query = self.query_generator.generate_mongo_query(query_type, params)

            # display mongo query
            print("\nMongoDB Query:")
            if isinstance(mongo_query, list):
                for stage in mongo_query:
                    print(stage)
            else:
                print(mongo_query)

            # execute query
            collection = self.db[self.selected_collection]
            if isinstance(mongo_query, list):
                results = collection.aggregate(mongo_query)
            else:
                results = collection.find(mongo_query)

            # display results
            print("\nResults:")
            results_found = False
            for res in results:
                results_found = True
                print(res)
            if not results_found:
                print("No results found.")

        except Exception as e:
            print(f"Error executing query: {e}")


    # delete current collection
    def delete_collection(self):
        if not self.selected_collection:
            print("No collection is currently selected.")
            return
        confirmation = input(f"Are you sure you want to delete the collection '{self.selected_collection}'? (yes/no): ").strip().lower()
        if confirmation == "yes":
            self.db[self.selected_collection].drop()
            print(f"Collection '{self.selected_collection}' has been deleted.")
            self.selected_collection = None
        else:
            print("Delete operation cancelled.")


    # switch collection
    def switch_collection(self):
        self.select_collection()

    def close(self):
        self.client.close()


# upload dataset to mongo
def upload_dataset():
    file_path = input("Enter the full path of the dataset file (CSV format): ").strip()
    if not os.path.exists(file_path) or not file_path.endswith('.csv'):
        print("Invalid file path or format. Please upload a valid CSV file.")
        return None
    return file_path


def upload_to_mongodb(chatdb, file_path, collection_name):
    """Handle dataset upload to MongoDB."""
    chatdb.upload_dataset(file_path, collection_name)


# mongo interface
def mongo_main():
    chatdb = ChatDBMongo()
    print("Welcome to the Sales MongoDB System :D! Type 'exit' to quit.")

    # prompt user to select a command
    while True:
        print("\nCommands: upload dataset, explore data, sample queries, query, switch collection, delete collection, exit")
        cmd = input("Enter a command: ").strip().lower()

        if cmd == "exit":
            break

        elif cmd == "upload dataset":
            dataset_path = upload_dataset()
            if dataset_path:
                collection_name = input("Enter a name for the new collection: ").strip()
                upload_to_mongodb(chatdb, dataset_path, collection_name)

        elif cmd == "delete collection":
            chatdb.delete_collection()

        elif cmd == "switch collection":
            chatdb.switch_collection()

        elif cmd == "explore data":
            chatdb.select_collection()
            chatdb.describe_collection()

        elif cmd == "sample queries":
            chatdb.show_sample_queries()

        elif cmd == "query":
            query = input("Enter your query: ").strip()
            chatdb.process_query(query)

        else:
            print("Invalid command. Please try again.")

    chatdb.close()
