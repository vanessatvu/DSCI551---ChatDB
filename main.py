import os
from sqlmain import ChatDB
from mongo_main import ChatDBMongo


def main():
    print("Welcome to the Sales ChatDB System :D")
    print("Choose your database system:")
    print("1. MongoDB")
    print("2. MySQL")

    while True:
        db_choice = input("Enter your choice (1 for MongoDB, 2 for MySQL): ").strip()

        if db_choice == "1":
            print("\nYou have selected the MongoDB Database System.")
            mongo_main() # call mysql interface
            break
        elif db_choice == "2":
            print("\nYou have selected the MySQL Database System.")
            sql_main()
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")


# mysql interface
def sql_main():
    chatdb = ChatDB()
    print("Welcome to the Sales MySQL System! Type 'exit' to quit.")

    while True:
        print("\nCommands: upload_dataset, explore, sample_queries, query, exit")
        cmd = input("Enter a command: ").strip().lower()

        if cmd == "exit":
            break
        elif cmd == "upload_dataset":
            # Prompt user to upload a dataset
            dataset_path = upload_dataset()
            if dataset_path:
                table_name = input("Enter a name for the new table: ").strip()
                chatdb.upload_dataset(dataset_path, table_name)
        elif cmd == "explore":
            chatdb.explore_tables()
        elif cmd == "sample_queries":
            chatdb.show_sample_queries()
        elif cmd == "query":
            query = input("Enter your query: ")
            chatdb.process_query(query)
        else:
            print("Invalid command. Please try again.")

    chatdb.close()


# mongo interface
def mongo_main():
    chatdb = ChatDBMongo()
    print("Welcome to the Sales MongoDB System ^-^! Type 'exit' to quit.")

    while True:
        print("\nCommands: upload dataset, explore data, delete dataset, switch dataset, sample queries, query, exit") # prompt user to select a command
        cmd = input("Enter a command: ").strip().lower()

        if cmd == "exit":
            print("Bye bye :)")
            break

        elif cmd == "upload dataset":
            dataset_path = upload_dataset()
            if dataset_path:
                collection_name = input("Enter a name for the new collection: ").strip()
                chatdb.upload_dataset(dataset_path, collection_name)

        elif cmd == "explore data":
            chatdb.list_collections()
            chatdb.select_collection()
            chatdb.describe_collection()

        elif cmd == "delete dataset":
            chatdb.delete_collection()

        elif cmd == "switch dataset":
            chatdb.switch_collection()

        elif cmd == "sample queries":
            chatdb.show_sample_queries()

        elif cmd == "query":
            query = input("Enter your query: ").strip()
            chatdb.process_query(query)

        else:
            print("Invalid command. Please try again.")

    chatdb.close()


# function to allow user to upload a dataset into the system
def upload_dataset():
    file_path = input("Enter the full path of the dataset file (CSV format): ").strip()
    if not os.path.exists(file_path) or not file_path.endswith('.csv'):
        print("Invalid file path or format. Please upload a valid CSV file.")
        return None
    return file_path


if __name__ == "__main__":
    main()
