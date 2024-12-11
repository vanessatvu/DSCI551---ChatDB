from pymongo import MongoClient

class MongoDBConfig:
    # Connection details for MongoDB
    HOST = 'localhost'
    PORT = 27017
    DATABASE = 'salesDB'

    @staticmethod
    def get_connection():
        """Connect to MongoDB server and return the database."""
        try:
            client = MongoClient(MongoDBConfig.HOST, MongoDBConfig.PORT)
            db = client[MongoDBConfig.DATABASE]
            print(f"Successfully connected to the database: {MongoDBConfig.DATABASE}")
            return db
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return None


class Config:
    NUMERIC_FILTERS = ["price", "quantity", "discount", "customer_age", "total_revenue"]
    STRING_FILTERS = ["location", "category", "payment_method"]

    VALID_TOTAL_METRICS = {
        "default": ["sales", "total_revenue"]
    }

    VALID_AVERAGE_METRICS = {
        "default": ["quantity", "price", "total_revenue"]
    }

    VALID_GROUPS = {
        "default": ["category", "location", "payment_method"]
    }


