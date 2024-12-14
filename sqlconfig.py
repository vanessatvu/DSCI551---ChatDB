#config.py

class DatabaseConfig:
    # Database connection configuration
    HOST = 'localhost'  # Database host (e.g., 'localhost' for local MySQL server)
    USER = 'root'       # Your MySQL username
    PASSWORD = 'Hello@12345'  # Your MySQL password
    DATABASE = 'chatDB'       # Name of your MySQL database

class Config:
    # Unified column mappings for datasets
    COLUMN_MAPPINGS = {
        "online_sales": {
            "Transaction ID": "transaction_id",
            "Date": "date",
            "Product Category": "category",
            "Product Name": "product_name",
            "Units Sold": "quantity",
            "Unit Price": "price",
            "Total Revenue": "total_revenue",
            "Region": "location",
            "Payment Method": "payment_method"
        },
        "customer_shopping": {
            "invoice_no": "transaction_id",
            "customer_id": "customer_id",
            "gender": "customer_gender",
            "age": "customer_age",
            "category": "category",
            "quantity": "quantity",
            "price": "price",
            "payment_method": "payment_method",
            "invoice_date": "date",
            "shopping_mall": "location"
        },
        "retail_sales": {
            "transaction_id": "transaction_id",
            "timestamp": "date",
            "customer_id": "customer_id",
            "product_id": "product_id",
            "product_category": "category",
            "quantity": "quantity",
            "price": "price",
            "discount": "discount",
            "payment_method": "payment_method",
            "customer_age": "customer_age",
            "customer_gender": "customer_gender",
            "customer_location": "location",
            "total_amount": "total_revenue"
        }
    }

    # Metrics and groups for dynamic query generation
    VALID_METRICS = {
        "online_sales": ["quantity", "price", "total_revenue"],
        "customer_shopping": ["quantity", "price", "total_revenue"],
        "retail_sales": ["quantity", "price", "total_revenue"]
    }

    VALID_GROUPS = {
        "online_sales": ["category", "location", "payment_method"],
        "customer_shopping": ["category", "location", "payment_method"],
        "retail_sales": ["category", "location", "payment_method"]
    }
