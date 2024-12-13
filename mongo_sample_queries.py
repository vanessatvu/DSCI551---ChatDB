import random

class SampleQueryGenerator:
    def __init__(self, total_metrics, average_metrics, valid_groups, numeric_filters, string_filters):
        self.total_metrics = total_metrics
        self.average_metrics = average_metrics
        self.groups = valid_groups
        self.numeric_filters = numeric_filters
        self.string_filters = string_filters

    def generate_query(self, query_type: str) -> dict:
        group = random.choice(self.groups)

        if query_type == "basic":
            metric = random.choice(self.total_metrics)
            if metric == "sales":
                return {
                    "natural_query": f"Find total sales by {group}",
                    "mongo_query": [{"$group": {"_id": f"${group}", "total_sales": {"$sum": 1}}}]
                }
            return {
                "natural_query": f"Find total {metric} by {group}",
                "mongo_query": [{"$group": {"_id": f"${group}", "total_metric": {"$sum": f"${metric}"}}}]
            }

        elif query_type == "grouped":
            metric = random.choice(self.average_metrics)
            return {
                "natural_query": f"What is the average {metric} by {group}?",
                "mongo_query": {
                    "$group": {"_id": f"${group}", "average_metric": {"$avg": f"${metric}"}}
                }
            }

        elif query_type == "filtered":
            filter_type = random.choice(["numeric", "string"])
            metric = random.choice(self.total_metrics)

            if filter_type == "numeric":
                filter_column = random.choice(self.numeric_filters)
                operator = random.choice(["<", ">", "="])
                value = random.randint(1, 100)
                return {
                    "natural_query": f"Find total {metric} by {group} where {filter_column} {operator} {value}",
                    "mongo_query": [
                        {"$match": {filter_column: {f"${operator}": value}}},
                        {"$group": {"_id": f"${group}", "total_metric": {"$sum": f"${metric}"}}}
                    ]
                }
            elif filter_type == "string":
                filter_column = random.choice(self.string_filters)
                random_value = random.choice(self.get_random_values(filter_column))
                return {
                    "natural_query": f"Find total {metric} by {group} where {filter_column} = {random_value}",
                    "mongo_query": [
                        {"$match": {filter_column: random_value}},
                        {"$group": {"_id": f"${group}", "total_metric": {"$sum": f"${metric}"}}}
                    ]
                }

        elif query_type == "advanced":
            n = random.randint(1, 10)
            return {
                "natural_query": f"Find top {n} sales by {group}",
                "mongo_query": [
                    {"$group": {"_id": f"${group}", "total_sales": {"$sum": 1}}},
                    {"$sort": {"total_sales": -1}},
                    {"$limit": n}
                ]
            }

    def get_random_values(self, filter_column):
        """Return sample string values for filters."""
        if filter_column == "location":
            return ["Asia", "Europe", "North America"]
        elif filter_column == "category":
            return ["Electronics", "Clothing", "Sports", "Books"]
        elif filter_column == "payment_method":
            return ["Credit Card", "Debit Card", "PayPal"]
        return []

    def generate_sample_queries(self, num_queries: int = 5) -> list:
        query_types = ["basic", "grouped", "filtered", "advanced"]
        return [self.generate_query(random.choice(query_types)) for _ in range(num_queries)]



# func to display both natural language and mongo queries
def display_sample_queries(samples):
    print("\nSample Queries:")
    for i, sample in enumerate(samples, 1):
        natural_query = sample["natural_query"]
        mongo_query = sample["mongo_query"]

        print(f"{i}. Natural Query:")
        print(f"   {natural_query}")
        print("   MongoDB Query:")

        # for multi stage queries
        if isinstance(mongo_query, list):
            for stage in mongo_query:
                print(f"       {stage}")

        # Hfor single stage queries
        elif isinstance(mongo_query, dict):
            print(f"       {mongo_query}")

        print()



