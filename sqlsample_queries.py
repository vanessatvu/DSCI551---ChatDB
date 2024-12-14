# sample_queries.py
import random

class SampleQueryGenerator:
    def __init__(self, valid_metrics, valid_groups):
        self.metrics = valid_metrics
        self.groups = valid_groups

    def generate_query(self, query_type: str) -> str:
        """Generate a query based on the query type."""
        metric = random.choice(self.metrics)
        group = random.choice(self.groups)
        if query_type == "basic":
            return f"Find total {metric} by {group}"
        elif query_type == "advanced":
            n = random.randint(1, 10)
            return f"Find top {n} {metric} by {group}"
        elif query_type == "grouped":
            return f"What is the average {metric} by {group}?"
        elif query_type == "filtered":
            filter_column = random.choice(self.groups)
            operator = random.choice([">", "<", "="])
            value = random.randint(1, 100)
            return f"Find total {metric} by {group} where {filter_column} {operator} {value}"
        return None

    def generate_sample_queries(self, num_queries: int = 5) -> list:
        """Generate multiple random queries."""
        query_types = ["basic", "advanced", "grouped", "filtered"]
        return [self.generate_query(random.choice(query_types)) for _ in range(num_queries)]
