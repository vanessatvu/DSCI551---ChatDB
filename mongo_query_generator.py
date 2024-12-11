import re
import spacy
from typing import Tuple, Optional


class QueryGenerator:
    '''
    args:
        total_metrics: list of metrics for total calculations - total sales, total revenue
        average_metrics: list of metrics for average calculations - for finding average price, average quantity, etc
        valid_group: list of valid group-by fields - category, location, payment_method, etc
        numeric_filters: list of numeric filter fields - price, quantity
        string_filters: list of string filter fields - payment_method, category
    '''
    def __init__(self, total_metrics, average_metrics, valid_groups, numeric_filters, string_filters):
        # Load spaCy for parsing & tokenization
        self.nlp = spacy.load("en_core_web_sm")

        # Parameters for class attributes
        self.total_metrics = total_metrics
        self.average_metrics = average_metrics
        self.valid_groups = valid_groups
        self.numeric_filters = numeric_filters
        self.string_filters = string_filters

        # Query template patterns
        self.query_patterns = [
            # Total template with filter
            {
                "pattern": r"total (\w+) by (\w+) where (\w+) (>|<|=) (.+)",
                "type": "aggregate_with_where",
                "mongo_template": [
                    {"$match": {"$${filter_column}": {"$${operator}": "$${value}"}}},
                    {"$group": {"_id": "$${group_by}", "total_$${metric}": {"$sum": "$${metric}"}}},
                    {"$sort": {"total_$${metric}": -1}}
                ],
            },
            # Total template by grouping
            {
                "pattern": r"total (\w+) by (\w+)",
                "type": "aggregate_by_category",
                "mongo_template": [
                    {"$group": {"_id": "$${group_by}", "total_$${metric}": {"$sum": "$${metric}"}}},
                    {"$sort": {"total_$${metric}": -1}}
                ],
            },
            # Average with filter
            {
                "pattern": r"average (\w+) by (\w+) where (\w+) (>|<|=) (.+)",
                "type": "average_with_where",
                "mongo_template": [
                    {"$match": {"$${filter_column}": {"$${operator}": "$${value}"}}},
                    {"$group": {"_id": "$${group_by}", "avg_$${metric}": {"$avg": "$${metric}"}}},
                    {"$sort": {"avg_$${metric}": -1}}
                ],
            },
            # Average by category
            {
                "pattern": r"average (\w+) by (\w+)",
                "type": "average_by_category",
                "mongo_template": [
                    {"$group": {"_id": "$${group_by}", "avg_$${metric}": {"$avg": "$${metric}"}}},
                    {"$sort": {"avg_$${metric}": -1}}
                ],
            },
            # Average for specific category (e.g., "average sales for electronics products")
            {
                "pattern": r"average (\w+) for (\w+) (.+)",
                "type": "average_for_specific",
                "mongo_template": [
                    {"$match": {"$${filter_column}": "$${filter_value}"}},
                    {"$group": {"_id": None, "avg_$${metric}": {"$avg": "$${metric}"}}}
                ],
            },
            # Finding top N
            {
                "pattern": r"top (\d+) (\w+) by (\w+)",
                "type": "top_n",
                "mongo_template": [
                    {"$group": {"_id": "$${group_by}", "total_$${metric}": {"$sum": "$${metric}"}}},
                    {"$sort": {"total_$${metric}": -1}},
                    {"$limit": "$${limit}"}
                ],
            },
        ]

    def parse_query(self, query: str) -> Tuple[Optional[str], Optional[dict]]:
        doc = self.nlp(query)  # Preserve original case during tokenization
        text = " ".join([token.text for token in doc])  # Tokenize queries into tokens
        # Iterate through patterns to find a match
        for pattern in self.query_patterns:
            match = re.search(pattern["pattern"], text)
            # If match is found, return the query type & MongoDB template
            if match:
                return pattern["type"], {"groups": match.groups(), "template": pattern["mongo_template"]}
        return None, None

    def generate_mongo_query(self, query_type: str, params: dict) -> list:
        groups = params["groups"]

        # Initialize variables
        metric = None
        group_by = None
        filter_column = None
        filter_value = None
        operator = None
        value = None

        # Extract parts of the query based on the type
        if query_type == "average_with_where":
            metric = groups[0]
            group_by = groups[1]
            filter_column = groups[2]
            operator = groups[3].lower()  # Normalize to lowercase to handle "is" as "="
            if filter_column in self.string_filters:
                filter_value = groups[4]
            elif filter_column in self.numeric_filters:
                value = float(groups[4])
        elif query_type == "average_by_category":
            metric = groups[0]
            group_by = groups[1]

        # Map natural language operators to MongoDB operators
        operator_map = {">": "$gt", "<": "$lt", "=": "$eq", "is": "$eq"}

        # Build MongoDB query
        mongo_query = []

        # Filtering stage for `average_with_where`
        if query_type == "average_with_where":
            if filter_column in self.string_filters:
                if operator in ["=", "is"]:
                    mongo_query.append({"$match": {filter_column: {"$regex": f"^{filter_value}$", "$options": "i"}}})
            elif filter_column in self.numeric_filters and operator:
                mongo_operator = operator_map.get(operator)
                if mongo_operator:
                    mongo_query.append({"$match": {filter_column: {mongo_operator: value}}})

        # Grouping stage
        group_stage = {
            "_id": f"${group_by}",
            "avg_metric": {"$avg": f"${metric}"}
        }
        mongo_query.append({"$group": group_stage})

        # Sorting stage
        mongo_query.append({"$sort": {"avg_metric": -1}})

        return mongo_query




