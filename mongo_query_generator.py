import re
import spacy
from typing import Tuple, Optional


class QueryGenerator:
    '''
    args:
        total_metrics: list of metrics for total calculations  - total sales, total revenue
        average_metrics: list of metrics for average calculations - for finding average price, average quantity, etc
        valid_group: list of valid group-by fields - category, location, payment_method etc
        numeric_filters: list of numeric filter fields - price, quantity
        string_filters: list of string filter fields - payment_method, category
    '''
    def __init__(self, total_metrics, average_metrics, valid_groups, numeric_filters, string_filters):
        # load spaCy for parsing & tokenization
        self.nlp = spacy.load("en_core_web_sm")

        # params for class attributes
        self.total_metrics = total_metrics
        self.average_metrics = average_metrics
        self.valid_groups = valid_groups
        self.numeric_filters = numeric_filters
        self.string_filters = string_filters

        # query template patterns
        self.query_patterns = [
            # total template with filter
            {
                "pattern": r"total (\w+) by (\w+) where (\w+) (>|<|=) (.+)",
                "type": "aggregate_with_where",
                "mongo_template": [
                    {"$match": {"$${filter_column}": {"$${operator}": "$${value}"}}},
                    {"$group": {"_id": "$${group_by}", "total_$${metric}": {"$sum": "$${metric}"}}},
                    {"$sort": {"total_$${metric}": -1}}
                ],
            },
            # total template by grouping
            {
                "pattern": r"total (\w+) by (\w+)",
                "type": "aggregate_by_category",
                "mongo_template": [
                    {"$group": {"_id": "$${group_by}", "total_$${metric}": {"$sum": "$${metric}"}}},
                    {"$sort": {"total_$${metric}": -1}}
                ],
            },
            # total for specific category (e.g., "total sales for electronics products")
            {
                "pattern": r"total (\w+) for (\w+) (.+)",
                "type": "aggregate_for_specific",
                "mongo_template": [
                    {"$match": {"$${filter_column}": "$${filter_value}"}},
                    {"$group": {"_id": None, "total_$${metric}": {"$sum": "$${metric}"}}}
                ],
            },
            # avg by category
            {
                "pattern": r"average (\w+) by (\w+)",
                "type": "average_by_category",
                "mongo_template": [
                    {"$group": {"_id": "$${group_by}", "avg_$${metric}": {"$avg": "$${metric}"}}},
                    {"$sort": {"avg_$${metric}": -1}}
                ],
            },
            # avg with filtering
            {
                "pattern": r"average (\w+) by (\w+) where (\w+) (>|<|=) (.+)",
                "type": "average_with_where",
                "mongo_template": [
                    {"$match": {"$${filter_column}": {"$${operator}": "$${value}"}}},
                    {"$group": {"_id": "$${group_by}", "avg_$${metric}": {"$avg": "$${metric}"}}},
                    {"$sort": {"avg_$${metric}": -1}}
                ],
            },
            # finding top N
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

    # function to parse natural language query
    def parse_query(self, query: str) -> Tuple[Optional[str], Optional[dict]]:
        doc = self.nlp(query)
        text = " ".join([token.text for token in doc])  # tokenize queries into tokens
        # iterate through patterns to find a match
        for pattern in self.query_patterns:
            match = re.search(pattern["pattern"], text)
            # if match is found, return the query type & MongoDB template
            if match:
                return pattern["type"], {"groups": match.groups(), "template": pattern["mongo_template"]}
        # for cases with no pattern matches
        return None, None


    #  function to generate mongodb query
    def generate_mongo_query(self, query_type: str, params: dict) -> list:
        groups = params["groups"]

        # intialize variables
        metric = None
        group_by = None
        limit = None
        filter_column = None
        filter_value = None
        operator = None
        value = None

        # extract parts of the query based on the type
        if query_type == "top_n":
            limit = int(groups[0])  # set first group as limit
            metric = groups[1]  # metric to be aggregated
            group_by = groups[2]  # third is grouping
        elif query_type == "aggregate_with_where":
            metric = groups[0]
            group_by = groups[1]
            filter_column = groups[2]
            operator = groups[3].lower()  # normalize to lowercase to handle is to =
            if filter_column in self.string_filters:
                filter_value = groups[4]  # handle string filters
            elif filter_column in self.numeric_filters:
                value = float(groups[4])  # handle numeric filters
        elif query_type == "average_by_category":
            metric = groups[0]
            group_by = groups[1]
        elif query_type == "aggregate_by_category":
            metric = groups[0]
            group_by = groups[1]

        # map natural language operators to mongo operators
        operator_map = {">": "$gt", "<": "$lt", "=": "$eq", "is": "$eq"}

        # build mongo query
        mongo_query = []

        # filtering stage for `average_with_where`
        if query_type == "average_with_where":
            if filter_column in self.string_filters:
                if operator in ["=", "is"]:
                    mongo_query.append({"$match": {filter_column: {"$regex": f"^{filter_value}$", "$options": "i"}}})
            elif filter_column in self.numeric_filters and operator:
                mongo_operator = operator_map.get(operator)
                if mongo_operator:
                    mongo_query.append({"$match": {filter_column: {mongo_operator: value}}})

        # filtering stage
        if query_type == "aggregate_with_where":
            if filter_column in self.string_filters:
                # case insensitive strings
                if operator in ["=", "is"]:
                    mongo_query.append({"$match": {filter_column: {"$regex": f"^{filter_value}$", "$options": "i"}}})
            elif filter_column in self.numeric_filters and operator:
                mongo_operator = operator_map.get(operator)
                if mongo_operator:
                    mongo_query.append({"$match": {filter_column: {mongo_operator: value}}})

        # grouping
        if query_type in ["aggregate_by_category", "aggregate_with_where", "average_by_category"]:
            group_stage = {
                "_id": f"${group_by}",
                "total_metric": {"$sum": 1} if metric == "sales" else {"$sum": f"${metric}"}
            }
            if query_type == "average_by_category":
                group_stage["avg_metric"] = {"$avg": f"${metric}"}
            mongo_query.append({"$group": group_stage})

        # top queries
        if query_type == "top_n":
            mongo_query.extend([
                {"$group": {
                    "_id": f"${group_by}",
                    "transactions": {
                        "$push": {"transaction": "$$ROOT", "metric": f"${metric}"}
                    }
                }},
                {"$sort": {"metric": -1}},
                {"$project": {
                    "transactions": {"$slice": ["$transactions", limit]}
                }}
            ])

        # sorting stage
        if query_type in ["aggregate_by_category", "aggregate_with_where", "average_by_category"]:
            mongo_query.append(
                {"$sort": {"total_metric": -1} if query_type != "average_by_category" else {"avg_metric": -1}})

        return mongo_query




