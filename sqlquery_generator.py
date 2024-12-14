#sqlquery_generator.py
import re
import spacy
from typing import Tuple, Optional

class QueryGenerator:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.query_patterns = [
            {
                "pattern": r"total (\w+) by (\w+) where (\w+) (>|<|=) (\d+)",
                "type": "aggregate_with_where",
                "sql_template": "SELECT {group_by}, SUM({metric}) AS total_{metric} FROM {table} WHERE {filter_column} {operator} {value} GROUP BY {group_by} ORDER BY total_{metric} DESC"
            },
            {
                "pattern": r"total (\w+) by (\w+)",
                "type": "aggregate_by_category",
                "sql_template": "SELECT {group_by}, SUM({metric}) AS total_{metric} FROM {table} GROUP BY {group_by} ORDER BY total_{metric} DESC"
            },
            {
                "pattern": r"average (\w+) by (\w+)",
                "type": "average_by_category",
                "sql_template": "SELECT {group_by}, AVG({metric}) AS avg_{metric} FROM {table} GROUP BY {group_by} ORDER BY avg_{metric} DESC"
            },
            {
                "pattern": r"top (\d+) (\w+) by (\w+)",
                "type": "top_n",
                "sql_template": "SELECT {group_by}, SUM({metric}) AS total_{metric} FROM {table} GROUP BY {group_by} ORDER BY total_{metric} DESC LIMIT {n}"
            }
        ]

    def parse_query(self, query: str) -> Tuple[Optional[str], Optional[dict]]:
        """Parse a natural language query."""
        doc = self.nlp(query.lower())
        text = " ".join([token.text for token in doc])
        for pattern in self.query_patterns:
            match = re.search(pattern["pattern"], text)
            if match:
                return pattern["type"], {"groups": match.groups(), "template": pattern["sql_template"]}
        return None, None
