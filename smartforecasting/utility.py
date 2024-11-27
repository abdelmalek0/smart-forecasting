import json
import re
from datetime import datetime
from typing import Tuple

import pandas as pd
from dateutil import parser

from logging_config import logger
from structs.models import DataSource


def parse_date(date_str):
    """Parse a date string into a datetime object, ensuring UTC timezone."""
    if not date_str:
        return None

    try:
        # Parse with dateutil to handle various formats and ensure UTC
        parsed_date = parser.parse(date_str)

        # Ensure the date is timezone-aware with UTC
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=pd.Timestamp.now().tzinfo)

        return parsed_date
    except (ValueError, TypeError):
        return None


def read_config(file_path: str) -> dict:
    """Read configuration from a JSON file."""
    with open(file_path, "r") as file:
        config = json.load(file)
    return config


def save_config(file_path: str, config: dict):
    """Save configuration to a JSON file."""
    with open(file_path, "w") as file:
        json.dump(config, file, indent=4)


def find_data_source_by_id(
    data_source_id: int, data_source_collection: list[dict]
) -> Tuple[dict, int] | None:
    logger.info("Searching for data source in the collection.")

    # Use a generator to find the first matching data source
    match = next(
        (
            (data_source, index)
            for index, data_source in enumerate(data_source_collection)
            if data_source.get("id") == data_source_id
        ),
        None,
    )

    if match:
        logger.info(f"Match found at index {match[1]}.")
    else:
        logger.info("No matching ID found.")
    return match


def convert_to_sql_datetime(timestamp: str) -> str:
    """
    Converts an ISO 8601 timestamp to SQL DATETIME format (YYYY-MM-DD HH:MM:SS).
    If the timestamp is already in SQL DATETIME format, no change is made.
    """
    # Regular expression to check if the timestamp is in ISO 8601 format
    iso8601_pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(Z|[\+\-]\d{2}:\d{2})?$"
    )

    # Check if the timestamp is in ISO 8601 format
    if iso8601_pattern.match(timestamp):
        # Convert ISO 8601 to SQL DATETIME
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # If it's already in SQL DATETIME format, return as is
    return timestamp
