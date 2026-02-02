"""
MongoDB Helper
==============
Helper functions for MongoDB operations.
"""

from pymongo import MongoClient
from typing import List, Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file in project root
# Go up one level from market_scraper/ to project root
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)


def get_mongodb_client(connection_string: str = None):
    """
    Get MongoDB client connection.
    
    Args:
        connection_string: MongoDB connection string (defaults to env var or localhost)
    
    Returns:
        MongoClient instance
    """
    if connection_string is None:
        connection_string = os.getenv(
            "MONGODB_URI",
            "mongodb://localhost:27017/"
        )
    
    return MongoClient(connection_string)


def insert_btc_records(
    records: List[Dict[str, Any]],
    metadata: Dict[str, Any] = None,
    collection_name: str = "btc_bronze_batch",
    database_name: str = None,
    connection_string: str = None
) -> int:
    """
    Insert BTC market records into MongoDB collection, skipping duplicates.
    
    Args:
        records: List of OHLCV record dictionaries
        metadata: Additional metadata to add to each record
        collection_name: MongoDB collection name
        database_name: MongoDB database name (defaults to BRONZE_DATABASE env var or 'bronze_db')
        connection_string: MongoDB connection string
    
    Returns:
        Number of documents inserted
    """
    client = get_mongodb_client(connection_string)
    
    if database_name is None:
        database_name = os.getenv("BRONZE_DATABASE", "bronze_db")
    
    db = client[database_name]
    collection = db[collection_name]
    
    if not records:
        print("  No records to insert")
        client.close()
        return 0
    
    try:
        # Add metadata to each record if provided
        documents = records.copy()
        if metadata:
            for doc in documents:
                doc.update(metadata)
        
        # Filter out duplicates based on datetime
        existing_datetimes = set(collection.distinct("datetime"))
        new_records = [d for d in documents if d.get("datetime") not in existing_datetimes]
        
        if not new_records:
            print(f"  All {len(documents)} records already exist in {database_name}.{collection_name}")
            client.close()
            return 0
        
        if len(new_records) < len(documents):
            print(f"  Skipping {len(documents) - len(new_records)} duplicate records")
        
        result = collection.insert_many(new_records)
        inserted_count = len(result.inserted_ids)
        
        print(f"  ✓ Inserted {inserted_count} records into {database_name}.{collection_name}")
        
        client.close()
        return inserted_count
    except Exception as e:
        print(f"  ✗ Error inserting records: {e}")
        client.close()
        raise

