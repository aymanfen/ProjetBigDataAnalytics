from pymongo import MongoClient
from typing import List, Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file in project root
# Go up one level from reddit_scraper/ to project root
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


def insert_reddit_posts(
    posts: List[Dict[str, Any]],
    collection_name: str = "reddit_bronze_batch",
    database_name: str = None,
    connection_string: str = None
) -> int:
    """
    Insert Reddit posts into MongoDB collection, skipping duplicates.
    
    Args:
        posts: List of post dictionaries
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
    
    if not posts:
        print("  No posts to insert")
        client.close()
        return 0
    
    try:
        # Filter out duplicates based on post ID
        existing_ids = set(collection.distinct("id"))
        new_posts = [p for p in posts if p.get("id") not in existing_ids]
        
        if not new_posts:
            print(f"  All {len(posts)} posts already exist in {database_name}.{collection_name}")
            client.close()
            return 0
        
        if len(new_posts) < len(posts):
            print(f"  Skipping {len(posts) - len(new_posts)} duplicate posts")
        
        result = collection.insert_many(new_posts)
        inserted_count = len(result.inserted_ids)
        
        print(f"  ✓ Inserted {inserted_count} posts into {database_name}.{collection_name}")
        
        client.close()
        return inserted_count
    except Exception as e:
        print(f"  ✗ Error inserting posts: {e}")
        client.close()
        raise

